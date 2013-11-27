#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tests for TestRunner class."""


import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.api import datastore_errors
from google.appengine.ext import testbed
from google.appengine.ext import ndb

from common import result_helper
from common import swarm_constants
from common import url_helper
from server import test_helper
from server import test_management
from server import test_request
from server import test_runner
from stats import runner_stats
from third_party.mox import mox

MACHINE_IDS = [
    '12345678-12345678-12345678-12345678',
    '23456789-23456789-23456789-23456789',
    '34567890-34567890-34567890-34567890',
    '87654321-87654321-87654321-87654321',
]


class TestRunnerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    self._mox.StubOutWithMock(url_helper, 'UrlOpen')

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testGetResultStringFromEmptyRunner(self):
    runner = test_helper.CreatePendingRunner()

    # Since the request hasn't been run yet there should be just be an
    # empty string for the result string.
    self.assertEqual('', runner.GetResultString())

  # Test with an exception.
  def testAssignRunnerToMachineException(self):
    exceptions = [
        datastore_errors.InternalError,
        datastore_errors.Timeout,
        datastore_errors.TransactionFailedError,
        test_runner.TxRunnerAlreadyAssignedError,
    ]

    runner = test_helper.CreatePendingRunner()
    for e in exceptions:
      def _Raise(_key, _machine_id):
        raise e
      self.assertFalse(test_runner.AssignRunnerToMachine(
          MACHINE_IDS[0], runner, _Raise))

  # Test proper behavior of AtomicAssignID.
  def testAtomicAssignID(self):
    runners = []

    # Create some pending runners.
    for _ in range(0, 2):
      runners.append(test_helper.CreatePendingRunner())

    # Make sure it assigns machine_id correctly.
    test_runner.AtomicAssignID(runners[0].key, MACHINE_IDS[0])
    runner = test_runner.TestRunner.gql(
        'WHERE machine_id = :1', MACHINE_IDS[0]).get()
    self.assertEqual(runner.machine_id, MACHINE_IDS[0])

    # Make sure it didn't touch the other machine.
    self.assertEqual(
        1, test_runner.TestRunner.gql('WHERE started = :1', None).count())

    # Try to reassign runner and raise exception.
    with self.assertRaises(test_runner.TxRunnerAlreadyAssignedError):
      test_runner.AtomicAssignID(runners[0].key, MACHINE_IDS[1])

  # Test with an exception.
  def testAssignRunnerToMachineFull(self):
    runner = test_helper.CreatePendingRunner()

    # First assignment should work correctly.
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    # Next assignment should fail since the runner is already assigned.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    runner.started = None
    runner.put()
    # This assignment should now work correctly since the runner doesn't think
    # it has has a machine (since it hasn't started yet).
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  # Test the case where the runner is deleted before the transaction is done.
  def testAssignDeletedRunnerToMachine(self):
    runner = test_helper.CreatePendingRunner()
    runner.key.delete()

    # Assignment should fail without an exception.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  def testShouldAutomaticallyRetryRunner(self):
    runner = test_helper.CreatePendingRunner()
    self.assertTrue(test_runner.ShouldAutomaticallyRetryRunner(runner))

    runner.automatic_retry_count = test_runner.MAX_AUTOMATIC_RETRIES
    self.assertFalse(test_runner.ShouldAutomaticallyRetryRunner(runner))

  # There were observed cases where machine_id was somehow set to None before
  # calling AutomaticallyRetryRunner. It is unclear how this is possible, so
  # handle this case gracefully.
  def testAutomaticallyRetryMachineIdNone(self):
    runner = test_helper.CreatePendingRunner(machine_id=None)
    test_runner.AutomaticallyRetryRunner(runner)

  def testRecordRunnerStatsAfterAutoRetry(self):
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    test_runner.AutomaticallyRetryRunner(runner)

    self.assertEqual(1, runner_stats.RunnerStats.query().count())
    r_stats = runner_stats.RunnerStats.query().get()
    self.assertFalse(r_stats.success)
    self.assertEqual(0, r_stats.automatic_retry_count)

  def testPingRunner(self):
    # Try with a few invalid keys.
    self.assertFalse(test_runner.PingRunner(1, None))
    self.assertFalse(test_runner.PingRunner('2', None))

    # Tests with a valid key
    runner = test_helper.CreatePendingRunner()

    # Runner hasn't started.
    self.assertFalse(test_runner.PingRunner(runner.key.urlsafe(), None))

    # Runner starts and can get pinged.
    runner.started = datetime.datetime.utcnow()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()
    self.assertTrue(test_runner.PingRunner(runner.key.urlsafe(),
                                           MACHINE_IDS[0]))

    # The machine ids don't match so fail.
    self.assertFalse(test_runner.PingRunner(runner.key.urlsafe(),
                                            MACHINE_IDS[1]))

    # Runner is done so it can't be pinged.
    runner.done = True
    runner.put()
    self.assertFalse(test_runner.PingRunner(runner.key.urlsafe(),
                                            MACHINE_IDS[0]))

    # Delete the runner and try to ping.
    runner.key.delete()
    self.assertFalse(test_runner.PingRunner(runner.key.urlsafe(),
                                            MACHINE_IDS[0]))

  def testAbortRunnerThenReattach(self):
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    # Retry the runner since its taken too long to ping.
    test_runner.AutomaticallyRetryRunner(runner)

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(1, runner.automatic_retry_count)
    self.assertEqual(None, runner.started)
    self.assertEqual(None, runner.machine_id)

    # Have the the machine ping the server and get reconnected to the runner.
    self.assertTrue(test_runner.PingRunner(runner.key.urlsafe(),
                                           MACHINE_IDS[0]))

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(0, runner.automatic_retry_count)
    self.assertNotEqual(None, runner.started)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertEqual([], runner.old_machine_ids)

  def testGetTestRunners(self):
    self.assertEqual(0, len(list(test_runner.GetTestRunners(
        'machine_id', ascending=True, limit=5, offset=0))))

    # Create some test requests.
    test_runner_count = 3
    for i in range(test_runner_count):
      runner = test_helper.CreatePendingRunner()
      runner.config_instance_index = i
      # Ensure the created times are far enough apart that we can reliably
      # sort the runners by it.
      runner.started = datetime.datetime.utcnow() + datetime.timedelta(days=i)
      runner.put()

    # Make sure limits are respected.
    self.assertEqual(1, len(list(test_runner.GetTestRunners(
        'machine_id', ascending=True, limit=1, offset=0))))
    self.assertEqual(3, len(list(test_runner.GetTestRunners(
        'machine_id', ascending=True, limit=5, offset=0))))

    # Make sure the results are sorted.
    test_runners = list(test_runner.GetTestRunners('started',
                                                   ascending=True,
                                                   limit=5, offset=0))
    self.assertEqual(test_runner_count, len(test_runners))
    for i, runner in enumerate(test_runners):
      self.assertEqual(i, runner.config_instance_index)

    # Make sure the results are sorted in descending order.
    test_runners = list(test_runner.GetTestRunners('started',
                                                   ascending=False,
                                                   limit=5, offset=0))
    self.assertEqual(test_runner_count, len(test_runners))
    for i, runner in enumerate(test_runners):
      self.assertEqual(test_runner_count - 1 - i,
                       runner.config_instance_index)

  def testFilterTestRunners(self):
    # Add a runner in each state (pending, running, failed, successful).
    pending_runner = test_helper.CreatePendingRunner()
    running_runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    # Hung runner, aborted without ever running.
    hung_runner = test_helper.CreatePendingRunner()
    test_management.AbortRunner(hung_runner)

    aborted_runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[1])
    test_management.AbortRunner(aborted_runner)

    failed_runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[1])
    failed_runner.done = True
    failed_runner.ran_successfully = False
    failed_runner.put()

    successful_runner = test_helper.CreatePendingRunner(
        machine_id=MACHINE_IDS[2])
    successful_runner.done = True
    successful_runner.ran_successfully = True
    successful_runner.put()

    def CheckQuery(expected_list, actual_query):
      expected_set = set(element.key for element in expected_list)
      actual_set = set(element.key for element in actual_query)

      self.assertEqual(expected_set, actual_set)

    unfiltered_query = test_runner.TestRunner.query()
    CheckQuery(unfiltered_query,
               test_runner.ApplyFilters(unfiltered_query))

    # Check all the status values.
    CheckQuery(unfiltered_query,
               test_runner.ApplyFilters(unfiltered_query, status='all'))

    CheckQuery([pending_runner],
               test_runner.ApplyFilters(unfiltered_query, status='pending'))

    CheckQuery([running_runner],
               test_runner.ApplyFilters(unfiltered_query, status='running'))

    CheckQuery([hung_runner, aborted_runner, failed_runner, successful_runner],
               test_runner.ApplyFilters(unfiltered_query, status='done'))

    CheckQuery(
        [pending_runner, running_runner, hung_runner, aborted_runner,
         failed_runner],
        test_runner.ApplyFilters(unfiltered_query,
                                 show_successfully_completed=False))

    # Give the pending runner a unique name and ensure we can filter by names.
    pending_runner.name = 'unique name'
    pending_runner.put()
    CheckQuery(
        [pending_runner],
        test_runner.ApplyFilters(unfiltered_query,
                                 test_name=pending_runner.name))

    CheckQuery([running_runner],
               test_runner.ApplyFilters(unfiltered_query,
                                        machine_id=MACHINE_IDS[0]))

  def testGetHangingRunners(self):
    self.assertEqual([], test_runner.GetHangingRunners())

    test_helper.CreatePendingRunner()
    self.assertEqual([], test_runner.GetHangingRunners())

    old_time = datetime.datetime.utcnow() - datetime.timedelta(
        minutes=2 * test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)

    # Create an older runner that is running and isn't hanging.
    old_runner = test_helper.CreatePendingRunner()
    old_runner.created = old_time
    old_runner.started = datetime.datetime.utcnow()
    old_runner.put()
    self.assertEqual([], test_runner.GetHangingRunners())

    # Create a runner that was automatically restart, and can never be viewed
    # as hanging.
    retried_runner = test_helper.CreatePendingRunner()
    retried_runner.created = old_time
    retried_runner.automatic_retry_count = 1
    retried_runner.put()
    self.assertEqual([], test_runner.GetHangingRunners())

    # Create a runner that was aborted before running.
    aborted_runner = test_helper.CreatePendingRunner()
    aborted_runner.created = old_time
    test_management.AbortRunner(aborted_runner)
    self.assertEqual([], test_runner.GetHangingRunners())

    # Create an older runner that will be marked as hanging.
    hanging_runner = test_helper.CreatePendingRunner()
    hanging_runner.created = old_time
    hanging_runner.put()

    found_hanging_runners = test_runner.GetHangingRunners()
    self.assertEqual(1, len(found_hanging_runners))
    self.assertEqual(hanging_runner.key, found_hanging_runners[0].key)

  def testGetRunnerFromUrlSafeKey(self):
    runner = test_helper.CreatePendingRunner()

    self.assertEqual(None, test_runner.GetRunnerFromUrlSafeKey(''))
    self.assertEqual(None, test_runner.GetRunnerFromUrlSafeKey('fake_key'))
    test_request_key = test_request.TestRequest.query().get().key.urlsafe()
    self.assertEqual(None,
                     test_runner.GetRunnerFromUrlSafeKey(test_request_key))

    self.assertEqual(
        runner.key,
        test_runner.GetRunnerFromUrlSafeKey(runner.key.urlsafe()).key)

    test_runner.DeleteRunner(runner)
    self.assertEqual(None,
                     test_runner.GetRunnerFromUrlSafeKey(runner.key.urlsafe()))

  def testGetRunnerResult(self):
    self.assertEqual(None, test_runner.GetRunnerResults('invalid key'))

    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    results = test_runner.GetRunnerResults(runner.key.urlsafe())
    self.assertNotEqual(None, results)
    self.assertEqual(runner.exit_codes, results['exit_codes'])
    self.assertEqual(runner.machine_id, results['machine_id'])
    self.assertEqual(runner.config_instance_index,
                     results['config_instance_index'])
    self.assertEqual(runner.num_config_instances,
                     results['num_config_instances'])
    self.assertEqual(runner.GetResultString(), results['output'])

  def testGetMessage(self):
    runner = test_helper.CreatePendingRunner()
    runner.GetMessage()

  def testRunnerCallerMachineIdMismatch(self):
    self._mox.StubOutWithMock(test_management.logging, 'warning')
    test_runner.logging.warning('The machine id of the runner, %s, doesn\'t '
                                'match the machine id given, %s',
                                MACHINE_IDS[0], MACHINE_IDS[1])

    self._mox.ReplayAll()

    test_management.ExecuteTestRequest(test_helper.GetRequestMessage())
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    self.assertFalse(runner.UpdateTestResult(MACHINE_IDS[1]))
    self._mox.VerifyAll()

  def testRunnerCallerOldMachine(self):
    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg()).AndReturn(
        'response')
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])
    runner.machine_id = MACHINE_IDS[0]
    runner.old_machine_ids = [MACHINE_IDS[1]]
    runner.put()

    self.assertTrue(runner.UpdateTestResult(MACHINE_IDS[1]))

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(MACHINE_IDS[1], runner.machine_id)
    self.assertEqual([MACHINE_IDS[0]], runner.old_machine_ids)
    self.assertTrue(runner.done)

    self._mox.VerifyAll()

  def testRunnerCallerOldMachineWithNoCurrent(self):
    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg()).AndReturn(
        'response')
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])
    runner.machine_id = None
    runner.old_machine_ids = [MACHINE_IDS[1]]
    runner.put()

    self.assertTrue(runner.UpdateTestResult(MACHINE_IDS[1]))

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(MACHINE_IDS[1], runner.machine_id)
    self.assertEqual([], runner.old_machine_ids)
    self.assertTrue(runner.done)

    self._mox.VerifyAll()

  def testHandleOverwriteTestResults(self):
    messages = [
        'first-message',
        'second-message',
        'third-message',
    ]

    self._mox.StubOutWithMock(logging, 'error')

    url_helper.UrlOpen(test_helper.DEFAULT_RESULT_URL,
                       data=mox.ContainsKeyValue('r', messages[0])).AndReturn(
                           'response')
    logging.error(mox.StrContains('additional response'), mox.IgnoreArg(),
                  mox.IgnoreArg())
    url_helper.UrlOpen(test_helper.DEFAULT_RESULT_URL,
                       data=mox.ContainsKeyValue('r', messages[2])).AndReturn(
                           'response')
    url_helper.UrlOpen(test_helper.DEFAULT_RESULT_URL,
                       data=mox.ContainsKeyValue('r', messages[0])).AndReturn(
                           'response')
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    # First results, always accepted.
    self.assertTrue(runner.UpdateTestResult(
        runner.machine_id,
        results=result_helper.StoreResults(messages[0])))
    # Ensure that there is only one stored result.
    self.assertEqual(1, result_helper.Results.query().count())

    # The first result resent, accepted since the strings are equal.
    self.assertTrue(runner.UpdateTestResult(
        runner.machine_id,
        results=result_helper.StoreResults(messages[0])))
    self.assertEqual(1, result_helper.Results.query().count())

    # Reject a different request without the overwrite flag.
    self.assertFalse(runner.UpdateTestResult(
        runner.machine_id,
        results=result_helper.StoreResults(messages[1]),
        overwrite=False))
    self.assertEqual(1, result_helper.Results.query().count())

    # Accept a different request with the overwrite flag.
    self.assertTrue(runner.UpdateTestResult(
        runner.machine_id,
        results=result_helper.StoreResults(messages[2]),
        overwrite=True))
    self.assertEqual(1, result_helper.Results.query().count())

    # Accept the first message as an error with overwrite.
    self.assertTrue(runner.UpdateTestResult(runner.machine_id,
                                            errors=messages[0],
                                            overwrite=True))

    # Accept the first message as an error again, since it is equal to what is
    # already stored.
    self.assertTrue(runner.UpdateTestResult(runner.machine_id,
                                            errors=messages[0]))

    # Make sure there are no results now, since errors are just stored as
    # strings.
    self.assertEqual(0, result_helper.Results.query().count())

    self._mox.VerifyAll()

  def testRecordRunnerStats(self):
    # Create a pending runner and execute it.
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])
    self.assertEqual(0, runner_stats.RunnerStats.query().count())

    # Return the results for the test and ensure the stats are updated.
    runner.UpdateTestResult(runner.machine_id, success=True)

    self.assertEqual(1, runner_stats.RunnerStats.query().count())
    r_stats = runner_stats.RunnerStats.query().get()
    self.assertNotEqual(None, r_stats.created_time)
    self.assertNotEqual(None, r_stats.assigned_time)
    self.assertNotEqual(None, r_stats.end_time)
    self.assertTrue(r_stats.success)

  def testDeleteRunner(self):
    test_helper.CreatePendingRunner()

    # Make sure the request and the runner are stored.
    self.assertEqual(1, test_runner.TestRunner.query().count())
    self.assertEqual(1, test_request.TestRequest.query().count())

    # Ensure the runner is deleted and that the request is deleted (since it
    # has no remaining runners).
    runner = test_runner.TestRunner.query().get()
    test_runner.DeleteRunner(runner)
    self.assertEqual(0, test_runner.TestRunner.query().count())
    self.assertEqual(0, test_request.TestRequest.query().count())

  def testDeleteRunnerFromKey(self):
    test_helper.CreatePendingRunner()

    # Make sure the request and the runner are stored.
    self.assertEqual(1, test_runner.TestRunner.query().count())
    self.assertEqual(1, test_request.TestRequest.query().count())

    # Try deleting with an invalid key and make sure nothing happens.
    test_runner.DeleteRunnerFromKey(test_request.TestRequest.query().get().key)
    self.assertEqual(1, test_runner.TestRunner.query().count())
    self.assertEqual(1, test_request.TestRequest.query().count())

    # Delete the runner by its key.
    key = test_runner.TestRunner.query().get().key.urlsafe()
    test_runner.DeleteRunnerFromKey(key)

    # Ensure the runner is deleted and that the request is deleted (since it
    # has no remaining runners).
    self.assertEqual(0, test_runner.TestRunner.query().count())
    self.assertEqual(0, test_request.TestRequest.query().count())

    # Now try deleting the Test Runner again, this should be a noop.
    test_runner.DeleteRunnerFromKey(key)
    self.assertEqual(0, test_runner.TestRunner.query().count())
    self.assertEqual(0, test_request.TestRequest.query().count())

  def testSwarmDeleteOldRunners(self):
    self._mox.StubOutWithMock(test_runner, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=swarm_constants.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS - 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=swarm_constants.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS + 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner()
    runner.ended = datetime.datetime.utcnow()
    runner.put()

    # Make sure that new runners aren't deleted.
    ndb.Future.wait_all(test_runner.DeleteOldRunners())
    self.assertEqual(1, test_runner.TestRunner.query().count())

    # Make sure that old runners are deleted.
    ndb.Future.wait_all(test_runner.DeleteOldRunners())
    self.assertEqual(0, test_runner.TestRunner.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
