#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for TestRunner class."""



import datetime
import logging
import unittest


from google.appengine.api import datastore_errors
from google.appengine.ext import testbed
from google.appengine.ext import ndb

from common import blobstore_helper
from server import dimension_mapping
from server import test_helper
from server import test_manager
from server import test_request
from server import test_runner
from stats import runner_stats
from third_party.mox import mox

MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']


class TestRunnerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

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
        test_runner.TxRunnerAlreadyAssignedError
        ]

    runner = test_helper.CreatePendingRunner()
    for e in exceptions:
      def _Raise(key, machine_id):  # pylint: disable=unused-argument
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
    self.assertRaises(test_runner.TxRunnerAlreadyAssignedError,
                      test_runner.AtomicAssignID,
                      runners[0].key, MACHINE_IDS[1])

  # Test with an exception.
  def testAssignRunnerToMachineFull(self):
    runner = test_helper.CreatePendingRunner()

    # First assignment should work correctly.
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    # Next assignment should fail.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    runner.started = None
    runner.put()
    # This assignment should now work correctly.
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  # Test the case where the runner is deleted before the tx is done.
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
    self.assertTrue(test_runner.AutomaticallyRetryRunner(runner))

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
    runner.started = datetime.datetime.now()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()
    self.assertTrue(test_runner.PingRunner(runner.key.urlsafe(),
                                           MACHINE_IDS[0]))

    # The machine ids don't match so fail.
    self.assertFalse(test_runner.PingRunner(runner.key.urlsafe(),
                                            MACHINE_IDS[1]))

    # Runner is done.
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
    self.assertTrue(test_runner.AutomaticallyRetryRunner(runner))

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
    self.assertEqual(0,
                     len(list(test_runner.GetTestRunners(
                         'machine_id', ascending=True, limit=0, offset=0))))

    # Create some test requests.
    test_runner_count = 3
    for i in range(test_runner_count):
      runner = test_helper.CreatePendingRunner()
      runner.config_instance_index = i
      # Ensure the created times are far enough apart that we can reliably
      # sort the runners by it.
      runner.started = datetime.datetime.now() + datetime.timedelta(days=i)
      runner.put()

    # Make sure the results are sorted.
    test_runners = test_runner.GetTestRunners('started',
                                              ascending=True,
                                              limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(i, test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))

    # Make sure the results are sorted in descending order.
    test_runners = test_runner.GetTestRunners('started',
                                              ascending=False,
                                              limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(test_runner_count - 1 - i,
                       test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))

  def testGetHangingRunners(self):
    self.assertEqual([], test_runner.GetHangingRunners())

    test_helper.CreatePendingRunner()
    self.assertEqual([], test_runner.GetHangingRunners())

    old_time = datetime.datetime.now() - datetime.timedelta(
        minutes=2 * test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)

    # Create an older runner that is running and isn't hanging.
    old_runner = test_helper.CreatePendingRunner()
    old_runner.created = old_time
    old_runner.started = datetime.datetime.now()
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
    manager = test_manager.TestRequestManager()
    manager.AbortRunner(aborted_runner)
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

  def testGetRunnerSummaryByDimension(self):
    self.assertEqual({}, test_runner.GetRunnerSummaryByDimension())

    # Add a pending runner and its dimensions.
    pending_runner = test_helper.CreatePendingRunner()
    dimensions = pending_runner.dimensions
    dimension_mapping.DimensionMapping(dimensions=dimensions).put()

    self.assertEqual({dimensions: (1, 0)},
                     test_runner.GetRunnerSummaryByDimension())

    # Add a running runner.
    running_runner = test_helper.CreatePendingRunner()
    running_runner.started = datetime.datetime.now()
    running_runner.put()

    self.assertEqual({dimensions: (1, 1)},
                     test_runner.GetRunnerSummaryByDimension())

    # Add a runner with a new dimension
    new_dimensions = running_runner.dimensions + 'extra dimensions'
    dimension_mapping.DimensionMapping(dimensions=new_dimensions).put()

    new_dimension_runner = test_helper.CreatePendingRunner()
    new_dimension_runner.dimensions = new_dimensions
    new_dimension_runner.put()

    self.assertEqual({dimensions: (1, 1),
                      new_dimensions: (1, 0)},
                     test_runner.GetRunnerSummaryByDimension())

    # Make both the runners on the original dimensions as done.
    pending_runner.done = True
    pending_runner.put()
    running_runner.done = True
    running_runner.put()

    self.assertEqual({dimensions: (0, 0),
                      new_dimensions: (1, 0)},
                     test_runner.GetRunnerSummaryByDimension())

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
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_runner.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS - 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_runner.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS + 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner()
    runner.ended = datetime.datetime.now()
    runner.put()

    # Make sure that new runners aren't deleted.
    ndb.Future.wait_all(test_runner.DeleteOldRunners())
    self.assertEqual(1, test_runner.TestRunner.query().count())

    # Make sure that old runners are deleted.
    ndb.Future.wait_all(test_runner.DeleteOldRunners())
    self.assertEqual(0, test_runner.TestRunner.query().count())

    self._mox.VerifyAll()

  def testDeleteOrphanedBlobs(self):
    self.assertEqual(0, test_runner.DeleteOrphanedBlobs())

    # Add a runner with a blob and don't delete the blob.
    runner = test_helper.CreatePendingRunner()
    runner.result_string_reference = blobstore_helper.CreateBlobstore(
        'owned blob')
    runner.put()
    self.assertEqual(0, test_runner.DeleteOrphanedBlobs())

    # Add an orphaned blob and delete it.
    blobstore_helper.CreateBlobstore('orphaned blob')
    self.assertEqual(1, test_runner.DeleteOrphanedBlobs())

if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
