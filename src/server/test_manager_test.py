#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests for TestRequestManager class."""




import datetime
import hashlib
import json
import logging
import os
import shutil
import StringIO
import subprocess
import sys
import tempfile
import unittest
import urllib2
import zipfile


from google.appengine.api import mail
from google.appengine.ext import blobstore
from google.appengine.ext import testbed
from google.appengine.ext import ndb

from common import blobstore_helper
from common import dimensions_utils
from common import swarm_constants
from common import test_request_message
from server import test_manager
from server import test_request
from server import test_runner
from stats import machine_stats
from stats import runner_stats
from swarm_bot import slave_machine
from third_party.mox import mox


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']

DEFAULT_RESULT_URL = 'http://all.your.resul.ts/are/belong/to/us'


class TestRequestManagerTest(unittest.TestCase):

  _SERVER_URL = 'http://my.server.com/'

  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    # Create a test manager instance for the tests.
    self._manager = test_manager.TestRequestManager()

    # Create default configurations.
    self._config_win = test_request_message.TestConfiguration(
        config_name='Windows', os='win-xp', browser='Unknown', cpu='Unknown')

    self._config_linux = test_request_message.TestConfiguration(
        config_name='Linux', os='linux', browser='Unknown', cpu='Unknown')

    self._request_message_config_name = 'c1'
    self._request_message_test_case_name = 'tc'

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def _GetRequestMessage(self, min_instances=1, additional_instances=0,
                         env_vars=None, result_url=DEFAULT_RESULT_URL,
                         store_result='all', restart_on_failure=False,
                         platform='win-xp'):
    """Return a properly formatted request message text.

    Args:
      min_instances: The minimum number of instance of the given config.
      additional_instances: The number of additional instances for of the given
          config.
      env_vars: A dictionary of environment variables for the request.
      result_url: The result url to use.
      store_result: Identifies which Runner and Request data should stay in
          storage after the tests are done running (fail means only the failures
          are kept).
      restart_on_failure: Identifies if the slave should be restarted if any
          of its tests fail.
      platform: The os to require in the test's configuration.

    Returns:
      A properly formatted request message text.
    """
    request = test_request_message.TestCase()
    request.test_case_name = self._request_message_test_case_name
    request.tests = [test_request_message.TestObject(
        test_name='t1', action=['ignore-me.exe'])]
    request.configurations = [
        test_request_message.TestConfiguration(
            config_name=self._request_message_config_name, os=platform,
            cpu='Unknown', data=['http://b.ina.ry/files2.zip'],
            browser='Unknown',
            min_instances=min_instances,
            additional_instances=additional_instances,
            tests=[test_request_message.TestObject(
                test_name='t2', action=['ignore-me-too.exe'])])]
    if env_vars:
      request.env_vars = env_vars.copy()
    request.result_url = result_url
    request.failure_email = 'john@doe.com'
    request.store_result = store_result
    request.restart_on_failure = restart_on_failure

    return test_request_message.Stringize(request, json_readable=True)

  def _GetInvalidRequestMessage(self):
    """Return an improperly formatted request message text."""

    return 'this is a bad request.'

  def _GetMachineRegisterRequest(self, machine_id=None, username=None,
                                 password=None, tag=None, try_count=None,
                                 version=None, platform='win-xp'):
    """Return a properly formatted register machine request.

    Args:
      machine_id: If provided, the id of the machine will be set to this.
      username: If provided, the user_name of the machine will be set to this.
      password: If provided, the password of the machine will be set to this.
      tag: If provided, the tag of the machine will be set to this.
      try_count: If provided, the try_count of the machine will be set to this.
      version: If provided, the version of the machine will be set to this.
      platform: The value of the os to use in the dimensions.

    Returns:
      A dictionary which can be fed into test_manager.ExecuteRegisterRequest().
    """

    config_dimensions = {'os': platform, 'cpu': 'Unknown', 'browser': 'Unknown'}
    attributes = {'dimensions': config_dimensions}
    if machine_id:
      attributes['id'] = str(machine_id)
    if username:
      attributes['username'] = username
    if password:
      attributes['password'] = password
    if tag:
      attributes['tag'] = tag
    if try_count:
      attributes['try_count'] = try_count
    if version:
      attributes['version'] = version

    return attributes

  def _SetupSendMailExpectations(self):
    mail.send_mail(sender='Test Request Server <no_reply@google.com>',
                   to='john@doe.com',
                   subject='%s:%s failed.' %
                   (self._request_message_test_case_name,
                    self._request_message_config_name),
                   body=mox.IgnoreArg(),
                   html=mox.IgnoreArg())

  def _ExecuteRegister(self, machine_id, try_count=0, platform='win-xp',
                       register_should_match=True):
    register_request = self._GetMachineRegisterRequest(machine_id=machine_id,
                                                       try_count=try_count,
                                                       platform=platform)
    response = self._manager.ExecuteRegisterRequest(register_request,
                                                    self._SERVER_URL)

    if register_should_match:
      self.assertTrue('commands' in response, response)
      self.assertTrue('result_url' in response, response)
      self.assertTrue('come_back' not in response, response)
    else:
      self.assertTrue('commands' not in response, response)
      self.assertTrue('result_url' not in response, response)
      self.assertTrue('come_back' in response, response)

    return response

  def testRequestGoodMachine(self):
    # A test request is received then one machine polls for a job.  This
    # machine matches the requirements of the test, so the TestRequestManager
    # should send it to that machine.
    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    self._ExecuteRegister(MACHINE_IDS[0])
    runner = test_runner.TestRunner.gql('WHERE machine_id = :1',
                                        MACHINE_IDS[0]).get()
    self.assertNotEqual(None, runner)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertNotEqual(None, runner.started)

  # By testing with a large number of configurations for a machine we are
  # unable to use the hashing method to find a match, so ensure we fall back
  # on the old direct comparision method.
  def testRequestGoodMachineWithLargeConfig(self):
    large_os_config = map(str, range(  # pylint: disable=g-long-lambda
        dimensions_utils.MAX_DIMENSIONS_PER_MACHINE * 2))

    self._manager.ExecuteTestRequest(self._GetRequestMessage(
        platform=large_os_config))

    self._ExecuteRegister(MACHINE_IDS[0], platform=large_os_config)
    runner = test_runner.TestRunner.gql('WHERE machine_id = :1',
                                        MACHINE_IDS[0]).get()
    self.assertNotEqual(None, runner)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertNotEqual(None, runner.started)

  def _AssignPendingRequestsTest(self, instances=1):
    self._manager.ExecuteTestRequest(
        self._GetRequestMessage(min_instances=instances))

    # Execute the runners.
    self.assertLessEqual(instances, len(MACHINE_IDS))
    for i in range(instances):
      self._ExecuteRegister(MACHINE_IDS[i])
      runner = test_runner.TestRunner.gql('WHERE machine_id = :1',
                                          MACHINE_IDS[i]).get()
      self.assertNotEqual(None, runner)

  def testMultiRunnerWithEnvironmentVariables(self):
    num_indexes = 2

    request_message = self._GetRequestMessage(
        min_instances=num_indexes, env_vars={'index': '%(instance_index)s'})

    self._manager.ExecuteTestRequest(request_message)

    for i in range(num_indexes):
      response = self._ExecuteRegister(MACHINE_IDS[i])

      # Validate shard indices are set correctly by parsing the commands.
      found_manifest = False
      for command in response['commands']:
        function_name, args = slave_machine.ParseRPC(command)
        if function_name == 'StoreFiles':
          found_manifest = True
          break

      self.assertEqual(found_manifest, True)
      for unused_path, name, content in args:
        if name == test_manager._TEST_RUN_SWARM_FILE_NAME:
          swarm_json = json.loads(content)
          self.assertEqual(str(i), swarm_json['env_vars']['index'])

  def _TestForRestartOnFailurePresence(self, restart_on_failure):
    self._manager.ExecuteTestRequest(self._GetRequestMessage(
        restart_on_failure=restart_on_failure))

    response = self._ExecuteRegister(MACHINE_IDS[0])

    found_command = False
    for command in response['commands']:
      function_name, args = slave_machine.ParseRPC(command)
      if function_name == 'RunCommands':
        found_command = True
        self.assertEqual('--restart_on_failure' in args, restart_on_failure)
    self.assertTrue(found_command)

  def testNoRestartOnFailureByDefault(self):
    self._TestForRestartOnFailurePresence(False)

  def testRestartOnFailurePropagated(self):
    self._TestForRestartOnFailurePresence(True)

  def _AddTestRunWithResultsExpectation(self, result_url, result_string):
    # Setup expectations for HandleTestResults().
    if result_url.startswith('mailto'):
      mail.send_mail(sender='Test Request Server <no_reply@google.com>',
                     to=result_url.split('//')[1],
                     subject='%s:%s succeeded.' %
                     (self._request_message_test_case_name,
                      self._request_message_config_name),
                     body=mox.StrContains(result_string),
                     html=mox.IgnoreArg())
    else:
      urllib2.urlopen(result_url, mox.StrContains('r=' + result_string))

  def _SetupHandleTestResults(self, result_url=DEFAULT_RESULT_URL,
                              result_string='', test_instances=1):
    # Setup a valid request waiting for completion from the runner.

    # Setup expectations for ExecuteTestRequest() and AssignPendingRequests().
    self._mox.StubOutWithMock(urllib2, 'urlopen')
    self._mox.StubOutWithMock(mail, 'send_mail')

    for _ in range(test_instances):
      self._AddTestRunWithResultsExpectation(result_url, result_string)

  def ExecuteHandleTestResults(self, success, result_url=DEFAULT_RESULT_URL,
                               store_result='all', test_instances=1,
                               store_results_successfully=True):
    self._manager.ExecuteTestRequest(
        self._GetRequestMessage(min_instances=test_instances,
                                result_url=result_url,
                                store_result=store_result))

    # Execute the tests by having a machine poll for them.
    for _ in range(test_instances):
      self._ExecuteRegister(MACHINE_IDS[0])

    # For each runner return the test results and ensure it is handled properly.
    for runner in test_runner.TestRunner.query():
      # Get the updated verison of the runner, the current one was
      # cached by the loop and only the key is guaranteed to be the same, so we
      # use it to get a fresh version.
      runner_key = runner.key
      runner = runner_key.get()

      result_blob_key = blobstore_helper.CreateBlobstore('results')
      self.assertEqual(store_results_successfully,
                       self._manager.UpdateTestResult(
                           runner, runner.machine_id,
                           result_blob_key=result_blob_key,
                           success=success))

      # If results aren't being stored we can't check the runner data because
      # it will have been deleted.
      if store_result == 'none' or (store_result == 'fail' and success):
        continue

      runner = runner_key.get()
      self.assertNotEqual(None, runner)
      self.assertEqual(success, runner.ran_successfully)
      self.assertTrue(runner.done)

      # Pretend that the runner sends a second response for this runner.
      # Make sure it does not change.
      self.assertFalse(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                      success=success))

      runner2 = runner_key.get()
      self.assertNotEqual(None, runner2)
      self.assertEqual(success, runner2.ran_successfully)
      self.assertTrue(runner2.done)
      self.assertEqual(runner.ended, runner2.ended)

  def testHandleSucceededStoreAllResults(self):
    self._SetupHandleTestResults()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True)

    self._mox.VerifyAll()

  def testHandleSucceededStoreNoResults(self):
    self._SetupHandleTestResults(test_instances=1)
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True, store_result='none',
                                  test_instances=1)
    # Check that the test instance has been handled by the single machine
    # and had its results cleared.
    self.assertEqual(0, test_runner.TestRunner.query().count())

    self._mox.VerifyAll()

  def testHandleFailedTestResults(self):
    self._SetupHandleTestResults()
    self._SetupSendMailExpectations()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=False)

    self._mox.VerifyAll()

  def testHandleOverwriteTestResults(self):
    messages = ['first-message',
                'second-message',
                'third-message']

    self._mox.StubOutWithMock(urllib2, 'urlopen')
    self._mox.StubOutWithMock(logging, 'error')
    self._AddTestRunWithResultsExpectation(DEFAULT_RESULT_URL, messages[0])
    logging.error(mox.StrContains('additional response'), mox.IgnoreArg(),
                  mox.IgnoreArg())
    self._AddTestRunWithResultsExpectation(DEFAULT_RESULT_URL, messages[2])
    urllib2.urlopen(DEFAULT_RESULT_URL, mox.StrContains('r=' + messages[0]))
    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    runner = test_runner.TestRunner.query().get()

    try:
      # Replace the async with a non-async version to ensure the blobs
      # are deleted before we check if they were deleted by the following
      # asserts.
      old_delete_async = test_manager.blobstore.delete_async
      test_manager.blobstore.delete_async = blobstore.delete

      # First results, always accepted.
      self.assertTrue(self._manager.UpdateTestResult(
          runner, runner.machine_id,
          result_blob_key=blobstore_helper.CreateBlobstore(messages[0])))
      # Always ensure that there is only one element in the blobstore.
      self.assertEqual(1, blobstore.BlobInfo.all().count())

      # The first result resent, accepted since the strings are equal.
      self.assertTrue(self._manager.UpdateTestResult(
          runner, runner.machine_id,
          result_blob_key=blobstore_helper.CreateBlobstore(messages[0])))
      self.assertEqual(1, blobstore.BlobInfo.all().count())

      # Non-first request without overwrite, rejected.
      self.assertFalse(self._manager.UpdateTestResult(
          runner, runner.machine_id,
          result_blob_key=blobstore_helper.CreateBlobstore(messages[1]),
          overwrite=False))
      self.assertEqual(1, blobstore.BlobInfo.all().count())

      # Non-first request with overwrite, accepted.
      self.assertTrue(self._manager.UpdateTestResult(
          runner, runner.machine_id,
          result_blob_key=blobstore_helper.CreateBlobstore(messages[2]),
          overwrite=True))
      self.assertEqual(1, blobstore.BlobInfo.all().count())
    finally:
      test_manager.blobstore.delete_async = old_delete_async

    # Accept the first message as an error with overwrite.
    self.assertTrue(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                   errors=messages[0],
                                                   overwrite=True))

    # Accept the first message as an error again, since it is equal to what is
    # already stored.
    self.assertTrue(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                   errors=messages[0]))

    # Make sure there are no blobs store now, since errors are just stored as
    # strings.
    self.assertEqual(0, blobstore.BlobInfo.all().count())

    self._mox.VerifyAll()

  def testRunnerCallerMachineIdMismatch(self):
    self._mox.StubOutWithMock(test_manager.logging, 'warning')
    test_manager.logging.warning('The machine id of the runner, %s, doesn\'t '
                                 'match the machine id given, %s',
                                 MACHINE_IDS[0], MACHINE_IDS[1])

    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())
    runner = test_runner.TestRunner.query().get()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()

    self.assertFalse(self._manager.UpdateTestResult(runner, MACHINE_IDS[1]))
    self._mox.VerifyAll()

  def testRunnerCallerOldMachine(self):
    self._mox.StubOutWithMock(urllib2, 'urlopen')
    urllib2.urlopen(mox.IgnoreArg(), mox.StrContains('r='))
    self._mox.ReplayAll()

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[0])
    runner.machine_id = MACHINE_IDS[0]
    runner.old_machine_ids = [MACHINE_IDS[1]]
    runner.put()

    self.assertTrue(self._manager.UpdateTestResult(runner, MACHINE_IDS[1]))

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(MACHINE_IDS[1], runner.machine_id)
    self.assertEqual([MACHINE_IDS[0]], runner.old_machine_ids)
    self.assertTrue(runner.done)

    self._mox.VerifyAll()

  def testRunnerCallerOldMachineWithNoCurrent(self):
    self._mox.StubOutWithMock(urllib2, 'urlopen')
    urllib2.urlopen(mox.IgnoreArg(), mox.StrContains('r='))
    self._mox.ReplayAll()

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[0])
    runner.machine_id = None
    runner.old_machine_ids = [MACHINE_IDS[1]]
    runner.put()

    self.assertTrue(self._manager.UpdateTestResult(runner, MACHINE_IDS[1]))

    runner = test_runner.TestRunner.query().get()
    self.assertEqual(MACHINE_IDS[1], runner.machine_id)
    self.assertEqual([], runner.old_machine_ids)
    self.assertTrue(runner.done)

    self._mox.VerifyAll()

  def _SetupAndExecuteTestResults(self, result_url):
    self._SetupHandleTestResults(result_url=result_url)
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True,
                                  result_url=result_url)

    self._mox.VerifyAll()

  def testEmailAsResultURL(self):
    self._SetupAndExecuteTestResults('mailto://john@doe.com')

  def testPostResultasHTTPS(self):
    self._SetupAndExecuteTestResults('https://secure.com/results')

  def testClearingAllRunnerAndRequest(self):
    self._SetupHandleTestResults()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True, store_result='none')
    self._mox.VerifyAll()

    self.assertEqual(0, test_runner.TestRunner.query().count())
    self.assertEqual(0, test_request.TestRequest.query().count())

  def testClearingFailedRunnerAndRequestSucceeded(self):
    self._SetupHandleTestResults()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True, store_result='fail')
    self._mox.VerifyAll()

    self.assertEqual(0, test_runner.TestRunner.query().count())
    self.assertEqual(0, test_request.TestRequest.query().count())

  def testClearingFailedRunnerAndRequestFailed(self):
    self._SetupHandleTestResults()
    self._SetupSendMailExpectations()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=False, store_result='fail')
    self._mox.VerifyAll()

    self.assertNotEqual(0, test_runner.TestRunner.query().count())
    self.assertNotEqual(0, test_request.TestRequest.query().count())

  def _GenerateFutureTimeExpectation(self):
    """Set the current time to way in the future and return it."""
    future_time = (datetime.datetime.now() +
                   datetime.timedelta(
                       seconds=(test_manager._TIMEOUT_FACTOR + 1000)))
    test_manager._GetCurrentTime().AndReturn(future_time)

    return future_time

  def testOnlyAbortStaleRunningRunner(self):
    self._AssignPendingRequestsTest()
    runner = test_runner.TestRunner.query().get()

    # Mark the runner as having pinged so it won't be considered stale and it
    # won't be aborted.
    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')
    runner.ping = self._GenerateFutureTimeExpectation()
    runner.put()
    self._mox.ReplayAll()

    ndb.Future.wait_all(self._manager.AbortStaleRunners())

    runner = test_runner.TestRunner.query().get()
    self.assertFalse(runner.done)
    self.assertEqual(0, runner.automatic_retry_count)

    self._mox.VerifyAll()

  def testAbortStaleRunnerWaitingForMachine(self):
    self._AssignPendingRequestsTest()
    runner = test_runner.TestRunner.query().get()

    # Mark the runner as having been created in the past so it will be
    # considered stale (i.e., it took too long to find a match).
    runner.created -= datetime.timedelta(
        seconds=(2 * test_manager.SWARM_RUNNER_MAX_WAIT_SECS))
    runner.put()

    # Don't abort the runner if it is running and it has been pinging the server
    # (no matter how old).
    runner.started = datetime.datetime.now()
    runner.ping = datetime.datetime.now() + datetime.timedelta(days=1)
    runner.put()
    self.assertFalse(runner.done)
    ndb.Future.wait_all(self._manager.AbortStaleRunners())

    runner = test_runner.TestRunner.query().get()
    self.assertFalse(runner.done)
    runner.started = None
    runner.put()

    # Don't abort the runner if it has been automatically retried, since
    # that means it has been matched with a machine before.
    runner.automatic_retry_count = 1
    runner.put()
    self.assertFalse(runner.done)
    ndb.Future.wait_all(self._manager.AbortStaleRunners())

    runner = test_runner.TestRunner.query().get()
    self.assertFalse(runner.done)
    runner.automatic_retry_count = 0
    runner.put()

    # Now the runner should be aborted, since it hasn't been matched within
    # SWARM_RUNNER_MAX_WAIT_SECS seconds.
    ndb.Future.wait_all(self._manager.AbortStaleRunners())

    runner = test_runner.TestRunner.query().get()
    self.assertTrue(runner.done)
    self.assertIn('Runner was unable to find a machine to run it within',
                  runner.GetResultString())

    # Check that the runner isn't aborted a second time.
    old_abort = self._manager.AbortRunner
    try:
      self._manager.AbortRunner = lambda runner, reason: self.fail()
      ndb.Future.wait_all(self._manager.AbortStaleRunners())
    finally:
      self._manager.AbortRunner = old_abort

  def testRetryAndThenAbortStaleRunners(self):
    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')
    attempts_to_reach_abort = test_runner.MAX_AUTOMATIC_RETRIES + 1

    for _ in range(attempts_to_reach_abort):
      self._GenerateFutureTimeExpectation()

    # Setup the functions when the runner is aborted because it is stale.
    self._mox.StubOutWithMock(urllib2, 'urlopen')
    urllib2.urlopen(DEFAULT_RESULT_URL, mox.IgnoreArg())
    self._mox.StubOutWithMock(mail, 'send_mail')
    self._SetupSendMailExpectations()
    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    for i in range(attempts_to_reach_abort):
      # Assign a machine to the runner.
      self._ExecuteRegister(MACHINE_IDS[0])

      runner = test_runner.TestRunner.query().get()
      self.assertFalse(runner.done)
      self.assertEqual(i, runner.automatic_retry_count)
      self.assertNotEqual(None, runner.started)

      ndb.Future.wait_all(self._manager.AbortStaleRunners())

      runner = test_runner.TestRunner.query().get()
      if i == test_runner.MAX_AUTOMATIC_RETRIES:
        self.assertTrue(runner.done)
        self.assertNotEqual(None, runner.started)
        self.assertIn('Runner has become stale', runner.GetResultString())
      else:
        self.assertFalse(runner.done)
        self.assertEqual([MACHINE_IDS[0]] * (i + 1), runner.old_machine_ids)
        self.assertEqual(i + 1, runner.automatic_retry_count)
        self.assertEqual(None, runner.started)

    self._mox.VerifyAll()

  def testSwarmErrorDeleteOldErrors(self):
    # Create error.
    error = test_manager.SwarmError(
        name='name', message='msg', info='info')
    error.put()
    self.assertEqual(1, test_manager.SwarmError.query().count())

    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.SWARM_ERROR_TIME_TO_LIVE_DAYS - 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.SWARM_ERROR_TIME_TO_LIVE_DAYS + 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)

    self._mox.ReplayAll()

    # First call shouldn't delete the error since its not stale yet.
    ndb.Future.wait_all(test_manager.DeleteOldErrors())
    self.assertEqual(1, test_manager.SwarmError.query().count())

    # Second call should remove the now stale error.
    ndb.Future.wait_all(test_manager.DeleteOldErrors())
    self.assertEqual(0, test_manager.SwarmError.query().count())

    self._mox.VerifyAll()

  def testResultWithUnicode(self):
    # Make sure we can handle results with unicode in them.
    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[0])

    self._manager.AbortRunner(runner, u'\u04bb')

  def testAssignSinglePendingRequest(self):
    # Test when there is 1 test request then 1 machine registers itself.
    self._AssignPendingRequests()

  def testAssignMultiplePendingRequest(self):
    # Test when there are 3 test requests then 3 machines register themselves.
    self._AssignPendingRequests(num_tests=3, num_machines=3)

  def testAssignMultiplePendingRequestLessMachines(self):
    # Test when there are 5 test requests then 2 machines register themselves.
    # This will result in 3 pending test.
    self._AssignPendingRequests(num_tests=5, num_machines=2)

  def testAssignMultiplePendingRequestLessTests(self):
    # Test when there are 3 test requests then 4 machines register themselves.
    self._AssignPendingRequests(num_tests=3, num_machines=4)

  def _AssignPendingRequests(self, num_tests=1, num_machines=1):
    num_running = min(num_tests, num_machines)
    for _ in range(num_tests):
      self._CreatePendingRequest()

    # Assign different ids to the machines if requested, or have the same
    # machine do all the tests.
    for i in range(num_machines):
      self._ExecuteRegister(
          MACHINE_IDS[i],
          register_should_match=(i < num_running))

    for i in range(num_running):
      self._AssertTestCount(MACHINE_IDS[i], 1)

    # If there were more tests than machines there should some pending tests.
    self._AssertPendingTestCount(max(0, num_tests - num_machines))

    # No test should be done.
    done_tests = test_runner.TestRunner.gql('WHERE done = :1', True)
    self.assertEqual(0, done_tests.count())

  # Asserts exactly 'expected_count' number of tests exist that have machine_id.
  def _AssertTestCount(self, machine_id, expected_count):
    tests = test_runner.TestRunner.gql('WHERE machine_id = :1', machine_id)
    self.assertEqual(expected_count, tests.count())

  # Asserts exactly 'expected_count' number of tests exist and are waiting
  # for a machine.
  def _AssertPendingTestCount(self, expected_count):
    tests = test_runner.TestRunner.gql('WHERE started = :1', None)
    self.assertEqual(expected_count, tests.count())

  def testNoPendingTestsOnRegisterNoTryCount(self):
    # A machine registers itself without an id and there are no tests pending.
    response = self._ExecuteRegister(machine_id=None,
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back', 'id']

    self.assertEqual(response.keys().sort(), expected_keys.sort())
    self.assertEqual(response['try_count'], 1)

    # Make sure the register request doesn't create a TestRunner.
    self.assertEqual(0, test_runner.TestRunner.query().count())

  def testNoPendingTestsOnRegisterWithTryCount(self):
    # A machine registers itself without an id and there are no tests pending
    try_count = 1234
    response = self._ExecuteRegister(machine_id=None, try_count=try_count,
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back', 'id']

    self.assertEqual(response.keys().sort(), expected_keys.sort())
    self.assertEqual(response['try_count'], try_count+1)

    # Make sure the register request doesn't create a TestRunner.
    self.assertEqual(0, test_runner.TestRunner.query().count())

    self._mox.VerifyAll()

  def testRequestBadAttributes(self):
    # An invalid machine register request is received which should raise
    # an exception.

    request_message = self._GetInvalidRequestMessage()
    self.assertRaisesRegexp(test_request_message.Error,
                            r'No JSON object could be decoded',
                            self._manager.ExecuteTestRequest,
                            request_message)

  def testValidateAndFixAttributes(self):
    # Test test_manager.ValidateAndFixAttributes
    attributes = {}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Missing mandatory attribute: dimensions',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

    # Test with empty dimensions.
    attributes = {'dimensions': ''}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value for dimensions',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

    attributes = {'dimensions': {'os': 'win-xp'}}
    result = self._manager.ValidateAndFixAttributes(attributes)
    self.assertIn('id', result)

    # Test with invalid attribute types.
    attributes = {'dimensions': {'os': 'win-xp'}, 'tag': 10}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value type for tag',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

    # Test with invalid id type (int), which should be replaced.
    invalid_id_type = 10
    attributes = {'dimensions': {'os': 'win-xp'}, 'id': invalid_id_type}
    results = self._manager.ValidateAndFixAttributes(attributes)
    self.assertNotEqual(invalid_id_type, results['id'])

    # Test with special id type: None.
    attributes = {'dimensions': {'os': 'win-xp'}, 'id': None}
    result = self._manager.ValidateAndFixAttributes(attributes)
    self.assertTrue('id' in result)
    self.assertNotEqual(result['id'], None)

    # Test with invalid id value.
    invalid_id_value = '12345'
    attributes = {'dimensions': {'os': 'win-xp'}, 'id': invalid_id_value}
    response = self._manager.ValidateAndFixAttributes(attributes)
    self.assertNotEqual(invalid_id_value, response['id'])

    # Test with invalid attribute name.
    attributes = {'dimensions': {'os': 'win-xp'}, 'wrong': 'invalid'}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attribute to machine: wrong',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

    # Make sure machine id is not regenerated every time.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0]}
    result = self._manager.ValidateAndFixAttributes(attributes)
    self.assertEqual(MACHINE_IDS[0], result['id'])

    # Test with missing try_count and make sure its set to 0.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0]}
    result = self._manager.ValidateAndFixAttributes(attributes)
    self.assertEqual(result['try_count'], 0)

    # Make sure version is accepted.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'version': hashlib.sha1().hexdigest()}
    result = self._manager.ValidateAndFixAttributes(attributes)

  def testValidateAndFixAttributesTryCount(self):
    # Test with bad try_count type.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': 'hello'}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value type for try_count',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

    # Test with given try_count.
    try_count = 1234
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': try_count}
    result = self._manager.ValidateAndFixAttributes(attributes)
    self.assertEqual(result['try_count'], try_count)

    # Test with given try_count but with invalid negative value.
    try_count = -1
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': try_count}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid negative value for try_count',
                            self._manager.ValidateAndFixAttributes,
                            attributes)

  def testComebackValues(self):
    for try_count in range(10):
      delay = self._manager._ComputeComebackValue(try_count)
      self.assertGreaterEqual(delay, 0)
      self.assertLessEqual(delay, test_manager.MAX_COMEBACK_SECS)

  def _CreatePendingRequest(self, config_name=None, config_index=0,
                            num_config_instances=1, machine_id=None):
    request = test_request.TestRequest(
        message=self._GetRequestMessage(),
        name=self._request_message_test_case_name)
    request.put()

    config_name = config_name or self._request_message_config_name
    runner = test_runner.TestRunner(
        request=request.key, config_name=config_name,
        config_hash=request.GetConfigurationDimensionHash(config_name),
        config_instance_index=config_index,
        num_config_instances=num_config_instances)
    if machine_id:
      runner.machine_id = machine_id
      runner.started = datetime.datetime.now()
    runner.put()

    return runner

  # Ensure that if we have a machine request a test that has the same id
  # as a machine that is supposed to be running a test, we log an error, since
  # it probably means we failed to get the results from the last test.
  def testRequestBeforeResult(self):
    self._mox.StubOutWithMock(logging, 'warning')

    logging.warning(mox.StrContains('unfinished test'), mox.IgnoreArg(),
                    mox.IgnoreArg())
    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    version = test_manager.SlaveVersion()
    register_request = self._GetMachineRegisterRequest(version=version)
    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)

    self._mox.VerifyAll()

    # Now mark the test as done, and ensure we don't get the warning.
    runner = test_runner.TestRunner.query().get()
    runner.done = True
    runner.put()

    self._mox.ReplayAll()

    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    self._mox.VerifyAll()

  def testRecordMachineRunnerAssignedCorrectlyCalled(self):
    matching_config = 'win-xp'
    request_message = self._GetRequestMessage(platform=matching_config)
    self._manager.ExecuteTestRequest(request_message)

    self.assertEqual(0, machine_stats.MachineStats.all().count())

    # Ensure query is recorded, even though there was no match.
    nonmatching_config = 'win-vista'
    self._manager.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(platform=nonmatching_config),
        self._SERVER_URL)
    self.assertEqual(1, machine_stats.MachineStats.all().count())

    # Ensure the query is recorded.
    self._manager.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(platform=matching_config),
        self._SERVER_URL)
    self.assertEqual(2, machine_stats.MachineStats.all().count())

  def testRecordRunnerStats(self):
    # Create a pending runner and execute it.
    self._manager.ExecuteTestRequest(self._GetRequestMessage())
    self._ExecuteRegister(MACHINE_IDS[0])
    self.assertEqual(0, runner_stats.RunnerStats.query().count())

    # Return the results for the test and ensure the stats are updated.
    runner = test_runner.TestRunner.query().get()
    self._manager.UpdateTestResult(runner, runner.machine_id, success=True)

    self.assertEqual(1, runner_stats.RunnerStats.query().count())
    r_stats = runner_stats.RunnerStats.query().get()
    self.assertNotEqual(None, r_stats.created_time)
    self.assertNotEqual(None, r_stats.assigned_time)
    self.assertNotEqual(None, r_stats.end_time)
    self.assertTrue(r_stats.success)

  def testGetUpdateWhenPollingForWork(self):
    # Drop the last character of the version string to ensure a version
    # mismatch.
    version = test_manager.SlaveVersion()[:-1]

    response = self._manager.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(version=version),
        self._SERVER_URL)

    self.assertTrue('id' in response)
    self.assertTrue('try_count' in response)
    self.assertTrue('commands' in response)
    self.assertTrue('result_url' in response)

    self.assertEqual([{'args': self._SERVER_URL + 'get_slave_code',
                       'function': 'UpdateSlave'}],
                     response['commands'])
    self.assertEqual(self._SERVER_URL + 'remote_error', response['result_url'])

  def testSlaveCodeZipped(self):
    zipped_code = test_manager.SlaveCodeZipped()

    temp_dir = tempfile.mkdtemp()
    try:
      with zipfile.ZipFile(StringIO.StringIO(zipped_code), 'r') as zip_file:
        zip_file.extractall(temp_dir)

      expected_slave_script = os.path.join(temp_dir,
                                           swarm_constants.SLAVE_MACHINE_SCRIPT)
      self.assertTrue(os.path.exists(expected_slave_script))

      expected_test_runner = os.path.join(temp_dir,
                                          swarm_constants.TEST_RUNNER_DIR,
                                          swarm_constants.TEST_RUNNER_SCRIPT)
      self.assertTrue(os.path.exists(expected_test_runner))

      common_dir = os.path.join(temp_dir, swarm_constants.COMMON_DIR)
      for common_file in swarm_constants.SWARM_BOT_COMMON_FILES:
        self.assertTrue(os.path.exists(
            os.path.join(common_dir, common_file)))

      # Try running the slave and ensure it can import the required files.
      # (It would crash if it failed to import them).
      subprocess.check_call([sys.executable, expected_slave_script, '-h'])
    finally:
      shutil.rmtree(temp_dir)


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
