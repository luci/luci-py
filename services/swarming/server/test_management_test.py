#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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
import zipfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

import test_case
from common import bot_archive
from common import rpc
from common import test_request_message
from server import dimensions_utils
from server import result_helper
from server import test_helper
from server import test_management
from server import test_request
from server import test_runner
from stats import machine_stats
from third_party.mox import mox


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']


class TestManagementTest(test_case.TestCase):

  _SERVER_URL = 'http://my.server.com/'

  def setUp(self):
    super(TestManagementTest, self).setUp()
    self._mox = mox.Mox()

    # Create default configurations.
    dimensions = dict(os='win-xp', browser='Unknown', cpu='Unknown')
    self._config_win = test_request_message.TestConfiguration(
        config_name='Windows', dimensions=dimensions)

    dimensions = dict(os='linux', browser='Unknown', cpu='Unknown')
    self._config_linux = test_request_message.TestConfiguration(
        config_name='Linux', dimensions=dimensions)

    self._request_message_config_name = 'c1'
    self._request_message_test_case_name = 'tc'

  def tearDown(self):
    self._mox.UnsetStubs()
    super(TestManagementTest, self).tearDown()

  def _GetInvalidRequestMessage(self):
    """Return an improperly formatted request message text."""

    return 'this is a bad request.'

  def _GetMachineRegisterRequest(self, machine_id=MACHINE_IDS[0], username=None,
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
      A dictionary which can be fed into
      test_management.ExecuteRegisterRequest().
    """

    config_dimensions = {'os': platform, 'cpu': 'Unknown', 'browser': 'Unknown'}
    attributes = {
        'dimensions': config_dimensions,
        'version': test_management.SlaveVersion()
    }
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

  def _ExecuteRegister(self, machine_id, try_count=0, platform='win-xp',
                       register_should_match=True):
    register_request = self._GetMachineRegisterRequest(machine_id=machine_id,
                                                       try_count=try_count,
                                                       platform=platform)
    response = test_management.ExecuteRegisterRequest(register_request,
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
    test_management.ExecuteTestRequest(test_helper.GetRequestMessage())

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
    large_os_config = map(str, range(
        dimensions_utils.MAX_DIMENSIONS_PER_MACHINE * 2))

    test_management.ExecuteTestRequest(test_helper.GetRequestMessage(
        platform=large_os_config))

    self._ExecuteRegister(MACHINE_IDS[0], platform=large_os_config)
    runner = test_runner.TestRunner.gql('WHERE machine_id = :1',
                                        MACHINE_IDS[0]).get()
    self.assertNotEqual(None, runner)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertNotEqual(None, runner.started)

  def testRunnersWithDifferentPriorities(self):
    test_management.ExecuteTestRequest(test_helper.GetRequestMessage(
        request_name='low-priority',
        priority=100))
    test_management.ExecuteTestRequest(
        test_helper.GetRequestMessage(request_name='high-priority',
                                      priority=1))
    self.assertEqual(2, test_request.TestRequest.query().count())

    self._ExecuteRegister(MACHINE_IDS[0])

    old_low_priority_runner = test_runner.TestRunner.query(
        test_runner.TestRunner.priority == 100).get()
    self.assertNotEqual(None, old_low_priority_runner)

    new_high_priority_runner = test_runner.TestRunner.query(
        test_runner.TestRunner.priority == 1).get()
    self.assertNotEqual(None, new_high_priority_runner)

    # Ensure that the low priority runner is older.
    self.assertTrue(
        old_low_priority_runner.created < new_high_priority_runner.created)

    # Ensure that the new runner executes, since it has higher priority, even
    # though it is newer.
    self.assertEqual(None, old_low_priority_runner.started)
    self.assertNotEqual(None, new_high_priority_runner.started)

  def testTestRequestMismatchFailedRunner(self):
    request = test_helper.GetRequestMessage(num_instances=2)

    test_management.ExecuteTestRequest(request)
    runner = test_runner.TestRunner.query().get()
    test_runner.DeleteRunner(runner)

    # The new request won't use the old request since one of its runners is
    # gone.
    test_management.ExecuteTestRequest(request)

    # Ensure that we created a new test request and two new runner.
    self.assertEqual(2, test_request.TestRequest.query().count())
    self.assertEqual(3, test_runner.TestRunner.query().count())

  def testTestRequestMismatchDeletedRunner(self):
    request = test_helper.GetRequestMessage()

    test_management.ExecuteTestRequest(request)
    runner = test_runner.TestRunner.query().get()
    runner.done = True
    runner.ran_successfully = False
    runner.put()

    # The new request won't use the old request since its runner failed.
    test_management.ExecuteTestRequest(request)

    # Ensure that we created a new test request and a new runner.
    self.assertEqual(2, test_request.TestRequest.query().count())
    self.assertEqual(2, test_runner.TestRunner.query().count())

  # TODO(csharp): Renable once request merging is enabled again.
  def disabled_testTestRequestMatch(self):
    request = test_helper.GetRequestMessage()

    response = test_management.ExecuteTestRequest(request)

    # The new request will just merge with the old request.
    merged_response = test_management.ExecuteTestRequest(request)

    # Ensure we actually return the same values.
    self.assertEqual(response, merged_response)

    # Ensure that we don't create any new values.
    self.assertEqual(1, test_request.TestRequest.query().count())
    self.assertEqual(1, test_runner.TestRunner.query().count())

  # TODO(csharp): Renable once request merging is enabled again.
  def disabled_testTestRequestMatchMultipleConfigs(self):
    num_configs = 2
    request = test_helper.GetRequestMessage(num_configs=num_configs)

    response = test_management.ExecuteTestRequest(request)
    self.assertEqual(1, test_request.TestRequest.query().count())

    # The new request will just merge with the old request since it is the old
    # request.
    merged_response = test_management.ExecuteTestRequest(request)

    # Ensure we actually return the same values.
    self.assertEqual(response, merged_response)

    # Ensure that we don't create any new values.
    self.assertEqual(1, test_request.TestRequest.query().count())
    self.assertEqual(num_configs, test_runner.TestRunner.query().count())

  def _AssignPendingRequestsTest(self, instances=1):
    test_management.ExecuteTestRequest(
        test_helper.GetRequestMessage(num_instances=instances))

    # Execute the runners.
    self.assertLessEqual(instances, len(MACHINE_IDS))
    for i in range(instances):
      self._ExecuteRegister(MACHINE_IDS[i])
      runner = test_runner.TestRunner.gql('WHERE machine_id = :1',
                                          MACHINE_IDS[i]).get()
      self.assertNotEqual(None, runner)

  def testMultiRunnerWithEnvironmentVariables(self):
    num_indexes = 2

    request_message = test_helper.GetRequestMessage(
        num_instances=num_indexes, env_vars={'index': '%(instance_index)s'})

    test_management.ExecuteTestRequest(request_message)

    for i in range(num_indexes):
      response = self._ExecuteRegister(MACHINE_IDS[i])

      # Validate shard indices are set correctly by parsing the commands.
      found_manifest = False
      for command in response['commands']:
        function_name, args = rpc.ParseRPC(command)
        if function_name == 'StoreFiles':
          found_manifest = True
          break

      self.assertEqual(found_manifest, True)
      for _unused_path, name, content in args:
        if name == test_management._TEST_RUN_SWARM_FILE_NAME:
          swarm_json = json.loads(content)
          self.assertEqual(str(i), swarm_json['env_vars']['index'])

  def _TestForRestartOnFailurePresence(self, restart_on_failure):
    test_management.ExecuteTestRequest(test_helper.GetRequestMessage(
        restart_on_failure=restart_on_failure))

    response = self._ExecuteRegister(MACHINE_IDS[0])

    found_command = False
    for command in response['commands']:
      function_name, args = rpc.ParseRPC(command)
      if function_name == 'RunCommands':
        found_command = True
        self.assertEqual('--restart_on_failure' in args, restart_on_failure)
    self.assertTrue(found_command)

  def testNoRestartOnFailureByDefault(self):
    self._TestForRestartOnFailurePresence(False)

  def testRestartOnFailurePropagated(self):
    self._TestForRestartOnFailurePresence(True)

  def ExecuteHandleTestResults(self, success,
                               result_string='results',
                               test_instances=1,
                               store_results_successfully=True):
    test_management.ExecuteTestRequest(
        test_helper.GetRequestMessage(num_instances=test_instances))

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

      results = result_helper.StoreResults(result_string)
      self.assertEqual(store_results_successfully,
                       runner.UpdateTestResult(runner.machine_id,
                                               results=results,
                                               success=success))

      self.assertEqual(success, runner.ran_successfully)
      self.assertTrue(runner.done)

      # Pretend that the runner sends a second response for this runner.
      # Make sure it does not change.
      original_end_time = runner.ended
      self.assertFalse(runner.UpdateTestResult(runner.machine_id,
                                               success=not success))

      self.assertEqual(success, runner.ran_successfully)
      self.assertTrue(runner.done)
      self.assertEqual(original_end_time, runner.ended)

  def testHandleSucceededStoreAllResults(self):
    self.ExecuteHandleTestResults(success=True)

  def testHandleFailedTestResults(self):
    self.ExecuteHandleTestResults(success=False)

  def _GenerateFutureTimeExpectation(self):
    """Set the current time to way in the future and return it."""
    future_time = (datetime.datetime.utcnow() +
                   datetime.timedelta(
                       seconds=(test_management._TIMEOUT_FACTOR + 1000)))
    test_management._GetCurrentTime().AndReturn(future_time)

    return future_time

  def testOnlyAbortStaleRunningRunner(self):
    self._AssignPendingRequestsTest()
    runner = test_runner.TestRunner.query().get()

    # Mark the runner as having pinged so it won't be considered stale and it
    # won't be aborted.
    self._mox.StubOutWithMock(test_management, '_GetCurrentTime')
    runner.ping = self._GenerateFutureTimeExpectation()
    runner.put()
    self._mox.ReplayAll()

    test_management.AbortStaleRunners()

    runner = test_runner.TestRunner.query().get()
    self.assertFalse(runner.done)
    self.assertEqual(0, runner.automatic_retry_count)

    self._mox.VerifyAll()

  def testAbortStaleRunnerWaitingForMachine(self):
    self._AssignPendingRequestsTest()
    runner = test_runner.TestRunner.query().get()

    # Make the runner's run_by time in the past, so it will be considered stale.
    runner.run_by -= datetime.timedelta(
        seconds=(2 * test_request_message.SWARM_RUNNER_MAX_WAIT_SECS))
    runner.put()

    # Don't abort the runner if it is running and it has been pinging the server
    # (no matter how old).
    runner.started = datetime.datetime.utcnow()
    runner.ping = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    runner.put()
    self.assertFalse(runner.done)
    test_management.AbortStaleRunners()

    runner = test_runner.TestRunner.query().get()
    self.assertFalse(runner.done)
    runner.started = None
    runner.put()

    # Now the runner should be aborted, since it hasn't been matched by its
    # run_by deadline.
    test_management.AbortStaleRunners()

    runner = test_runner.TestRunner.query().get()
    self.assertIsNotNone(runner.started)
    self.assertIsNotNone(runner.ended)
    self.assertTrue(runner.done)
    self.assertIn('Runner was unable to find a machine to run it within',
                  runner.GetResultString())

    # Check that the runner isn't aborted a second time.
    self.mock(test_management, 'AbortRunner', lambda *_: self.fail())
    test_management.AbortStaleRunners()

  def testRetryAndThenAbortStaleRunners(self):
    self._mox.StubOutWithMock(test_management, '_GetCurrentTime')
    attempts_to_reach_abort = test_runner.MAX_AUTOMATIC_RETRIES + 1

    for _ in range(attempts_to_reach_abort):
      self._GenerateFutureTimeExpectation()

    self._mox.ReplayAll()

    test_management.ExecuteTestRequest(test_helper.GetRequestMessage())

    for i in range(attempts_to_reach_abort):
      # Assign a machine to the runner.
      self._ExecuteRegister(MACHINE_IDS[0])

      runner = test_runner.TestRunner.query().get()
      self.assertFalse(runner.done)
      self.assertEqual(i, runner.automatic_retry_count)
      self.assertIsNotNone(runner.started)
      self.assertIsNotNone(runner.ping)

      test_management.AbortStaleRunners()

      runner = test_runner.TestRunner.query().get()
      if i == test_runner.MAX_AUTOMATIC_RETRIES:
        self.assertTrue(runner.done)
        self.assertIsNotNone(runner.started)
        self.assertIsNotNone(runner.ended)
        self.assertIn('Runner has become stale', runner.GetResultString())
      else:
        self.assertFalse(runner.done)
        self.assertIsNone(runner.started)
        self.assertEqual(i + 1, runner.automatic_retry_count)
        self.assertEqual([MACHINE_IDS[0]] * (i + 1), runner.old_machine_ids)

    self._mox.VerifyAll()

  def testSwarmErrorDeleteOldErrors(self):
    # Create error.
    error = test_management.SwarmError(
        name='name', message='msg', info='info')
    error.put()
    self.assertEqual(1, test_management.SwarmError.query().count())

    self._mox.StubOutWithMock(test_management, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=test_management.SWARM_ERROR_TIME_TO_LIVE_DAYS - 1))
    test_management._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=test_management.SWARM_ERROR_TIME_TO_LIVE_DAYS + 1))
    test_management._GetCurrentTime().AndReturn(mock_now)

    self._mox.ReplayAll()

    # First call shouldn't delete the error since its not stale yet.
    ndb.delete_multi(test_management.QueryOldErrors())
    self.assertEqual(1, test_management.SwarmError.query().count())

    # Second call should remove the now stale error.
    ndb.delete_multi(test_management.QueryOldErrors())
    self.assertEqual(0, test_management.SwarmError.query().count())

    self._mox.VerifyAll()

  def testResultWithUnicode(self):
    # Make sure we can handle results with unicode in them.
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_IDS[0])

    test_management.AbortRunner(runner, u'\u04bb')

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
      test_helper.CreatePendingRunner()

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
    # A machine registers itself and there are no tests pending.
    response = self._ExecuteRegister(MACHINE_IDS[0],
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back']

    self.assertEqual(response.keys().sort(), expected_keys.sort())
    self.assertEqual(response['try_count'], 1)

    # Make sure the register request doesn't create a TestRunner.
    self.assertEqual(0, test_runner.TestRunner.query().count())

  def testNoPendingTestsOnRegisterWithTryCount(self):
    # A machine registers itself and there are no tests pending
    try_count = 1234
    response = self._ExecuteRegister(MACHINE_IDS[0],
                                     try_count=try_count,
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back']

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
                            test_management.ExecuteTestRequest,
                            request_message)

  def testValidateAndFixAttributes(self):
    # Test test_management.ValidateAndFixAttributes
    attributes = {'id': MACHINE_IDS[0]}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Missing mandatory attribute: dimensions',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    # Test with empty dimensions.
    attributes = {'dimensions': '',
                  'id': MACHINE_IDS[0]}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value for dimensions',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0]}
    result = test_management.ValidateAndFixAttributes(attributes)

    # Test with invalid attribute types.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'tag': 10}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value type for tag',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    # Test with invalid id type.
    invalid_id_type = 10
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': invalid_id_type}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value for id',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    # Test with invalid attribute name.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'wrong': 'invalid'}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attribute to machine: wrong',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    # Test with missing try_count and make sure its set to 0.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0]}
    result = test_management.ValidateAndFixAttributes(attributes)
    self.assertEqual(result['try_count'], 0)

    # Make sure version is accepted.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'version': hashlib.sha1().hexdigest()}
    result = test_management.ValidateAndFixAttributes(attributes)

  def testValidateAndFixAttributesTryCount(self):
    # Test with bad try_count type.
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': 'hello'}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid attrib value type for try_count',
                            test_management.ValidateAndFixAttributes,
                            attributes)

    # Test with given try_count.
    try_count = 1234
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': try_count}
    result = test_management.ValidateAndFixAttributes(attributes)
    self.assertEqual(result['try_count'], try_count)

    # Test with given try_count but with invalid negative value.
    try_count = -1
    attributes = {'dimensions': {'os': 'win-xp'},
                  'id': MACHINE_IDS[0],
                  'try_count': try_count}
    self.assertRaisesRegexp(test_request_message.Error,
                            r'Invalid negative value for try_count',
                            test_management.ValidateAndFixAttributes,
                            attributes)

  def testComebackValues(self):
    for try_count in range(10):
      delay = test_management._ComputeComebackValue(try_count)
      self.assertGreaterEqual(delay, 0)
      self.assertLessEqual(delay, test_management.MAX_COMEBACK_SECS)

  # Ensure that if we have a machine request a test that has the same id
  # as a machine that is supposed to be running a test, we log an error, since
  # it probably means we failed to get the results from the last test.
  def testRequestBeforeResult(self):
    self._mox.StubOutWithMock(logging, 'warning')

    logging.error(
        mox.StrContains('unfinished test'), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    test_management.ExecuteTestRequest(test_helper.GetRequestMessage())

    register_request = self._GetMachineRegisterRequest()
    test_management.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    test_management.ExecuteRegisterRequest(register_request, self._SERVER_URL)

    self._mox.VerifyAll()

    # Now mark the test as done, and ensure we don't get the warning.
    runner = test_runner.TestRunner.query().get()
    runner.done = True
    runner.put()

    self._mox.ReplayAll()

    test_management.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    self._mox.VerifyAll()

  def testRecordMachineRunnerAssignedCorrectlyCalled(self):
    matching_config = 'win-xp'
    request_message = test_helper.GetRequestMessage(platform=matching_config)
    test_management.ExecuteTestRequest(request_message)

    self.assertEqual(0, machine_stats.MachineStats.query().count())

    # Ensure query is recorded, even though there was no match.
    nonmatching_config = 'win-vista'
    test_management.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(machine_id=MACHINE_IDS[0],
                                        platform=nonmatching_config),
        self._SERVER_URL)
    self.assertEqual(1, machine_stats.MachineStats.query().count())

    # Ensure the query is recorded.
    test_management.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(machine_id=MACHINE_IDS[1],
                                        platform=matching_config),
        self._SERVER_URL)
    self.assertEqual(2, machine_stats.MachineStats.query().count())

  def testStoreStartSlaveScriptClearCache(self):
    # When a new start slave script is uploaded, we should recalculate the
    # version hash since it will have changed.
    old_version = test_management.SlaveVersion()

    test_management.StoreStartSlaveScript('dummy_script')

    self.assertNotEqual(old_version,
                        test_management.SlaveVersion())

  def testGetUpdateWhenPollingForWork(self):
    # Drop the last character of the version string to ensure a version
    # mismatch.
    version = test_management.SlaveVersion()
    response = test_management.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(version=version[:-1]),
        self._SERVER_URL)

    self.assertTrue('try_count' in response)
    self.assertTrue('commands' in response)
    self.assertTrue('result_url' in response)

    self.assertEqual(
        [
          {
            'args': self._SERVER_URL + 'get_slave_code/' + version,
            'function': 'UpdateSlave',
          },
        ],
        response['commands'])
    self.assertEqual(self._SERVER_URL + 'remote_error', response['result_url'])

  def testSlaveCodeZipped(self):
    zipped_code = test_management.SlaveCodeZipped()

    temp_dir = tempfile.mkdtemp(prefix='swarming')
    try:
      with zipfile.ZipFile(StringIO.StringIO(zipped_code), 'r') as zip_file:
        zip_file.extractall(temp_dir)

      for i in bot_archive.FILES:
        self.assertTrue(os.path.isfile(os.path.join(temp_dir, i)), i)

      # Try running the slave and ensure it can import the required files.
      # (It would crash if it failed to import them).
      subprocess.check_output(
          [sys.executable, os.path.join(temp_dir, 'slave_machine.py'), '-h'])
    finally:
      shutil.rmtree(temp_dir)


if __name__ == '__main__':
  if '-v' in sys.argv:
    logging.basicConfig(level=logging.DEBUG)
  else:
    # We don't want the application logs to interfere with our own messages.
    # You can comment it out for more information when debugging.
    logging.disable(logging.ERROR)

  unittest.main()
