#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests for TestRequestManager class."""




import datetime
import json
import logging
import StringIO
import time
import unittest
import urllib2


from google.appengine.api import files
from google.appengine.api import mail
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import testbed
from common import dimensions_utils
from common import test_request_message
from server import test_manager
from third_party.mox import mox
from test_runner import slave_machine


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']

DEFAULT_RESULT_URL = 'http://all.your.resul.ts/are/belong/to/us'


def CreateBlobstore(blobstore_data):
  """Create a blostore with the desired value and return the key to it.

     This uses the experimental blobstore file api, which can possible have
     issues.

  Args:
    blobstore_data: The data to add to the blobstore.

  Returns:
    The blob key to access the stored blobstore.
  """
  file_name = files.blobstore.create(mime_type='application/octet-stream')

  with files.open(file_name, 'a') as f:
    f.write(blobstore_data)

  files.finalize(file_name)

  return files.blobstore.get_blob_key(file_name)


class TestRequestManagerTest(unittest.TestCase):

  _SERVER_URL = 'http://my.server.com/'

  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    # Create a test manager instance for the tests.
    self._manager = test_manager.TestRequestManager(
        use_blobstore_file_api=False)

    # Mock out LoadFile.
    self._mox.StubOutWithMock(self._manager, '_LoadFile')

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
                         os='win-xp'):
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
      os: The os to require in the test's configuration.

    Returns:
      A properly formatted request message text.
    """
    if not hasattr(self, '_test_request_text'):
      test_request = test_request_message.TestCase()
      test_request.test_case_name = self._request_message_test_case_name
      test_request.tests = [test_request_message.TestObject(
          test_name='t1', action=['ignore-me.exe'])]
      test_request.configurations = [
          test_request_message.TestConfiguration(
              config_name=self._request_message_config_name, os=os,
              cpu='Unknown', data=['http://b.ina.ry/files2.zip'],
              browser='Unknown',
              min_instances=min_instances,
              additional_instances=additional_instances,
              tests=[test_request_message.TestObject(
                  test_name='t2', action=['ignore-me-too.exe'])])]
      if env_vars:
        test_request.env_vars = env_vars.copy()
      test_request.result_url = result_url
      test_request.failure_email = 'john@doe.com'
      test_request.store_result = store_result
      test_request.restart_on_failure = restart_on_failure
      self._test_request_text = test_request_message.Stringize(
          test_request, json_readable=True)
    return self._test_request_text

  def _GetInvalidRequestMessage(self):
    """Return an improperly formatted request message text."""

    return 'this is a bad request.'

  def _GetMachineRegisterRequest(self, machine_id=None, username=None,
                                 password=None, tag=None, try_count=None,
                                 os='win-xp'):
    """Return a properly formatted register machine request.

    Args:
      machine_id: If provided, the id of the machine will be set to this.
      username: If provided, the user_name of the machine will be set to this.
      password: If provided, the password of the machine will be set to this.
      tag: If provided, the tag of the machine will be set to this.
      try_count: If provided, the try_count of the machine will be set to this.
      os: The value of the os to use in the dimensions.

    Returns:
      A dictionary which can be fed into test_manager.ExecuteRegisterRequest().
    """

    config_dimensions = {'os': os, 'cpu': 'Unknown', 'browser': 'Unknown'}
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

    return attributes

  def _SetupSendMailExpectations(self):
    mail.send_mail(sender='Test Request Server <no_reply@google.com>',
                   to='john@doe.com',
                   subject='%s:%s failed.' %
                   (self._request_message_test_case_name,
                    self._request_message_config_name),
                   body=mox.IgnoreArg(),
                   html=mox.IgnoreArg())

  def _SetupLoadFileExpectations(self, contents=None, raise_error=False):
    if raise_error:
      self._manager._LoadFile(
          mox.IgnoreArg()).MultipleTimes().AndRaise(IOError('File not found'))
    else:
      self._manager._LoadFile(
          mox.IgnoreArg()).MultipleTimes().AndReturn(contents)

  def _ExecuteRegister(self, machine_id, try_count=0, os='win-xp',
                       register_should_match=True):
    register_request = self._GetMachineRegisterRequest(machine_id=machine_id,
                                                       try_count=try_count,
                                                       os=os)
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

    # Setup expectations.
    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='script contents')
    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    self._ExecuteRegister(MACHINE_IDS[0])
    runner = test_manager.TestRunner.gql('WHERE machine_id = :1',
                                         MACHINE_IDS[0]).get()
    self.assertNotEqual(None, runner)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertNotEqual(None, runner.started)

    self._mox.VerifyAll()

  # By testing with a large number of configurations for a machine we are
  # unable to use the hashing method to find a match, so ensure we fall back
  # on the old direct comparision method.
  def testRequestGoodMachineWithLargeConfig(self):
    # Setup expectations.
    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='script contents')
    self._mox.ReplayAll()

    large_os_config = map(str, range(  # pylint: disable-msg=C6402
        dimensions_utils.MAX_DIMENSIONS_PER_MACHINE * 2))

    self._manager.ExecuteTestRequest(self._GetRequestMessage(
        os=large_os_config))

    self._ExecuteRegister(MACHINE_IDS[0], os=large_os_config)
    runner = test_manager.TestRunner.gql('WHERE machine_id = :1',
                                         MACHINE_IDS[0]).get()
    self.assertNotEqual(None, runner)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertNotEqual(None, runner.started)

    self._mox.VerifyAll()

  def testGetTestRequestKeys(self):
    self._manager.ExecuteTestRequest(
        self._GetRequestMessage(min_instances=1))

    test_request = test_manager.TestRequest.all().get()
    self.assertNotEqual(None, test_request)
    self.assertEqual(1, len(test_request.GetAllKeys()))

    # Ensure it works with no keys.
    empty_test_request = test_manager.TestRequest(
        name=self._request_message_test_case_name)
    self.assertEqual(0, len(empty_test_request.GetAllKeys()))

  def testGetTestRequestKeysMultipleKeys(self, instances=2):
    self._manager.ExecuteTestRequest(
        self._GetRequestMessage(min_instances=instances))

    test_request = test_manager.TestRequest.all().get()
    self.assertNotEqual(None, test_request)
    self.assertEqual(instances, len(test_request.GetAllKeys()))

  def _AssignPendingRequestsTest(self, instances=1):
    self._manager.ExecuteTestRequest(
        self._GetRequestMessage(min_instances=instances))

    # Setup expectations.
    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='script contents')
    self._mox.ReplayAll()

    # Execute the runners.
    self.assertLessEqual(instances, len(MACHINE_IDS))
    for i in range(instances):
      self._ExecuteRegister(MACHINE_IDS[i])
      runner = test_manager.TestRunner.gql('WHERE machine_id = :1',
                                           MACHINE_IDS[i]).get()
      self.assertNotEqual(None, runner)

    self._mox.VerifyAll()

  def testMultiRunnerWithEnvironmentVariables(self):
    num_indexes = 2

    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='mock_contents')

    self._mox.ReplayAll()

    request_message = self._GetRequestMessage(
        min_instances=num_indexes, env_vars={'index': '%(instance_index)s'})

    self._manager.ExecuteTestRequest(request_message)

    for i in range(num_indexes):
      response = self._ExecuteRegister(MACHINE_IDS[0])

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

    self._mox.VerifyAll()

  def _TestForRestartOnFailurePresence(self, restart_on_failure):
    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='mock_contents')

    self._mox.ReplayAll()

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

    self._mox.VerifyAll()

  def testNoRestartOnFailureByDefault(self):
    self._TestForRestartOnFailurePresence(False)

  def testRestartOnFailurePropagated(self):
    self._TestForRestartOnFailurePresence(True)

  def _AddTestRunWithResultsExpectation(self, result_url, result_string):
    if not self._manager.use_blobstore_file_api:
      # Writing the result to the blobstore.
      blob_key = CreateBlobstore(result_string)
      urllib2.urlopen(mox.IsA(urllib2.Request)).AndReturn(
          StringIO.StringIO(blob_key))

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

    # _Loadfile should be mocked when we have running tests.
    self._SetupLoadFileExpectations(contents='script contents')

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
    for runner in test_manager.TestRunner.all():
      # Get the updated verison of the runner, the current one was
      # cached by the loop and only the key is guaranteed to be the same, so we
      # use it to get a fresh version.
      runner_key = runner.key()
      runner = test_manager.TestRunner.get(runner_key)

      self.assertEqual(store_results_successfully,
                       self._manager.UpdateTestResult(runner, runner.machine_id,
                                                      result_string='results',
                                                      success=success))

      # If results aren't being stored we can't check the runner data because
      # it will have been deleted.
      if store_result == 'none' or (store_result == 'fail' and success):
        continue

      runner = test_manager.TestRunner.get(runner_key)
      self.assertNotEqual(None, runner)
      self.assertEqual(success, runner.ran_successfully)
      self.assertTrue(runner.done)

      # Pretend that the runner sends a second response for this runner.
      # Make sure it does not change.
      self.assertFalse(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                      success=success))

      runner2 = test_manager.TestRunner.get(runner_key)
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
    self.assertEqual(0, test_manager.TestRunner.all().count())

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

    runner = test_manager.TestRunner.all().get()

    # First results, always accepted.
    self.assertTrue(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                   result_string=messages[0]))

    # The first result resent, accepted since the strings are equal.
    self.assertTrue(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                   result_string=messages[0]))

    # Non-first request without overwrite, rejected.
    self.assertFalse(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                    result_string=messages[1],
                                                    overwrite=False))

    # Non-first request with overwrite, accepted.
    self.assertTrue(self._manager.UpdateTestResult(runner, runner.machine_id,
                                                   result_string=messages[2],
                                                   overwrite=True))

    # Make sure that only one blobstore is stored, since only one
    # is referenced.
    self.assertEqual(1, blobstore.BlobInfo.all().count())

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
    runner = test_manager.TestRunner.all().get()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()

    self.assertFalse(self._manager.UpdateTestResult(runner, MACHINE_IDS[1]))
    self._mox.VerifyAll()

  def testFileErrorInResultsOnce(self):
    # Force the uses of the blobstore file api since we are testing it.
    self._manager.SetUseBlobstoreFileApi(True)

    # This must be a class variable because otherwise python gets confused
    # instead of MockCreate and thinks attempt is just a local variable.
    self.attempt = 0
    old_create = files.blobstore.create

    def MockCreate(mime_type):
      if self.attempt < 1:
        self.attempt += 1
        raise files.ApiTemporaryUnavailableError()

      return old_create(mime_type)

    self._SetupHandleTestResults()
    self._mox.StubOutWithMock(files.blobstore, 'create')
    self._mox.StubOutWithMock(time, 'sleep')
    files.blobstore.create = MockCreate
    time.sleep(mox.IgnoreArg())
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True)
    self.assertEqual(1, self.attempt)

    self._mox.VerifyAll()

  def testFileErrorInResultsForever(self):
    # Force the uses of the blobstore file api since we are testing it.
    self._manager.SetUseBlobstoreFileApi(True)

    # Make sure that the code gives up trying to create a blobstore if it fails
    # too often.
    self._SetupHandleTestResults()
    self._mox.StubOutWithMock(files.blobstore, 'create')
    for _ in range(test_manager.MAX_BLOBSTORE_WRITE_TRIES):
      files.blobstore.create(mox.IgnoreArg()).AndRaise(
          files.ApiTemporaryUnavailableError())
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True,
                                  store_results_successfully=False)

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

    self.assertEqual(None, test_manager.TestRunner.all().get())
    self.assertEqual(None, test_manager.TestRequest.all().get())

  def testClearingFailedRunnerAndRequestSucceeded(self):
    self._SetupHandleTestResults()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True, store_result='fail')
    self._mox.VerifyAll()

    self.assertEqual(None, test_manager.TestRunner.all().get())
    self.assertEqual(None, test_manager.TestRequest.all().get())

  def testClearingFailedRunnerAndRequestFailed(self):
    self._SetupHandleTestResults()
    self._SetupSendMailExpectations()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=False, store_result='fail')
    self._mox.VerifyAll()

    self.assertNotEqual(None, test_manager.TestRunner.all().get())
    self.assertNotEqual(None, test_manager.TestRequest.all().get())

  def testGetResults(self):
    self._SetupHandleTestResults()
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True)
    self._mox.VerifyAll()

    test_runner = test_manager.TestRunner.all().get()
    self.assertNotEqual(None, test_runner)

    results = self._manager.GetResults(test_runner)

    self.assertEqual(test_runner.exit_codes, results['exit_codes'])
    self.assertEqual(test_runner.machine_id, results['machine_id'])
    self.assertEqual(test_runner.GetResultString(), results['output'])

    machine = test_manager.MachineAssignment.gql('WHERE machine_id = :1',
                                                 test_runner.machine_id).get()
    self.assertIsNotNone(machine)
    self.assertEqual(machine.tag, results['machine_tag'])

  def _GenerateFutureTimeExpectation(self):
    """Set the current time to way in the future and return it."""
    future_time = (datetime.datetime.now() +
                   datetime.timedelta(
                       seconds=(test_manager._TIMEOUT_FACTOR + 1000)))
    test_manager._GetCurrentTime().AndReturn(future_time)

    return future_time

  def testOnlyAbortStaleRunningRunner(self):
    self._AssignPendingRequestsTest()
    runner = test_manager.TestRunner.all().get()

    # Mark the runner as having pinged so it won't be considered stale and it
    # won't be aborted.
    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')
    runner.ping = self._GenerateFutureTimeExpectation()
    runner.put()
    self._mox.ReplayAll()

    self._manager.AbortStaleRunners()

    runner = test_manager.TestRunner.all().get()
    self.assertFalse(runner.done)
    self.assertEqual(0, runner.automatic_retry_count)

    self._mox.VerifyAll()

  def testAbortStaleRunnerWaitingForMachine(self):
    self._AssignPendingRequestsTest()
    runner = test_manager.TestRunner.all().get()

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
    self._manager.AbortStaleRunners()

    runner = test_manager.TestRunner.all().get()
    self.assertFalse(runner.done)
    runner.started = None
    runner.put()

    # Don't abort the runner if it has been automatically retried, since
    # that means it has been matched with a machine before.
    runner.automatic_retry_count = 1
    runner.put()
    self.assertFalse(runner.done)
    self._manager.AbortStaleRunners()

    runner = test_manager.TestRunner.all().get()
    self.assertFalse(runner.done)
    runner.automatic_retry_count = 0
    runner.put()

    # Now the runner should be aborted, since it hasn't been matched within
    # SWARM_RUNNER_MAX_WAIT_SECS seconds.
    self._manager.AbortStaleRunners()

    runner = test_manager.TestRunner.all().get()
    self.assertTrue(runner.done)
    self.assertIn('Runner was unable to find a machine to run it within',
                  runner.GetResultString())

  def testRetryAndThenAbortStaleRunners(self):
    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')
    attempts_to_reach_abort = test_manager.MAX_AUTOMATIC_RETRIES + 1

    self._SetupLoadFileExpectations(contents='contents')
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

      runner = test_manager.TestRunner.all().get()
      self.assertFalse(runner.done)
      self.assertEqual(i, runner.automatic_retry_count)
      self.assertNotEqual(None, runner.started)

      self._manager.AbortStaleRunners()

      runner = test_manager.TestRunner.all().get()
      if i == test_manager.MAX_AUTOMATIC_RETRIES:
        self.assertTrue(runner.done)
        self.assertNotEqual(None, runner.started)
        self.assertIn('Runner has become stale', runner.GetResultString())
      else:
        self.assertFalse(runner.done)
        self.assertEqual(i + 1, runner.automatic_retry_count)
        self.assertEqual(None, runner.started)

    self._mox.VerifyAll()

  def testSwarmDeleteOldRunners(self):
    self._SetupHandleTestResults()

    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS - 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS + 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True)
    self.assertEqual(1, test_manager.TestRunner.all().count())

    # Make sure that new runners aren't deleted.
    test_manager.DeleteOldRunners()
    self.assertEqual(1, test_manager.TestRunner.all().count())

    # Make sure that old runners are deleted.
    test_manager.DeleteOldRunners()
    self.assertEqual(0, test_manager.TestRunner.all().count())

    self._mox.VerifyAll()

  def testSwarmDeleteOldRunnerStats(self):
    self._SetupHandleTestResults()

    self._mox.StubOutWithMock(test_manager, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.RUNNER_STATS_EVALUATION_CUTOFF_DAYS - 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_manager.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 1))
    test_manager._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    self.ExecuteHandleTestResults(success=True)
    self.assertEqual(1, test_manager.RunnerAssignment.all().count())

    # Make sure that new runner stats aren't deleted.
    test_manager.DeleteOldRunnerStats()
    self.assertEqual(1, test_manager.RunnerAssignment.all().count())

    # Make sure that old runner stats are deleted.
    test_manager.DeleteOldRunnerStats()
    self.assertEqual(0, test_manager.RunnerAssignment.all().count())

    self._mox.VerifyAll()

  def testSwarmErrorDeleteOldErrors(self):
    # Create error.
    error = test_manager.SwarmError(
        name='name', message='msg', info='info')
    error.put()
    self.assertEqual(1, test_manager.SwarmError.all().count())

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
    test_manager.DeleteOldErrors()
    self.assertEqual(1, test_manager.SwarmError.all().count())

    # Second call should remove the now stale error.
    test_manager.DeleteOldErrors()
    self.assertEqual(0, test_manager.SwarmError.all().count())

    self._mox.VerifyAll()

  def testGetMatchingTestRequests(self):
    self._manager.ExecuteTestRequest(self._GetRequestMessage(min_instances=1))

    matches = test_manager.GetAllMatchingTestRequests(
        self._request_message_test_case_name)
    self.assertEqual(1, len(matches))

    # Ensure it works with no matches.
    self.assertNotEqual('unknown', self._request_message_test_case_name)
    matches = test_manager.GetAllMatchingTestRequests('unknown')
    self.assertEqual(0, len(matches))

  def testGetMatchingTestRequestsMultiple(self):
    self._manager.ExecuteTestRequest(self._GetRequestMessage(min_instances=1))
    self._manager.ExecuteTestRequest(self._GetRequestMessage(min_instances=1))

    matches = test_manager.GetAllMatchingTestRequests(
        self._request_message_test_case_name)
    self.assertEqual(2, len(matches))

  def testDeleteRunnerFromKey(self):
    self._manager.ExecuteTestRequest(self._GetRequestMessage(min_instances=1))

    # Make sure the request and the runner are stored.
    self.assertEqual(1, test_manager.TestRunner.all().count())
    self.assertEqual(1, test_manager.TestRequest.all().count())

    # Try deleting with an invalid key and make sure nothing happens.
    self._manager.DeleteRunner(db.Key())
    self.assertEqual(1, test_manager.TestRunner.all().count())
    self.assertEqual(1, test_manager.TestRequest.all().count())

    # Delete the runner by its key.
    key = test_manager.TestRunner.all().get().key()
    self._manager.DeleteRunner(key)

    # Ensure the runner is deleted and that the request is deleted (since it
    # has no remaining runners).
    self.assertEqual(0, test_manager.TestRunner.all().count())
    self.assertEqual(0, test_manager.TestRequest.all().count())

    # Now try deleting the Test Runner again, this should be a noop.
    self._manager.DeleteRunner(key)
    self.assertEqual(0, test_manager.TestRunner.all().count())
    self.assertEqual(0, test_manager.TestRequest.all().count())

  def testGetResultStringFromEmptyRunner(self):
    test_runner = self._CreatePendingRequest()

    # Since the request hasn't been run yet there should be just be an
    # empty string for the result string.
    self.assertEqual('', test_runner.GetResultString())

  def testResultWithUnicode(self):
    # Make sure we can handle results with unicode in them.
    test_runner = self._CreatePendingRequest()

    self._manager.AbortRunner(test_runner, u'\u04bb')

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

  def testAssignMultiplePendingRequestLessDifferentMachines(self):
    # Test when there are 5 test requests then 3 machines register themselves.
    self._AssignPendingRequests(num_tests=5, num_machines=3, different_ids=True)

  def testAssignMultiplePendingRequestLessTests(self):
    # Test when there are 3 test requests then 4 machines register themselves.
    self._AssignPendingRequests(num_tests=3, num_machines=4)

  def testAssignMultiplePendingRequestIOError(self):
    # Test when there are 4 test requests then 2 machines register themselves,
    # but there are IO errors on the server disk.
    self._AssignPendingRequests(num_tests=4, num_machines=2, io_error=True)

  def _AssignPendingRequests(self, num_tests=1, num_machines=1,
                             different_ids=False, io_error=False):

    num_running = min(num_tests, num_machines)

    # If there are running tests, then _Loadfile should be mocked.
    if num_running:
      self._SetupLoadFileExpectations(contents='script contents',
                                      raise_error=io_error)
    self._mox.ReplayAll()

    for _ in range(num_tests):
      self._CreatePendingRequest()

    # Assign different ids to the machines if requested, or have the same
    # machine do all the tests.
    for i in range(num_machines):
      self._ExecuteRegister(
          MACHINE_IDS[i if different_ids else 0],
          register_should_match=(not io_error and i < num_running))

    if io_error:
      # No tests should be assigned in the case of IO errors.
      if different_ids:
        for i in range(num_running):
          self._AssertTestCount(MACHINE_IDS[i], 0)
      else:
        self._AssertTestCount(MACHINE_IDS[0], 0)

      # All tests should still be pending upon errors.
      self._AssertPendingTestCount(num_tests)
    else:
      # No IO errors, so there should be some tests assigned.
      if different_ids:
        for i in range(num_running):
          self._AssertTestCount(MACHINE_IDS[i], 1)
      else:
        self._AssertTestCount(MACHINE_IDS[0], num_running)

      # If there were more tests than machines there should some pending tests.
      self._AssertPendingTestCount(max(0, num_tests - num_machines))

    # No test should be done.
    done_tests = test_manager.TestRunner.gql('WHERE done = :1', True)
    self.assertEqual(0, done_tests.count())

    self._mox.VerifyAll()

  # Asserts exactly 'expected_count' number of tests exist that have machine_id.
  def _AssertTestCount(self, machine_id, expected_count):
    tests = test_manager.TestRunner.gql('WHERE machine_id = :1', machine_id)
    self.assertEqual(expected_count, tests.count())

  # Asserts exactly 'expected_count' number of tests exist and are waiting
  # for a machine.
  def _AssertPendingTestCount(self, expected_count):
    tests = test_manager.TestRunner.gql('WHERE started = :1', None)
    self.assertEqual(expected_count, tests.count())

  def testNoPendingTestsOnRegisterNoTryCount(self):
    # A machine registers itself without an id and there are no tests pending.
    response = self._ExecuteRegister(machine_id=None,
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back', 'id']

    self.assertEqual(response.keys().sort(), expected_keys.sort())
    self.assertEqual(response['try_count'], 1)

    # Make sure the register request doesn't create a TestRunner.
    self.assertEqual(0, test_manager.TestRunner.all().count())

  def testNoPendingTestsOnRegisterWithTryCount(self):
    # A machine registers itself without an id and there are no tests pending
    try_count = 1234
    response = self._ExecuteRegister(machine_id=None, try_count=try_count,
                                     register_should_match=False)

    expected_keys = ['try_count', 'come_back', 'id']

    self.assertEqual(response.keys().sort(), expected_keys.sort())
    self.assertEqual(response['try_count'], try_count+1)

    # Make sure the register request doesn't create a TestRunner.
    self.assertEqual(0, test_manager.TestRunner.all().count())

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
    request = test_manager.TestRequest(
        message=self._GetRequestMessage(),
        name=self._request_message_test_case_name)
    request.put()

    config_name = config_name or self._request_message_config_name
    runner = test_manager.TestRunner(
        test_request=request, config_name=config_name,
        config_hash=request.GetConfigurationDimensionHash(config_name),
        config_instance_index=config_index,
        num_config_instances=num_config_instances)
    if machine_id:
      runner.machine_id = machine_id
      runner.started = datetime.datetime.now()
    runner.put()

    return runner

  # Test with an exception.
  def testAssignRunnerToMachineTxError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise test_manager.TxRunnerAlreadyAssignedError

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[1])

    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with another exception.
  def testAssignRunnerToMachineTimeout(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.Timeout

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[1])

    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with yet another exception.
  def testAssignRunnerToMachineTransactionFailedError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.TransactionFailedError

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[1])

    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Same as above, but the transaction stops throwing exception
  # before the server gives up. So everything should be fine.
  def testAssignRunnerToMachineTransactionTempFailedError(self):
    def _StaticVar(varname, value):
      def _Decorate(func):
        setattr(func, varname, value)
        return func
      return _Decorate

    @_StaticVar('error_count', test_manager.MAX_TRANSACTION_RETRY_COUNT)
    def _RaiseTempError(key, machine_id):  # pylint: disable-msg=W0613
      _RaiseTempError.error_count -= 1
      if _RaiseTempError.error_count:
        raise db.TransactionFailedError
      else:
        return True

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[1])

    self.assertTrue(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseTempError))

  # Test with one more exception.
  def testAssignRunnerToMachineInternalError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.InternalError

    runner = self._CreatePendingRequest(machine_id=MACHINE_IDS[1])

    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test proper behavior of AtomicAssignID.
  def testAtomicAssignID(self):
    runners = []

    # Create some pending runners.
    for _ in range(0, 2):
      runners.append(self._CreatePendingRequest())

    # Make sure it assigns machine_id correctly.
    test_manager.AtomicAssignID(runners[0].key(), MACHINE_IDS[0])
    runner = test_manager.TestRunner.gql(
        'WHERE machine_id = :1', MACHINE_IDS[0]).get()
    self.assertEqual(runner.machine_id, MACHINE_IDS[0])

    # Make sure it didn't touch the other machine.
    self.assertEqual(
        1, test_manager.TestRunner.gql('WHERE started = :1', None).count())

    # Try to reassign runner and raise exception.
    self.assertRaises(test_manager.TxRunnerAlreadyAssignedError,
                      test_manager.AtomicAssignID,
                      runners[0].key(), MACHINE_IDS[1])

  # Test with an exception.
  def testAssignRunnerToMachineFull(self):
    runner = self._CreatePendingRequest()

    # First assignment should work correctly.
    self.assertTrue(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_manager.AtomicAssignID))

    # Next assignment should fail.
    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_manager.AtomicAssignID))

    runner.started = None
    runner.put()
    # This assignment should now work correctly.
    self.assertTrue(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_manager.AtomicAssignID))

  # Test the case where the runner is deleted before the tx is done.
  def testAssignDeletedRunnerToMachine(self):
    runner = self._CreatePendingRequest()
    runner.delete()

    # Assignment should fail without an exception.
    self.assertFalse(self._manager._AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_manager.AtomicAssignID))

  # Make sure file I/O exceptions are propagated.
  def testGetCommandsFileException(self):
    self._SetupLoadFileExpectations(raise_error=True)

    self._mox.ReplayAll()

    runner = self._CreatePendingRequest()

    self.assertRaises(test_manager.PrepareRemoteCommandsError,
                      self._manager._GetTestRunnerCommands,
                      runner, self._SERVER_URL)

    self._mox.VerifyAll()

  def testDeleteMachineAssignments(self):
    # Try to delete with bad keys.
    self.assertFalse(test_manager.DeleteMachineAssignment('bad key'))
    self.assertFalse(test_manager.DeleteMachineAssignment(1))

    # Add and then delete a machine assignment.
    machine_assignment = test_manager.MachineAssignment()
    machine_assignment.put()
    self.assertEqual(1, test_manager.MachineAssignment.all().count())
    self.assertTrue(
        test_manager.DeleteMachineAssignment(machine_assignment.key()))

    # Try and delete the machine assignment again.
    self.assertFalse(
        test_manager.DeleteMachineAssignment(machine_assignment.key()))

  # Ensure that if we have a machine request a test that has the same id
  # as a machine that is supposed to be running a test, we log an error, since
  # it probably means we failed to get the results from the last test.
  def testRequestBeforeResult(self):
    self._mox.StubOutWithMock(logging, 'error')

    self._SetupLoadFileExpectations()
    logging.error(mox.StrContains('unfinished test'), mox.IgnoreArg(),
                  mox.IgnoreArg())
    self._mox.ReplayAll()

    self._manager.ExecuteTestRequest(self._GetRequestMessage())

    register_request = self._GetMachineRegisterRequest()
    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)

    self._mox.VerifyAll()

    # Now mark the test as done, and ensure we don't get the warning.
    test_runner = test_manager.TestRunner.all().get()
    test_runner.done = True
    test_runner.put()

    self._mox.ReplayAll()

    self._manager.ExecuteRegisterRequest(register_request, self._SERVER_URL)
    self._mox.VerifyAll()

  def testRecordMachineRunnerAssignment(self):
    machine_tag = 'tag'
    self.assertEqual(0, test_manager.MachineAssignment.all().count())

    # Assign a runner.
    self._manager._RecordMachineAssignment(MACHINE_IDS[0], machine_tag)
    self.assertEqual(1, test_manager.MachineAssignment.all().count())

    # Assign another runner.
    self._manager._RecordMachineAssignment(MACHINE_IDS[1], machine_tag)
    self.assertEqual(2, test_manager.MachineAssignment.all().count())

  def testRecordMachineRunnerAssignedCorrectlyCalled(self):
    matching_config = 'win-xp'
    request_message = self._GetRequestMessage(os=matching_config)
    self._manager.ExecuteTestRequest(request_message)

    self.assertEqual(0, test_manager.MachineAssignment.all().count())

    # Ensure nothing is added since no runner was matched.
    nonmatching_config = 'win-vista'
    self._manager.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(os=nonmatching_config),
        self._SERVER_URL)
    self.assertEqual(0, test_manager.MachineAssignment.all().count())

    # Ensure the runner match is recorded.
    self._manager.ExecuteRegisterRequest(
        self._GetMachineRegisterRequest(os=matching_config),
        self._SERVER_URL)
    self.assertEqual(1, test_manager.MachineAssignment.all().count())

  def testGetRunnerResults(self):
    # Create a test.
    runner = self._CreatePendingRequest()

    result = {'exit_codes': [0, 1], 'hostname': '0.0.0.0',
              'output': 'test output'}
    self._mox.StubOutWithMock(self._manager, 'GetResults')
    self._manager.GetResults(mox.IgnoreArg()).AndReturn(result)

    self._mox.ReplayAll()

    # Invalid keys.
    self.assertEqual(None, self._manager.GetRunnerResults(None))
    self.assertEqual(None, self._manager.GetRunnerResults('d3d'))
    self.assertEqual(None, self._manager.GetRunnerResults(
        test_manager.TestRequest.all().get().key()))

    # Valid key.
    self.assertNotEqual(None, self._manager.GetRunnerResults(runner.key()))

    self._mox.VerifyAll()

  def testPingRunner(self):
    # Try with a few invalid keys.
    self.assertFalse(test_manager.PingRunner(1, None))
    self.assertFalse(test_manager.PingRunner('2', None))

    # Tests with a valid key
    test_runner = self._CreatePendingRequest()

    # Runner hasn't started.
    self.assertFalse(test_manager.PingRunner(test_runner.key(), None))

    # Runner starts and can get pinged.
    test_runner.started = datetime.datetime.now()
    test_runner.machine_id = MACHINE_IDS[0]
    test_runner.put()
    self.assertTrue(test_manager.PingRunner(test_runner.key(),
                                            MACHINE_IDS[0]))

    # The machine ids don't match so fail.
    self.assertFalse(test_manager.PingRunner(test_runner.key(),
                                             MACHINE_IDS[1]))

    # Runner is done.
    test_runner.done = True
    test_runner.put()
    self.assertFalse(test_manager.PingRunner(test_runner.key(),
                                             MACHINE_IDS[0]))

    # Delete the runner and try to ping.
    test_runner.delete()
    self.assertFalse(test_manager.PingRunner(test_runner.key(),
                                             MACHINE_IDS[0]))

  def testGetAllMachines(self):
    self.assertEqual(0, len(list(test_manager.GetAllMachines())))

    self._manager._RecordMachineAssignment(MACHINE_IDS[0], 'b')
    self._manager._RecordMachineAssignment(MACHINE_IDS[1], 'a')

    # Ensure that the returned values are sorted by tags.
    machines = test_manager.GetAllMachines('tag')
    self.assertEqual(MACHINE_IDS[1], machines.next().machine_id)
    self.assertEqual(MACHINE_IDS[0], machines.next().machine_id)
    self.assertEqual(0, len(list(machines)))

  def testRecordRunnerAssignment(self):
    self._SetupLoadFileExpectations(contents='script contents')
    self._mox.ReplayAll()

    # Create a pending runner.
    self._manager.ExecuteTestRequest(self._GetRequestMessage())
    self.assertEqual(0, test_manager.RunnerAssignment.all().count())

    # Assign the runner and ensure the assignment is marked
    self._ExecuteRegister(MACHINE_IDS[0])
    self.assertEqual(1, test_manager.RunnerAssignment.all().count())

    self._mox.VerifyAll()

  def testGetStatsForOneRunnerAssignment(self):
    self._SetupLoadFileExpectations(contents='script contents')
    self._mox.ReplayAll()

    self.assertEqual({}, test_manager.GetRunnerWaitStats())

    # Create a pending Request that should have no affect on the stats.
    self._manager.ExecuteTestRequest(self._GetRequestMessage())
    self.assertEqual({}, test_manager.GetRunnerWaitStats())

    # Start the runner and ensure the mean, median and max times are now set.
    self._ExecuteRegister(MACHINE_IDS[0])

    runner = test_manager.TestRunner.all().get()
    wait = runner.GetWaitTime()
    expected_waits = {runner.GetDimensionsString(): (wait, wait, wait)}
    self.assertEqual(expected_waits, test_manager.GetRunnerWaitStats())

    self._mox.VerifyAll()

  def testGetStatsForMultipleRunners(self):
    config_dimensions = '{"os": "windows"}'

    median_time = 500
    median_count = 10
    for _ in range(median_count):
      runner_assignment = test_manager.RunnerAssignment(
          dimensions=config_dimensions, wait_time=median_time)
      runner_assignment.put()

    max_time = 1000
    max_count = 5
    for _ in range(max_count):
      runner_assignment = test_manager.RunnerAssignment(
          dimensions=config_dimensions, wait_time=max_time)
      runner_assignment.put()

    mean_wait = ((max_time * max_count + median_time * median_count) /
                 (max_count + median_count))

    expected_waits = {config_dimensions: (mean_wait, median_time, max_time)}
    self.assertEqual(expected_waits, test_manager.GetRunnerWaitStats())

  def testGetTestRunners(self):
    self.assertEqual(0,
                     len(list(test_manager.GetTestRunners(
                         'machine_id', ascending=True, limit=0, offset=0))))

    # Create some test requests.
    test_runner_count = 3
    for i in range(test_runner_count):
      self._CreatePendingRequest(config_index=i,
                                 num_config_instances=test_runner_count)

    # Make sure the results are sorted.
    test_runners = test_manager.GetTestRunners('config_instance_index',
                                               ascending=True,
                                               limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(i, test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))

    # Make sure the results are sorted in descending order.
    test_runners = test_manager.GetTestRunners('config_instance_index',
                                               ascending=False,
                                               limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(test_runner_count - 1 - i,
                       test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
