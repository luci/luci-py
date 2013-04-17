#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests the app engine handlers in main.py."""



import datetime
import json
import os
import unittest


from google.appengine.ext import testbed
from common import blobstore_helper
from common import dimensions_utils
from common import test_request_message
from server import admin_user
from server import main as main_app
from server import test_manager
from server import test_request
from server import test_runner
from server import user_manager
from stats import machine_stats
from stats import runner_stats
from third_party.mox import mox

# A simple machine id constant to use in tests.
MACHINE_ID = '12345678-12345678-12345678-12345678'


class AppTest(unittest.TestCase):
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Some tests require this to be set.
    os.environ['CURRENT_VERSION_ID'] = '1.1'

    self.app = webtest.TestApp(main_app.CreateApplication())

    # Create local instance of test manager and make sure main_app uses it.
    self.test_request_manager = main_app.CreateTestManager()
    main_app.CreateTestManager = (lambda: self.test_request_manager)

    # The default name to use for test requests.
    self._default_test_request_name = 'test name'

    # A basic config hash to use when creating runners.
    self.config_hash = dimensions_utils.GenerateDimensionHash({})

    # Authenticate with none as IP.
    user_manager.AddWhitelist(None)

    # Setup mox handler.
    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()
    self._mox.UnsetStubs()
    self._mox.ResetAll()

  def _GetRequestMessage(self):
    if not hasattr(self, '_test_request_message'):
      test_case = test_request_message.TestCase()
      test_case.test_case_name = self._default_test_request_name
      test_case.tests = [test_request_message.TestObject(
          test_name='t1', action=['ignore-me.exe'])]
      test_case.configurations = [
          test_request_message.TestConfiguration(
              config_name='c1', os='win-xp',
              tests=[test_request_message.TestObject(
                  test_name='t2', action=['ignore-me-too.exe'])])]
      self._test_request_message = test_request_message.Stringize(
          test_case, json_readable=True)

    return self._test_request_message

  def _GetRequest(self):
    return test_request.TestRequest.all().get()

  def _CreateTestRunner(self, machine_id=None, exit_code=None, started=None):
    request = test_request.TestRequest(message=self._GetRequestMessage(),
                                       name=self._default_test_request_name)
    request.put()

    runner = test_runner.TestRunner(request=request,
                                    machine_id=machine_id,
                                    config_hash=self.config_hash,
                                    config_name='c1',
                                    config_instance_index=0,
                                    num_config_instances=1,
                                    exit_code=exit_code, started=started)
    runner.put()

    return runner

  def testMatchingTestCasesHandler(self):
    # Test when no matching tests.
    response = self.app.get('/get_matching_test_cases',
                            {'name': self._default_test_request_name})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('No matching Test Cases' in response.body)

    # Test with a single matching runner.
    runner = self._CreateTestRunner()
    response = self.app.get('/get_matching_test_cases',
                            {'name': self._default_test_request_name})
    self.assertEqual('200 OK', response.status)
    self.assertTrue(str(runner.key()) in response.body)

    # Test with a multiple matching runners.
    additional_test_runner = self._CreateTestRunner()

    # pylint: disable-msg=C6402
    response = self.app.get('/get_matching_test_cases',
                            {'name': self._default_test_request_name})
    self.assertEqual('200 OK', response.status)
    self.assertTrue(str(runner.key()) in response.body)
    self.assertTrue(str(additional_test_runner.key()) in response.body)

  def testGetResultHandler(self):
    handlers = ['/get_result', '/secure/get_result']

    # Test when no matching key
    for handler in handlers:
      response = self.app.get(handler, {'r': 'fake_key'}, status=204)
      self.assertTrue('204' in response.status)

    # Create test and runner.
    runner = self._CreateTestRunner(exit_code=0)

    self._mox.StubOutWithMock(self.test_request_manager, 'GetResults')
    self.test_request_manager.GetResults(mox.IgnoreArg()).MultipleTimes(
        ).AndReturn({'exit_codes': [0, 1], 'hostname': '0.0.0.0',
                     'output': 'test output'})
    self._mox.ReplayAll()

    # Invalid key.
    for handler in handlers:
      response = self.app.get(handler,
                              {'r': self._GetRequest().key()})
      self.assertTrue('204' in response.status)

    # Valid key.
    for handler in handlers:
      response = self.app.get(handler, {'r': runner.key()})
      self.assertEquals('200 OK', response.status)

      try:
        results = json.loads(response.body)
      except (ValueError, TypeError), e:
        self.fail(e)
      self.assertEqual([0, 1], results['exit_codes'])
      self.assertEqual('0.0.0.0', results['hostname'])
      self.assertEqual('test output', results['output'])

    self._mox.VerifyAll()

  def testMachineList(self):
    self._mox.StubOutWithMock(main_app.template, 'render')
    main_app.template.render(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn('')
    self._mox.ReplayAll()

    # Add a machine to display.
    machine_stats.MachineStats(machine_id='id', tag='tag')

    response = self.app.get('/secure/machine_list')
    self.assertTrue('200' in response.status)

    self._mox.VerifyAll()

  def testDeleteMachineStats(self):
    # Add a machine assignment to delete.
    m_stats = machine_stats.MachineStats()
    m_stats.put()

    # Delete the machine assignment.
    response = self.app.post('/secure/delete_machine_stats?r=%s' %
                             m_stats.key())
    self.assertTrue('200' in response.status)

    # Attempt to delete the assignment again and fail.
    response = self.app.post('/secure/delete_machine_stats?r=%s' %
                             m_stats.key())
    self.assertTrue('204' in response.status)

  def testMainHandler(self):
    self._mox.StubOutWithMock(main_app.template, 'render')
    main_app.template.render(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn('')
    self._mox.ReplayAll()

    # Add a test runner to show on the page.
    self._CreateTestRunner()

    response = self.app.get('/secure/main')
    self.assertTrue('200' in response.status)

    self._mox.VerifyAll()

  def testCleanupResultsHandler(self):
    # Try to clean up with an invalid key.
    response = self.app.post('/cleanup_results', {'r': 'fake_key'})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('Key deletion failed.' in response.body)

    runner = self._CreateTestRunner()

    # Try to clean up with valid key but belonging to other class.
    response = self.app.post('/cleanup_results',
                             {'r': self._GetRequest().key()})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('Key deletion failed.' in response.body)

    # Try to clean up with a valid key.
    response = self.app.post('/cleanup_results', {'r': runner.key()})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('Key deleted.' in response.body)

  def testRetryHandler(self):
    # Test when no matching key
    response = self.app.post('/secure/retry', {'r': 'fake_key'}, status=204)
    self.assertTrue('204' in response.status)

    # Test with matching key.
    runner = self._CreateTestRunner(exit_code=0)

    response = self.app.post('/secure/retry', {'r': runner.key()})
    self.assertEquals('200 OK', response.status)
    self.assertTrue('Runner set for retry' in response.body)

  def testRegisterHandler(self):
    # Missing attributes field.
    response = self.app.post('/poll_for_test', {'something': 'nothing'})
    self.assertEquals('200 OK', response.status)
    self.assertEquals('Error: Invalid attributes: ', response.body)

    # Invalid attributes field.
    response = self.app.post('/poll_for_test', {'attributes': 'nothing'})
    self.assertEquals('200 OK', response.status)
    self.assertEquals('Error: Invalid attributes: nothing', response.body)

    # Invalid empty attributes field.
    response = self.app.post('/poll_for_test', {'attributes': None})
    self.assertEquals('200 OK', response.status)
    self.assertEquals('Error: Invalid attributes: None', response.body)

    # Valid attributes but missing dimensions.
    response = self.app.post('/poll_for_test', {'attributes': '{}'})
    self.assertEquals('200 OK', response.status)
    self.assertEquals('Error: Missing mandatory attribute: dimensions',
                      response.body)

    # Valid attributes.
    attributes = ('{"dimensions": {"os": ["win-xp"]},'
                  '"id": "%s"}' % MACHINE_ID)
    response = self.app.post('/poll_for_test', {'attributes': attributes})
    self.assertEquals('200 OK', response.status)
    response = json.loads(response.body)
    self.assertEquals(sorted(['try_count', 'id', 'come_back']),
                      sorted(response.keys()))
    self.assertEquals(MACHINE_ID, response['id'])

  def _PostResults(self, runner, result, expect_errors=False):
    url_parameters = {
        'r': runner.key(),
        'id': runner.machine_id,
        's': True,
        'result_output': result,
        }
    return self.app.post('/result', url_parameters, expect_errors=expect_errors)

  def testResultHandler(self):
    runner = self._CreateTestRunner(machine_id=MACHINE_ID,
                                    started=datetime.datetime.now())

    result = 'result string'
    response = self._PostResults(runner, result)
    self.assertEquals('200 OK', response.status)

    # Get the lastest version of the runner and ensure it has the correct
    # values.
    runner = test_runner.TestRunner.all().get()
    self.assertTrue(runner.ran_successfully)
    self.assertEqual(result, runner.GetResultString())

  def testResultHandlerBlobstoreFailure(self):
    self._mox.StubOutWithMock(blobstore_helper, 'CreateBlobstore')
    blobstore_helper.CreateBlobstore(mox.IgnoreArg()).AndReturn(None)
    self._mox.ReplayAll()

    runner = self._CreateTestRunner(machine_id=MACHINE_ID,
                                    started=datetime.datetime.now())

    result = 'result string'
    response = self._PostResults(runner, result, expect_errors=True)
    self.assertEquals('500 Internal Server Error', response.status)
    self.assertEquals(
        'The server was unable to save the results to the blobstore',
        response.body)

    # Get the lastest version of the runner and ensure it hasn't been marked as
    # done.
    runner = test_runner.TestRunner.all().get()
    self.assertFalse(runner.done)

    self._mox.VerifyAll()

  def testChangeWhitelistHandlerParams(self):
    # Make sure the link redirects to the right place.
    response = self.app.post('/secure/change_whitelist', {})
    self.assertEquals('301 Moved Permanently', response.status)
    self.assertEquals(
        'http://localhost/secure/user_profile', response.location)

    # All of these requests are invalid so none of them should make a call
    # to ModifyUserProfileWhitelist.
    self.app.post('/secure/change_whitelist', {'i': ''})
    self.app.post('/secure/change_whitelist', {'i': '123'})
    self.app.post('/secure/change_whitelist', {'p': 'password'})
    self.app.post('/secure/change_whitelist', {'i': '123', 'a': 'true'})

  def testChangeWhitelistHandler(self):
    ip = ['123', '456']
    password = [None, 'pa$$w0rd']

    user_manager.DeleteWhitelist(None)
    self.assertEqual(0, user_manager.MachineWhitelist.all().count())

    # Whitelist an ip.
    self.app.post('/secure/change_whitelist', {'i': ip[0], 'a': 'True'})

    self.assertEqual(1, user_manager.MachineWhitelist.all().count())

    # Whitelist an ip with a password.
    self.app.post(
        '/secure/change_whitelist', {'i': ip[1], 'a': 'True', 'p': password[1]})
    self.assertEqual(2, user_manager.MachineWhitelist.all().count())

    # Make sure ips & passwords are sorted correctly.
    for i in range(2):
      whitelist = user_manager.MachineWhitelist.gql(
          'WHERE ip = :1 AND password = :2', ip[i], password[i])
      self.assertEqual(1, whitelist.count(), msg='Iteration %d' % i)

    # Remove whitelisted ip.
    self.app.post('/secure/change_whitelist', {'i': ip[0], 'a': 'False'})
    self.assertEqual(1, user_manager.MachineWhitelist.all().count())

    # Make sure whitelists are removed based on IP and not password.
    self.app.post(
        '/secure/change_whitelist', {'i': ip[1], 'a': 'False', 'p': 'Invalid'})
    self.assertEqual(0, user_manager.MachineWhitelist.all().count())

  # Test non-secure hanlders to make sure they check the remote machine to be
  # whitelisted before allowing them to perform the task.
  def testUnsecureHandlerAuthentication(self):
    password = '4321'

    # List of non-secure handlers and their method.
    handlers = [('/cleanup_results', self.app.post),
                ('/get_matching_test_cases', self.app.get),
                ('/get_result', self.app.get),
                ('/poll_for_test', self.app.post),
                ('/remote_error', self.app.post),
                ('/result', self.app.post),
                ('/test', self.app.post)]

    # Make sure non-whitelisted requests are rejected.
    user_manager.DeleteWhitelist(None)
    for handler, method in handlers:
      response = method(handler, {}, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Whitelist a machine.
    user_manager.AddWhitelist(None, password=password)

    # Make sure whitelisted requests are accepted.
    for handler, method in handlers:
      response = method(handler, {'password': password}, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Make sure invalid passwords are rejected.
    for handler, method in handlers:
      response = method(
          handler, {'password': 'something else'}, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

  def testRemoteErrorHandler(self):
    self.assertEqual(0, test_manager.SwarmError.all().count())

    error_message = 'error message'

    response = self.app.post('/remote_error', {'m': error_message})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('Error logged' in response.body)

    self.assertEqual(1, test_manager.SwarmError.all().count())
    error = test_manager.SwarmError.all().get()
    self.assertEqual(error.message, error_message)

  def testRunnerPing(self):
    # Try with an invalid runner key
    response = self.app.post('/runner_ping', {'r': '1'}, expect_errors=True)
    self.assertTrue('402' in response.status)
    self.assertEqual('Runner failed to ping.', response.body)

    # Start a test and successfully ping it
    runner = self._CreateTestRunner(machine_id=MACHINE_ID,
                                    started=datetime.datetime.now())
    response = self.app.post('/runner_ping', {'r': runner.key(),
                                              'id': runner.machine_id})
    self.assertEqual('200 OK', response.status)
    self.assertEqual('Runner successfully pinged.', response.body)

  def testCleanupData(self):
    # All cron job requests must be gets.
    response = self.app.get('/tasks/cleanup_data',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertEqual('200 OK', response.status)

  def testAbortStaleRunners(self):
    # All cron job requests must be gets.
    response = self.app.get('/tasks/abort_stale_runners',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertEqual('200 OK', response.status)

  def testSendEReporter(self):
    # Ensure this function correctly complains if the admin email isn't set.
    response = self.app.get('/tasks/sendereporter', expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Invalid admin email, \'\'. Must be a valid email.',
                     response.body)

    # Ensure this function complains when a garbage email is set.
    admin = admin_user.AdminUser.all().get()
    admin.email = None
    admin.put()
    response = self.app.get('/tasks/sendereporter', expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Invalid admin email, \'None\'. Must be a valid email.',
                     response.body)

    # Ensure this function works with a valid admin email.
    admin.email = 'admin@app.com'
    admin.put()

    response = self.app.get('/tasks/sendereporter')
    self.assertTrue('200 OK' in response.status)

  def testStatsHandler(self):
    self._mox.StubOutWithMock(main_app.template, 'render')
    main_app.template.render(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn('')
    self._mox.ReplayAll()

    # Ensure there are some runner stats.
    runner = self._CreateTestRunner()
    runner_stats.RecordRunnerStats(runner)

    response = self.app.get('/secure/stats')
    self.assertTrue('200' in response.status)


if __name__ == '__main__':
  unittest.main()
