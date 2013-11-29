#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import unittest

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env

test_env.setup_test_env()

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

import handlers
from common import dimensions_utils
from common import swarm_constants
from common import url_helper
from server import admin_user
from server import dimension_mapping
from server import test_helper
from server import test_management
from server import test_request
from server import test_runner
from server import user_manager
from stats import daily_stats
from stats import machine_stats
from stats import runner_stats
from stats import runner_summary
from third_party import webtest
from third_party.mox import mox


# A simple machine id constant to use in tests.
MACHINE_ID = '12345678-12345678-12345678-12345678'


class AppTest(unittest.TestCase):
  def setUp(self):
    super(AppTest, self).setUp()
    handlers.ALLOW_ACCESS_FROM_DOMAINS = ('example.com',)

    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Ensure the test can find queue.yaml.
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
    self.taskqueue_stub._root_path = APP_DIR

    self.app = webtest.TestApp(handlers.CreateApplication())

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
    super(AppTest, self).tearDown()

  def _GetRoutes(self):
    """Returns the list of all routes handled."""
    return [
        r.template for r in self.app.app.router.match_routes
    ]

  def _GetRequest(self):
    return test_request.TestRequest.query().get()

  def _ReplaceCurrentUser(self, email):
    if email:
      self.testbed.setup_env(USER_EMAIL=email, overwrite=True)
    else:
      self.testbed.setup_env(overwrite=True)

  def assertResponse(self, response, status, body):
    self.assertEqual(status, response.status, response)
    self.assertEqual(body, response.body, repr(response.body))

  def testMatchingTestCasesHandler(self):
    # Ensure that matching works even when the datastore is not being
    # consistent.
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

    # Mock out all the authenication, since it doesn't work with the server
    # being only eventually consisten (but it is ok for the machines to take
    # a while to appear).
    self._mox.StubOutWithMock(handlers, 'IsAuthenticatedMachine')
    for _ in range(3):
      handlers.IsAuthenticatedMachine(mox.IgnoreArg()).AndReturn(True)
    self._mox.ReplayAll()

    # Test when no matching tests.
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME},
        expect_errors=True)
    self.assertResponse(response, '404 Not Found', '[]')

    # Test with a single matching runner.
    runner = test_helper.CreatePendingRunner()
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME})
    self.assertEqual('200 OK', response.status)
    self.assertTrue(str(runner.key.urlsafe()) in response.body, response.body)

    # Test with a multiple matching runners.
    additional_test_runner = test_helper.CreatePendingRunner()

    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME})
    self.assertEqual('200 OK', response.status)
    self.assertTrue(str(runner.key.urlsafe()) in response.body, response.body)
    self.assertTrue(str(additional_test_runner.key.urlsafe()) in response.body,
                    response.body)

    self._mox.VerifyAll()

  def testGetResultHandler(self):
    handler_urls = ['/get_result', '/secure/get_result']

    # Test when no matching key
    for handler in handler_urls:
      response = self.app.get(handler, {'r': 'fake_key'}, status=204)
      self.assertTrue('204' in response.status)

    # Create test and runner.
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID,
                                             exit_codes='[0]')

    # Invalid key.
    for handler in handler_urls:
      response = self.app.get(handler,
                              {'r': self._GetRequest().key.urlsafe()})
      self.assertTrue('204' in response.status)

    # Valid key.
    for handler in handler_urls:
      response = self.app.get(handler, {'r': runner.key.urlsafe()})
      self.assertEqual('200 OK', response.status)

      try:
        results = json.loads(response.body)
      except (ValueError, TypeError), e:
        self.fail(e)
      self.assertEqual(runner.exit_codes, results['exit_codes'])
      self.assertEqual(runner.machine_id, results['machine_id'])
      self.assertEqual(runner.GetResultString(), results['output'])

  def testGetToken(self):
    response = self.app.get('/get_token')
    self.assertResponse(response, '200 OK', 'dummy_token')

  def testGetSlaveCode(self):
    response = self.app.get('/get_slave_code')
    self.assertEqual('200 OK', response.status)

  def testMachineList(self):
    self._mox.ReplayAll()

    # Add a machine to display.
    machine_stats.MachineStats.get_or_insert(MACHINE_ID, tag='tag')

    response = self.app.get('/secure/machine_list')
    self.assertTrue('200' in response.status)

    self._mox.VerifyAll()

  def testDeleteMachineStats(self):
    # Add a machine assignment to delete.
    m_stats = machine_stats.MachineStats.get_or_insert(MACHINE_ID)

    # Delete the machine assignment.
    response = self.app.post('/delete_machine_stats?r=%s' %
                             m_stats.key.string_id())
    self.assertResponse(response, '200 OK', 'Machine Assignment removed.')

    # Attempt to delete the assignment again and fail.
    response = self.app.post('/delete_machine_stats?r=%s' %
                             m_stats.key.string_id())
    self.assertResponse(response, '204 No Content', '')

  def testMainHandler(self):
    self._mox.ReplayAll()

    # Add a test runner to show on the page.
    test_helper.CreatePendingRunner()

    response = self.app.get('/secure/main')
    self.assertEqual('200 OK', response.status)

    self._mox.VerifyAll()

  def testCleanupResultsHandler(self):
    # Try to clean up with an invalid key.
    response = self.app.post('/cleanup_results', {'r': 'fake_key'})
    self.assertResponse(response, '200 OK', 'Key deletion failed.')

    # Try to clean up with a valid key.
    runner = test_helper.CreatePendingRunner()
    response = self.app.post('/cleanup_results', {'r': runner.key.urlsafe()})
    self.assertResponse(response, '200 OK', 'Key deleted.')

  def testRetryHandler(self):
    # Test when no matching key
    response = self.app.post('/secure/retry', {'r': 'fake_key'}, status=204)
    self.assertResponse(response, '204 No Content', '')

    # Test with matching key.
    runner = test_helper.CreatePendingRunner(exit_codes='[0]')

    response = self.app.post('/secure/retry', {'r': runner.key.urlsafe()})
    self.assertResponse(response, '200 OK', 'Runner set for retry.')

  def testShowMessageHandler(self):
    response = self.app.get('/secure/show_message', {'r': 'fake_key'})
    self.assertEqual('200 OK', response.status)
    self.assertTrue(
        response.body.startswith('Cannot find message'), response.body)

    runner = test_helper.CreatePendingRunner()
    response = self.app.get('/secure/show_message',
                            {'r': runner.key.urlsafe()})
    expected = (
        'Test Request Message:\n'
        '{"admin": false,"cleanup": null,"configurations": ['
        '{"additional_instances": 0,"config_name": "c1","data": '
        '["http://b.ina.ry/files2.zip"],"dimensions": {"browser": "Unknown", '
        '"cpu": "Unknown", "os": "win-xp"},"env_vars": null,"min_instances": 1,'
        '"priority": 10,"tests": [{"action": ["ignore-me-too.exe"],'
        '"decorate_output": true,"env_vars": null,"hard_time_out": 3600.0,'
        '"io_time_out": 1200.0,"test_name": "t2"}]}],"data": [],"encoding": '
        '"ascii","env_vars": null,"failure_email": "john@doe.com","label": '
        'null,"output_destination": null,"restart_on_failure": false,'
        '"result_url": "http://all.your.resul.ts/are/belong/to/us",'
        '"store_result": "all","test_case_name": "tc","tests": [{"action": '
        '["ignore-me.exe"],"decorate_output": true,"env_vars": null,'
        '"hard_time_out": 3600.0,"io_time_out": 1200.0,"test_name": "t1"}],'
        '"verbose": false,"working_dir": "c:\\\\swarm_tests"}'
        '\n\nTest Runner Message:\nConfiguration Name: c1\nConfiguration '
        'Instance Index: 0 / 1')
    self.assertResponse(response, '200 OK', expected)

  def testUploadStartSlaveHandler(self):
    response = self.app.post('/secure/upload_start_slave', expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ')

    response = self.app.post(
        '/secure/upload_start_slave',
        upload_files=[('script', 'script', 'script_body')])
    self.assertResponse(response, '200 OK', '')

  def testRegisterHandler(self):
    # Missing attributes field.
    response = self.app.post('/poll_for_test', {'something': 'nothing'})
    self.assertResponse(
        response, '200 OK',
        'Error: Invalid attributes: : No JSON object could be decoded')

    # Invalid attributes field.
    response = self.app.post('/poll_for_test', {'attributes': 'nothing'})
    self.assertResponse(
        response, '200 OK',
        'Error: Invalid attributes: nothing: No JSON object could be decoded')

    # Invalid empty attributes field.
    response = self.app.post('/poll_for_test', {'attributes': None})
    self.assertResponse(
        response, '200 OK',
        'Error: Invalid attributes: None: No JSON object could be decoded')

    # Valid attributes but missing dimensions.
    attributes = '{"id": "%s"}' % MACHINE_ID
    response = self.app.post('/poll_for_test', {'attributes': attributes})
    self.assertResponse(
        response, '200 OK',
        'Error: Missing mandatory attribute: dimensions')

    # Valid attributes.
    attributes = ('{"dimensions": {"os": ["win-xp"]},'
                  '"id": "%s"}' % MACHINE_ID)
    response = self.app.post('/poll_for_test', {'attributes': attributes})
    self.assertEqual('200 OK', response.status)
    response = json.loads(response.body)
    self.assertEqual(sorted(['try_count', 'come_back']),
                     sorted(response.keys()))

  def _PostResults(self, runner_key, machine_id, result, expect_errors=False):
    url_parameters = {
        'r': runner_key,
        'id': machine_id,
        's': True,
        }
    files = [(swarm_constants.RESULT_STRING_KEY,
              swarm_constants.RESULT_STRING_KEY,
              result)]
    return self.app.post('/result', url_parameters, upload_files=files,
                         expect_errors=expect_errors)

  def testResultHandler(self):
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    url_helper.UrlOpen(
        test_helper.DEFAULT_RESULT_URL, data=mox.IgnoreArg()).AndReturn(
            'response')
    self._mox.ReplayAll()

    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID)

    result = 'result string'
    response = self._PostResults(runner.key.urlsafe(), runner.machine_id,
                                 result)
    self.assertResponse(
        response, '200 OK', 'Successfully update the runner results.')

    # Get the lastest version of the runner and ensure it has the correct
    # values.
    runner = test_runner.TestRunner.query().get()
    self.assertTrue(runner.ran_successfully)
    self.assertEqual(result, runner.GetResultString())

    # Delete the runner and try posting the results again. This can happen
    # if two machines are running the same test (due to flaky connections),
    # and the results were then deleted before the second machine returned.
    runner.key.delete()
    response = self._PostResults(runner.key.urlsafe(), runner.machine_id,
                                 result)
    self.assertEqual('200 OK', response.status)
    self.assertTrue(
        response.body.startswith('The runner, with key '), response.body)

    self._mox.VerifyAll()

  def testUserProfile(self):
    # Make sure the template renders.
    response = self.app.get('/secure/user_profile', {})
    self.assertEqual('200 OK', response.status)

  def testChangeWhitelistHandlerParams(self):
    # Make sure the link redirects to the right place.
    response = self.app.post('/secure/change_whitelist', {})
    self.assertEqual('301 Moved Permanently', response.status)
    self.assertEqual(
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
    self.assertEqual(0, user_manager.MachineWhitelist.query().count())

    # Whitelist an ip.
    self.app.post('/secure/change_whitelist', {'i': ip[0], 'a': 'True'})

    self.assertEqual(1, user_manager.MachineWhitelist.query().count())

    # Whitelist an ip with a password.
    self.app.post(
        '/secure/change_whitelist', {'i': ip[1], 'a': 'True', 'p': password[1]})
    self.assertEqual(2, user_manager.MachineWhitelist.query().count())

    # Make sure ips & passwords are sorted correctly.
    for i in range(2):
      whitelist = user_manager.MachineWhitelist.query().filter(
          user_manager.MachineWhitelist.ip == ip[i]).filter(
              user_manager.MachineWhitelist.password == password[i])
      self.assertEqual(1, whitelist.count(), msg='Iteration %d' % i)

    # Remove whitelisted ip.
    self.app.post('/secure/change_whitelist', {'i': ip[0], 'a': 'False'})
    self.assertEqual(1, user_manager.MachineWhitelist.query().count())

    # Make sure whitelists are removed based on IP and not password.
    self.app.post(
        '/secure/change_whitelist', {'i': ip[1], 'a': 'False', 'p': 'Invalid'})
    self.assertEqual(0, user_manager.MachineWhitelist.query().count())

  def testUnsecureHandlerMachineAuthentication(self):
    # Test non-secure handlers to make sure they check the remote machine to be
    # whitelisted before allowing them to perform the task.
    password = '4321'

    # List of non-secure handlers and their method.
    handlers_to_check = [
        ('/cleanup_results', self.app.post),
        ('/get_matching_test_cases', self.app.get),
        ('/get_result', self.app.get),
        ('/get_slave_code', self.app.get),
        ('/poll_for_test', self.app.post),
        ('/remote_error', self.app.post),
        ('/result', self.app.post),
        ('/test', self.app.post),
    ]

    # Make sure non-whitelisted requests are rejected.
    user_manager.DeleteWhitelist(None)
    for handler, method in handlers_to_check:
      response = method(handler, {}, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          'Remote machine not whitelisted for operation')

    # Whitelist a machine.
    user_manager.AddWhitelist(None, password=password)

    # Make sure whitelisted requests are accepted.
    for handler, method in handlers_to_check:
      response = method(handler, {'password': password}, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Make sure invalid passwords are rejected.
    for handler, method in handlers_to_check:
      response = method(
          handler, {'password': 'something else'}, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          'Remote machine not whitelisted for operation')

  # Test that some specific non-secure handlers allow access to authenticated
  # user. Also verify that authenticated user still can't use other handlers.
  def testUnsecureHandlerUserAuthentication(self):
    # List of non-secure handlers and their method that can be called by user.
    allowed = [('/get_matching_test_cases', self.app.get),
               ('/get_result', self.app.get),
               ('/test', self.app.post)]

    # List of non-secure handlers that should not be accessible to the user.
    forbidden = [('/cleanup_results', self.app.post),
                 ('/get_slave_code', self.app.get),
                 ('/poll_for_test', self.app.post),
                 ('/remote_error', self.app.post),
                 ('/result', self.app.post)]

    # Reset state to non-whitelisted, anonymous machine.
    user_manager.DeleteWhitelist(None)
    self._ReplaceCurrentUser(None)

    # Make sure all anonymous requests are rejected.
    for handler, method in allowed + forbidden:
      response = method(handler, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          'Remote machine not whitelisted for operation')

    # Make sure all requests from unknown account are rejected.
    self._ReplaceCurrentUser('someone@denied.com')
    for handler, method in allowed + forbidden:
      response = method(handler, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          'Remote machine not whitelisted for operation')

    # Make sure for a known account 'allowed' methods are accessible
    # and 'forbidden' are not.
    self._ReplaceCurrentUser('someone@example.com')
    for handler, method in allowed:
      response = method(handler, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)
    for handler, method in forbidden:
      response = method(handler, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          'Remote machine not whitelisted for operation')

  # Test that all handlers are accessible only to authenticated user or machine.
  # Assumes all routes are defined with plain paths
  # (i.e. '/some/handler/path' and not regexps).
  def testAllHandlersAreSecured(self):
    # URL prefixes that correspond to 'login: admin' areas in app.yaml.
    # Handlers that correspond to this prefixes are protected by GAE itself.
    secured_paths = ['/secure/', '/_ereporter']

    # Handlers that are explicitly allowed to be called by anyone.
    allowed_urls = set([
        '/',
        '/graphs/daily_stats',
        '/runner_summary',
        '/server_ping',
        '/stats',
        '/waits_by_minute',
    ])

    # Grab the set of all routes.
    app = self.app.app
    routes = set(app.router.match_routes)
    routes.update(app.router.build_routes.itervalues())

    # Get all routes that are not protected by GAE auth mechanism.
    unprotected = []
    for route in routes:
      if route.template in allowed_urls:
        continue
      for path in secured_paths:
        if route.template.startswith(path):
          break
      else:
        unprotected.append(route)

    # Helper function that executes GET or POST handler for corresponding route
    # and asserts it returns 403 or 405.
    def CheckProtected(route, method):
      assert method in ('GET', 'POST')
      # Get back original path from regexp.
      path = route.template
      if path[0] == '^':
        path = path[1:]
      if path[-1] == '$':
        path = path[:-1]

      response = getattr(self.app, method.lower())(path, expect_errors=True)
      message = ('%s handler is not protected: %s, '
                 'returned %s' % (method, path, response))
      self.assertIn(response.status_int, (403, 405), msg=message)

    # Reset state to non-whitelisted, anonymous machine.
    user_manager.DeleteWhitelist(None)
    self._ReplaceCurrentUser(None)
    # Try to execute 'get' and 'post' and verify they fail with 403 or 405.
    for route in unprotected:
      CheckProtected(route, 'GET')
      CheckProtected(route, 'POST')

  def testRemoteErrorHandler(self):
    self.assertEqual(0, test_management.SwarmError.query().count())

    error_message = 'error message'

    response = self.app.post('/remote_error', {'m': error_message})
    self.assertResponse(response, '200 OK', 'Error logged')

    self.assertEqual(1, test_management.SwarmError.query().count())
    error = test_management.SwarmError.query().get()
    self.assertEqual(error.message, error_message)

  def testRunnerPing(self):
    # Try with an invalid runner key
    response = self.app.post('/runner_ping', {'r': '1'}, expect_errors=True)
    self.assertResponse(
        response, '402 Payment Required', 'Runner failed to ping.')

    # Start a test and successfully ping it
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID)
    response = self.app.post('/runner_ping', {'r': runner.key.urlsafe(),
                                              'id': runner.machine_id})
    self.assertResponse(response, '200 OK', 'Runner successfully pinged.')

  def testStatPages(self):
    stat_urls = ['/graphs/daily_stats',
                 '/runner_summary',
                 '/stats',
                 '/waits_by_minute',
                ]

    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    url_helper.UrlOpen(
        test_helper.DEFAULT_RESULT_URL, data=mox.IgnoreArg()).AndReturn(
            'response')
    self._mox.ReplayAll()

    # Create a pending, active and done runner.
    test_helper.CreatePendingRunner()
    test_helper.CreatePendingRunner(machine_id=MACHINE_ID)
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID)
    runner.UpdateTestResult(machine_id=MACHINE_ID)

    # Ensure the dimension mapping is created.
    dimension_mapping.DimensionMapping(dimensions=runner.dimensions).put()

    # Ensure a RunnerSummary is created.
    runner_summary.GenerateSnapshotSummary()

    # Ensure wait stats are generated.
    runner_summary.GenerateWaitSummary()

    # Add some basic stats items to ensure the loop bodies are executed.
    runner = test_helper.CreatePendingRunner()
    runner_stats.RecordRunnerStats(runner)
    daily_stats.DailyStats(date=datetime.datetime.utcnow().date()).put()

    for stat_url in stat_urls:
      response = self.app.get(stat_url)
      self.assertEqual('200 OK', response.status)

    self._mox.VerifyAll()

  def testCronTriggerTask(self):
    triggers = [
        ('cleanup', '/secure/cron/trigger_cleanup_data'),
        ('stats', '/secure/cron/trigger_generate_daily_stats'),
        ('stats', '/secure/cron/trigger_generate_recent_stats'),
    ]

    for i, (task_name, url) in enumerate(triggers):
      response = self.app.get(url, headers={'X-AppEngine-Cron': 'true'})
      self.assertResponse(response, '200 OK', 'Success.')
      tasks = self.taskqueue_stub.get_filtered_tasks()
      self.assertEqual(i + 1, len(tasks))
      task = tasks[-1]
      self.assertEqual('POST', task.method)
      # I'm not sure why but task.queue_name is always None
      self.assertEqual(task_name, task.headers['X-AppEngine-QueueName'])

  def testTaskQueueUrls(self):
    # Tests all the cron tasks are securely handled.
    task_queue_urls = [
        r for r in self._GetRoutes() if r.startswith('/secure/task_queues/')
    ]
    task_queues = [
        ('cleanup', '/secure/task_queues/cleanup_data'),
        ('stats', '/secure/task_queues/generate_daily_stats'),
        ('stats', '/secure/task_queues/generate_recent_stats'),
    ]
    self.assertEqual(len(task_queues), len(task_queue_urls), task_queue_urls)

    for task_name, url in task_queues:
      response = self.app.post(url,
                               headers={'X-AppEngine-QueueName': task_name})
      self.assertResponse(response, '200 OK', 'Success.')
      self.assertEqual([], self.taskqueue_stub.get_filtered_tasks())

  def testCronJobTasks(self):
    # Tests all the cron tasks are securely handled.
    cron_job_urls = [
        r for r in self._GetRoutes() if r.startswith('/secure/cron/')
    ]
    self.assertTrue(cron_job_urls, cron_job_urls)

    # For ereporter.
    admin_user.AdminUser(email='admin@denied.com').put()
    for cron_job_url in cron_job_urls:
      response = self.app.get(cron_job_url,
                              headers={'X-AppEngine-Cron': 'true'})
      self.assertResponse(response, '200 OK', 'Success.')

      # Only cron job requests can be gets for this handler.
      response = self.app.get(cron_job_url, expect_errors=True)
      self.assertResponse(
          response, '405 Method Not Allowed',
          'Only internal cron jobs can do this')

  def testDetectHangingRunners(self):
    response = self.app.get('/secure/cron/detect_hanging_runners',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

    # Test when there is a hanging runner.
    runner = test_helper.CreatePendingRunner()
    runner.created = datetime.datetime.utcnow() - datetime.timedelta(
        minutes=2 * test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)
    runner.put()

    response = self.app.get('/secure/cron/detect_hanging_runners',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

  def testSendEReporter_NoAdmin(self):
    # Ensure this function correctly complains if the admin email isn't set.
    response = self.app.get('/secure/cron/sendereporter', expect_errors=True,
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(
        response, '400 Bad Request',
        'Invalid admin email, \'\'. Must be a valid email.')

  def testSendEReporter_GarbageAdmin(self):
    # Ensure this function complains when a garbage email is set.
    admin_user.AdminUser().put()
    response = self.app.get('/secure/cron/sendereporter', expect_errors=True,
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(
        response, '400 Bad Request',
        'Invalid admin email, \'None\'. Must be a valid email.')

  def testSendEReporter_Ok(self):
    # Ensure this function works with a valid admin email.
    version = os.environ['CURRENT_VERSION_ID']
    major, minor = version.split('.')
    yesterday = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
    handlers.ereporter.ExceptionRecord(
        key_name=handlers.ereporter.ExceptionRecord.get_key_name(
            'X', version, yesterday),
        signature='X',
        major_version=major,
        minor_version=int(minor),
        date=yesterday,
        stacktrace='I blew up',
        http_method='GET',
        url='/nowhere',
        handler='No one').put()
    admin_user.AdminUser(email='admin@denied.com').put()

    response = self.app.get('/secure/cron/sendereporter',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

  def testCancelHandler(self):
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg()).AndReturn(
        'response')
    self._mox.ReplayAll()

    response = self.app.post(handlers._SECURE_CANCEL_URL, {'r': 'invalid_key'})
    self.assertEqual('200 OK', response.status)
    self.assertTrue('Cannot find runner' in response.body, response.body)

    runner = test_helper.CreatePendingRunner()
    response = self.app.post(handlers._SECURE_CANCEL_URL,
                             {'r': runner.key.urlsafe()})
    self.assertResponse(response, '200 OK', 'Runner canceled.')

    self._mox.VerifyAll()


if __name__ == '__main__':
  logging.disable(logging.ERROR)
  unittest.main()
