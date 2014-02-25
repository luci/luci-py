#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import sys
import unittest

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env

test_env.setup_test_env()

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

from components import auth

import handlers
import test_case
import webtest

from common import swarm_constants
from common import url_helper
from server import admin_user
from server import dimension_mapping
from server import dimensions_utils
from server import result_helper
from server import test_helper
from server import test_management
from server import test_request
from server import test_runner
from server import user_manager
from stats import daily_stats
from stats import machine_stats
from stats import runner_stats
from stats import runner_summary
from third_party.mox import mox


# A simple machine id constant to use in tests.
MACHINE_ID = '12345678-12345678-12345678-12345678'

# Sample email of unknown user. It is effectively anonymous.
UNKNOWN_EMAIL = 'unknown@example.com'
# Sample email of some known user (but not admin).
USER_EMAIL = 'user@example.com'
# Sample email of admin user. It has all permissions.
ADMIN_EMAIL = 'admin@example.com'

# remote_addr of a fake bot that makes requests in tests.
FAKE_IP = 'fake-ip'


class AppTest(test_case.TestCase):
  def setUp(self):
    super(AppTest, self).setUp()
    self.testbed.init_logservice_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.init_user_stub()

    # Ensure the test can find queue.yaml.
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
    self.taskqueue_stub._root_path = APP_DIR

    # By default requests in tests are coming from bot with fake IP.
    self.app = webtest.TestApp(
        handlers.CreateApplication(),
        extra_environ={'REMOTE_ADDR': FAKE_IP})

    # Whitelist that fake bot.
    user_manager.AddWhitelist(FAKE_IP)

    # A basic config hash to use when creating runners.
    self.config_hash = dimensions_utils.GenerateDimensionHash({})

    # Mock expected permission structure.
    def mocked_has_permission(_action, resource):
      ident = auth.get_current_identity()
      # Admin has access to everything. Known users have access to client
      # portion, unknown users are effectively anonymous (have no access).
      if ident.is_user:
        return (ident.name == ADMIN_EMAIL) or (ident.name == USER_EMAIL and
            resource == 'swarming/clients')
      # Bots have access to client and bot portions.
      return ident.is_bot and resource in ('swarming/clients', 'swarming/bots')
    self.mock(auth.api, 'has_permission', mocked_has_permission)

    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()
    super(AppTest, self).tearDown()

  def _GetRoutes(self):
    """Returns the list of all routes handled."""
    return [
        r.template for r in self.app.app.router.match_routes
    ]

  def _GetRequest(self):
    return test_request.TestRequest.query().get()

  def _ReplaceCurrentUser(self, email, **kwargs):
    if email:
      self.testbed.setup_env(USER_EMAIL=email, overwrite=True, **kwargs)
    else:
      self.testbed.setup_env(overwrite=True, **kwargs)

  def assertResponse(self, response, status, body):
    self.assertEqual(status, response.status, response.status)
    self.assertEqual(body, response.body, repr(response.body))

  def testMatchingTestCasesHandler(self):
    # Ensure that matching works even when the datastore is not being
    # consistent.
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

    # Mock out all the authentication, since it doesn't work with the server
    # being only eventually consistent (but it is ok for the machines to take
    # a while to appear).
    self.mock(handlers.user_manager, 'IsWhitelistedMachine', lambda *_arg: True)

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

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Test when no matching key
    for handler in handler_urls:
      response = self.app.get(handler, {'r': 'fake_key'}, status=204)
      self.assertTrue('204' in response.status)

    # Create test and runner.
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID,
                                             exit_codes='[0]')
    results = result_helper.StoreResults('\xe9 Invalid utf-8 string')
    runner.results_reference = results.key
    runner.put()

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

      expected_result_string = runner.GetResultString().decode('utf-8',
                                                               'replace')
      self.assertEqual(expected_result_string, results['output'])

  def testGetSlaveCode(self):
    response = self.app.get('/get_slave_code')
    self.assertEqual('200 OK', response.status)

  def testMachineList(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    self._mox.ReplayAll()

    # Add a machine to display.
    machine_stats.MachineStats.get_or_insert(MACHINE_ID, tag='tag')

    response = self.app.get('/secure/machine_list')
    self.assertTrue('200' in response.status)

    self._mox.VerifyAll()

  def testMachineListJson(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    machine_stats.MachineStats(
        id=MACHINE_ID,
        tag='tag',
        last_seen=datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
        dimensions='{"foo": "bar"}').put()

    response = self.app.get('/secure/machine_list/json')
    self.assertEqual('200 OK', response.status)
    actual = json.loads(response.body)
    expected = {
        u'machine_death_timeout': 10800,
        u'machine_update_time': 3600,
        u'machines': [
          {
            u'dimensions': u'{"foo": "bar"}',
            u'last_seen': u'2000-01-02 03:04:05',
            u'machine_id': u'12345678-12345678-12345678-12345678',
            u'tag': u'tag',
          },
       ],
    }
    self.assertEqual(expected, actual)

  def testDeleteMachineStats(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

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
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

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
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Test when no matching key
    response = self.app.post('/secure/retry', {'r': 'fake_key'}, status=204)
    self.assertResponse(response, '204 No Content', '')

    # Test with matching key.
    runner = test_helper.CreatePendingRunner(exit_codes='[0]')

    response = self.app.post('/secure/retry', {'r': runner.key.urlsafe()})
    self.assertResponse(response, '200 OK', 'Runner set for retry.')

  def testShowMessageHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    response = self.app.get('/secure/show_message', {'r': 'fake_key'},
        status=404)

    runner = test_helper.CreatePendingRunner()
    response = self.app.get('/secure/show_message',
                            {'r': runner.key.urlsafe()})
    self.assertEqual(runner.GetAsDict(), response.json)

  def testUploadStartSlaveHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    response = self.app.post('/upload_start_slave', expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ')

    response = self.app.post(
        '/upload_start_slave',
        upload_files=[('script', 'script', 'script_body')])
    self.assertResponse(response, '200 OK', 'Success.')

  def testRegisterHandler(self):
    # Missing attributes field.
    response = self.app.post(
        '/poll_for_test', {'something': 'nothing'}, expect_errors=True)
    self.assertResponse(
        response,
        '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is either '
        'malformed or otherwise incorrect.\n\n'
        ' Invalid attributes: : No JSON object could be decoded  ')

    # Invalid attributes field.
    response = self.app.post(
        '/poll_for_test', {'attributes': 'nothing'}, expect_errors=True)
    self.assertResponse(
        response,
        '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is either '
        'malformed or otherwise incorrect.\n\n'
        ' Invalid attributes: nothing: No JSON object could be decoded  ')

    # Invalid empty attributes field.
    response = self.app.post(
        '/poll_for_test', {'attributes': None}, expect_errors=True)
    self.assertResponse(
        response,
        '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is either '
        'malformed or otherwise incorrect.\n\n'
        ' Invalid attributes: None: No JSON object could be decoded  ')

    # Valid attributes but missing dimensions.
    attributes = '{"id": "%s"}' % MACHINE_ID
    response = self.app.post(
        '/poll_for_test', {'attributes': attributes}, expect_errors=True)
    self.assertResponse(
        response,
        '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is '
        'either malformed or otherwise incorrect.\n\n'
        ' Missing mandatory attribute: dimensions  ')

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
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Make sure the template renders.
    response = self.app.get('/secure/user_profile', {})
    self.assertEqual('200 OK', response.status)

  def testChangeWhitelistHandlerParams(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Make sure the link redirects to the right place.
    # setUp adds one item.
    self.assertEqual(
        [{'ip': FAKE_IP, 'password': None}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    response = self.app.post(
        '/secure/change_whitelist', {'a': 'True'},
        extra_environ={'REMOTE_ADDR': 'foo'})
    self.assertEqual(
        [{'ip': FAKE_IP, 'password': None}, {'ip': u'foo', 'password': None}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    self.assertEqual('301 Moved Permanently', response.status)
    self.assertEqual(
        'http://localhost/secure/user_profile', response.location)

    # All of these requests are invalid so none of them modify entites.
    self.app.post('/secure/change_whitelist', {'i': ''}, expect_errors=True)
    self.app.post('/secure/change_whitelist', {'i': '123'}, expect_errors=True)
    self.app.post(
        '/secure/change_whitelist', {'p': 'password'}, expect_errors=True)
    self.app.post(
        '/secure/change_whitelist', {'i': '123', 'a': 'true'},
        expect_errors=True)
    self.assertEqual(
        [{'ip': FAKE_IP, 'password': None}, {'ip': u'foo', 'password': None}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])

  def testChangeWhitelistHandler(self):
    ip = ['123', '456']
    password = [None, 'pa$$w0rd']

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    user_manager.DeleteWhitelist(FAKE_IP)
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
    user_manager.DeleteWhitelist(FAKE_IP)
    for handler, method in handlers_to_check:
      response = method(handler, {}, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Whitelist a machine.
    user_manager.AddWhitelist(FAKE_IP, password=password)

    # Make sure whitelisted requests are accepted.
    for handler, method in handlers_to_check:
      response = method(handler, {'password': password}, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Make sure invalid passwords are rejected.
    for handler, method in handlers_to_check:
      response = method(
          handler, {'password': 'something else'}, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

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
    user_manager.DeleteWhitelist(FAKE_IP)
    self._ReplaceCurrentUser(None)

    # Make sure all anonymous requests are rejected.
    for handler, method in allowed + forbidden:
      response = method(handler, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Make sure all requests from unknown account are rejected.
    self._ReplaceCurrentUser(UNKNOWN_EMAIL)
    for handler, method in allowed + forbidden:
      response = method(handler, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

    # Make sure for a known account 'allowed' methods are accessible
    # and 'forbidden' are not.
    self._ReplaceCurrentUser(USER_EMAIL)
    for handler, method in allowed:
      response = method(handler, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)
    for handler, method in forbidden:
      response = method(handler, expect_errors=True)
      self.assertEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

  # Test that all handlers are accessible only to authenticated user or machine.
  # Assumes all routes are defined with plain paths
  # (i.e. '/some/handler/path' and not regexps).
  def testAllSwarmingHandlersAreSecured(self):
    # URL prefixes that correspond to routes that are not protected by
    # swarming app code. It may be routes that do not require login or routes
    # protected by GAE itself via 'login: admin' in app.yaml.
    allowed_paths = (
        '/auth/',
        '/secure/',
    )

    # Handlers that are explicitly allowed to be called by anyone.
    allowed_urls = set([
        '/',
        '/auth',
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
    routes_to_check = []
    for route in routes:
      if (route.template not in allowed_urls and
          not route.template.startswith(allowed_paths)):
        routes_to_check.append(route)

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
    user_manager.DeleteWhitelist(FAKE_IP)
    self._ReplaceCurrentUser(None)
    # Try to execute 'get' and 'post' and verify they fail with 403 or 405.
    for route in routes_to_check:
      CheckProtected(route, 'GET')
      CheckProtected(route, 'POST')

  def testRemoteErrorHandler(self):
    self.assertEqual(0, test_management.SwarmError.query().count())

    error_message = 'error message'

    response = self.app.post('/remote_error', {'m': error_message})
    self.assertResponse(response, '200 OK', 'Success.')

    self.assertEqual(1, test_management.SwarmError.query().count())
    error = test_management.SwarmError.query().get()
    self.assertEqual(error.message, error_message)

  def testRunnerPing(self):
    # Try with an invalid runner key
    response = self.app.post('/runner_ping', {'r': '1'}, expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is either '
        'malformed or otherwise incorrect.\n\n'
        ' Runner failed to ping.  ')

    # Start a test and successfully ping it
    runner = test_helper.CreatePendingRunner(machine_id=MACHINE_ID)
    response = self.app.post('/runner_ping', {'r': runner.key.urlsafe(),
                                              'id': runner.machine_id})
    self.assertResponse(response, '200 OK', 'Success.')

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

  def testTestRequest(self):
    # Ensure that a test request fails without a request.
    response = self.app.post('/test', expect_errors=True)
    self.assertResponse(response, '400 Bad Request',
                        '400 Bad Request\n\nThe server could not comply with '
                        'the request since it is either malformed or otherwise '
                        'incorrect.\n\n No request parameter found.  ')

    # Ensure invalid requests are rejected.
    request = {
        "configurations": [{
            "config_name": "win",
            "dimensions": {"os": "win"},
        }],
        "test_case_name": "",
        "tests":[{
            "action": ["python", "run_test.py"],
            "test_name": "Run Test",
        }],
    }
    response = self.app.post('/test', {'request': json.dumps(request)},
                             expect_errors=True)
    self.assertEquals('400 Bad Request', response.status)

    # Ensure that valid requests are accepted.
    request['test_case_name'] = 'test_case'
    response = self.app.post('/test', {'request': json.dumps(request)},
                             expect_errors=True)
    self.assertEquals('200 OK', response.status)

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
          response, '403 Forbidden',
          '403 Forbidden\n\nAccess was denied to this resource.\n\n '
          'Only internal cron jobs can do this  ')

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

  def testSendEReporter(self):
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    response = self.app.get('/secure/cron/ereporter2/mail',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

  def testCancelHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

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

  def testKnownAuthResources(self):
    # This test is supposed to catch typos and new types of auth resources.
    # It walks over all AuthenticatedHandler routes and ensures @require
    # decorator use resources from this set.
    expected = {
        'auth/management',
        'swarming/bots',
        'swarming/clients',
        'swarming/management',
    }
    for route in auth.get_authenticated_routes(handlers.CreateApplication()):
      per_method = route.handler.get_methods_permissions()
      for method, permissions in per_method.iteritems():
        self.assertTrue(
            expected.issuperset(resource for _, resource in permissions),
            msg='Unexpected auth resource in %s of %s: %s' %
                (method, route, permissions))


if __name__ == '__main__':
  logging.disable(logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
