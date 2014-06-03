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
import urllib

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env
test_env.setup_test_env()

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import deferred

import webtest

import handlers_frontend
from common import swarm_constants
from components import auth
from components import stats_framework
from components import utils
from server import admin_user
from server import bot_management
from server import errors
from server import stats
from server import task_common
from server import test_helper
from server import user_manager
from support import test_case
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
  APP_DIR = APP_DIR

  def setUp(self):
    super(AppTest, self).setUp()
    self.testbed.init_user_stub()

    # By default requests in tests are coming from bot with fake IP.
    app = handlers_frontend.CreateApplication()
    app.router.add(('/_ah/queue/deferred', deferred.TaskHandler))
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': FAKE_IP,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # Whitelist that fake bot.
    user_manager.AddWhitelist(FAKE_IP)

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
    self.mock(stats_framework, 'add_entry', self._parse_line)

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {})
    self.assertEqual(True, actual, line)

  def tearDown(self):
    try:
      self._mox.UnsetStubs()
    finally:
      super(AppTest, self).tearDown()

  def _GetRoutes(self):
    """Returns the list of all routes handled."""
    return [
        r.template for r in self.app.app.router.match_routes
    ]

  def _ReplaceCurrentUser(self, email, **kwargs):
    if email:
      self.testbed.setup_env(USER_EMAIL=email, overwrite=True, **kwargs)
    else:
      self.testbed.setup_env(overwrite=True, **kwargs)

  def assertResponse(self, response, status, body):
    self.assertEqual(status, response.status, response.status)
    self.assertEqual(body, response.body, repr(response.body))

  def getXsrfToken(self):
    return self.app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json['xsrf_token']

  def testMatchingTestCasesHandler(self):
    # Ensure that matching works even when the datastore is not being
    # consistent.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(
        require_indexes=True, consistency_policy=policy, root_path=self.APP_DIR)

    # Mock out all the authentication, since it doesn't work with the server
    # being only eventually consistent (but it is ok for the machines to take
    # a while to appear).
    self.mock(user_manager, 'IsWhitelistedMachine', lambda *_arg: True)

    # Test when no matching tests.
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME},
        expect_errors=True)
    self.assertResponse(response, '404 Not Found', '[]')

    # Test with a single matching runner.
    result_summary, _run_result = test_helper.CreateRunner()
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)

    # Test with a multiple matching runners.
    result_summary_2, _run_result = test_helper.CreateRunner(
        machine_id='foo', exit_codes='0', results='Rock on')

    response = self.app.get(
        '/get_matching_test_cases',
        {'name': test_helper.REQUEST_MESSAGE_TEST_CASE_NAME})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary_2.key),
        response.body)

  def testGetResultHandler(self):
    handler_urls = ['/get_result', '/restricted/get_result']

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Test when no matching key
    for handler in handler_urls:
      response = self.app.get(handler, {'r': 'fake_key'}, status=400)

    # Create test and runner.
    result_summary, run_result = test_helper.CreateRunner(
        machine_id=MACHINE_ID,
        exit_codes='0',
        results='\xe9 Invalid utf-8 string')

    # Valid key.
    for handler in handler_urls:
      packed = task_common.pack_result_summary_key(result_summary.key)
      response = self.app.get(handler, {'r': packed})
      self.assertEqual('200 OK', response.status)

      try:
        results = json.loads(response.body)
      except (ValueError, TypeError), e:
        self.fail(e)
      # TODO(maruel): Stop using the DB directly in HTTP handlers test.
      self.assertEqual(
          ','.join(map(str, run_result.exit_codes)),
          ''.join(results['exit_codes']))
      self.assertEqual(result_summary.bot_id, results['machine_id'])
      expected_result_string = '\n'.join(
          o.get().GetResults().decode('utf-8', 'replace')
          for o in result_summary.outputs)
      self.assertEqual(expected_result_string, results['output'])

  def testGetSlaveCode(self):
    response = self.app.get('/get_slave_code')
    self.assertEqual('200 OK', response.status)
    self.assertEqual(
        len(bot_management.get_swarming_bot_zip('http://localhost')),
        response.content_length)

  def testGetSlaveCodeHash(self):
    response = self.app.get(
        '/get_slave_code/%s' %
        bot_management.get_slave_version('http://localhost'))
    self.assertEqual('200 OK', response.status)
    self.assertEqual(
        len(bot_management.get_swarming_bot_zip('http://localhost')),
        response.content_length)

  def testGetSlaveCodeInvalidHash(self):
    response = self.app.get('/get_slave_code/' + '1' * 40, expect_errors=True)
    self.assertEqual('404 Not Found', response.status)

  def testBots(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Add a machine to display.
    bot_management.tag_bot_seen('id1', 'localhost', {})

    response = self.app.get('/restricted/bots')
    self.assertTrue('200' in response.status)

  def testApiBots(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    bot = bot_management.tag_bot_seen('id1', 'localhost', {'foo': 'bar'})
    bot.last_seen = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    bot.put()

    response = self.app.get('/swarming/api/v1/bots')
    self.assertEqual('200 OK', response.status)
    expected = {
        u'machine_death_timeout':
            int(bot_management.MACHINE_DEATH_TIMEOUT.total_seconds()),
        u'machine_update_time':
            int(bot_management.MACHINE_UPDATE_TIME.total_seconds()),
        u'machines': [
          {
            u'dimensions': {u'foo': u'bar'},
            u'hostname': u'localhost',
            u'id': u'id1',
            u'last_seen': u'2000-01-02 03:04:05',
          },
       ],
    }
    self.assertEqual(expected, response.json)

  def testDeleteMachineStats(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Add a machine assignment to delete.
    bot_management.tag_bot_seen('id1', 'localhost', {'foo': 'bar'})

    # Delete the machine assignment.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '200 OK', 'Machine Assignment removed.')

    # Attempt to delete the assignment again and silently pass.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '204 No Content', '')

  def testMainHandler(self):
    response = self.app.get('/')
    self.assertEqual('200 OK', response.status)

  def testRestrictedHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    self._mox.ReplayAll()

    # Add a test runner to show on the page.
    test_helper.CreateRunner()

    response = self.app.get('/restricted')
    self.assertEqual('200 OK', response.status)

    self._mox.VerifyAll()

  def testRetryHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Test when no matching key
    response = self.app.post(
        '/restricted/retry', {'r': 'fake_key'}, expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Unable to retry runner', response.body)

    # Test with matching key.
    result_summary, _run_result = test_helper.CreateRunner(exit_codes='0')
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.post('/restricted/retry', {'r': packed})
    self.assertResponse(response, '200 OK', 'Runner set for retry.')

  def testShowMessageHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    response = self.app.get('/restricted/show_message', {'r': 'fake_key'},
        status=404)

    result_summary, _run_result = test_helper.CreateRunner()
    # TODO(maruel): Must support sending back individual tries.
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.get('/restricted/show_message', {'r': packed})
    self.assertEqual(
        utils.to_json_encodable(result_summary.to_dict()), response.json)

  def testUploadStartSlaveHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    xsrf_token = self.getXsrfToken()
    response = self.app.get('/restricted/upload_start_slave')
    response = self.app.post(
        '/restricted/upload_start_slave?xsrf_token=%s' % xsrf_token,
        expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ')

    response = self.app.post(
        '/restricted/upload_start_slave?xsrf_token=%s' % xsrf_token,
        upload_files=[('script', 'script', 'script_body')])
    self.assertEqual('11 bytes stored.', response.body)

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

  def testRegisterHandlerNoVersion(self):
    attributes = {
      'dimensions': {'os': ['win-xp']},
      'id': MACHINE_ID,
    }
    response = self.app.post(
        '/poll_for_test', {'attributes': json.dumps(attributes)})
    self.assertEqual('200 OK', response.status)
    expected = {
      u'commands': [
        {
          u'args':
              u'http://localhost/get_slave_code/%s' %
              bot_management.get_slave_version('http://localhost'),
          u'function': u'UpdateSlave',
        },
      ],
      u'result_url': u'http://localhost/remote_error',
      u'try_count': 0,
    }
    self.assertEqual(expected, response.json)

  def testRegisterHandlerVersion(self):
    attributes = {
      'dimensions': {'os': ['win-xp']},
      'id': MACHINE_ID,
      'version': bot_management.get_slave_version('http://localhost'),
    }
    response = self.app.post(
        '/poll_for_test', {'attributes': json.dumps(attributes)})
    self.assertEqual('200 OK', response.status)
    expected_1 = {u'come_back': 1.0, u'try_count': 1}
    expected_2 = {u'come_back': 2.25, u'try_count': 1}
    self.assertTrue(response.json in (expected_1, expected_2), response.json)

  def _PostResults(self, run_result, result, expect_errors=False):
    url_parameters = {
      'id': run_result.bot_id,
      'r': task_common.pack_run_result_key(run_result.key),
    }
    files = [(swarm_constants.RESULT_STRING_KEY,
              swarm_constants.RESULT_STRING_KEY,
              result)]
    return self.app.post('/result', url_parameters, upload_files=files,
                         expect_errors=expect_errors)

  def testResultHandler(self):
    # TODO(maruel): Stop using the DB directly.
    result_summary, run_result = test_helper.CreateRunner(machine_id=MACHINE_ID)
    result = 'result string'
    response = self._PostResults(run_result, result)
    self.assertResponse(
        response, '200 OK', 'Successfully update the runner results.')

    packed = task_common.pack_result_summary_key(result_summary.key)
    resp = self.app.get('/get_result?r=%s' % packed, status=200)
    expected = {
      u'config_instance_index': 0,
      u'exit_codes': u'',
      u'machine_id': u'12345678-12345678-12345678-12345678',
      u'machine_tag': u'12345678-12345678-12345678-12345678',
      u'num_config_instances': 1,
      u'output': u'result string',
    }
    self.assertEqual(expected, resp.json)

  def testWhitelistIPHandlerParams(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Make sure the template renders.
    response = self.app.get('/restricted/whitelist_ip', {})
    self.assertEqual('200 OK', response.status)
    xsrf_token = self.getXsrfToken()

    # Make sure the link redirects to the right place.
    # setUp adds one item.
    self.assertEqual(
        [{'ip': FAKE_IP}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    response = self.app.post(
        '/restricted/whitelist_ip', {'a': 'True', 'xsrf_token': xsrf_token},
        extra_environ={'REMOTE_ADDR': 'foo'})
    self.assertEqual(
        [{'ip': FAKE_IP}, {'ip': u'foo'}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    self.assertEqual(200, response.status_code)

    # All of these requests are invalid so none of them modify entites.
    self.app.post(
        '/restricted/whitelist_ip', {'i': '', 'xsrf_token': xsrf_token},
        expect_errors=True)
    self.app.post(
        '/restricted/whitelist_ip', {'i': '123', 'xsrf_token': xsrf_token},
        expect_errors=True)
    self.app.post(
        '/restricted/whitelist_ip',
        {'i': '123', 'a': 'true', 'xsrf_token': xsrf_token},
        expect_errors=True)
    self.assertEqual(
        [{'ip': FAKE_IP}, {'ip': u'foo'}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])

  def testWhitelistIPHandler(self):
    ip = ['1.2.3.4', '1:2:3:4:5:6:7:8']

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    user_manager.DeleteWhitelist(FAKE_IP)
    self.assertEqual(0, user_manager.MachineWhitelist.query().count())
    xsrf_token = self.getXsrfToken()

    # Whitelist IPs.
    self.app.post(
        '/restricted/whitelist_ip',
        {'i': ip[0], 'a': 'True', 'xsrf_token': xsrf_token})
    self.assertEqual(1, user_manager.MachineWhitelist.query().count())
    self.app.post(
        '/restricted/whitelist_ip',
        {'i': ip[1], 'a': 'True', 'xsrf_token': xsrf_token})
    self.assertEqual(2, user_manager.MachineWhitelist.query().count())

    for i in range(2):
      whitelist = user_manager.MachineWhitelist.query().filter(
          user_manager.MachineWhitelist.ip == ip[i])
      self.assertEqual(1, whitelist.count(), msg='Iteration %d' % i)

    # Remove whitelisted ip.
    self.app.post(
        '/restricted/whitelist_ip',
        {'i': ip[0], 'a': 'False', 'xsrf_token': xsrf_token})
    self.assertEqual(1, user_manager.MachineWhitelist.query().count())

  def testUnsecureHandlerMachineAuthentication(self):
    # Test non-secure handlers to make sure they check the remote machine to be
    # whitelisted before allowing them to perform the task.
    # List of non-secure handlers and their method.
    handlers_to_check = [
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
    user_manager.AddWhitelist(FAKE_IP)

    # Make sure whitelisted requests are accepted.
    for handler, method in handlers_to_check:
      response = method(handler, {}, expect_errors=True)
      self.assertNotEqual(
          '403 Forbidden', response.status, msg='Handler: ' + handler)

  # Test that some specific non-secure handlers allow access to authenticated
  # user. Also verify that authenticated user still can't use other handlers.
  def testUnsecureHandlerUserAuthentication(self):
    # List of non-secure handlers and their method that can be called by user.
    allowed = [('/get_matching_test_cases', self.app.get),
               ('/get_result', self.app.get),
               ('/test', self.app.post)]

    # List of non-secure handlers that should not be accessible to the user.
    forbidden = [
        ('/get_slave_code', self.app.get),
        ('/poll_for_test', self.app.post),
        ('/remote_error', self.app.post),
        ('/result', self.app.post),
    ]

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
      # It's protected but not accessible as-is.
      '/get_slave_code/<version:[0-9a-f]{40}>',
      '/restricted/',
    )

    # Handlers that are explicitly allowed to be called by anyone.
    allowed_urls = set([
      '/',
      '/_ah/warmup',
      '/auth',
      '/server_ping',
      '/stats',
      '/stats/dimensions/<dimensions:.+>',
      '/stats/user/<user:.+>',
      '/swarming/api/v1/bots/dead/count',
      '/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
      '/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
      '/swarming/api/v1/stats/user/<user:.+>/<resolution:[a-z]+>',
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

  def testStatsUrls(self):
    quoted = urllib.quote('{"os":"amiga"}')
    urls = (
      '/stats',
      '/stats/dimensions/' + quoted,
      '/stats/user/joe',
      '/swarming/api/v1/stats/summary/days',
      '/swarming/api/v1/stats/summary/hours',
      '/swarming/api/v1/stats/summary/minutes',
      '/swarming/api/v1/stats/dimensions/%s/days' % quoted,
      '/swarming/api/v1/stats/dimensions/%s/hours' % quoted,
      '/swarming/api/v1/stats/dimensions/%s/minutes' % quoted,
      '/swarming/api/v1/stats/user/joe/days',
      '/swarming/api/v1/stats/user/joe/hours',
      '/swarming/api/v1/stats/user/joe/minutes',
    )
    for url in urls:
      self.app.get(url, status=200)

  def testRemoteErrorHandler(self):
    self.assertEqual(0, errors.SwarmError.query().count())

    error_message = 'error message'

    response = self.app.post('/remote_error', {'m': error_message})
    self.assertResponse(response, '200 OK', 'Success.')

    self.assertEqual(1, errors.SwarmError.query().count())
    error = errors.SwarmError.query().get()
    self.assertEqual(error.message, error_message)

  def testRunnerPingFail(self):
    # Try with an invalid runner key
    response = self.app.post('/runner_ping', {'r': '1'}, expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\n'
        'The server could not comply with the request since it is either '
        'malformed or otherwise incorrect.\n\n'
        ' Runner failed to ping.  ')

  def testRunnerPing(self):
    # Start a test and successfully ping it
    _result_summary, run_result = test_helper.CreateRunner(
        machine_id=MACHINE_ID)
    packed = task_common.pack_run_result_key(run_result.key)
    response = self.app.post(
        '/runner_ping', {'r': packed, 'id': run_result.bot_id})
    self.assertResponse(response, '200 OK', 'Success.')

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
    triggers = (
      '/internal/cron/trigger_cleanup_data',
    )

    for url in triggers:
      response = self.app.get(url, headers={'X-AppEngine-Cron': 'true'})
      self.assertResponse(response, '200 OK', 'Success.')
      self.assertEqual(1, self.execute_tasks())

  def testTaskQueueUrls(self):
    # Tests all the cron tasks are securely handled.
    task_queue_urls = sorted(
      r for r in self._GetRoutes() if r.startswith('/internal/taskqueue/')
    )
    task_queues = [
      ('cleanup', '/internal/taskqueue/cleanup_data'),
    ]
    self.assertEqual(sorted(zip(*task_queues)[1]), task_queue_urls)

    for task_name, url in task_queues:
      response = self.app.post(
          url, headers={'X-AppEngine-QueueName': task_name})
      self.assertResponse(response, '200 OK', 'Success.')

  def testCronJobTasks(self):
    # Tests all the cron tasks are securely handled.
    cron_job_urls = [
        r for r in self._GetRoutes() if r.startswith('/internal/cron/')
    ]
    self.assertTrue(cron_job_urls, cron_job_urls)

    # For ereporter.
    admin_user.AdminUser(email='admin@denied.com').put()
    for cron_job_url in cron_job_urls:
      response = self.app.get(cron_job_url,
                              headers={'X-AppEngine-Cron': 'true'})
      self.assertEqual(200, response.status_code)

      # Only cron job requests can be gets for this handler.
      response = self.app.get(cron_job_url, expect_errors=True)
      self.assertResponse(
          response, '403 Forbidden',
          '403 Forbidden\n\nAccess was denied to this resource.\n\n '
          'Only internal cron jobs can do this  ')
    # The actual number doesn't matter, just make sure they are unqueued.
    self.execute_tasks()

  def testSendEReporter(self):
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    response = self.app.get('/internal/cron/ereporter2/mail',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

  def testCancelHandler(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    response = self.app.post(
        '/restricted/cancel', {'r': 'invalid_key'}, expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Unable to cancel runner', response.body)

    result_summary, _run_result = test_helper.CreateRunner()
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.post('/restricted/cancel', {'r': packed})
    self.assertResponse(response, '200 OK', 'Runner canceled.')

  def testKnownAuthResources(self):
    # This test is supposed to catch typos and new types of auth resources.
    # It walks over all AuthenticatedHandler routes and ensures @require
    # decorator use resources from this set.
    expected = {
        'auth/management',
        'auth/management/groups/{group}',
        'swarming/bots',
        'swarming/clients',
        'swarming/management',
    }
    for route in auth.get_authenticated_routes(
        handlers_frontend.CreateApplication()):
      per_method = route.handler.get_methods_permissions()
      for method, permissions in per_method.iteritems():
        self.assertTrue(
            expected.issuperset(resource for _, resource in permissions),
            msg='Unexpected auth resource in %s of %s: %s' %
                (method, route, permissions))

  def test_dead_machines_count(self):
    # TODO(maruel): Convert this test case to mock time and use the APIs instead
    # of editing the DB directly.
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)
    bot = bot_management.tag_bot_seen('id1', 'localhost', {})
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)

    # Borderline. If this test becomes flaky, increase the 1 second value.
    bot.last_seen = (
        task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT +
        datetime.timedelta(seconds=1))
    bot.put()
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)

    # Make the machine old and ensure it is marked as dead.
    bot.last_seen = task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT
    bot.put()
    self.assertEqual('1', self.app.get('/swarming/api/v1/bots/dead/count').body)

  def test_bootstrap_default(self):
    actual = self.app.get('/bootstrap').body
    with open('swarm_bot/bootstrap.py', 'rb') as f:
      expected = f.read()
    header = 'host_url = \'http://localhost\'\n'
    self.assertEqual(header + expected, actual)

  def test_bootstrap_custom(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    self.app.get('/restricted/upload_bootstrap')
    data = {
      'script': 'foo',
      'xsrf_token': self.getXsrfToken(),
    }
    r = self.app.post('/restricted/upload_bootstrap', data)
    self.assertEqual('3 bytes stored.', r.body)

    actual = self.app.get('/bootstrap').body
    expected = 'host_url = \'http://localhost\'\nfoo'
    self.assertEqual(expected, actual)

  def test_convert_test_case(self):
    data = {
      'configurations': [
        {
          'config_name': 'ignored',
          'deadline_to_run': 63,
          'dimensions': {
            'OS': 'Windows-3.1.1',
            'hostname': 'localhost',
          },
          # Do not block sharded requests, simply ignore the sharding request
          # and run it as a single shard.
          'num_instances': 23,
          'priority': 50,
        },
      ],
      'data': [
        ('http://localhost/foo', 'foo.zip'),
        ('http://localhost/bar', 'bar.zip'),
      ],
      'env_vars': {
        'foo': 'bar',
        'joe': '2',
      },
      'requestor': 'Jesus',
      'test_case_name': 'Request name',
      'tests': [
        {
          'action': ['command1', 'arg1'],
          'hard_time_out': 66.1,
          'io_time_out': 68.1,
          'test_name': 'very ignored',
        },
        {
          'action': ['command2', 'arg2'],
          'test_name': 'very ignored but must be different',
          'hard_time_out': 60000000,
          'io_time_out': 60000000,
        },
      ],
    }
    actual = handlers_frontend.convert_test_case(json.dumps(data))
    expected = {
      'name': u'Request name',
      'user': u'Jesus',
      'properties': {
        'commands': [
          [u'command1', u'arg1'],
          [u'command2', u'arg2'],
        ],
        'data': [
          [u'http://localhost/foo', u'foo.zip'],
          [u'http://localhost/bar', u'bar.zip'],
        ],
        'dimensions': {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'},
        'env': {u'foo': u'bar', u'joe': u'2'},
        'execution_timeout_secs': 66,
        'io_timeout_secs': 68,
      },
      'priority': 50,
      'scheduling_expiration_secs': 63,
    }
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
