#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import itertools
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
from google.appengine.ext import ndb

import webtest

import handlers_frontend
from common import swarm_constants
from components import auth
from components import stats_framework
from server import acl
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


def clear_ip_whitelist():
  """Removes all IPs from the whitelist."""
  entries = user_manager.MachineWhitelist.query().fetch(keys_only=True)
  ndb.delete_multi(entries)


class AppTest(test_case.TestCase):
  # TODO(maruel): Make 3 test classes, one focused on bot API, one on client API
  # and one on the web frontend and backend processing.
  APP_DIR = APP_DIR

  def setUp(self):
    super(AppTest, self).setUp()
    self._version = None
    self.testbed.init_user_stub()
    self.testbed.init_search_stub()

    # By default requests in tests are coming from bot with fake IP.
    app = handlers_frontend.CreateApplication()
    app.router.add(('/_ah/queue/deferred', deferred.TaskHandler))
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': FAKE_IP,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # WSGI app that implements auth REST API.
    self.auth_app = webtest.TestApp(
        auth.create_wsgi_application(debug=True),
        extra_environ={
          'REMOTE_ADDR': FAKE_IP,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # Whitelist that fake bot.
    user_manager.AddWhitelist(FAKE_IP)

    # Mock expected groups structure.
    # TODO(maruel): Mock privileged_user too.
    def mocked_is_group_member(group, identity=None):
      identity = identity or auth.get_current_identity()
      if group == acl.ADMINS_GROUP:
        return identity.is_user and identity.name == ADMIN_EMAIL
      if group == acl.USERS_GROUP:
        return identity.is_user and identity.name == USER_EMAIL
      if group == acl.BOTS_GROUP:
        return identity.is_bot
      return False
    self.mock(handlers_frontend.auth, 'is_group_member', mocked_is_group_member)

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
    return self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json['xsrf_token']

  def client_create_task(self, name):
    """Simulate a client that creates a task."""
    request = {
      'configurations': [
        {
          'config_name': 'X',
          'dimensions': {'os': 'Amiga'},
          'priority': 10,
        },
      ],
      'test_case_name': name,
      'tests':[{'action': ['python', 'run_test.py'], 'test_name': 'Y'}],
    }
    return self.app.post('/test', {'request': json.dumps(request)}).json

  def bot_poll(self, bot):
    """Simulates a bot that polls for task."""
    if not self._version:
      attributes = {
        'dimensions': {'cpu': '48', 'os': 'Amiga'},
        'id': bot,
        'ip': FAKE_IP,
      }
      result = self.app.post(
          '/poll_for_test', {'attributes': json.dumps(attributes)}).json
      self._version = result['commands'][0]['args'][-40:]

    attributes = {
      'dimensions': {'cpu': '48', 'os': 'Amiga'},
      'id': bot,
      'ip': FAKE_IP,
      'version': self._version,
    }
    return self.app.post(
        '/poll_for_test', {'attributes': json.dumps(attributes)}).json


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
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Test when no matching key
    response = self.app.get('/get_result', {'r': 'fake_key'}, status=400)

    # Create test and runner.
    result_summary, run_result = test_helper.CreateRunner(
        machine_id=MACHINE_ID,
        exit_codes='0',
        results='\xe9 Invalid utf-8 string')

    # Valid key.
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.get('/get_result', {'r': packed})
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

    # Add bots to display.
    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {}, '123456789')
    bot_management.tag_bot_seen(
        'id2', 'localhost:8080', '127.0.0.2', '8.8.8.8', {'foo': 'bar'},
        '123456789')

    response = self.app.get('/restricted/bots')
    self.assertTrue('200' in response.status)

  def testApiBots(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789')
    bot.last_seen = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    bot.put()

    response = self.app.get('/swarming/api/v1/bots')
    self.assertEqual('200 OK', response.status)
    expected = {
        u'machine_death_timeout':
            int(bot_management.MACHINE_DEATH_TIMEOUT.total_seconds()),
        u'machines': [
          {
            u'dimensions': {u'foo': u'bar'},
            u'external_ip': u'8.8.4.4',
            u'hostname': u'localhost',
            u'id': u'id1',
            u'internal_ip': u'127.0.0.1',
            u'last_seen': u'2000-01-02 03:04:05',
            u'task': None,
            u'version': u'123456789',
          },
       ],
    }
    self.assertEqual(expected, response.json)

  def testDeleteMachineStats(self):
    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Add a machine assignment to delete.
    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789')

    # Delete the machine assignment.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '200 OK', 'Machine Assignment removed.')

    # Attempt to delete the assignment again and silently pass.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '204 No Content', '')

  def testMainHandler(self):
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    response = self.app.get('/')
    self.assertEqual('200 OK', response.status)

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
      'ip': '8.8.4.4',
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
    # Start with clean IP whitelist. It removes FAKE_IP added in setUp.
    clear_ip_whitelist()

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    # Make sure the template renders.
    response = self.app.get('/restricted/whitelist_ip', {})
    self.assertEqual('200 OK', response.status)
    xsrf_token = self.getXsrfToken()

    # Make sure the link redirects to the right place.
    self.assertEqual(
        [],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    response = self.app.post(
        '/restricted/whitelist_ip', {'a': 'True', 'xsrf_token': xsrf_token},
        extra_environ={'REMOTE_ADDR': 'foo'})
    self.assertEqual(
        [{'ip': u'foo'}],
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
        [{'ip': u'foo'}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])

  def testWhitelistIPHandler(self):
    ip = ['1.2.3.4', '1:2:3:4:5:6:7:8']

    # Act under admin identity.
    self._ReplaceCurrentUser(ADMIN_EMAIL)

    clear_ip_whitelist()
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
    # URL prefixes that correspond to routes that are not protected by swarming
    # app code. It may be routes that do not require login or routes protected
    # by GAE itself via 'login: admin' in app.yaml.
    using_app_login_prefixes = (
      '/auth/',
    )

    public_urls = frozenset([
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
    routes_to_check = [
      route for route in routes
      if (route.template not in public_urls and
          not route.template.startswith(using_app_login_prefixes))
    ]

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
      if '<' in route.template:
        # Sadly, the url cannot be used as-is. Figure out a way to test them
        # easily.
        continue
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
    self.app.get('/restricted/errors', status=403)

    error_message = 'error message'
    response = self.app.post('/remote_error', {'m': error_message})
    self.assertResponse(response, '200 OK', 'Success.')

    self.assertEqual(1, errors.SwarmError.query().count())
    error = errors.SwarmError.query().get()
    self.assertEqual(error.message, error_message)

    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self.app.get('/restricted/errors', status=200)

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
    response = self.app.post('/test', {'request': json.dumps(request)})
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

  def test_dead_machines_count(self):
    # TODO(maruel): Convert this test case to mock time and use the APIs instead
    # of editing the DB directly.
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)
    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {}, '123456789')
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)

    # Borderline. If this test becomes flaky, increase the 1 second value.
    bot.last_seen = (
        task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT +
        datetime.timedelta(seconds=1))
    bot.put()
    self.assertEqual('0', self.app.get('/swarming/api/v1/bots/dead/count').body)

    # Make the machine old and ensure it is marked as dead.
    bot.last_seen = (
        task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT -
        datetime.timedelta(seconds=1))
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

  def test_task_list_empty(self):
    # Just assert it doesn't throw.
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/12345', status=404)

  def _check_task(self, task, priority):
    # The value is using both timestamp and random value, so it is not
    # deterministic by definition.
    key = task['test_keys'][0].pop('test_key')
    self.assertTrue(int(key, 16))
    self.assertEqual('0', key[-1])
    expected = {
      u'priority': priority,
      u'test_case_name': u'hi',
      u'test_keys': [
        {
          u'config_name': u'hi',
          u'instance_index': 0,
          u'num_instances': 1,
        },
      ],
    }
    self.assertEqual(expected, task)
    return key

  def test_add_task_admin(self):
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    task = self.client_create_task('hi')
    self._check_task(task, 10)

  def test_add_task_bot(self):
    task = self.client_create_task('hi')
    # The bot has access to use high priority. By default on dev server
    # localhost is whitelisted as a bot.
    self._check_task(task, 10)

  def test_add_task_and_list_user(self):
    # Add a task via the API as a user, then assert it can be viewed.
    self._ReplaceCurrentUser(USER_EMAIL)
    task = self.client_create_task('hi')
    # Since the priority 10 was too high for a user (not an admin, neither a
    # bot), it was reset to 100.
    key = self._check_task(task, 100)

    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/%s' % key, status=200)

    reaped = self.bot_poll('bot1')
    self.assertEqual('RunManifest', reaped['commands'][0]['function'])
    # This can only work once a bot reaped the task.
    self.app.get('/user/task/%s' % (key[:-1] + '1'), status=200)

  def test_task_list_query(self):
    # Try all the combinations of task queries to ensure the index exist.
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self._check_task(self.client_create_task('hi'), 10)

    sort_choices = [i[0] for i in handlers_frontend.TasksHandler.SORT_CHOICES]
    state_choices = sum(
        ([i[0] for i in j]
          for j in handlers_frontend.TasksHandler.STATE_CHOICES),
        [])

    for sort, state in itertools.product(sort_choices, state_choices):
      url = '/user/tasks?sort=%s&state=%s' % (sort, state)
      # See require_index in ../components/support/test_case.py in case of
      # NeedIndexError.
      resp = self.app.get(url, expect_errors=True)
      self.assertEqual(200, resp.status_code, (resp.body, sort, state))
      self.app.get(url + '&task_name=hi', status=200)

    self.app.get('/user/tasks?sort=foo', status=400)
    self.app.get('/user/tasks?state=foo', status=400)

    self.app.get('/user/tasks?task_name=hi', status=200)

  def test_bot_list_empty(self):
    # Just assert it doesn't throw.
    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self.app.get('/restricted/bots', status=200)
    self.app.get('/restricted/bot/unknown_bot', status=200)

  def test_bot_listing(self):
    # Create a task, create 2 bots, one with a task assigned, the other without.
    self.client_create_task('hi')
    self.bot_poll('bot1')
    self.bot_poll('bot2')

    self._ReplaceCurrentUser(ADMIN_EMAIL)
    self.app.get('/restricted/bots', status=200)
    self.app.get('/restricted/bot/bot1', status=200)
    self.app.get('/restricted/bot/bot2', status=200)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
