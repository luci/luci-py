#!/usr/bin/env python
# coding: utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import itertools
import json
import logging
import os
import re
import sys
import unittest
import urllib

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env
test_env.setup_test_env()

from google.appengine.ext import deferred
from google.appengine.ext import ndb

import webtest

import handlers_frontend
from components import auth
from components import stats_framework
from components import template
from server import acl
from server import bot_management
from server import stats
from server import user_manager
from support import test_case


# remote_addr of a fake bot that makes requests in tests.
FAKE_IP = '192.168.0.0'


def clear_ip_whitelist():
  """Removes all IPs from the whitelist."""
  entries = user_manager.MachineWhitelist.query().fetch(keys_only=True)
  ndb.delete_multi(entries)


class AppTestBase(test_case.TestCase):
  APP_DIR = APP_DIR

  def setUp(self):
    super(AppTestBase, self).setUp()
    self._version = None
    self.testbed.init_user_stub()
    self.testbed.init_search_stub()

    # By default requests in tests are coming from bot with fake IP.
    app = handlers_frontend.create_application(True)
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

    # Note that auth.ADMIN_GROUP != acl.ADMINS_GROUP.
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'super-admin@example.com'), '')
    auth.bootstrap_group(
        acl.ADMINS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'admin@example.com'), '')
    auth.bootstrap_group(
        acl.PRIVILEGED_USERS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'priv@example.com'), '')
    auth.bootstrap_group(
        acl.USERS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'user@example.com'), '')
    auth.bootstrap_group(
        acl.BOTS_GROUP,
        auth.Identity(auth.IDENTITY_BOT, FAKE_IP), '')

    self.mock(stats_framework, 'add_entry', self._parse_line)

  def tearDown(self):
    try:
      template.reset()
    finally:
      super(AppTestBase, self).tearDown()

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {})
    self.assertEqual(True, actual, line)

  def set_as_anonymous(self):
    clear_ip_whitelist()
    self.testbed.setup_env(USER_EMAIL='', overwrite=True)

  def set_as_super_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='super-admin@example.com', overwrite=True)

  def set_as_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='admin@example.com', overwrite=True)

  def set_as_privileged_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='priv@example.com', overwrite=True)

  def set_as_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='user@example.com', overwrite=True)

  def set_as_bot(self):
    self.set_as_anonymous()
    user_manager.AddWhitelist(FAKE_IP)

  def assertResponse(self, response, status, body):
    self.assertEqual(status, response.status, response.status)
    self.assertEqual(body, response.body, repr(response.body))

  def getXsrfToken(self):
    resp = self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json
    return str(resp['xsrf_token'])

  def _client_token(self):
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers,
        params=params).json
    return str(response['xsrf_token'])

  def _bot_token(self, bot='bot1'):
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'attributes': {
        'dimensions': {
          'id': bot,
          'os': ['Amiga'],
        },
        'id': bot,
        'ip': '127.0.0.1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake',
        headers=headers,
        params=params).json
    token = str(response['xsrf_token'])
    params['attributes']['version'] = response['bot_version']
    params['state'] = {
      'running_time': 1234.0,
      'sleep_streak': 0,
      'started_ts': 1410990411.111,
    }
    return token, params

  def post_with_token(self, url, params, token):
    """Does an HTTP POST with a JSON API and a XSRF token."""
    return self.app.post_json(
        url, params=params, headers={'X-XSRF-Token': token}).json

  def bot_poll(self, bot='bot1'):
    """Simulates a bot that polls for task."""
    token, params = self._bot_token(bot)
    return self.post_with_token('/swarming/api/v1/bot/poll', params, token)

  def bot_complete_task(self, token, task_id, bot_id='bot1'):
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 1,
      'id': bot_id,
      'output': u'rÉsult string',
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

  def client_create_task(self, name, extra_command=None):
    """Simulate a client that creates a task."""
    # TODO(maruel): Switch to new API.
    request = {
      'configurations': [
        {
          'config_name': 'X',
          'dimensions': {'os': 'Amiga'},
          'priority': 10,
        },
      ],
      'test_case_name': name,
      'tests': [
        {'action': ['python', 'run_test.py'], 'test_name': 'Y'},
      ],
    }
    if extra_command:
      # Add a second command
      request['tests'].append({'action': extra_command, 'test_name': 'Z'})
    raw = self.app.post('/test', {'request': json.dumps(request)}).json
    task_id = raw['test_keys'][0]['test_key']
    return raw, task_id


class FrontendTest(AppTestBase):
  def testBots(self):
    self.set_as_admin()

    # Add bots to display.
    state = {
      'dict': {'random': 'values'},
      'float': 0.,
      'list': ['of', 'things'],
      'str': u'uni',
    }
    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {}, '123456789', False,
        state)
    bot_management.tag_bot_seen(
        'id2', 'localhost:8080', '127.0.0.2', '8.8.8.8', {'foo': 'bar'},
        '123456789', False, {'ram': 65})

    response = self.app.get('/restricted/bots', status=200)
    self.assertGreater(len(response.body), 1000)

  def test_delete_Bot(self):
    self.set_as_admin()

    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False, {'ram': 65})
    response = self.app.get('/restricted/bots', status=200)
    self.assertTrue('id1' in response.body)

    response = self.app.post(
        '/restricted/bot/id1/delete',
        params={},
        headers={'X-XSRF-Token': self.getXsrfToken()})
    self.assertFalse('id1' in response.body)

    response = self.app.get('/restricted/bots', status=200)
    self.assertFalse('id1' in response.body)

  def test_root(self):
    response = self.app.get('/', status=200)
    self.assertGreater(len(response.body), 1000)

  def testUploadStartSlaveHandler(self):
    self.set_as_admin()
    xsrf_token = self.getXsrfToken()
    response = self.app.get('/restricted/upload/bot_config')
    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        expect_errors=True)
    self.assertResponse(
        response, '400 Bad Request',
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ')

    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        upload_files=[('script', 'script', 'script_body')])
    self.assertIn('script_body', response.body)

  def testWhitelistIPHandlerParams(self):
    self.set_as_admin()

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
    self.set_as_admin()
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

  def testAllSwarmingHandlersAreSecured(self):
    # Test that all handlers are accessible only to authenticated user or
    # bots. Assumes all routes are defined with plain paths (i.e.
    # '/some/handler/path' and not regexps).

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
      '/ereporter2/api/v1/on_error',
      '/server_ping',
      '/stats',
      '/stats/dimensions/<dimensions:.+>',
      '/stats/user/<user:.+>',
      '/swarming/api/v1/client/list',
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

    self.set_as_anonymous()
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

  def test_bootstrap_default(self):
    self.set_as_bot()
    actual = self.app.get('/bootstrap').body
    with open(os.path.join(APP_DIR, 'swarming_bot/bootstrap.py'), 'rb') as f:
      expected = f.read()
    header = 'host_url = \'http://localhost\'\n'
    self.assertEqual(header + expected, actual)

  def test_bootstrap_custom(self):
    # Act under admin identity.
    self.set_as_admin()
    self.app.get('/restricted/upload/bootstrap')
    data = {
      'script': 'script_body',
      'xsrf_token': self.getXsrfToken(),
    }
    r = self.app.post('/restricted/upload/bootstrap', data)
    self.assertIn('script_body', r.body)

    actual = self.app.get('/bootstrap').body
    expected = 'host_url = \'http://localhost\'\nscript_body'
    self.assertEqual(expected, actual)

  def test_task_list_empty(self):
    # Just assert it doesn't throw.
    self.set_as_privileged_user()
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/12345', status=404)

  def test_add_task_and_list_user(self):
    # Add a task via the API as a user, then assert it can be viewed.
    self.set_as_user()
    _, task_id = self.client_create_task('hi')

    self.set_as_privileged_user()
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/%s' % task_id, status=200)

    self.set_as_bot()
    token, _ = self._bot_token()
    reaped = self.bot_poll()
    self.bot_complete_task(token, reaped['manifest']['task_id'])
    # Add unicode chars.

    # This can only work once a bot reaped the task.
    self.set_as_privileged_user()
    self.app.get('/user/task/%s' % reaped['manifest']['task_id'], status=200)

  def test_task_denied(self):
    # Add a task via the API as a user, then assert it can't be viewed by
    # anonymous user.
    self.set_as_user()
    _, task_id = self.client_create_task('hi')

    self.set_as_anonymous()
    self.app.get('/user/tasks', status=403)
    self.app.get('/user/task/%s' % task_id, status=403)

  def test_task_list_query(self):
    # Try all the combinations of task queries to ensure the index exist.
    self.set_as_privileged_user()
    self.client_create_task('hi')

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

  def test_task_cancel(self):
    self.set_as_privileged_user()
    _, task_id = self.client_create_task('hi')

    self.set_as_admin()
    # Just ensure it doesn't crash when it shows the 'Cancel' button.
    self.app.get('/user/tasks')

    xsrf_token = self.getXsrfToken()
    self.app.post(
        '/user/tasks/cancel', {'task_id': task_id, 'xsrf_token': xsrf_token})

    # Ensure there's no task available anymore by polling.
    self.set_as_bot()
    reaped = self.bot_poll('bot1')
    self.assertEqual('sleep', reaped['cmd'])

  def test_bot_list_empty(self):
    # Just assert it doesn't throw.
    self.set_as_admin()
    self.app.get('/restricted/bots', status=200)
    self.app.get('/restricted/bot/unknown_bot', status=200)

  def test_bot_listing(self):
    # Create a task, create 2 bots, one with a task assigned, the other without.
    self.set_as_admin()
    self.client_create_task('hi')
    self.bot_poll('bot1')
    self.bot_poll('bot2')

    response = self.app.get('/restricted/bots', status=200)
    reg = re.compile(r'<a\s+href="(.+?)">Next page</a>')
    self.assertFalse(reg.search(response.body))
    self.app.get('/restricted/bot/bot1', status=200)
    self.app.get('/restricted/bot/bot2', status=200)

    response = self.app.get('/restricted/bots?limit=1', status=200)
    url = reg.search(response.body).group(1)
    self.assertTrue(
        url.startswith('/restricted/bots?limit=1&sort_by=__key__&cursor='), url)
    response = self.app.get(url, status=200)
    self.assertFalse(reg.search(response.body))

    for sort_by in handlers_frontend.BotsListHandler.ACCEPTABLE_BOTS_SORTS:
      response = self.app.get(
          '/restricted/bots?limit=1&sort_by=%s' % sort_by, status=200)
      self.assertTrue(reg.search(response.body), sort_by)


class BackendTest(AppTestBase):
  def _GetRoutes(self):
    """Returns the list of all routes handled."""
    return [
        r.template for r in self.app.app.router.match_routes
    ]

  def testCronJobTasks(self):
    # Tests all the cron tasks are securely handled.
    cron_job_urls = [
        r for r in self._GetRoutes() if r.startswith('/internal/cron/')
    ]
    self.assertTrue(cron_job_urls, cron_job_urls)

    # For ereporter.
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
    self.set_as_admin()
    response = self.app.get('/internal/cron/ereporter2/mail',
                            headers={'X-AppEngine-Cron': 'true'})
    self.assertResponse(response, '200 OK', 'Success.')

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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
