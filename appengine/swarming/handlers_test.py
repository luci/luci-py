#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import itertools
import json
import logging
import os
import re
import StringIO
import sys
import unittest
import urllib
import zipfile

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env
test_env.setup_test_env()

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import deferred
from google.appengine.ext import ndb

import webtest

import handlers_frontend
from common import test_request_message
from components import auth
from components import ereporter2
from components import stats_framework
from components import template
from components import utils
from server import acl
from server import bot_archive
from server import bot_management
from server import stats
from server import task_common
from server import task_result
from server import task_scheduler
from server import task_to_run
from server import user_manager
from support import test_case


# remote_addr of a fake bot that makes requests in tests.
FAKE_IP = '192.168.0.0'


def clear_ip_whitelist():
  """Removes all IPs from the whitelist."""
  entries = user_manager.MachineWhitelist.query().fetch(keys_only=True)
  ndb.delete_multi(entries)


def GetRequestMessage(requestor=None):
  """Return a properly formatted request message text.

  Returns:
    A properly formatted request message text.
  """
  tests = [
    test_request_message.TestObject(test_name='t1', action=['ignore-me.exe']),
  ]
  configurations = [
    test_request_message.TestConfiguration(
        config_name='c1_0',
        dimensions=dict(os='win-xp', cpu='Unknown', browser='Unknown'),
        num_instances=1,
        priority=10)
  ]
  request = test_request_message.TestCase(
      restart_on_failure=False,
      requestor=requestor,
      test_case_name='tc',
      configurations=configurations,
      data=[('http://b.ina.ry/files2.zip', 'files2.zip')],
      env_vars=None,
      tests=tests)
  return test_request_message.Stringize(request, json_readable=True)


def CreateRunner(config_name=None, machine_id=None, ran_successfully=None,
                 exit_codes=None, created=None, started=None,
                 ended=None, requestor=None, results=None):
  """Creates entities to represent a new task request and a bot running it.

  The entities are TaskRequest, TaskToRun, TaskResultSummary, TaskRunResult,
  ResultChunk and Results.

  The entity may be in a pending, running or completed state.

  Args:
    config_name: The config name for the runner.
    machine_id: The machine id for the runner. Also, if this is given the
      runner is marked as having started.
    ran_succesfully: True if the runner ran successfully.
    exit_codes: The exit codes for the runner. Also, if this is given the
      runner is marked as having finished.
    created: timedelta from now to mark this request was created in the future.
    started: timedelta from .created to when the task execution started.
    ended: timedelta from .started to when the bot completed the task.
    requestor: string representing the user name that requested the task.
    results: string that represents stdout the bot sent back.

  Returns:
    The pending runner created.
  """
  config_name = config_name or ('c1_0')
  request_message = GetRequestMessage(requestor=requestor)
  data = handlers_frontend.convert_test_case(request_message)
  request, result_summary = task_scheduler.make_request(data)
  if created:
    # For some reason, pylint is being obnoxious here and generates a W0201
    # warning that cannot be disabled. See http://www.logilab.org/19607
    setattr(request, 'created_ts', datetime.datetime.utcnow() + created)
    request.put()

  # The task is reaped by a bot, so it is in a running state. Pending state is
  # not supported here.
  task_key = task_to_run.request_to_task_to_run_key(request)
  task = task_to_run.is_task_reapable(task_key, None)
  task.put()

  machine_id = machine_id or 'localhost'
  run_result = task_result.new_run_result(request, 1, machine_id)

  if ran_successfully or exit_codes:
    exit_codes = map(int, exit_codes.split(',')) if exit_codes else [0]
    # The entity needs to be saved before it can be updated, since
    # bot_update_task() accepts the key of the entity.
    ndb.put_multi(task_result.prepare_put_run_result(run_result))
    for index, code in enumerate(exit_codes):
      with task_scheduler.bot_update_task(
          run_result.key, machine_id, index, results, 0, code, 0.1) as entities:
        ndb.put_multi(entities)
    # Refresh it from the DB.

  if started:
    run_result.started_ts = run_result.created + started
  if ended:
    run_result.completed_ts = run_result.started_ts + ended

  # Mark the job as at least running.
  ndb.put_multi(task_result.prepare_put_run_result(run_result))

  return result_summary, run_result


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
    return self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json['xsrf_token']

  def _client_token(self):
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers,
        params=params).json
    token = response['xsrf_token'].encode('ascii')
    return token

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
    token = response['xsrf_token'].encode('ascii')
    params['attributes']['version'] = response['bot_version']
    params['sleep_streak'] = 0
    return token, params

  def post_with_token(self, url, params, token):
    """Does an HTTP POST with a JSON API and a XSRF token."""
    return self.app.post_json(
        url, params=params, headers={'X-XSRF-Token': token}).json

  def bot_poll(self, bot='bot1'):
    """Simulates a bot that polls for task."""
    token, params = self._bot_token(bot)
    return self.post_with_token('/swarming/api/v1/bot/poll', params, token)

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
    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {}, '123456789', False)
    bot_management.tag_bot_seen(
        'id2', 'localhost:8080', '127.0.0.2', '8.8.8.8', {'foo': 'bar'},
        '123456789', False)

    response = self.app.get('/restricted/bots')
    self.assertTrue('200' in response.status)

  def testDeleteMachineStats(self):
    self.set_as_admin()

    # Add a machine assignment to delete.
    bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)

    # Delete the machine assignment.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '200 OK', 'Machine Assignment removed.')

    # Attempt to delete the assignment again and silently pass.
    response = self.app.post('/delete_machine_stats', {'r': 'id1'})
    self.assertResponse(response, '204 No Content', '')

  def testMainHandler(self):
    response = self.app.get('/')
    self.assertEqual('200 OK', response.status)

  def testUploadStartSlaveHandler(self):
    self.set_as_admin()
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
    with open(os.path.join(APP_DIR, 'swarm_bot/bootstrap.py'), 'rb') as f:
      expected = f.read()
    header = 'host_url = \'http://localhost\'\n'
    self.assertEqual(header + expected, actual)

  def test_bootstrap_custom(self):
    # Act under admin identity.
    self.set_as_admin()
    self.app.get('/restricted/upload_bootstrap')
    data = {
      'script': 'script_body',
      'xsrf_token': self.getXsrfToken(),
    }
    r = self.app.post('/restricted/upload_bootstrap', data)
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
    reaped = self.bot_poll('bot1')

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


class NewBotApiTest(AppTestBase):
  def setUp(self):
    super(NewBotApiTest, self).setUp()
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Bot API test cases run by default as bot.
    self.set_as_bot()

  def client_get_results(self, task_id):
    return self.app.get(
        '/swarming/api/v1/client/task/%s' % task_id).json

  def test_handshake(self):
    # Bare minimum:
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'attributes': {
        'id': 'bot1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected attributes missing: [u\'dimensions\', u\'ip\']; did you make '
          'a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_handshake_extra(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      # Works with unknown items but logs an error. This permits catching typos.
      'foo': 1,
      'attributes': {
        'bar': 2,
        'id': 'bot1',
        'ip': '127.0.0.1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
      'Unexpected attributes missing: [u\'dimensions\'] superfluous: [u\'bar\']'
          '; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_poll_bad_version(self):
    token, params = self._bot_token()
    old_version = params['attributes']['version']
    params['attributes']['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_sleep(self):
    # A bot polls, gets nothing.
    token, params = self._bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # Sleep again
    params['sleep_streak'] += 1
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

  def test_poll_update(self):
    token, params = self._bot_token()
    old_version = params['attributes']['version']
    params['attributes']['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_task(self):
    # Successfully poll a task.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    token, params = self._bot_token()
    # Enqueue a task.
    _, task_id = self.client_create_task('hi')
    self.assertEqual('00', task_id[-2:])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-2] + '01'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'commands': [[u'python', u'run_test.py']],
        u'data': [],
        u'env': {},
        u'hard_timeout': 3600,
        u'io_timeout': 1200,
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)
    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'outputs': [],
      u'started_ts': str_now,
      u'state': task_result.State.RUNNING,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_bot_error(self):
    token, params = self._bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # The bot fails somehow.
    error_params = {
      'id': params['attributes']['id'],
      'message': 'Something happened',
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/error', error_params, token)
    self.assertEqual({}, response)

    # A bot error currently does not result in permanent quarantine. It will
    # eventually.
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

  def test_update(self):
    # Runs a task with 2 commands up to completion.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self._bot_token()
    self.client_create_task('hi', extra_command=['cleanup.py'])

    def _params(**kwargs):
      out = {
        'command_index': 0,
        'duration': None,
        'exit_code': None,
        'id': 'bot1',
        'output': None,
        'output_chunk_start': 0,
        'task_id': task_id,
      }
      out.update(**kwargs)
      return out

    def _expected(**kwargs):
      out = {
        u'abandoned_ts': None,
        u'bot_id': u'bot1',
        u'completed_ts': None,
        u'durations': [],
        u'exit_codes': [],
        u'failure': False,
        u'internal_failure': False,
        u'modified_ts': str_now,
        u'outputs': [],
        u'started_ts': str_now,
        u'state': task_result.State.RUNNING,
        u'try_number': 1,
      }
      out.update(**kwargs)
      return out

    def _cycle(params, expected):
      response = self.post_with_token(
          '/swarming/api/v1/bot/task_update', params, token)
      self.assertEqual({u'ok': True}, response)
      response = self.client_get_results(task_id)
      self.assertEqual(expected, response)

    # 1. Initial task update with no data.
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']
    params = _params()
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)
    response = self.client_get_results(task_id)
    self.assertEqual(_expected(), response)

    # 2. Task update with some output.
    params = _params(output='Oh ')
    expected = _expected(outputs=[u'Oh '])
    _cycle(params, expected)

    # 3. Task update with some more output.
    params = _params(output='hi', output_chunk_start=3)
    expected = _expected(outputs=[u'Oh hi'])
    _cycle(params, expected)

    # 4. Task update with completion of first command.
    params = _params(duration=0.2, exit_code=0)
    expected = _expected(exit_codes=[0], outputs=[u'Oh hi'])
    expected = _expected(durations=[0.2], exit_codes=[0], outputs=[u'Oh hi'])
    _cycle(params, expected)

    # 5. Task update with completion of second command along with full output.
    params = _params(
        command_index=1, duration=0.1, exit_code=23, output='Ahaha')
    expected = _expected(
        completed_ts=str_now,
        durations=[0.2, 0.1],
        exit_codes=[0, 23],
        failure=True,
        outputs=[u'Oh hi', u'Ahaha'],
        state=task_result.State.COMPLETED)
    _cycle(params, expected)

  def test_task_error(self):
    # E.g. local_test_runner blew up.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self._bot_token()
    self.client_create_task('hi')
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    # Let's say it failed to start local_test_runner because the new bot code is
    # broken. The end result is still BOT_DIED. The big change is that it
    # doesn't need to wait for a cron job to set this status.
    params = {
      'id': params['attributes']['id'],
      'message': 'Oh',
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_error', params, token)

    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': str_now,
      u'bot_id': u'bot1',
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'internal_failure': True,
      u'modified_ts': str_now,
      u'outputs': [],
      u'started_ts': str_now,
      u'state': task_result.State.BOT_DIED,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_get_slave_code(self):
    code = self.app.get('/get_slave_code')
    expected = set(('config.json', 'start_slave.py')).union(bot_archive.FILES)
    with zipfile.ZipFile(StringIO.StringIO(code.body), 'r') as z:
      self.assertEqual(expected, set(z.namelist()))


class NewClientApiTest(AppTestBase):
  def setUp(self):
    super(NewClientApiTest, self).setUp()
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()

  def test_handshake(self):
    # Bare minimum:
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers, params=params).json
    self.assertEqual(
        [u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(u'default-version', response['server_version'])

  def test_handshake_extra(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('client', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      # Works with unknown items but logs an error. This permits catching typos.
      'foo': 1,
    }
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers, params=params).json
    self.assertEqual(
        [u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_get_results_unknown(self):
    response = self.app.get(
        '/swarming/api/v1/client/task/12300', status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_get_results(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    # Note: this is still the old API.
    _, task_id = self.client_create_task('hi')
    response = self.app.get(
        '/swarming/api/v1/client/task/' + task_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_id': None,
      u'completed_ts': None,
      u'created_ts': str_now,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'outputs': [],
      u'started_ts': None,
      u'state': task_result.State.PENDING,
      u'try_number': None,
      u'user': u'unknown',
    }
    self.assertEqual(expected, response)
    self.assertEqual('00', task_id[-2:])

    # No bot started yet.
    run_id = task_id[:-2] + '01'
    response = self.app.get(
        '/swarming/api/v1/client/task/' + run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

    self.set_as_bot()
    self.bot_poll('bot1')

    self.set_as_user()
    response = self.app.get(
        '/swarming/api/v1/client/task/' + run_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'outputs': [],
      u'started_ts': str_now,
      u'state': task_result.State.RUNNING,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_get_results_denied(self):
    # Asserts that a non-public task can not be seen by an anonymous user.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    # Note: this is still the old API.
    _, task_id = self.client_create_task('hi')

    self.set_as_anonymous()
    self.app.get('/swarming/api/v1/client/task/' + task_id, status=403)
    self.assertEqual('00', task_id[-2:])

  def test_api_bots(self):
    self.set_as_admin()
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)
    bot.put()

    actual = self.app.get('/swarming/api/v1/client/bots', status=200).json
    expected = {
      u'bots': [
        {
          u'created_ts': u'2000-01-02 03:04:05',
          u'dimensions': {u'foo': u'bar'},
          u'external_ip': u'8.8.4.4',
          u'hostname': u'localhost',
          u'id': u'id1',
          u'internal_ip': u'127.0.0.1',
          u'is_dead': False,
          u'last_seen_ts': u'2000-01-02 03:04:05',
          u'quarantined': False,
          u'task': None,
          u'version': u'123456789',
        },
      ],
      u'cursor': None,
      u'death_timeout': bot_management.BOT_DEATH_TIMEOUT.total_seconds(),
      u'limit': 1000,
      u'now': unicode(now.strftime(utils.DATETIME_FORMAT)),
    }
    self.assertEqual(expected, actual)

    # Test with limit.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['limit'] = 1
    self.assertEqual(expected, actual)

    bot = bot_management.tag_bot_seen(
        'id2', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)
    bot.put()

    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['cursor'] = actual['cursor']
    self.assertTrue(actual['cursor'])
    self.assertEqual(expected, actual)

    # Test with cursor.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1&cursor=%s' % actual['cursor'],
        status=200).json
    expected['cursor'] = None
    expected['bots'][0]['id'] = u'id2'
    self.assertEqual(expected, actual)


class OldClientApiTest(AppTestBase):
  def setUp(self):
    super(OldClientApiTest, self).setUp()
    # Client API test cases run by default as user.
    self.set_as_user()

  def _check_task(self, task, priority):
    # The value is using both timestamp and random value, so it is not
    # deterministic by definition.
    task_id = task['test_keys'][0].pop('test_key')
    self.assertTrue(int(task_id, 16))
    self.assertEqual('00', task_id[-2:])
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

  def test_add_task_admin(self):
    # Admins can trigger high priority tasks.
    self.set_as_admin()
    task, _ = self.client_create_task('hi')
    self._check_task(task, 10)

  def test_add_task_bot(self):
    # The bot has access to use high priority. By default on dev server
    # localhost is whitelisted as a bot.
    self.set_as_bot()
    task, _ = self.client_create_task('hi')
    self._check_task(task, 10)

  def test_add_task_and_list_user(self):
    task, _ = self.client_create_task('hi')
    # Since the priority 10 was too high for a user (not an admin, neither a
    # bot), it was reset to 100.
    self._check_task(task, 100)

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
        {'name': 'tc'},
        expect_errors=True)
    self.assertResponse(response, '404 Not Found', '[]')

    # Test with a single matching runner.
    result_summary, _run_result = CreateRunner('tc')
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': 'tc'})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)

    # Test with a multiple matching runners.
    result_summary_2, _run_result = CreateRunner(
        machine_id='foo', exit_codes='0', results='Rock on')

    response = self.app.get(
        '/get_matching_test_cases',
        {'name': 'tc'})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary_2.key),
        response.body)

  def testGetResultHandler(self):
    self.set_as_privileged_user()

    # Test when no matching key
    response = self.app.get('/get_result', {'r': 'fake_key'}, status=400)

    # Create test and runner.
    result_summary, run_result = CreateRunner(
        machine_id='bot1',
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
        o.decode('utf-8', 'replace')
        for o in result_summary.get_outputs())
    self.assertEqual(expected_result_string, results['output'])
    self.assertEqual(u'\ufffd Invalid utf-8 string', results['output'])

  def testRetryHandler(self):
    self.set_as_admin()

    # Test when no matching key
    response = self.app.post(
        '/restricted/retry', {'r': 'fake_key'}, expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Unable to retry runner', response.body)

    # Test with matching key.
    result_summary, _run_result = CreateRunner(exit_codes='0')
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.post('/restricted/retry', {'r': packed})
    self.assertResponse(response, '200 OK', 'Runner set for retry.')

  def test_convert_test_case(self):
    self.set_as_bot()
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
      'tags': [],
    }
    self.assertEqual(expected, actual)

  def testCancelHandler(self):
    self.set_as_admin()

    response = self.app.post(
        '/restricted/cancel', {'r': 'invalid_key'}, expect_errors=True)
    self.assertEqual('400 Bad Request', response.status)
    self.assertEqual('Unable to cancel runner', response.body)

    result_summary, _run_result = CreateRunner()
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.post('/restricted/cancel', {'r': packed})
    self.assertResponse(response, '200 OK', 'Runner canceled.')


  def testResultHandlerNotDone(self):
    result_summary, _run_result = CreateRunner(machine_id='bot1')

    packed = task_common.pack_result_summary_key(result_summary.key)
    resp = self.app.get('/get_result?r=%s' % packed, status=200)
    expected = {
      u'config_instance_index': 0,
      u'exit_codes': None,
      u'machine_id': u'bot1',
      u'machine_tag': u'bot1',
      u'num_config_instances': 1,
      u'output': None,
    }
    self.assertEqual(expected, resp.json)

  def testResultHandler(self):
    # TODO(maruel): Stop using the DB directly.
    self.set_as_bot()
    self.client_create_task('hi')
    token, _ = self._bot_token()
    res = self.bot_poll()
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 1,
      'id': 'bot1',
      'output': 'result string',
      'output_chunk_start': 0,
      'task_id': res['manifest']['task_id'],
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

    self.set_as_privileged_user()
    resp = self.app.get(
        '/get_result?r=%s' % res['manifest']['task_id'], status=200)
    expected = {
      u'config_instance_index': 0,
      u'exit_codes': u'1',
      u'machine_id': u'bot1',
      u'machine_tag': u'bot1',
      u'num_config_instances': 1,
      u'output': u'result string',
    }
    self.assertEqual(expected, resp.json)

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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
