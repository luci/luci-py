#!/usr/bin/env python
# coding: utf-8
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import json
import logging
import os
import sys
import unittest
import urllib

# Setups environment.
import test_env_handlers

import webtest

import handlers_backend
import handlers_frontend
import template
from server import bot_code
from server import bot_management
from server import task_result


class AppTestBase(test_env_handlers.AppTestBase):
  @staticmethod
  def wsgi_app():
    return None

  def setUp(self):
    super(AppTestBase, self).setUp()
    template.bootstrap()
    # By default requests in tests are coming from bot with fake IP.
    self.app = webtest.TestApp(
        self.wsgi_app(),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

  def tearDown(self):
    try:
      template.reset()
    finally:
      super(AppTestBase, self).tearDown()


class FrontendTest(AppTestBase):
  @staticmethod
  def wsgi_app():
    return handlers_frontend.create_application(True)

  def test_root(self):
    response = self.app.get('/', status=200)
    self.assertGreater(len(response.body), 600)

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
      '/oldui',
      '/_ah/warmup',
      '/api/config/v1/validate',
      '/auth',
      '/ereporter2/api/v1/on_error',
      '/stats',
      '/api/swarming/v1/server/permissions',
      '/swarming/api/v1/client/list',
      '/swarming/api/v1/bot/server_ping',
      '/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
      '/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
      '/swarming/api/v1/stats/user/<user:.+>/<resolution:[a-z]+>',
      '/user/tasks',
      '/restricted/bots',
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

      headers = {}
      body = ''
      if method == 'POST' and path.startswith('/swarming/api/v1/bot/'):
        headers = {'Content-Type': 'application/json'}
        body = json.dumps({'id': 'bot-id', 'task_id': 'task_id'})

      response = getattr(self.app, method.lower())(
          path, body, expect_errors=True, headers=headers)
      message = ('%s handler is not protected: %s, '
                 'returned %s' % (method, path, response))
      self.assertIn(response.status_int, (302, 403, 405), msg=message)
      if response.status_int == 302:
        # See user_service_stub.py, _DEFAULT_LOGIN_URL.
        login_url = 'https://www.google.com/accounts/Login?continue='
        self.assertTrue(response.headers['Location'].startswith(login_url))

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
      '/swarming/api/v1/stats/summary/days',
      '/swarming/api/v1/stats/summary/hours',
      '/swarming/api/v1/stats/summary/minutes',
      '/swarming/api/v1/stats/dimensions/%s/days' % quoted,
      '/swarming/api/v1/stats/dimensions/%s/hours' % quoted,
      '/swarming/api/v1/stats/dimensions/%s/minutes' % quoted,
    )
    for url in urls:
      self.app.get(url, status=403)
    self.set_as_user()
    for url in urls:
      self.app.get(url, status=200)

  def test_task_redirect(self):
    self.set_as_anonymous()
    self.app.get('/user/tasks', status=302)
    self.app.get('/user/task/123', status=302)

  def test_bot_redirect(self):
    self.set_as_anonymous()
    self.app.get('/restricted/bots', status=302)
    self.app.get('/restricted/bot/bot321', status=302)


class FrontendAdminTest(AppTestBase):
  @staticmethod
  def wsgi_app():
    return handlers_frontend.create_application(True)

  # Admin-specific management pages.
  def test_bootstrap_default(self):
    self.set_as_bot()
    self.mock(bot_code, 'generate_bootstrap_token', lambda: 'bootstrap-token')
    actual = self.app.get('/bootstrap').body
    path = os.path.join(self.APP_DIR, 'swarming_bot', 'config', 'bootstrap.py')
    with open(path, 'rb') as f:
      expected = f.read()
    header = (
        u'#!/usr/bin/env python\n'
        'host_url = \'http://localhost\'\n'
        'bootstrap_token = \'bootstrap-token\'\n')
    self.assertEqual(header + expected, actual)

  def test_bootstrap_custom(self):
    self.set_as_admin()
    self.mock(bot_code, 'generate_bootstrap_token', lambda: 'bootstrap-token')
    xsrf_token = self.get_xsrf_token()
    self.app.get('/restricted/upload/bootstrap')
    response = self.app.post(
        '/restricted/upload/bootstrap?xsrf_token=%s' % xsrf_token,
        status=400)
    self.assertEqual(
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ', response.body)

    script = u'print(\'script_bodé\')'
    response = self.app.post(
        '/restricted/upload/bootstrap?xsrf_token=%s' % xsrf_token,
        upload_files=[
          ('script', 'script', script.encode('utf-8')),
        ])
    self.assertIn(u'script_bodé'.encode('utf-8'), response.body)

    actual = self.app.get('/bootstrap').body
    header = (
        u'#!/usr/bin/env python\n'
        u'host_url = \'http://localhost\'\n'
        u'bootstrap_token = \'bootstrap-token\'\n')
    expected =  (header + script).encode('utf-8')
    self.assertEqual(expected, actual)

  def test_upload_bot_config(self):
    self.set_as_admin()
    xsrf_token = self.get_xsrf_token()
    self.app.get('/restricted/upload/bot_config')
    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        status=400)
    self.assertEqual(
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ', response.body)

    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        upload_files=[
          ('script', 'script', u'print(\'script_bodé\')'.encode('utf-8')),
        ])
    self.assertIn(u'script_bodé'.encode('utf-8'), response.body)
    # TODO(maruel): Assert swarming_bot.zip now contains the new code.

  def test_config(self):
    self.set_as_admin()
    self.app.get('/restricted/config')


class BackendTest(AppTestBase):
  @staticmethod
  def wsgi_app():
    return handlers_backend.create_application(True)

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
      self.app.get(
          cron_job_url, headers={'X-AppEngine-Cron': 'true'}, status=200)

      # Only cron job requests can be gets for this handler.
      response = self.app.get(cron_job_url, status=403)
      self.assertEqual(
          '403 Forbidden\n\nAccess was denied to this resource.\n\n '
          'Only internal cron jobs can do this  ',
          response.body)
    # The actual number doesn't matter, just make sure they are unqueued.
    self.execute_tasks()

  def testCronBotsAggregateTask(self):
    # Tests that the aggregation works
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['beta'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['alpha'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, task_id='987', task_name=None)

    self.app.get('/internal/cron/aggregate_bots_dimensions',
        headers={'X-AppEngine-Cron': 'true'}, status=200)
    actual = bot_management.DimensionAggregation.KEY.get()
    expected = bot_management.DimensionAggregation(
        key=bot_management.DimensionAggregation.KEY,
        dimensions=[
            bot_management.DimensionValues(
                dimension='foo', values=['alpha', 'beta'])
        ],
        ts=now)
    self.assertEqual(expected, actual)

  def testCronTagsAggregateTask(self):
    self.set_as_admin()
    now = datetime.datetime(2011, 1, 2, 3, 4, 5)
    self.mock_now(now)

    self.client_create_task_raw(tags=['alpha:beta', 'gamma:delta'])
    self.client_create_task_raw(tags=['alpha:epsilon', 'zeta:theta'])

    self.app.get('/internal/cron/aggregate_tasks_tags',
        headers={'X-AppEngine-Cron': 'true'}, status=200)
    actual = task_result.TagAggregation.KEY.get()
    expected = task_result.TagAggregation(
        key=task_result.TagAggregation.KEY,
        tags=[
            task_result.TagValues(tag='alpha', values=['beta', 'epsilon']),
            task_result.TagValues(tag='gamma', values=['delta']),
            task_result.TagValues(tag='os', values=['Amiga']),
            task_result.TagValues(tag='pool', values=['default']),
            task_result.TagValues(tag='priority', values=['10']),
            task_result.TagValues(tag='service_account', values=['none']),
            task_result.TagValues(tag='user', values=['joe@localhost']),
            task_result.TagValues(tag='zeta', values=['theta']),
        ],
        ts=now)
    self.assertEqual(expected, actual)

  def testTaskQueueUrls(self):
    # Tests all the task queue tasks are securely handled.
    # TODO(maruel): Test mapreduce.
    task_queue_urls = sorted(
      r for r in self._GetRoutes() if r.startswith('/internal/taskqueue/')
      if not r.startswith('/internal/taskqueue/mapreduce/launch/')
    )
    # Format: (<queue-name>, <base-url>, <argument>).
    task_queues = [
      ('cancel-tasks', '/internal/taskqueue/cancel-tasks', ''),
      ('machine-provider-manage',
       '/internal/taskqueue/machine-provider-manage', ''),
      ('pubsub', '/internal/taskqueue/pubsub/', 'abcabcabc'),
      ('tsmon', '/internal/taskqueue/tsmon/', 'executors'),
    ]
    self.assertEqual(len(task_queues), len(task_queue_urls))
    for i, url in enumerate(task_queue_urls):
      self.assertTrue(
          url.startswith(task_queues[i][1]),
          '%s does not start with %s' % (url, task_queues[i][1]))

    for _, url, arg in task_queues:
      self.app.post(
          url+arg, headers={'X-AppEngine-QueueName': 'bogus name'}, status=403)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
