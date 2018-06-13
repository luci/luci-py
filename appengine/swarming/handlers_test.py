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

# Setups environment.
import test_env_handlers

import webtest

import handlers_backend
import handlers_frontend
import template
from components import utils
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

  def test_all_swarming_handlers_secured(self):
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
      '/api/discovery/v1/apis',
      '/api/static/proxy.html',
      '/api/swarming/v1/server/permissions',
      '/swarming/api/v1/client/list',
      '/swarming/api/v1/bot/server_ping',
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

    # Produces a body to POST to a /swarming/api/v1/bot/* request.
    def fake_body_for_bot_request(path):
      body = {'id': 'bot-id', 'task_id': 'task_id'}
      if path == '/swarming/api/v1/bot/oauth_token':
        body.update({'account_id': 'system', 'scopes': ['a', 'b']})
      return body

    # Helper function that executes GET or POST handler for corresponding route
    # and asserts it returns 403 or 405.
    def check_protected(route, method):
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
        body = json.dumps(fake_body_for_bot_request(path))

      response = getattr(self.app, method.lower())(
          path, body, expect_errors=True, headers=headers)
      message = ('%s handler is not protected: %s, '
                 'returned %s' % (method, path, response))
      self.assertIn(response.status_int, (302, 403, 405), msg=message)
      if response.status_int == 302:
        # There's two reasons, either login or redirect to api-explorer.
        options = (
          # See user_service_stub.py, _DEFAULT_LOGIN_URL.
          'https://www.google.com/accounts/Login?continue=',
          'https://apis-explorer.appspot.com/apis-explorer',
        )
        self.assertTrue(
            response.headers['Location'].startswith(options), route)

    self.set_as_anonymous()
    # Try to execute 'get' and 'post' and verify they fail with 403 or 405.
    for route in routes_to_check:
      if '<' in route.template:
        # Sadly, the url cannot be used as-is. Figure out a way to test them
        # easily.
        continue
      check_protected(route, 'GET')
      check_protected(route, 'POST')

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
        '# coding: utf-8\n'
        'host_url = \'http://localhost\'\n'
        'bootstrap_token = \'bootstrap-token\'\n')
    self.assertEqual(header + expected, actual)

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

  def setUp(self):
    super(BackendTest, self).setUp()
    self._enqueue_task_orig = self.mock(
        utils, 'enqueue_task', self._enqueue_task)

  def _enqueue_task(self, url, **kwargs):
    return self._enqueue_task_orig(url, use_dedicated_module=False, **kwargs)

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
        version='123456789', quarantined=False, maintenance_msg=None,
        task_id=None, task_name=None)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['alpha'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, maintenance_msg=None,
        task_id='987', task_name=None)

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
    self.assertEqual(1, self.execute_tasks())
    self.client_create_task_raw(tags=['alpha:epsilon', 'zeta:theta'])
    self.assertEqual(0, self.execute_tasks())

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
            task_result.TagValues(tag='priority', values=['20']),
            task_result.TagValues(tag='service_account', values=['none']),
            task_result.TagValues(
                tag='swarming.pool.template', values=['no_config']),
            task_result.TagValues(tag='user', values=['joe@localhost']),
            task_result.TagValues(tag='zeta', values=['theta']),
        ],
        ts=now)
    self.assertEqual(expected, actual)

  def testCronCountTaskBotDistributionHandler(self):
    self.set_as_admin()
    now = datetime.datetime(2011, 1, 2, 3, 4, 5)
    self.mock_now(now)

    self.client_create_task_raw(tags=['alpha:beta', 'gamma:delta'])
    self.assertEqual(1, self.execute_tasks())
    self.client_create_task_raw(tags=['alpha:epsilon', 'zeta:theta'])
    self.assertEqual(0, self.execute_tasks())

    self.app.get('/internal/cron/count_task_bot_distribution',
        headers={'X-AppEngine-Cron': 'true'}, status=200)

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
      ('rebuild-task-cache', '/internal/taskqueue/rebuild-task-cache', ''),
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
