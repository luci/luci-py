#!/usr/bin/env python
# coding: utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import itertools
import logging
import os
import re
import sys
import unittest
import urllib

# Setups environment.
import test_env_handlers

import webtest

from google.appengine.ext import deferred

import handlers_frontend
from components import template
from server import bot_management
from server import config
from server import user_manager


class AppTestBase(test_env_handlers.AppTestBase):
  def setUp(self):
    super(AppTestBase, self).setUp()
    # By default requests in tests are coming from bot with fake IP.
    app = handlers_frontend.create_application(True)
    app.router.add(('/_ah/queue/deferred', deferred.TaskHandler))
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': self.fake_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

  def tearDown(self):
    try:
      template.reset()
    finally:
      super(AppTestBase, self).tearDown()


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
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1']}, state=state, version='123456789',
        quarantined=False, task_id=None, task_name=None)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2', external_ip='8.8.8.8',
        dimensions={'id': ['id2']}, state={'ram': 65}, version='123456789',
        quarantined=False, task_id=None, task_name=None)

    response = self.app.get('/restricted/bots', status=200)
    self.assertGreater(len(response.body), 1000)

  def test_delete_bot(self):
    self.set_as_admin()

    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1']}, state={'foo': 'bar'}, version='123456789',
        quarantined=False, task_id=None, task_name=None)
    response = self.app.get('/restricted/bots', status=200)
    self.assertTrue('id1' in response.body)

    response = self.app.post(
        '/restricted/bot/id1/delete',
        params={},
        headers={'X-XSRF-Token': self.get_xsrf_token()})
    self.assertFalse('id1' in response.body)

    response = self.app.get('/restricted/bots', status=200)
    self.assertFalse('id1' in response.body)

  def test_root(self):
    response = self.app.get('/', status=200)
    self.assertGreater(len(response.body), 1000)

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

  def test_task_list_empty(self):
    # Just assert it doesn't throw.
    self.set_as_privileged_user()
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/12345', status=404)

  def test_add_task_and_list_user(self):
    # Add a task via the API as a user, then assert it can be viewed.
    self.set_as_user()
    _, task_id = self.client_create_task()

    self.set_as_privileged_user()
    self.app.get('/user/tasks', status=200)
    self.app.get('/user/task/%s' % task_id, status=200)

    self.set_as_bot()
    token, _ = self.get_bot_token()
    reaped = self.bot_poll()
    self.bot_complete_task(token, task_id=reaped['manifest']['task_id'])
    # Add unicode chars.

    # This can only work once a bot reaped the task.
    self.set_as_privileged_user()
    self.app.get('/user/task/%s' % reaped['manifest']['task_id'], status=200)

  def test_task_deduped(self):
    self.set_as_user()
    _, task_id_1 = self.client_create_task(properties=dict(idempotent=True))

    self.set_as_bot()
    task_id_bot = self.bot_run_task()
    self.assertEqual(task_id_1, task_id_bot[:-1] + '0')
    self.assertEqual('1', task_id_bot[-1:])

    # Create a second task. Results will be returned immediately without the bot
    # running anything.
    self.set_as_user()
    _, task_id_2 = self.client_create_task(
        name='ho', properties=dict(idempotent=True))

    self.set_as_bot()
    resp = self.bot_poll()
    self.assertEqual('sleep', resp['cmd'])

    self.set_as_privileged_user()
    # Look at the results. It's the same as the previous run, even if task_id_2
    # was never executed.
    response = self.app.get('/user/task/%s' % task_id_2, status=200)
    self.assertTrue('bar' in response.body, response.body)
    self.assertTrue('Was deduped from' in response.body, response.body)

  def test_task_denied(self):
    # Add a task via the API as a user, then assert it can't be viewed by
    # anonymous user.
    self.set_as_user()
    _, task_id = self.client_create_task()

    self.set_as_anonymous()
    self.app.get('/user/tasks', status=403)
    self.app.get('/user/task/%s' % task_id, status=403)

  @staticmethod
  def _sort_state_product():
    sort_choices = [i[0] for i in handlers_frontend.TasksHandler.SORT_CHOICES]
    state_choices = sum(
        ([i[0] for i in j]
          for j in handlers_frontend.TasksHandler.STATE_CHOICES),
        [])
    return itertools.product(sort_choices, state_choices)

  def test_task_list_query(self):
    # Try all the combinations of task queries to ensure the index exist.
    self.set_as_privileged_user()
    self.client_create_task()
    for sort, state in self._sort_state_product():
      url = '/user/tasks?sort=%s&state=%s' % (sort, state)
      # See require_index in ../components/support/test_case.py in case of
      # NeedIndexError. Do not use status=200 so the output is printed in case
      # of failure.
      resp = self.app.get(url, expect_errors=True)
      self.assertEqual(200, resp.status_code, (resp.body, sort, state))

    self.app.get('/user/tasks?sort=foo', status=400)
    self.app.get('/user/tasks?state=foo', status=400)

  def test_task_search_task_name(self):
    # Try all the combinations of task queries to ensure the index exist.
    self.set_as_privileged_user()
    self.client_create_task()
    self.app.get('/user/tasks?task_name=hi', status=200)
    for sort, state in self._sort_state_product():
      url = '/user/tasks?sort=%s&state=%s' % (sort, state)
      self.app.get(url + '&task_name=hi', status=200)

  def test_task_search_task_tag(self):
    # Try all the combinations of task queries to ensure the index exist.
    self.set_as_privileged_user()
    self.client_create_task()
    self.set_as_bot()
    token, _ = self.get_bot_token()
    reaped = self.bot_poll()
    self.bot_complete_task(token, task_id=reaped['manifest']['task_id'])
    self.set_as_privileged_user()
    self.app.get('/user/tasks?task_tag=yo:dawg', status=200)
    for sort, state in self._sort_state_product():
      url = '/user/tasks?sort=%s&state=%s' % (sort, state)
      self.app.get(url + '&task_tag=yo:dawg', status=200)

  def test_task_cancel(self):
    self.set_as_privileged_user()
    _, task_id = self.client_create_task()

    self.set_as_admin()
    # Just ensure it doesn't crash when it shows the 'Cancel' button.
    self.app.get('/user/tasks')

    xsrf_token = self.get_xsrf_token()
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
    self.client_create_task()
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


class FrontendAdminTest(AppTestBase):
  # Admin-specific management pages.
  def test_bootstrap_default(self):
    self.set_as_bot()
    actual = self.app.get('/bootstrap').body
    path = os.path.join(self.APP_DIR, 'swarming_bot/bootstrap.py')
    with open(path, 'rb') as f:
      expected = f.read()
    header = 'host_url = \'http://localhost\'\n'
    self.assertEqual(header + expected, actual)

  def test_bootstrap_custom(self):
    # Act under admin identity.
    self.set_as_admin()
    self.app.get('/restricted/upload/bootstrap')
    data = {
      'script': 'script_body',
      'xsrf_token': self.get_xsrf_token(),
    }
    r = self.app.post('/restricted/upload/bootstrap', data)
    self.assertIn('script_body', r.body)

    actual = self.app.get('/bootstrap').body
    expected = 'host_url = \'http://localhost\'\nscript_body'
    self.assertEqual(expected, actual)

  def test_upload_bot_config(self):
    self.set_as_admin()
    xsrf_token = self.get_xsrf_token()
    response = self.app.get('/restricted/upload/bot_config')
    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        status=400)
    self.assertEqual(
        '400 Bad Request\n\nThe server could not comply with the request since '
        'it is either malformed or otherwise incorrect.\n\n No script uploaded'
        '  ', response.body)

    response = self.app.post(
        '/restricted/upload/bot_config?xsrf_token=%s' % xsrf_token,
        upload_files=[('script', 'script', 'script_body')])
    self.assertIn('script_body', response.body)
    # TODO(maruel): Assert swarming_bot.zip now contains the new code.

  def test_config(self):
    self.set_as_admin()
    self.app.get('/restricted/config')
    params = {
      'google_analytics': 'foobar',
      'xsrf_token': self.get_xsrf_token(),
    }
    self.app.post('/restricted/config', params)
    self.assertEqual('foobar', config.settings().google_analytics)
    self.assertIn('foobar', self.app.get('/').body)

  def testWhitelistIPHandlerParams(self):
    self.set_as_admin()

    # Make sure the template renders.
    self.app.get('/restricted/whitelist_ip', {}, status=200)
    xsrf_token = self.get_xsrf_token()

    # Make sure the link redirects to the right place.
    self.assertEqual(
        [],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])
    self.app.post(
        '/restricted/whitelist_ip', {'a': 'True', 'xsrf_token': xsrf_token},
        extra_environ={'REMOTE_ADDR': 'foo'}, status=200)
    self.assertEqual(
        [{'ip': u'foo'}],
        [t.to_dict() for t in user_manager.MachineWhitelist.query().fetch()])

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
    xsrf_token = self.get_xsrf_token()

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

  def testSendEReporter(self):
    self.set_as_admin()
    response = self.app.get(
        '/internal/cron/ereporter2/mail', headers={'X-AppEngine-Cron': 'true'})
    self.assertEqual('Success.', response.body)

  def testCronTriggerTask(self):
    triggers = (
      '/internal/cron/trigger_cleanup_data',
    )

    for url in triggers:
      response = self.app.get(
          url, headers={'X-AppEngine-Cron': 'true'}, status=200)
      self.assertEqual('Success.', response.body)
      self.assertEqual(1, self.execute_tasks())

  def testTaskQueueUrls(self):
    # Tests all the cron tasks are securely handled.
    # TODO(maruel): Test mapreduce.
    task_queue_urls = sorted(
      r for r in self._GetRoutes() if r.startswith('/internal/taskqueue/')
      if r != '/internal/taskqueue/mapreduce/launch/<job_id:[^\\/]+>'
    )
    task_queues = [
      ('cleanup', '/internal/taskqueue/cleanup_data'),
    ]
    self.assertEqual(sorted(zip(*task_queues)[1]), task_queue_urls)

    for task_name, url in task_queues:
      response = self.app.post(
          url, headers={'X-AppEngine-QueueName': task_name}, status=200)
      self.assertEqual('Success.', response.body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
