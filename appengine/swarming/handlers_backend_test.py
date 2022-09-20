#!/usr/bin/env vpython
# coding: utf-8
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import json
import logging
import os
import sys
import unittest

# Sets up environment.
import test_env_handlers

import webapp2
from google.appengine.ext import ndb

import webtest

import handlers_backend
from components import utils
from server import bot_management
from server import task_queues
from server import task_request
from server import task_result


class BackendTest(test_env_handlers.AppTestBase):
  # These test fail with 'AppError: Bad response: 500 Internal Server Error'
  # Need to run in sequential_test_runner.py
  no_run = 1

  def _GetRoutes(self, prefix):
    """Returns the list of all routes handled."""
    return [
        r for r in self.app.app.router.match_routes
        if r.template.startswith(prefix)
    ]

  def setUp(self):
    super(BackendTest, self).setUp()
    # By default requests in tests are coming from bot with fake IP.
    self.app = webtest.TestApp(
        handlers_backend.create_application(True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self._enqueue_task_orig = self.mock(
        utils, 'enqueue_task', self._enqueue_task)
    self._enqueue_task_async_orig = self.mock(utils, 'enqueue_task_async',
                                              self._enqueue_task_async)

  def _enqueue_task(self, *args, **kwargs):
    return self._enqueue_task_orig(*args, use_dedicated_module=False, **kwargs)

  def _enqueue_task_async(self, *args, **kwargs):
    kwargs['use_dedicated_module'] = False
    return self._enqueue_task_async_orig(*args, **kwargs)

  def test_crons(self):
    # Tests all the cron tasks are securely handled.
    prefix = '/internal/cron/'
    cron_job_urls = [r.template for r in self._GetRoutes(prefix)]
    self.assertTrue(cron_job_urls)

    for cron_job_url in cron_job_urls:
      rest = cron_job_url[len(prefix):]
      section = rest.split('/', 2)[0]
      self.assertIn(section, ('cleanup', 'monitoring', 'important'), rest)
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

  def test_cron_monitoring_bots_aggregate_dimensions(self):
    # Tests that the aggregation works
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    bot_management.bot_event(
        event_type='request_sleep', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['beta'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, maintenance_msg=None,
        task_id=None, task_name=None, register_dimensions=True)
    bot_management.bot_event(
        event_type='request_sleep', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['alpha'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, maintenance_msg=None,
        task_id='987', task_name=None, register_dimensions=True)

    self.app.get('/internal/cron/monitoring/bots/aggregate_dimensions',
        headers={'X-AppEngine-Cron': 'true'}, status=200)
    agg_key = bot_management.get_aggregation_key('all')
    actual = agg_key.get()
    expected = bot_management.DimensionAggregation(
        key=agg_key,
        dimensions=[
            bot_management.DimensionValues(
                dimension='foo', values=['alpha', 'beta'])
        ],
        ts=now)
    self.assertEqual(expected, actual)

  def test_taskqueues(self):
    # Tests all the task queue tasks are securely handled.
    task_queue_routes = sorted(
        self._GetRoutes('/internal/taskqueue/'), key=lambda x: x.template)
    # This help to keep queue.yaml and handlers_backend.py up to date.
    # Format: (<queue-name>, <base-url>, <argument>).
    expected_task_queues = sorted([
        ('cancel-task-on-bot',
         '/internal/taskqueue/important/tasks/cancel-task-on-bot'),
        ('cancel-tasks', '/internal/taskqueue/important/tasks/cancel'),
        ('cancel-children-tasks',
         '/internal/taskqueue/important/tasks/cancel-children-tasks'),
        ('task-expire', '/internal/taskqueue/important/tasks/expire'),
        ('delete-tasks', '/internal/taskqueue/cleanup/tasks/delete'),
        ('es-notify-tasks',
         '/internal/taskqueue/important/external_scheduler/notify-tasks'),
        ('es-notify-kick',
         '/internal/taskqueue/important/external_scheduler/notify-kick'),
        ('pubsub',
         '/internal/taskqueue/important/pubsub/notify-task/abcabcabc'),
        ('buildbucket-notify',
         '/internal/taskqueue/important/buildbucket/notify-task/abcabcabc'),
        ('rebuild-task-cache',
         '/internal/taskqueue/important/task_queues/rebuild-cache'),
        ('update-bot-matches',
         '/internal/taskqueue/important/task_queues/update-bot-matches'),
        ('rescan-matching-task-sets',
         '/internal/taskqueue/important/task_queues/rescan-matching-task-sets'),
        ('named-cache-task',
         '/internal/taskqueue/important/named_cache/update-pool'),
        ('monitoring-bq-bots-events',
         '/internal/taskqueue/monitoring/bq/bots/events/2020-01-01T01:01'),
        ('monitoring-bq-tasks-requests',
         '/internal/taskqueue/monitoring/bq/tasks/requests/2020-01-01T01:01'),
        ('monitoring-bq-tasks-results-run',
         '/internal/taskqueue/monitoring/bq/tasks/results/run/'
         '2020-01-01T01:01'),
        ('monitoring-bq-tasks-results-summary',
         '/internal/taskqueue/monitoring/bq/tasks/results/summary/'
         '2020-01-01T01:01'),
    ],
                                  key=lambda x: x[1])
    self.assertEqual(len(expected_task_queues), len(task_queue_routes))

    for i, route in enumerate(task_queue_routes):
      expected_url = expected_task_queues[i][1]
      request = webapp2.Request.blank(expected_url)
      if not route.match(request):
        self.fail('Failed to route url %s with %r.' % (expected_url, route))

    # A request with wrong queue name should fail with 403
    for _, url in expected_task_queues:
      try:
        self.app.post(
            url, headers={'X-AppEngine-QueueName': 'bogus name'}, status=403)
      except Exception as e:
        self.fail('%s: %s' % (url, e))

  def test_taskqueue_important_task_queues_rebuild_cache_fail(self):
    self.set_as_admin()

    @ndb.tasklet
    def rebuild_task_cache_async(_body):
      raise ndb.Return(False)

    self.mock(task_queues, 'rebuild_task_cache_async', rebuild_task_cache_async)
    self.app.post(
        '/internal/taskqueue/important/task_queues/rebuild-cache',
        headers={'X-AppEngine-QueueName': 'rebuild-task-cache'}, status=429)

  def test_taskqueue_monitoring_bq_bots_events(self):
    self.set_as_admin()
    now = datetime.datetime(2020, 1, 2, 3, 4, 0)
    def task_bq_events(start, end):
      self.assertEqual(start, now)
      self.assertEqual(end, now + datetime.timedelta(seconds=60))
      return 0, 0
    self.mock(bot_management, 'task_bq_events', task_bq_events)
    self.app.post(
        '/internal/taskqueue/monitoring/bq/bots/events/2020-01-02T03:04',
        headers={'X-AppEngine-QueueName': 'monitoring-bq-bots-events'})

  def test_taskqueue_monitoring_bq_tasks_requests(self):
    self.set_as_admin()
    now = datetime.datetime(2020, 1, 2, 3, 4, 0)
    def task_bq(start, end):
      self.assertEqual(start, now)
      self.assertEqual(end, now + datetime.timedelta(seconds=60))
      return 0, 0
    self.mock(task_request, 'task_bq', task_bq)
    self.app.post(
        '/internal/taskqueue/monitoring/bq/tasks/requests/2020-01-02T03:04',
        headers={'X-AppEngine-QueueName': 'monitoring-bq-tasks-requests'})

  def test_taskqueue_monitoring_bq_tasks_results_run(self):
    self.set_as_admin()
    now = datetime.datetime(2020, 1, 2, 3, 4, 0)
    def task_bq_run(start, end):
      self.assertEqual(start, now)
      self.assertEqual(end, now + datetime.timedelta(seconds=60))
      return 0, 0
    self.mock(task_result, 'task_bq_run', task_bq_run)
    self.app.post(
        '/internal/taskqueue/monitoring/bq/tasks/results/run/2020-01-02T03:04',
        headers={'X-AppEngine-QueueName': 'monitoring-bq-tasks-results-run'})

  def test_taskqueue_monitoring_bq_tasks_results_summary(self):
    self.set_as_admin()
    now = datetime.datetime(2020, 1, 2, 3, 4, 0)
    def task_bq_summary(start, end):
      self.assertEqual(start, now)
      self.assertEqual(end, now + datetime.timedelta(seconds=60))
      return 0, 0
    self.mock(task_result, 'task_bq_summary', task_bq_summary)
    self.app.post(
        '/internal/taskqueue/monitoring/bq/tasks/results/summary/'
          '2020-01-02T03:04',
        headers={
          'X-AppEngine-QueueName': 'monitoring-bq-tasks-results-summary',
        })


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
