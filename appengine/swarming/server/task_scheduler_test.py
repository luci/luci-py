#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import random
import sys
import unittest

# Setups environment.
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)
import test_env_handlers

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

import webtest

import event_mon_metrics
import handlers_backend

from components import auth
from components import auth_testing
from components import datastore_utils
from components import pubsub
from components import utils
from test_support import test_case

from server import bot_management
from server import config
from server import task_pack
from server import task_queues
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_to_run
from server.task_result import State

from proto import config_pb2


# pylint: disable=W0212,W0612


def get_results(request_key):
  """Fetches all task results for a specified TaskRequest ndb.Key.

  Returns:
    tuple(TaskResultSummary, list of TaskRunResult that exist).
  """
  result_summary_key = task_pack.request_key_to_result_summary_key(request_key)
  result_summary = result_summary_key.get()
  # There's two way to look at it, either use a DB query or fetch all the
  # entities that could exist, at most 255. In general, there will be <3
  # entities so just fetching them by key would be faster. This function is
  # exclusively used in unit tests so it's not performance critical.
  q = task_result.TaskRunResult.query(ancestor=result_summary_key)
  q = q.order(task_result.TaskRunResult.key)
  return result_summary, q.fetch()


class TaskSchedulerApiTest(test_env_handlers.AppTestBase):
  def setUp(self):
    super(TaskSchedulerApiTest, self).setUp()
    self.now = datetime.datetime(2014, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now)
    auth_testing.mock_get_current_identity(self)
    event_mon_metrics.initialize()
    # Setup the backend to handle task queues for 'task-dimensions'.
    self.app = webtest.TestApp(
        handlers_backend.create_application(True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self._enqueue_orig = self.mock(utils, 'enqueue_task', self._enqueue)
    # See mock_pub_sub()
    self._pub_sub_mocked = False
    self.publish_successful = True
    self._random = 0x88
    self.mock(random, 'getrandbits', self._getrandbits)
    self.bot_dimensions = {
      u'foo': [u'bar'],
      u'id': [u'localhost'],
      u'os': [u'Windows', u'Windows-3.1.1'],
      u'pool': [u'default'],
    }

  def _enqueue(self, *args, **kwargs):
    return self._enqueue_orig(*args, use_dedicated_module=False, **kwargs)

  def _getrandbits(self, bits):
    self.assertEqual(16, bits)
    self._random += 1
    return self._random

  def mock_pub_sub(self):
    self.assertFalse(self._pub_sub_mocked)
    self._pub_sub_mocked = True
    calls = []
    def pubsub_publish(**kwargs):
      if not self.publish_successful:
        raise pubsub.TransientError('Fail')
      calls.append(('directly', kwargs))
    self.mock(pubsub, 'publish', pubsub_publish)
    return calls

  def _gen_request(self, properties=None, **kwargs):
    """Creates a TaskRequest."""
    props = {
      'command': [u'command1'],
      'dimensions': {u'os': u'Windows-3.1.1', u'pool': u'default'},
      'env': {},
      'execution_timeout_secs': 24*60*60,
      'io_timeout_secs': None,
    }
    props.update(properties or {})
    now = utils.utcnow()
    args = {
      'created_ts': now,
      'name': 'yay',
      'priority': 50,
      'properties': task_request.TaskProperties(**props),
      'expiration_ts': now + datetime.timedelta(seconds=60),
      'tags': [u'tag:1'],
      'user': 'Jesus',
    }
    args.update(kwargs)
    ret = task_request.TaskRequest(**args)
    task_request.init_new_request(ret, True, None)
    return ret

  def _gen_result_summary_pending(self, **kwargs):
    """Returns the dict for a TaskResultSummary for a pending task."""
    expected = {
      'abandoned_ts': None,
      'bot_dimensions': None,
      'bot_id': None,
      'bot_version': None,
      'cipd_pins': None,
      'children_task_ids': [],
      'completed_ts': None,
      'costs_usd': [],
      'cost_saved_usd': None,
      'created_ts': self.now,
      'deduped_from': None,
      'duration': None,
      'exit_code': None,
      'failure': False,
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'yay',
      'outputs_ref': None,
      'properties_hash': None,
      'server_versions': [u'v1a'],
      'started_ts': None,
      'state': State.PENDING,
      'tags': [
        u'os:Windows-3.1.1',
        u'pool:default',
        u'priority:50',
        u'service_account:none',
        u'tag:1',
        u'user:Jesus',
      ],
      'try_number': None,
      'user': u'Jesus',
    }
    expected.update(kwargs)
    return expected

  def _gen_result_summary_reaped(self, **kwargs):
    """Returns the dict for a TaskResultSummary for a pending task."""
    kwargs.setdefault(u'bot_dimensions', self.bot_dimensions.copy())
    kwargs.setdefault(u'bot_id', u'localhost')
    kwargs.setdefault(u'bot_version', u'abc')
    kwargs.setdefault(u'state', State.RUNNING)
    kwargs.setdefault(u'try_number', 1)
    return self._gen_result_summary_pending(**kwargs)

  def _gen_run_result(self, **kwargs):
    expected = {
      'abandoned_ts': None,
      'bot_dimensions': self.bot_dimensions,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'cipd_pins': None,
      'children_task_ids': [],
      'completed_ts': None,
      'cost_usd': 0.,
      'duration': None,
      'exit_code': None,
      'failure': False,
      'internal_failure': False,
      'modified_ts': self.now,
      'outputs_ref': None,
      'server_versions': [u'v1a'],
      'started_ts': self.now,
      'state': State.RUNNING,
      'try_number': 1,
    }
    expected.update(**kwargs)
    return expected

  def _quick_schedule(self, nb_task=1, **kwargs):
    """Schedules a task.

    nb_task is 1 if a GAE task queue rebuild-task-cache was enqueued.
    """
    request = self._gen_request(**kwargs)
    result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual(nb_task, self.execute_tasks())
    return result_summary

  def _register_bot(self, bot_dimensions, nb_task=1):
    """Registers the bot so the task queues knows there's a worker than can run
    the task.
    """
    bot_management.bot_event(
        'bot_connected', bot_dimensions[u'id'][0], '1.2.3.4', 'joe@localhost',
        bot_dimensions, {'state': 'real'}, '1234', False, None, None)
    task_queues.assert_bot(bot_dimensions)
    self.assertEqual(nb_task, self.execute_tasks())

  def _quick_reap(self, nb_task=1, **kwargs):
    """Reaps a task."""
    self._quick_schedule(**kwargs)
    self._register_bot(self.bot_dimensions, nb_task=nb_task)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    return run_result

  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    # TODO(maruel): Remove this once coverage is asserted.
    module = task_scheduler
    expected = set(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - set(i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing)

  def test_bot_reap_task(self):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    actual_request, _, run_result  = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)

  def test_bot_reap_task_not_enough_time(self):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    actual_request, _, run_result  = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', datetime.datetime(1969, 1, 1))
    self.failIf(actual_request)
    self.failIf(run_result)
    self.failUnless(task_to_run.TaskToRun.query().get().queue_number)

  def test_bot_reap_task_enough_time(self):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    actual_request, _, run_result  = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', datetime.datetime(3000, 1, 1))
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.failIf(task_to_run.TaskToRun.query().get().queue_number)

  def test_exponential_backoff(self):
    self.mock(
        task_scheduler.random, 'random',
        lambda: task_scheduler._PROBABILITY_OF_QUICK_COMEBACK)
    self.mock(utils, 'is_dev', lambda: False)
    data = [
      (0, 2),
      (1, 2),
      (2, 3),
      (3, 5),
      (4, 8),
      (5, 11),
      (6, 17),
      (7, 26),
      (8, 38),
      (9, 58),
      (10, 60),
      (11, 60),
    ]
    for value, expected in data:
      actual = int(round(task_scheduler.exponential_backoff(value)))
      self.assertEqual(expected, actual, (value, expected, actual))

  def test_exponential_backoff_quick(self):
    self.mock(
        task_scheduler.random, 'random',
        lambda: task_scheduler._PROBABILITY_OF_QUICK_COMEBACK - 0.01)
    self.assertEqual(1.0, task_scheduler.exponential_backoff(235))

  def test_task_handle_pubsub_task(self):
    calls = []
    def publish_mock(**kwargs):
      calls.append(kwargs)
    self.mock(task_scheduler.pubsub, 'publish', publish_mock)
    task_scheduler.task_handle_pubsub_task({
      'topic': 'projects/abc/topics/def',
      'task_id': 'abcdef123',
      'auth_token': 'token',
      'userdata': 'userdata',
    })
    self.assertEqual([
      {
        'attributes': {'auth_token': 'token'},
        'message': '{"task_id":"abcdef123","userdata":"userdata"}',
        'topic': 'projects/abc/topics/def',
    }], calls)

  def _task_ran_successfully(self, nb_task=1):
    """Runs an idempotent task successfully and returns the task_id."""
    request = self._gen_request(properties={'idempotent': True})
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions, nb_task=nb_task)
    actual_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)
    # It's important to complete the task with success.
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    # An idempotent task has properties_hash set after it succeeded.
    self.assertTrue(result_summary.key.get().properties_hash)
    return unicode(run_result.task_id)

  def _task_deduped(self, new_ts, deduped_from, task_id, nb_task=1, now=None):
    """Runs a task that was deduped."""
    request = self._gen_request(properties={'idempotent': True})
    task_request.init_new_request(request, True, None)
    result_summary_1 = task_scheduler.schedule_request(request, None)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)
    self._register_bot(self.bot_dimensions, nb_task=nb_task)
    actual_request_2, _, run_result_2 = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(None, actual_request_2)
    result_summary_duped, run_results_duped = get_results(request.key)
    # A deduped task cannot be deduped against so properties_hash is None.
    expected = self._gen_result_summary_reaped(
        completed_ts=now or self.now,
        cost_saved_usd=0.1,
        created_ts=new_ts,
        deduped_from=deduped_from,
        duration=0.1,
        exit_code=0,
        id=task_id,
        # Only this value is updated to 'now', the rest uses the previous run
        # timestamps.
        modified_ts=new_ts,
        started_ts=now or self.now,
        state=State.COMPLETED,
        try_number=0)
    self.assertEqual(expected, result_summary_duped.to_dict())
    self.assertEqual([], run_results_duped)

  def test_task_idempotent(self):
    # First task is idempotent.
    task_id = self._task_ran_successfully()

    # Second task is deduped against first task.
    new_ts = self.mock_now(self.now, config.settings().reusable_task_age_secs-1)
    self._task_deduped(new_ts, task_id, '1d8dc670a0008a10')

  def test_task_idempotent_old(self):
    # First task is idempotent.
    self._task_ran_successfully()

    # Second task is scheduled, first task is too old to be reused.
    new_ts = self.mock_now(self.now, config.settings().reusable_task_age_secs)
    request = self._gen_request(properties={'idempotent': True})
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual(1, self.execute_tasks())
    # The task was enqueued for execution.
    self.assertNotEqual(None, task_to_run.TaskToRun.query().get().queue_number)

  def test_task_idempotent_three(self):
    # First task is idempotent.
    task_id = self._task_ran_successfully()

    # Second task is deduped against first task.
    new_ts = self.mock_now(self.now, config.settings().reusable_task_age_secs-1)
    self._task_deduped(new_ts, task_id, '1d8dc670a0008a10')

    # Third task is scheduled, second task is not dedupable, first task is too
    # old.
    new_ts = self.mock_now(self.now, config.settings().reusable_task_age_secs)
    request = self._gen_request(properties={'idempotent': True})
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    # The task was enqueued for execution.
    self.assertNotEqual(None, task_to_run.TaskToRun.query().get().queue_number)

  def test_task_idempotent_variable(self):
    # Test the edge case where config.settings().reusable_task_age_secs is being
    # modified. This ensure TaskResultSummary.order(TRS.key) works.
    cfg = config.settings()
    cfg.reusable_task_age_secs = 10
    self.mock(config, 'settings', lambda: cfg)

    # First task is idempotent.
    self._task_ran_successfully()

    # Second task is scheduled, first task is too old to be reused.
    second_ts = self.mock_now(self.now, 10)
    task_id = self._task_ran_successfully(nb_task=0)

    # Now any of the 2 tasks could be reused. Assert the right one (the most
    # recent) is reused.
    cfg.reusable_task_age_secs = 100

    # Third task is deduped against second task. That ensures ordering works
    # correctly.
    third_ts = self.mock_now(self.now, 20)
    self._task_deduped(
        third_ts, task_id, '1d69ba3ea8008b10', nb_task=0, now=second_ts)

  def test_task_parent_children(self):
    # Parent task creates a child task.
    parent_id = self._task_ran_successfully()
    request = self._gen_request(parent_task_id=parent_id)
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual([], result_summary.children_task_ids)
    self.assertEqual(parent_id, request.parent_task_id)

    parent_run_result_key = task_pack.unpack_run_result_key(parent_id)
    parent_res_summary_key = task_pack.run_result_key_to_result_summary_key(
        parent_run_result_key)
    expected = [result_summary.task_id]
    self.assertEqual(expected, parent_run_result_key.get().children_task_ids)
    self.assertEqual(expected, parent_res_summary_key.get().children_task_ids)

  def test_task_parent_isolated(self):
    request = self._gen_request(
        properties={
          'command': [],
          'inputs_ref': {
            'isolated': '1' * 40,
            'isolatedserver': 'http://localhost:1',
            'namespace': 'default-gzip',
          },
        })
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    actual_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)
    # It's important to terminate the task with success.
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))

    parent_id = run_result.task_id
    request = self._gen_request(parent_task_id=parent_id)
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual([], result_summary.children_task_ids)
    self.assertEqual(parent_id, request.parent_task_id)

    parent_run_result_key = task_pack.unpack_run_result_key(parent_id)
    parent_res_summary_key = task_pack.run_result_key_to_result_summary_key(
        parent_run_result_key)
    expected = [result_summary.task_id]
    self.assertEqual(expected, parent_run_result_key.get().children_task_ids)
    self.assertEqual(expected, parent_res_summary_key.get().children_task_ids)

  def test_get_results(self):
    # TODO(maruel): Split in more focused tests.
    created_ts = self.now
    self.mock_now(created_ts)
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)

    # The TaskRequest was enqueued, the TaskResultSummary was created but no
    # TaskRunResult exist yet since the task was not scheduled on any bot.
    result_summary, run_results = get_results(request.key)
    expected = self._gen_result_summary_pending(
        created_ts=created_ts, id='1d69b9f088008910', modified_ts=created_ts)
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual([], run_results)

    # A bot reaps the TaskToRun.
    reaped_ts = self.now + datetime.timedelta(seconds=60)
    self.mock_now(reaped_ts)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(request, reaped_request)
    self.assertTrue(run_result)
    result_summary, run_results = get_results(request.key)
    expected = self._gen_result_summary_reaped(
        created_ts=created_ts,
        costs_usd=[0.0],
        id='1d69b9f088008910',
        modified_ts=reaped_ts,
        started_ts=reaped_ts)
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      self._gen_run_result(
        id='1d69b9f088008911', modified_ts=reaped_ts, started_ts=reaped_ts),
    ]
    self.assertEqual(expected, [i.to_dict() for i in run_results])

    # The bot completes the task.
    done_ts = self.now + datetime.timedelta(seconds=120)
    self.mock_now(done_ts)
    outputs_ref = task_request.FilesRef(
        isolated='a'*40, isolatedserver='http://localhost', namespace='c')
    performance_stats = task_result.PerformanceStats(
        bot_overhead=0.1,
        isolated_download=task_result.OperationStats(
          duration=0.1,
          initial_number_items=10,
          initial_size=1000,
          items_cold='aa',
          items_hot='bb'),
        isolated_upload=task_result.OperationStats(
          duration=0.1,
          items_cold='aa',
          items_hot='bb'))
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=3.,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=outputs_ref,
            performance_stats=performance_stats))
    # Simulate an unexpected retry, e.g. the response of the previous RPC never
    # got the the client even if it succeedded.
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=3.,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=outputs_ref,
            performance_stats=performance_stats))
    result_summary, run_results = get_results(request.key)
    expected = self._gen_result_summary_reaped(
        completed_ts=done_ts,
        costs_usd=[0.1],
        created_ts=created_ts,
        duration=3.0,
        exit_code=0,
        id='1d69b9f088008910',
        modified_ts=done_ts,
        outputs_ref={
          'isolated': u'a'*40,
          'isolatedserver': u'http://localhost',
          'namespace': u'c',
        },
        started_ts=reaped_ts,
        state=State.COMPLETED,
        try_number=1)
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      self._gen_run_result(
          completed_ts=done_ts,
          cost_usd=0.1,
          duration=3.0,
          exit_code=0,
          id='1d69b9f088008911',
          modified_ts=done_ts,
          outputs_ref={
            'isolated': u'a'*40,
            'isolatedserver': u'http://localhost',
            'namespace': u'c',
          },
          started_ts=reaped_ts,
          state=State.COMPLETED),
    ]
    self.assertEqual(expected, [t.to_dict() for t in run_results])

  def test_exit_code_failure(self):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(request, reaped_request)
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=1,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    result_summary, run_results = get_results(request.key)

    expected = self._gen_result_summary_reaped(
        completed_ts=self.now,
        costs_usd=[0.1],
        duration=0.1,
        exit_code=1,
        failure=True,
        id='1d69b9f088008910',
        started_ts=self.now,
        state=State.COMPLETED,
        try_number=1)
    self.assertEqual(expected, result_summary.to_dict())

    expected = [
      self._gen_run_result(
          completed_ts=self.now,
          cost_usd=0.1,
          duration=0.1,
          exit_code=1,
          failure=True,
          id='1d69b9f088008911',
          started_ts=self.now,
          state=State.COMPLETED),
    ]
    self.assertEqual(expected, [t.to_dict() for t in run_results])

  def test_schedule_request(self):
    # It is tested indirectly in the other functions.
    self.assertTrue(self._quick_schedule())

  def mock_dim_acls(self, mapping):
    self.mock(config, 'settings', lambda: config_pb2.SettingsCfg(
      dimension_acls=config_pb2.DimensionACLs(entry=[
        config_pb2.DimensionACLs.Entry(dimension=[d], usable_by=g)
        for d, g in sorted(mapping.iteritems())
      ]),
    ))

  def test_schedule_request_forbidden_dim(self):
    self.mock_dim_acls({u'pool:bad': u'noone'})
    self._quick_schedule(properties={'dimensions': {u'pool': u'good'}})
    with self.assertRaises(auth.AuthorizationError):
      self._quick_schedule(properties={'dimensions': {u'pool': u'bad'}})

  def test_schedule_request_forbidden_dim_via_star(self):
    self.mock_dim_acls({u'abc:*': u'noone'})
    self._quick_schedule(properties={'dimensions': {u'pool': u'default'}})
    with self.assertRaises(auth.AuthorizationError):
      self._quick_schedule(
        properties={'dimensions': {u'pool': u'default', u'abc': u'blah'}})

  def test_schedule_request_id_without_pool(self):
    self.mock_dim_acls({u'pool:good': u'mocked'})
    with self.assertRaises(datastore_errors.BadValueError):
      self._quick_schedule(properties={'dimensions': {u'id': u'abc'}})
    auth_testing.mock_is_admin(self)
    with self.assertRaises(datastore_errors.BadValueError):
      self._quick_schedule(properties={'dimensions': {u'id': u'abc'}})

  def test_schedule_request_id_and_pool(self):
    self.mock_dim_acls({u'pool:good': u'mocked'})
    self.mock_dim_acls({u'pool:bad': u'unknown'})

    def mocked_is_group_member(group, ident):
      if group == 'mocked' and ident == auth_testing.DEFAULT_MOCKED_IDENTITY:
        return True
      return False
    self.mock(auth, 'is_group_member', mocked_is_group_member)

    self._quick_schedule(
      properties={'dimensions': {u'id': u'abc', u'pool': u'unknown'}},
      nb_task=0)
    self._quick_schedule(
      properties={'dimensions': {u'id': u'abc', u'pool': u'good'}}, nb_task=0)
    with self.assertRaises(auth.AuthorizationError):
      self._quick_schedule(
        properties={'dimensions': {u'id': u'abc', u'pool': u'bad'}})

  def test_bot_update_task(self):
    run_result = self._quick_reap(nb_task=0)
    self.assertEqual(
        task_result.State.RUNNING,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hi',
            output_chunk_start=0,
            exit_code=None,
            duration=None,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hey',
            output_chunk_start=2,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual('hihey', run_result.key.get().get_output())

  def test_bot_update_task_new_overwrite(self):
    run_result = self._quick_reap(nb_task=0)
    self.assertEqual(
        task_result.State.RUNNING,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hi',
            output_chunk_start=0,
            exit_code=None,
            duration=None,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual(
        task_result.State.RUNNING,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hey',
            output_chunk_start=1,
            exit_code=None,
            duration=None,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual('hhey', run_result.key.get().get_output())

  def test_bot_update_exception(self):
    run_result = self._quick_reap(nb_task=0)
    def r(*_):
      raise datastore_utils.CommitError('Sorry!')

    self.mock(ndb, 'put_multi', r)
    self.assertEqual(
        None,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hi',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))

  def test_bot_update_pubsub_error(self):
    pub_sub_calls = self.mock_pub_sub()
    request = self._gen_request(pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    _, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual('localhost', run_result.bot_id)

    # Attempt to terminate the task with success, but make PubSub call fail.
    self.publish_successful = False
    self.assertEqual(
        None,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual(1, self.execute_tasks(status=500))

    # Bot retries bot_update, now PubSub works and notification is sent.
    self.publish_successful = True
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    self.assertEqual(0, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # notification is sent

  def _bot_update_timeouts(self, hard, io):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(
        task_result.State.TIMED_OUT,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost',
            cipd_pins=None,
            output='hi',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=hard,
            io_timeout=io,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    expected = self._gen_result_summary_reaped(
        completed_ts=self.now,
        costs_usd=[0.1],
        duration=0.1,
        exit_code=0,
        failure=True,
        id='1d69b9f088008910',
        started_ts=self.now,
        state=State.TIMED_OUT,
        try_number=1)
    self.assertEqual(expected, result_summary.key.get().to_dict())

    expected = self._gen_run_result(
        completed_ts=self.now,
        cost_usd=0.1,
        duration=0.1,
        exit_code=0,
        failure=True,
        id='1d69b9f088008911',
        started_ts=self.now,
        state=State.TIMED_OUT,
        try_number=1)
    self.assertEqual(expected, run_result.key.get().to_dict())

  def test_bot_update_hard_timeout(self):
    self._bot_update_timeouts(True, False)

  def test_bot_update_io_timeout(self):
    self._bot_update_timeouts(False, True)

  def test_task_priority(self):
    # Create N tasks of various priority not in order.
    priorities = [200, 100, 20, 30, 50, 40, 199]
    # Call the expected ordered list out for clarity.
    expected = [20, 30, 40, 50, 100, 199, 200]
    self.assertEqual(expected, sorted(priorities))

    self._register_bot(self.bot_dimensions, nb_task=0)
    # Triggers many tasks of different priorities.
    for i, p in enumerate(priorities):
      self._quick_schedule(priority=p, nb_task=int(not i))
    self.assertEqual(0, self.execute_tasks())

    # Make sure they are scheduled in priority order. Bot polling should hand
    # out tasks in the expected order. In practice the order is not 100%
    # deterministic when running on GAE but it should be deterministic in the
    # unit test.
    for i, e in enumerate(expected):
      request, _, _ = task_scheduler.bot_reap_task(
          self.bot_dimensions, 'abc', None)
      self.assertEqual(request.priority, e)
    self.assertEqual(0, self.execute_tasks())

  def test_bot_kill_task(self):
    pub_sub_calls = self.mock_pub_sub()
    request = self._gen_request(pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # PENDING -> RUNNING

    self.assertEqual(
        None, task_scheduler.bot_kill_task(run_result.key, 'localhost'))
    expected = self._gen_result_summary_reaped(
        abandoned_ts=self.now,
        costs_usd=[0.],
        id='1d69b9f088008910',
        internal_failure=True,
        started_ts=self.now,
        state=State.BOT_DIED)
    self.assertEqual(expected, result_summary.key.get().to_dict())
    expected = self._gen_run_result(
        abandoned_ts=self.now,
        id='1d69b9f088008911',
        internal_failure=True,
        state=State.BOT_DIED)
    self.assertEqual(expected, run_result.key.get().to_dict())
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(pub_sub_calls)) # RUNNING -> BOT_DIED

  def test_bot_kill_task_wrong_bot(self):
    request = self._gen_request()
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    expected = (
      'Bot bot1 sent task kill for task 1d69b9f088008911 owned by bot '
      'localhost')
    self.assertEqual(
        expected, task_scheduler.bot_kill_task(run_result.key, 'bot1'))

  def test_cancel_task(self):
    request = self._gen_request(pubsub_topic='projects/abc/topics/def')
    pub_sub_calls = self.mock_pub_sub()
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    ok, was_running = task_scheduler.cancel_task(request, result_summary.key)
    self.assertEqual(True, ok)
    self.assertEqual(False, was_running)
    result_summary = result_summary.key.get()
    self.assertEqual(task_result.State.CANCELED, result_summary.state)
    self.assertEqual(2, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # sent completion notification

  def test_cancel_task_running(self):
    request = self._gen_request(pubsub_topic='projects/abc/topics/def')
    pub_sub_calls = self.mock_pub_sub()
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    reaped_request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    ok, was_running = task_scheduler.cancel_task(request, result_summary.key)
    self.assertEqual(False, ok)
    self.assertEqual(True, was_running)
    result_summary = result_summary.key.get()
    self.assertEqual(task_result.State.RUNNING, result_summary.state)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # PENDING -> RUNNING

  def test_cron_abort_expired_task_to_run(self):
    request = self._gen_request(pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    pub_sub_calls = self.mock_pub_sub()
    result_summary = task_scheduler.schedule_request(request, None)
    abandoned_ts = self.mock_now(self.now, request.expiration_secs+1)
    self.assertEqual(
        ['1d69b9f088008910'],
        task_scheduler.cron_abort_expired_task_to_run('f.local'))
    self.assertEqual([], task_result.TaskRunResult.query().fetch())
    expected = self._gen_result_summary_pending(
        abandoned_ts=abandoned_ts,
        id='1d69b9f088008910',
        modified_ts=abandoned_ts,
        state=task_result.State.EXPIRED)
    self.assertEqual(expected, result_summary.key.get().to_dict())
    self.assertEqual(2, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # pubsub completion notification

  def test_cron_abort_expired_task_to_run_retry(self):
    pub_sub_calls = self.mock_pub_sub()
    now = utils.utcnow()
    request = self._gen_request(
        properties={'idempotent': True},
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=600),
        pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    result_summary = task_scheduler.schedule_request(request, None)

    # Fake first try bot died.
    self._register_bot(self.bot_dimensions)
    _request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # PENDING -> RUNNING
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))
    self.assertEqual(task_result.State.BOT_DIED, run_result.key.get().state)
    self.assertEqual(
        task_result.State.PENDING, run_result.result_summary_key.get().state)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(pub_sub_calls)) # RUNNING -> PENDING

    # BOT_DIED is kept instead of EXPIRED.
    abandoned_ts = self.mock_now(self.now, request.expiration_secs+1)
    self.assertEqual(
        ['1d69b9f088008910'],
        task_scheduler.cron_abort_expired_task_to_run('f.local'))
    self.assertEqual(1, len(task_result.TaskRunResult.query().fetch()))
    expected = self._gen_result_summary_reaped(
        abandoned_ts=abandoned_ts,
        costs_usd=[0.],
        id='1d69b9f088008910',
        internal_failure=True,
        modified_ts=abandoned_ts,
        started_ts=self.now,
        state=task_result.State.BOT_DIED)
    self.assertEqual(expected, result_summary.key.get().to_dict())

    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(3, len(pub_sub_calls)) # PENDING -> BOT_DIED

  def test_cron_handle_bot_died(self):
    pub_sub_calls = self.mock_pub_sub()

    # Test first retry, then success.
    now = utils.utcnow()
    request = self._gen_request(
        properties={'idempotent': True},
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=600),
        pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(pub_sub_calls))
    self._register_bot(self.bot_dimensions, nb_task=0)
    request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(
        task_result.State.RUNNING, run_result.result_summary_key.get().state)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # PENDING -> RUNNING
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(pub_sub_calls)) # RUNNING -> PENDING

    # Refresh and compare:
    expected = self._gen_result_summary_reaped(
        costs_usd=[0.],
        id='1d69b9f088008910',
        modified_ts=now_1,
        state=task_result.State.PENDING,
        try_number=1)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())
    expected = self._gen_run_result(
        abandoned_ts=now_1,
        id='1d69b9f088008911',
        internal_failure=True,
        modified_ts=now_1,
        state=task_result.State.BOT_DIED)
    self.assertEqual(expected, run_result.key.get().to_dict())

    # Task was retried.
    now_2 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    bot_dimensions_second = self.bot_dimensions.copy()
    bot_dimensions_second[u'id'] = [u'localhost-second']
    self._register_bot(bot_dimensions_second, nb_task=0)
    _request, _, run_result = task_scheduler.bot_reap_task(
        bot_dimensions_second, 'abc', None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(3, len(pub_sub_calls)) # PENDING -> RUNNING
    logging.info('%s', [t.to_dict() for t in task_to_run.TaskToRun.query()])
    self.assertEqual(2, run_result.try_number)
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost-second',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    expected = self._gen_result_summary_reaped(
        bot_dimensions=bot_dimensions_second,
        bot_id=u'localhost-second',
        completed_ts=now_2,
        costs_usd=[0., 0.1],
        duration=0.1,
        exit_code=0,
        id='1d69b9f088008910',
        modified_ts=now_2,
        properties_hash=request.properties_hash.encode('hex'),
        started_ts=now_2,
        state=task_result.State.COMPLETED,
        try_number=2)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())
    self.assertEqual(0.1, run_result.key.get().cost_usd)

    self.assertEqual(0, self.execute_tasks())
    self.assertEqual(4, len(pub_sub_calls)) # RUNNING -> COMPLETED

  def test_cron_handle_bot_died_no_update_not_idempotent(self):
    # A bot reaped a task but the handler returned HTTP 500, leaving the task in
    # a lingering state.
    pub_sub_calls = self.mock_pub_sub()

    # Test first retry, then success.
    now = utils.utcnow()
    request = self._gen_request(
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=600),
        pubsub_topic='projects/abc/topics/def')
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(pub_sub_calls))
    self._register_bot(self.bot_dimensions, nb_task=0)
    request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(
        task_result.State.RUNNING, run_result.result_summary_key.get().state)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(pub_sub_calls)) # PENDING -> RUNNING
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(pub_sub_calls)) # RUNNING -> PENDING

    # Refresh and compare:
    expected = self._gen_result_summary_reaped(
        costs_usd=[0.],
        id='1d69b9f088008910',
        modified_ts=now_1,
        state=task_result.State.PENDING,
        try_number=1)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())
    expected = self._gen_run_result(
        abandoned_ts=now_1,
        id='1d69b9f088008911',
        internal_failure=True,
        modified_ts=now_1,
        state=task_result.State.BOT_DIED)
    self.assertEqual(expected, run_result.key.get().to_dict())

    # Task was retried.
    now_2 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    bot_dimensions_second = self.bot_dimensions.copy()
    bot_dimensions_second[u'id'] = [u'localhost-second']
    self._register_bot(bot_dimensions_second, nb_task=0)
    _request, _, run_result = task_scheduler.bot_reap_task(
        bot_dimensions_second, 'abc', None)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(3, len(pub_sub_calls)) # PENDING -> RUNNING
    logging.info('%s', [t.to_dict() for t in task_to_run.TaskToRun.query()])
    self.assertEqual(2, run_result.try_number)
    self.assertEqual(
        task_result.State.COMPLETED,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost-second',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    expected = self._gen_result_summary_reaped(
        bot_dimensions=bot_dimensions_second,
        bot_id=u'localhost-second',
        completed_ts=now_2,
        costs_usd=[0., 0.1],
        duration=0.1,
        exit_code=0,
        id='1d69b9f088008910',
        modified_ts=now_2,
        started_ts=now_2,
        state=task_result.State.COMPLETED,
        try_number=2)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())
    self.assertEqual(0.1, run_result.key.get().cost_usd)

    self.assertEqual(0, self.execute_tasks())
    self.assertEqual(4, len(pub_sub_calls)) # RUNNING -> COMPLETED

  def test_bot_poll_http_500_but_bot_reapears_after_BOT_PING_TOLERANCE(self):
    # A bot reaped a task, sleeps for over BOT_PING_TOLERANCE (2 minutes), then
    # sends a ping.
    # In the meantime the cron job ran, saw the job idle with 0 update for more
    # than BOT_PING_TOLERANCE, re-enqueue it.
    run_result = self._quick_reap(
        expiration_ts=self.now+(3*task_result.BOT_PING_TOLERANCE),
       nb_task=0)
    to_run_key = task_to_run.request_to_task_to_run_key(
        run_result.request_key.get())
    self.assertEqual(None, to_run_key.get().queue_number)

    # See _handle_dead_bot() with special case about non-idempotent task that
    # were never updated.
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    #logging.info('%s', [t.to_dict() for t in task_to_run.TaskToRun.query()])
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))

    # Now the task is available. Bot magically wakes up (let's say a laptop that
    # went to sleep). The update is denied.
    self.assertEqual(
        None,
        task_scheduler.bot_update_task(
            run_result_key=run_result.key,
            bot_id='localhost-second',
            cipd_pins=None,
            output='Foo1',
            output_chunk_start=0,
            exit_code=0,
            duration=0.1,
            hard_timeout=False,
            io_timeout=False,
            cost_usd=0.1,
            outputs_ref=None,
            performance_stats=None))
    # Confirm it is denied.
    run_result = run_result.key.get()
    self.assertEqual(State.BOT_DIED, run_result.state)
    result_summary = run_result.result_summary_key.get()
    self.assertEqual(State.PENDING, result_summary.state)
    self.assertTrue(to_run_key.get().queue_number)

  def test_cron_handle_bot_died_same_bot_denied(self):
    # Test first retry, then success.
    now = utils.utcnow()
    request = self._gen_request(
        properties={'idempotent': True},
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=600))
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    _request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))

    # Refresh and compare:
    # The interesting point here is that even though the task is PENDING, it has
    # worker information from the initial BOT_DIED task.
    expected = self._gen_run_result(
        abandoned_ts=now_1,
        id='1d69b9f088008911',
        internal_failure=True,
        modified_ts=now_1,
        state=task_result.State.BOT_DIED)
    self.assertEqual(expected, run_result.key.get().to_dict())
    expected = self._gen_result_summary_pending(
        bot_dimensions=self.bot_dimensions.copy(),
        bot_version=u'abc',
        bot_id=u'localhost',
        costs_usd=[0.],
        id='1d69b9f088008910',
        modified_ts=now_1,
        try_number=1)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())

    # Task was retried but the same bot polls again, it's denied the task.
    now_2 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(None, request)
    self.assertEqual(None, run_result)
    logging.info('%s', [t.to_dict() for t in task_to_run.TaskToRun.query()])

  def test_cron_handle_bot_died_second(self):
    # Test two tries internal_failure's leading to a BOT_DIED status.
    now = utils.utcnow()
    request = self._gen_request(
        properties={'idempotent': True},
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=600))
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    _request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(([], 1, 0), task_scheduler.cron_handle_bot_died('f.local'))
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    # It must be a different bot.
    bot_dimensions_second = self.bot_dimensions.copy()
    bot_dimensions_second[u'id'] = [u'localhost-second']
    self._register_bot(bot_dimensions_second, nb_task=0)
    # No task to run because the task dimensions were already seen.
    _request, _, run_result = task_scheduler.bot_reap_task(
        bot_dimensions_second, 'abc', None)
    now_2 = self.mock_now(self.now + 2 * task_result.BOT_PING_TOLERANCE, 3)
    self.assertEqual(
        (['1d69b9f088008912'], 0, 0),
        task_scheduler.cron_handle_bot_died('f.local'))
    self.assertEqual(([], 0, 0), task_scheduler.cron_handle_bot_died('f.local'))
    expected = self._gen_result_summary_reaped(
        abandoned_ts=now_2,
        bot_dimensions=bot_dimensions_second,
        bot_id=u'localhost-second',
        costs_usd=[0., 0.],
        id='1d69b9f088008910',
        internal_failure=True,
        modified_ts=now_2,
        started_ts=now_1,
        state=task_result.State.BOT_DIED,
        try_number=2)
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())

  def test_cron_handle_bot_died_ignored_expired(self):
    now = utils.utcnow()
    request = self._gen_request(
        created_ts=now, expiration_ts=now+datetime.timedelta(seconds=600))
    task_request.init_new_request(request, True, None)
    _result_summary = task_scheduler.schedule_request(request, None)
    self._register_bot(self.bot_dimensions)
    _request, _, run_result = task_scheduler.bot_reap_task(
        self.bot_dimensions, 'abc', None)
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 601)
    self.assertEqual(
        (['1d69b9f088008911'], 0, 0),
        task_scheduler.cron_handle_bot_died('f.local'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
