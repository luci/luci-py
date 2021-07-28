#!/usr/bin/env vpython
# -*- coding: utf-8 -*-
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import random
import string
import sys
import unittest

import mock
from parameterized import parameterized

import test_env
test_env.setup_test_env()

from google.protobuf import duration_pb2

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

import webtest

from components import auth_testing
from components import utils
from test_support import test_case

from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from server import bq_state
from server import large
from server import task_pack
from server import task_request
from server import task_result
from server import task_to_run
import ts_mon_metrics

# pylint: disable=W0212


def _gen_properties(**kwargs):
  """Creates a TaskProperties."""
  args = {
      'command': [u'command1'],
      'containment': {
          u'lower_priority': True,
          u'containment_type': None,
          u'limit_processes': None,
          u'limit_total_committed_memory': None,
      },
      'dimensions': {
          u'pool': [u'default']
      },
      'env': {},
      'execution_timeout_secs': 24 * 60 * 60,
      'io_timeout_secs': None,
  }
  args.update(kwargs or {})
  args['dimensions_data'] = args.pop('dimensions')
  return task_request.TaskProperties(**args)


def _gen_request_slice(**kwargs):
  """Creates a TaskRequest."""
  now = utils.utcnow()
  args = {
      'created_ts': now,
      'manual_tags': [u'tag:1'],
      'name': 'Request name',
      'priority': 50,
      'task_slices': [
          task_request.TaskSlice(
              expiration_secs=60, properties=_gen_properties()),
      ],
      'user': 'Jesus',
      'bot_ping_tolerance_secs': 120,
  }
  args.update(kwargs)
  ret = task_request.TaskRequest(**args)
  task_request.init_new_request(ret, True, task_request.TEMPLATE_AUTO)
  ret.key = task_request.new_request_key()
  ret.put()
  return ret


def _gen_request(properties=None, **kwargs):
  """Creates a TaskRequest."""
  return _gen_request_slice(
      task_slices=[
          task_request.TaskSlice(
              expiration_secs=60, properties=properties or _gen_properties()),
      ],
      **kwargs)


def _gen_summary_result(**kwargs):
  """Creates a TaskRunResult."""
  request = _gen_request(**kwargs)
  result_summary = task_result.new_result_summary(request)
  result_summary.modified_ts = utils.utcnow()
  ndb.transaction(result_summary.put)
  return result_summary.key.get()


def _gen_run_result(**kwargs):
  """Creates a TaskRunResult."""
  result_summary = _gen_summary_result(**kwargs)
  request = result_summary.request_key.get()
  to_run = task_to_run.new_task_to_run(request, 0)
  run_result = task_result.new_run_result(request, to_run, 'localhost', 'abc',
                                          {}, result_summary.resultdb_info)
  run_result.started_ts = result_summary.modified_ts
  run_result.modified_ts = utils.utcnow()
  run_result.dead_after_ts = utils.utcnow() + datetime.timedelta(
      seconds=request.bot_ping_tolerance_secs)
  ndb.transaction(lambda: result_summary.set_from_run_result(
      run_result, request))
  ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))
  return run_result.key.get()


def _safe_cmp(a, b):
  # cmp(datetime.datetime.utcnow(), None) throws TypeError. Workaround.
  return cmp(utils.encode_to_json(a), utils.encode_to_json(b))


def get_entities(entity_model):
  return sorted((i.to_dict() for i in entity_model.query().fetch()),
                cmp=_safe_cmp)


class TestCase(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def setUp(self):
    super(TestCase, self).setUp()
    auth_testing.mock_get_current_identity(self)


class TaskResultApiTest(TestCase):

  def setUp(self):
    super(TaskResultApiTest, self).setUp()
    self.now = datetime.datetime(2014, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now)
    self.mock(random, 'getrandbits', lambda _: 0x88)

  def assertEntities(self, expected, entity_model):
    self.assertEqual(expected, get_entities(entity_model))

  def _gen_summary(self, **kwargs):
    """Returns TaskResultSummary.to_dict()."""
    out = {
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
        'current_task_slice': 0,
        'deduped_from': None,
        'duration': None,
        'exit_code': None,
        'expiration_delay': None,
        'failure': False,
        # Constant due to the mock of both utils.utcnow() and
        # random.getrandbits().
        'id': '1d69b9f088008810',
        'internal_failure': False,
        'modified_ts': None,
        'name': u'Request name',
        'outputs_ref': None,
        'priority': 50,
        'cas_output_root': None,
        'resultdb_info': None,
        'server_versions': [u'v1a'],
        'started_ts': None,
        'state': task_result.State.PENDING,
        'tags': [
            u'authenticated:user:mocked@example.com',
            u'pool:default',
            u'priority:50',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:no_config',
            u'tag:1',
            u'use_cas_1143123:0',
            u'use_isolate_1143123:0',
            u'user:Jesus',
        ],
        'try_number': None,
        'user': u'Jesus',
    }
    out.update(kwargs)
    return out

  def _gen_result(self, **kwargs):
    """Returns TaskRunResult.to_dict()."""
    out = {
        'abandoned_ts': None,
        'bot_dimensions': {
            u'id': [u'localhost'],
            u'foo': [u'bar', u'biz']
        },
        'bot_id': u'localhost',
        'bot_version': u'abc',
        'children_task_ids': [],
        'cipd_pins': None,
        'completed_ts': None,
        'cost_usd': 0.,
        'current_task_slice': 0,
        'dead_after_ts': None,
        'duration': None,
        'exit_code': None,
        'failure': False,
        # Constant due to the mock of both utils.utcnow() and
        # random.getrandbits().
        'id': '1d69b9f088008811',
        'internal_failure': False,
        'killing': None,
        'modified_ts': None,
        'outputs_ref': None,
        'cas_output_root': None,
        'resultdb_info': None,
        'server_versions': [u'v1a'],
        'started_ts': None,
        'state': task_result.State.RUNNING,
        'try_number': 1,
    }
    out.update(kwargs)
    return out

  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    module = task_result
    expected = set(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - set(i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing)

  def test_State(self):
    for i in task_result.State.STATES:
      self.assertTrue(task_result.State.to_string(i))
    with self.assertRaises(ValueError):
      task_result.State.to_string(0)

    self.assertEqual(
        set(task_result.State._NAMES), set(task_result.State.STATES))
    items = (
        task_result.State.STATES_RUNNING + task_result.State.STATES_DONE +
        task_result.State.STATES_ABANDONED)
    self.assertEqual(set(items), set(task_result.State.STATES))
    self.assertEqual(len(items), len(set(items)))

  def test_state_to_string(self):
    # Same code as State.to_string() except that it works for
    # TaskResultSummary too.
    class Foo(ndb.Model):
      deduped_from = None
      state = task_result.StateProperty()
      failure = ndb.BooleanProperty(default=False)
      internal_failure = ndb.BooleanProperty(default=False)

    for i in task_result.State.STATES:
      self.assertTrue(task_result.State.to_string(i))
    for i in task_result.State.STATES:
      self.assertTrue(task_result.state_to_string(Foo(state=i)))
    f = Foo(state=task_result.State.COMPLETED)
    f.deduped_from = '123'
    self.assertEqual('Deduped', task_result.state_to_string(f))

  def test_new_result_summary(self):
    request = _gen_request()
    actual = task_result.new_result_summary(request)
    actual.modified_ts = self.now
    # Trigger _pre_put_hook().
    actual.put()
    expected = self._gen_summary(modified_ts=self.now)
    self.assertEqual(expected, actual.to_dict())
    self.assertEqual(50, actual.request.priority)
    self.assertEqual(True, actual.can_be_canceled)
    actual.state = task_result.State.RUNNING
    self.assertEqual(True, actual.can_be_canceled)
    actual.state = task_result.State.TIMED_OUT
    actual.duration = 0.1
    actual.completed_ts = self.now
    self.assertEqual(False, actual.can_be_canceled)

    actual.children_task_ids = [
        '1d69ba3ea8008810',
        '3d69ba3ea8008810',
        '2d69ba3ea8008810',
    ]
    actual.modified_ts = utils.utcnow()
    ndb.transaction(actual.put)
    expected = [u'1d69ba3ea8008810', u'2d69ba3ea8008810', u'3d69ba3ea8008810']
    self.assertEqual(expected, actual.key.get().children_task_ids)

  def test_new_run_result(self):
    request = _gen_request()
    to_run = task_to_run.new_task_to_run(request, 0)
    actual = task_result.new_run_result(
        request, to_run, u'localhost', u'abc', {
            u'id': [u'localhost'],
            u'foo': [u'bar', u'biz']
        },
        task_result.ResultDBInfo(hostname='hostname', invocation='invocation'))
    actual.modified_ts = self.now
    actual.started_ts = self.now
    actual.dead_after_ts = self.now + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    # Trigger _pre_put_hook().
    actual.put()
    expected = self._gen_result(
        modified_ts=self.now,
        started_ts=self.now,
        dead_after_ts=self.now +
        datetime.timedelta(seconds=request.bot_ping_tolerance_secs),
        resultdb_info={
            'hostname': 'hostname',
            'invocation': 'invocation'
        })
    self.assertEqual(expected, actual.to_dict())
    self.assertEqual(50, actual.request.priority)
    self.assertEqual(True, actual.can_be_canceled)
    self.assertEqual(0, actual.current_task_slice)

  def test_result_summary_post_hook_sends_metric_at_completion(self):
    request = _gen_request()
    summary = task_result.new_result_summary(request)
    summary.modified_ts = self.now

    # on_task_completed should not be called when state is pending.
    self.mock(ts_mon_metrics, 'on_task_completed', self.fail)
    summary.put()

    # change state to completed
    summary.completed_ts = self.now
    summary.modified_ts = self.now
    summary.started_ts = self.now
    summary.duration = 1.
    summary.exit_code = 0
    summary.state = task_result.State.COMPLETED

    # on_task_completed should be called when state got updated to
    # completed.
    calls = []

    def on_task_completed(smry):
      calls.append(smry)

    self.mock(ts_mon_metrics, 'on_task_completed', on_task_completed)
    summary.put()
    self.assertEqual(len(calls), 1)

  def test_result_summary_post_hook_call_finalize_invocation(self):
    request = _gen_request(resultdb_update_token='secret')
    summary = task_result.new_result_summary(request)
    summary.modified_ts = self.now

    # Store current state for summary._prev_state
    summary.put()

    # change state to completed
    summary.completed_ts = self.now
    summary.modified_ts = self.now
    summary.started_ts = self.now
    summary.duration = 1.
    summary.exit_code = 0
    summary.state = task_result.State.COMPLETED
    summary.try_number = 1

    def on_task_completed(_):
      pass

    self.mock(ts_mon_metrics, 'on_task_completed', on_task_completed)

    @ndb.tasklet
    def nop_async(_run_id, _update_token):
      pass

    with mock.patch('server.resultdb.finalize_invocation_async',
                    mock.Mock(side_effect=nop_async)) as mocked:
      summary.put()
      mocked.assert_called_once_with('1d69b9f088008811', 'secret')

  def test_result_summary_post_hook_sends_metric_at_no_resource_failure(self):
    request = _gen_request()
    summary = task_result.new_result_summary(request)
    summary.completed_ts = self.now
    summary.modified_ts = self.now
    summary.state = task_result.State.NO_RESOURCE

    # on_task_completed should be called even at the initial write if it's the
    # end with no resource error.
    calls = []

    def on_task_completed(smry):
      calls.append(smry)

    self.mock(ts_mon_metrics, 'on_task_completed', on_task_completed)
    summary.put()

    self.assertEqual(len(calls), 1)

  def test_new_run_result_duration_no_exit_code(self):
    request = _gen_request()
    to_run = task_to_run.new_task_to_run(request, 0)
    actual = task_result.new_run_result(request, to_run, u'localhost', u'abc', {
        u'id': [u'localhost'],
        u'foo': [u'bar', u'biz']
    }, None)
    actual.completed_ts = self.now
    actual.modified_ts = self.now
    actual.started_ts = self.now
    actual.duration = 1.
    actual.state = task_result.State.COMPLETED
    # Trigger _pre_put_hook().
    with self.assertRaises(datastore_errors.BadValueError):
      actual.put()
    actual.state = task_result.State.TIMED_OUT
    actual.put()
    expected = self._gen_result(
        completed_ts=self.now,
        duration=1.,
        modified_ts=self.now,
        failure=True,
        started_ts=self.now,
        state=task_result.State.TIMED_OUT)
    self.assertEqual(expected, actual.to_dict())

  def test_integration(self):
    # Creates a TaskRequest, along its TaskResultSummary and TaskToRun. Have a
    # bot reap the task, and complete the task. Ensure the resulting
    # TaskResultSummary and TaskRunResult are properly updated.
    #
    # Force tedious chunking.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 2)

    request = _gen_request()
    result_summary = task_result.new_result_summary(request)
    to_run = task_to_run.new_task_to_run(request, 0)
    result_summary.modified_ts = utils.utcnow()
    ndb.transaction(lambda: ndb.put_multi([result_summary, to_run]))
    expected = self._gen_summary(modified_ts=self.now)
    self.assertEqual(expected, result_summary.to_dict())

    # Nothing changed 2 secs later except latency.
    self.mock_now(self.now, 2)
    self.assertEqual(expected, result_summary.to_dict())

    # Task is reaped after 2 seconds (4 secs total).
    reap_ts = self.now + datetime.timedelta(seconds=4)
    self.mock_now(reap_ts)
    to_run.queue_number = None
    to_run.put()
    run_result = task_result.new_run_result(request, to_run, u'localhost',
                                            u'abc', {},
                                            result_summary.resultdb_info)
    run_result.started_ts = utils.utcnow()
    run_result.modified_ts = run_result.started_ts
    run_result.dead_after_ts = utils.utcnow() + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))
    expected = self._gen_summary(
        bot_dimensions={},
        bot_version=u'abc',
        bot_id=u'localhost',
        costs_usd=[0.],
        modified_ts=reap_ts,
        state=task_result.State.RUNNING,
        started_ts=reap_ts,
        try_number=1)
    self.assertEqual(expected, result_summary.key.get().to_dict())

    # Task completed after 2 seconds (6 secs total), the task has been running
    # for 2 seconds.
    complete_ts = self.now + datetime.timedelta(seconds=6)
    self.mock_now(complete_ts)
    run_result.completed_ts = complete_ts
    run_result.duration = 0.1
    run_result.exit_code = 0
    run_result.state = task_result.State.COMPLETED
    run_result.modified_ts = utils.utcnow()
    run_result.dead_after_ts = None
    task_result.PerformanceStats(
        key=task_pack.run_result_key_to_performance_stats_key(run_result.key),
        bot_overhead=0.1,
        cache_trim=task_result.OperationStats(duration=0.01),
        package_installation=task_result.OperationStats(duration=0.01),
        named_caches_install=task_result.OperationStats(duration=0.01),
        named_caches_uninstall=task_result.OperationStats(duration=0.01),
        isolated_download=task_result.CASOperationStats(
            duration=0.05,
            initial_number_items=10,
            initial_size=10000,
            items_cold=large.pack([1, 2]),
            items_hot=large.pack([3, 4, 5])),
        isolated_upload=task_result.CASOperationStats(
            duration=0.01, items_cold=large.pack([10])),
        cleanup=task_result.OperationStats(duration=0.01)).put()
    ndb.transaction(lambda: ndb.put_multi(run_result.append_output('foo', 0)))
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))
    expected = self._gen_summary(
        bot_dimensions={},
        bot_version=u'abc',
        bot_id=u'localhost',
        completed_ts=complete_ts,
        costs_usd=[0.],
        duration=0.1,
        exit_code=0,
        modified_ts=complete_ts,
        state=task_result.State.COMPLETED,
        started_ts=reap_ts,
        try_number=1)
    self.assertEqual(expected, result_summary.key.get().to_dict())
    expected = {
        'bot_overhead': 0.1,
        'cache_trim': {
            'duration': 0.01,
        },
        'named_caches_install': {
            'duration': 0.01,
        },
        'named_caches_uninstall': {
            'duration': 0.01,
        },
        'isolated_download': {
            'duration': 0.05,
            'initial_number_items': 10,
            'initial_size': 10000,
            'items_cold': large.pack([1, 2]),
            'items_hot': large.pack([3, 4, 5]),
            'num_items_cold': 2,
            'total_bytes_items_cold': 3,
            'num_items_hot': 3,
            'total_bytes_items_hot': 12,
        },
        'isolated_upload': {
            'duration': 0.01,
            'initial_number_items': None,
            'initial_size': None,
            'items_cold': large.pack([10]),
            'items_hot': None,
            'num_items_cold': 1,
            'total_bytes_items_cold': 10,
            'num_items_hot': None,
            'total_bytes_items_hot': None,
        },
        'package_installation': {
            'duration': 0.01,
        },
        'cleanup': {
            'duration': 0.01,
        },
    }
    self.assertEqual(expected, result_summary.performance_stats.to_dict())
    self.assertEqual('foo', result_summary.get_output(0, 0))
    self.assertEqual(
        datetime.timedelta(seconds=2),
        result_summary.duration_as_seen_by_server)
    self.assertEqual(
        datetime.timedelta(seconds=0.1),
        result_summary.duration_now(utils.utcnow()))
    self.assertEqual(datetime.timedelta(seconds=4), result_summary.pending)
    self.assertEqual(
        datetime.timedelta(seconds=4),
        result_summary.pending_now(utils.utcnow()))

    self.assertEqual(
        task_pack.pack_result_summary_key(result_summary.key),
        result_summary.task_id)
    self.assertEqual(complete_ts, result_summary.ended_ts)
    self.assertEqual(
        task_pack.pack_run_result_key(run_result.key), run_result.task_id)
    self.assertEqual(complete_ts, run_result.ended_ts)

  def test_yield_result_summary_by_parent_task_id(self):
    # prepare parent task
    parent_run_result = _gen_run_result()
    parent_run_result_id = parent_run_result.task_id
    parent_summary = parent_run_result.key.parent().get()

    # create child task result summaries
    self.mock_now(self.now, 1)
    child_summary_1 = _gen_summary_result(parent_task_id=parent_run_result_id)
    self.mock_now(self.now, 2)
    child_summary_2 = _gen_summary_result(parent_task_id=parent_run_result_id)

    # should find the children by parent_task_id
    result_summary_iter = task_result.yield_result_summary_by_parent_task_id(
        parent_summary.task_id)
    expected = [child_summary_1, child_summary_2]
    self.assertEqual(sorted(expected), sorted(s for s in result_summary_iter))

  def test_yield_active_run_result_keys(self):
    request = _gen_request()
    result_summary = task_result.new_result_summary(request)
    result_summary.modified_ts = utils.utcnow()
    ndb.transaction(result_summary.put)
    to_run = task_to_run.new_task_to_run(request, 0)
    run_result = task_result.new_run_result(request, to_run, 'localhost', 'abc',
                                            {}, result_summary.resultdb_info)
    run_result.started_ts = utils.utcnow()
    run_result.modified_ts = run_result.started_ts
    run_result.dead_after_ts = run_result.started_ts + datetime.timedelta(
        seconds=1)
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi((run_result, result_summary)))

    self.assertEqual([run_result.key],
                     list(task_result.yield_active_run_result_keys()))

    run_result.completed_ts = run_result.started_ts
    run_result.put()
    self.assertEqual([], list(task_result.yield_active_run_result_keys()))

  def test_set_from_run_result(self):
    request = _gen_request()
    result_summary = task_result.new_result_summary(request)
    to_run = task_to_run.new_task_to_run(request, 0)
    run_result = task_result.new_run_result(request, to_run, 'localhost', 'abc',
                                            {}, result_summary.resultdb_info)
    run_result.started_ts = utils.utcnow()
    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    result_summary.modified_ts = utils.utcnow()
    run_result.modified_ts = utils.utcnow()
    run_result.dead_after_ts = utils.utcnow() + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))

    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi([result_summary]))

    self.assertFalse(result_summary.need_update_from_run_result(run_result))

  def test_set_from_run_result_two_server_versions(self):
    request = _gen_request()
    result_summary = task_result.new_result_summary(request)
    to_run = task_to_run.new_task_to_run(request, 0)
    run_result = task_result.new_run_result(request, to_run, 'localhost', 'abc',
                                            {}, result_summary.resultdb_info)
    run_result.started_ts = utils.utcnow()
    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    result_summary.modified_ts = utils.utcnow()
    run_result.modified_ts = utils.utcnow()
    run_result.dead_after_ts = utils.utcnow() + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))

    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi([result_summary]))

    run_result.signal_server_version('new-version')
    run_result.modified_ts = utils.utcnow()
    run_result.dead_after_ts = utils.utcnow() + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi((result_summary, run_result)))
    self.assertEqual(['v1a', 'new-version'],
                     run_result.key.get().server_versions)
    self.assertEqual(['v1a', 'new-version'],
                     result_summary.key.get().server_versions)

  def test_run_result_duration(self):
    run_result = task_result.TaskRunResult(
        started_ts=datetime.datetime(2010, 1, 1, 0, 0, 0),
        completed_ts=datetime.datetime(2010, 1, 1, 0, 2, 0))
    self.assertEqual(
        datetime.timedelta(seconds=120), run_result.duration_as_seen_by_server)
    self.assertEqual(
        datetime.timedelta(seconds=120),
        run_result.duration_now(utils.utcnow()))

    run_result = task_result.TaskRunResult(
        started_ts=datetime.datetime(2010, 1, 1, 0, 0, 0),
        abandoned_ts=datetime.datetime(2010, 1, 1, 0, 1, 0))
    self.assertEqual(None, run_result.duration_as_seen_by_server)
    self.assertEqual(None, run_result.duration_now(utils.utcnow()))

  def test_run_result_timeout(self):
    request = _gen_request()
    result_summary = task_result.new_result_summary(request)
    result_summary.modified_ts = utils.utcnow()
    ndb.transaction(result_summary.put)
    to_run = task_to_run.new_task_to_run(request, 0)
    run_result = task_result.new_run_result(request, to_run, 'localhost', 'abc',
                                            {}, result_summary.resultdb_info)
    run_result.state = task_result.State.TIMED_OUT
    run_result.duration = 0.1
    run_result.exit_code = -1
    run_result.started_ts = utils.utcnow()
    run_result.completed_ts = run_result.started_ts
    run_result.modified_ts = run_result.started_ts
    ndb.transaction(lambda: result_summary.set_from_run_result(
        run_result, request))
    ndb.transaction(lambda: ndb.put_multi((run_result, result_summary)))
    run_result = run_result.key.get()
    result_summary = result_summary.key.get()
    self.assertEqual(True, run_result.failure)
    self.assertEqual(True, result_summary.failure)

  def test_result_task_state(self):

    def check(expected, **kwargs):
      self.assertEqual(expected,
                       task_result.TaskResultSummary(**kwargs).task_state)

    # That's an incorrect state:
    check(swarming_pb2.TASK_STATE_INVALID, state=task_result.State.BOT_DIED)
    check(swarming_pb2.PENDING, state=task_result.State.PENDING)
    # https://crbug.com/915342: PENDING_DEDUPING
    check(swarming_pb2.RUNNING, state=task_result.State.RUNNING)
    # https://crbug.com/796757: RUNNING_OVERHEAD_SETUP
    # https://crbug.com/813412: RUNNING_OVERHEAD_TEARDOWN
    # https://crbug.com/916560: TERMINATING
    check(
        swarming_pb2.RAN_INTERNAL_FAILURE,
        internal_failure=True,
        state=task_result.State.BOT_DIED)
    # https://crbug.com/902807: DUT_FAILURE
    # https://crbug.com/916553: BOT_DISAPPEARED
    # https://crbug.com/916559: PREEMPTED
    check(swarming_pb2.COMPLETED, state=task_result.State.COMPLETED)
    check(swarming_pb2.TIMED_OUT, state=task_result.State.TIMED_OUT)
    # https://crbug.com/916556: TIMED_OUT_SILENCE
    check(swarming_pb2.KILLED, state=task_result.State.KILLED)
    # https://crbug.com/916553: MISSING_INPUTS
    check(
        swarming_pb2.DEDUPED,
        state=task_result.State.COMPLETED,
        deduped_from=u'123')
    check(swarming_pb2.EXPIRED, state=task_result.State.EXPIRED)
    check(swarming_pb2.CANCELED, state=task_result.State.CANCELED)
    check(swarming_pb2.NO_RESOURCE, state=task_result.State.NO_RESOURCE)
    # https://crbug.com/916562: LOAD_SHED
    # https://crbug.com/916557: RESOURCE_EXHAUSTED

  # TODO(crbug.com/1115778): remove after RBE-CAS migration.
  def test_TaskRunResult_to_proto_isolated(self):
    cipd_client_pkg = task_request.CipdPackage(
        package_name=u'infra/tools/cipd/${platform}',
        version=u'git_revision:deadbeef')
    run_result = _gen_run_result(
        properties=_gen_properties(
            cipd_input={
                u'client_package': cipd_client_pkg,
                u'packages': [
                    task_request.CipdPackage(
                        package_name=u'rm', path=u'bin', version=u'latest'),
                ],
                u'server': u'http://localhost:2'
            },
            containment=task_request.Containment(lower_priority=True),
        ),)
    run_result.started_ts = self.now + datetime.timedelta(seconds=20)
    run_result.abandoned_ts = self.now + datetime.timedelta(seconds=30)
    run_result.completed_ts = self.now + datetime.timedelta(seconds=40)
    run_result.modified_ts = self.now + datetime.timedelta(seconds=50)
    run_result.duration = 1.
    run_result.current_task_slice = 2
    run_result.exit_code = 1
    run_result.children_task_ids = [u'12310']
    run_result.outputs_ref = task_request.FilesRef(
        isolated=u'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
        isolatedserver=u'http://localhost:1',
        namespace=u'default-gzip')
    run_result.cipd_pins = task_result.CipdPins(
        client_package=cipd_client_pkg,
        packages=[
            task_request.CipdPackage(
                package_name=u'rm', path=u'bin', version=u'stable'),
        ])
    task_result.PerformanceStats(
        key=task_pack.run_result_key_to_performance_stats_key(run_result.key),
        bot_overhead=0.1,
        cache_trim=task_result.OperationStats(duration=0.001),
        package_installation=task_result.OperationStats(duration=0.002),
        named_caches_install=task_result.OperationStats(duration=0.003),
        named_caches_uninstall=task_result.OperationStats(duration=0.004),
        isolated_download=task_result.CASOperationStats(
            duration=0.05,
            initial_number_items=10,
            initial_size=10000,
            items_cold=large.pack([1, 2]),
            items_hot=large.pack([3, 4, 5])),
        isolated_upload=task_result.CASOperationStats(
            duration=0.01, items_cold=large.pack([10])),
        cleanup=task_result.OperationStats(duration=0.01)).put()

    # Note: It cannot be both TIMED_OUT and have run_result.deduped_from set.
    run_result.state = task_result.State.TIMED_OUT
    run_result.bot_dimensions = {u'id': [u'bot1'], u'pool': [u'default']}
    run_result.dead_after_ts = None
    run_result.put()

    props_h = '5c7429e5ab9a21f37ec8c39dcbafbe41127fa67075b92ab0642861bb06578a12'
    expected = swarming_pb2.TaskResult(
        request=swarming_pb2.TaskRequest(
            task_slices=[
                swarming_pb2.TaskSlice(
                    properties=swarming_pb2.TaskProperties(
                        cipd_inputs=[
                            swarming_pb2.CIPDPackage(
                                package_name=u'rm',
                                version=u'latest',
                                dest_path=u'bin',
                            ),
                        ],
                        containment=swarming_pb2.Containment(
                            lower_priority=True),
                        command=[u'command1'],
                        dimensions=[
                            swarming_pb2.StringListPair(
                                key=u'pool', values=[u'default']),
                        ],
                        execution_timeout=duration_pb2.Duration(seconds=86400),
                        grace_period=duration_pb2.Duration(seconds=30),
                    ),
                    expiration=duration_pb2.Duration(seconds=60),
                    properties_hash=props_h,
                ),
            ],
            priority=50,
            service_account=u'none',
            name=u'Request name',
            authenticated=u"user:mocked@example.com",
            tags=[
                u"authenticated:user:mocked@example.com",
                u'pool:default',
                u'priority:50',
                u'realm:none',
                u'service_account:none',
                u'swarming.pool.template:no_config',
                u'tag:1',
                u"use_cas_1143123:0",
                u"use_isolate_1143123:0",
                u'user:Jesus',
            ],
            user=u'Jesus',
            task_id=u'1d69b9f088008810',
        ),
        duration=duration_pb2.Duration(seconds=1),
        state=swarming_pb2.TIMED_OUT,
        state_category=swarming_pb2.CATEGORY_EXECUTION_DONE,
        try_number=1,
        current_task_slice=2,
        bot=swarming_pb2.Bot(
            bot_id=u'bot1',
            pools=[u'default'],
            dimensions=[
                swarming_pb2.StringListPair(key=u'id', values=[u'bot1']),
                swarming_pb2.StringListPair(key=u'pool', values=[u'default']),
            ],
        ),
        server_versions=[u'v1a'],
        children_task_ids=[u'12310'],
        #deduped_from=u'123410',
        task_id=u'1d69b9f088008810',
        run_id=u'1d69b9f088008811',
        cipd_pins=swarming_pb2.CIPDPins(
            server=u'http://localhost:2',
            client_package=swarming_pb2.CIPDPackage(
                package_name=u'infra/tools/cipd/${platform}',
                version=u'git_revision:deadbeef',
            ),
            packages=[
                swarming_pb2.CIPDPackage(
                    package_name=u'rm',
                    version=u'stable',
                    dest_path=u'bin',
                ),
            ],
        ),
        performance=swarming_pb2.TaskPerformance(
            total_overhead=duration_pb2.Duration(nanos=100000000),
            other_overhead=duration_pb2.Duration(nanos=20000000),
            setup=swarming_pb2.TaskOverheadStats(
                duration=duration_pb2.Duration(nanos=56000000),
                cold=swarming_pb2.CASEntriesStats(
                    num_items=2,
                    total_bytes_items=3,
                ),
                hot=swarming_pb2.CASEntriesStats(
                    num_items=3,
                    total_bytes_items=12,
                ),
            ),
            setup_overhead=swarming_pb2.TaskSetupOverhead(
                duration=duration_pb2.Duration(nanos=56000000),
                cache_trim=swarming_pb2.CacheTrimOverhead(
                    duration=duration_pb2.Duration(nanos=1000000)),
                cipd=swarming_pb2.CIPDOverhead(
                    duration=duration_pb2.Duration(nanos=2000000)),
                named_cache=swarming_pb2.NamedCacheOverhead(
                    duration=duration_pb2.Duration(nanos=3000000)),
                cas=swarming_pb2.CASOverhead(
                    duration=duration_pb2.Duration(nanos=50000000),
                    cold=swarming_pb2.CASEntriesStats(
                        num_items=2,
                        total_bytes_items=3,
                    ),
                    hot=swarming_pb2.CASEntriesStats(
                        num_items=3,
                        total_bytes_items=12,
                    ),
                ),
            ),
            teardown=swarming_pb2.TaskOverheadStats(
                duration=duration_pb2.Duration(nanos=24000000),
                cold=swarming_pb2.CASEntriesStats(
                    num_items=1,
                    total_bytes_items=10,
                ),
            ),
            teardown_overhead=swarming_pb2.TaskTeardownOverhead(
                duration=duration_pb2.Duration(nanos=24000000),
                cas=swarming_pb2.CASOverhead(
                    duration=duration_pb2.Duration(nanos=10000000),
                    cold=swarming_pb2.CASEntriesStats(
                        num_items=1,
                        total_bytes_items=10,
                    )),
                named_cache=swarming_pb2.NamedCacheOverhead(
                    duration=duration_pb2.Duration(nanos=4000000)),
                cleanup=swarming_pb2.CleanupOverhead(
                    duration=duration_pb2.Duration(nanos=10000000)),
            ),
        ),
        exit_code=1,
        outputs=swarming_pb2.CASTree(
            digest=u'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
            server=u'http://localhost:1',
            namespace=u'default-gzip'))
    expected.request.create_time.FromDatetime(self.now)
    expected.create_time.FromDatetime(self.now)
    expected.start_time.FromDatetime(self.now + datetime.timedelta(seconds=20))
    expected.abandon_time.FromDatetime(self.now +
                                       datetime.timedelta(seconds=30))
    expected.end_time.FromDatetime(self.now + datetime.timedelta(seconds=40))

    actual = swarming_pb2.TaskResult()
    run_result.to_proto(actual)
    self.assertEqual(unicode(expected), unicode(actual))

  def test_TaskRunResult_to_proto(self):
    cipd_client_pkg = task_request.CipdPackage(
        package_name=u'infra/tools/cipd/${platform}',
        version=u'git_revision:deadbeef')
    # Grand parent entity must have a valid key id and be stored.
    # This task uses user:Jesus, which will be inherited automatically.
    grand_parent = _gen_request()
    grand_parent.key = task_request.new_request_key()
    grand_parent.put()
    # Parent entity must have a valid key id and be stored.
    # Create them 1 second apart to differentiate create_time.
    self.mock_now(self.now, 1)
    grand_parent_run_id = grand_parent.task_id[:-1] + u'1'
    parent = _gen_request(parent_task_id=grand_parent_run_id)
    parent.key = task_request.new_request_key()
    parent.put()
    self.mock_now(self.now, 2)

    run_result = _gen_run_result(
        parent_task_id=parent.task_id[:-1] + u'1',
        properties=_gen_properties(
            cipd_input={
                u'client_package': cipd_client_pkg,
                u'packages': [
                    task_request.CipdPackage(
                        package_name=u'rm', path=u'bin', version=u'latest'),
                ],
                u'server': u'http://localhost:2'
            },
            containment=task_request.Containment(lower_priority=True),
        ),
    )
    run_result.started_ts = self.now + datetime.timedelta(seconds=20)
    run_result.abandoned_ts = self.now + datetime.timedelta(seconds=30)
    run_result.completed_ts = self.now + datetime.timedelta(seconds=40)
    run_result.modified_ts = self.now + datetime.timedelta(seconds=50)
    run_result.duration = 1.
    run_result.current_task_slice = 2
    run_result.exit_code = 1
    run_result.children_task_ids = [u'12310']
    run_result.cas_output_root = task_request.CASReference(
        cas_instance=u'projects/test/instances/default',
        digest={
            'hash': u'12345',
            'size_bytes': 1,
        })
    run_result.cipd_pins = task_result.CipdPins(
        client_package=cipd_client_pkg,
        packages=[
            task_request.CipdPackage(
                package_name=u'rm', path=u'bin', version=u'stable'),
        ])
    task_result.PerformanceStats(
        key=task_pack.run_result_key_to_performance_stats_key(run_result.key),
        bot_overhead=0.1,
        cache_trim=task_result.OperationStats(duration=0.001),
        package_installation=task_result.OperationStats(duration=0.002),
        named_caches_install=task_result.OperationStats(duration=0.003),
        named_caches_uninstall=task_result.OperationStats(duration=0.004),
        isolated_download=task_result.CASOperationStats(
            duration=0.05,
            initial_number_items=10,
            initial_size=10000,
            items_cold=large.pack([1, 2]),
            items_hot=large.pack([3, 4, 5])),
        isolated_upload=task_result.CASOperationStats(
            duration=0.01, items_cold=large.pack([10])),
        cleanup=task_result.OperationStats(duration=0.01)).put()

    # Note: It cannot be both TIMED_OUT and have run_result.deduped_from set.
    run_result.state = task_result.State.TIMED_OUT
    run_result.bot_dimensions = {u'id': [u'bot1'], u'pool': [u'default']}
    run_result.dead_after_ts = None
    run_result.put()

    props_h = '5c7429e5ab9a21f37ec8c39dcbafbe41127fa67075b92ab0642861bb06578a12'
    expected = swarming_pb2.TaskResult(
        request=swarming_pb2.TaskRequest(
            task_slices=[
                swarming_pb2.TaskSlice(
                    properties=swarming_pb2.TaskProperties(
                        cipd_inputs=[
                            swarming_pb2.CIPDPackage(
                                package_name=u'rm',
                                version=u'latest',
                                dest_path=u'bin',
                            ),
                        ],
                        containment=swarming_pb2.Containment(
                            lower_priority=True),
                        command=[u'command1'],
                        dimensions=[
                            swarming_pb2.StringListPair(
                                key=u'pool', values=[u'default']),
                        ],
                        execution_timeout=duration_pb2.Duration(seconds=86400),
                        grace_period=duration_pb2.Duration(seconds=30),
                    ),
                    expiration=duration_pb2.Duration(seconds=60),
                    properties_hash=props_h,
                ),
            ],
            priority=50,
            service_account=u'none',
            name=u'Request name',
            authenticated=u"user:mocked@example.com",
            tags=[
                u"authenticated:user:mocked@example.com",
                u'parent_task_id:1d69b9f470008811',
                u'pool:default',
                u'priority:50',
                u'realm:none',
                u'service_account:none',
                u'swarming.pool.template:no_config',
                u'tag:1',
                u"use_cas_1143123:0",
                u"use_isolate_1143123:0",
                u'user:Jesus',
            ],
            user=u'Jesus',
            task_id=u'1d69b9f858008810',
            parent_task_id='1d69b9f470008810',
            parent_run_id='1d69b9f470008811',
        ),
        duration=duration_pb2.Duration(seconds=1),
        state=swarming_pb2.TIMED_OUT,
        state_category=swarming_pb2.CATEGORY_EXECUTION_DONE,
        try_number=1,
        current_task_slice=2,
        bot=swarming_pb2.Bot(
            bot_id=u'bot1',
            pools=[u'default'],
            dimensions=[
                swarming_pb2.StringListPair(key=u'id', values=[u'bot1']),
                swarming_pb2.StringListPair(key=u'pool', values=[u'default']),
            ],
        ),
        server_versions=[u'v1a'],
        children_task_ids=[u'12310'],
        task_id=u'1d69b9f858008810',
        run_id=u'1d69b9f858008811',
        cipd_pins=swarming_pb2.CIPDPins(
            server=u'http://localhost:2',
            client_package=swarming_pb2.CIPDPackage(
                package_name=u'infra/tools/cipd/${platform}',
                version=u'git_revision:deadbeef',
            ),
            packages=[
                swarming_pb2.CIPDPackage(
                    package_name=u'rm',
                    version=u'stable',
                    dest_path=u'bin',
                ),
            ],
        ),
        performance=swarming_pb2.TaskPerformance(
            total_overhead=duration_pb2.Duration(nanos=100000000),
            other_overhead=duration_pb2.Duration(nanos=20000000),
            setup=swarming_pb2.TaskOverheadStats(
                duration=duration_pb2.Duration(nanos=56000000),
                cold=swarming_pb2.CASEntriesStats(
                    num_items=2,
                    total_bytes_items=3,
                ),
                hot=swarming_pb2.CASEntriesStats(
                    num_items=3,
                    total_bytes_items=12,
                ),
            ),
            setup_overhead=swarming_pb2.TaskSetupOverhead(
                duration=duration_pb2.Duration(nanos=56000000),
                cache_trim=swarming_pb2.CacheTrimOverhead(
                    duration=duration_pb2.Duration(nanos=1000000)),
                cipd=swarming_pb2.CIPDOverhead(
                    duration=duration_pb2.Duration(nanos=2000000)),
                named_cache=swarming_pb2.NamedCacheOverhead(
                    duration=duration_pb2.Duration(nanos=3000000)),
                cas=swarming_pb2.CASOverhead(
                    duration=duration_pb2.Duration(nanos=50000000),
                    cold=swarming_pb2.CASEntriesStats(
                        num_items=2,
                        total_bytes_items=3,
                    ),
                    hot=swarming_pb2.CASEntriesStats(
                        num_items=3,
                        total_bytes_items=12,
                    ),
                ),
            ),
            teardown=swarming_pb2.TaskOverheadStats(
                duration=duration_pb2.Duration(nanos=24000000),
                cold=swarming_pb2.CASEntriesStats(
                    num_items=1,
                    total_bytes_items=10,
                ),
            ),
            teardown_overhead=swarming_pb2.TaskTeardownOverhead(
                duration=duration_pb2.Duration(nanos=24000000),
                cas=swarming_pb2.CASOverhead(
                    duration=duration_pb2.Duration(nanos=10000000),
                    cold=swarming_pb2.CASEntriesStats(
                        num_items=1,
                        total_bytes_items=10,
                    )),
                named_cache=swarming_pb2.NamedCacheOverhead(
                    duration=duration_pb2.Duration(nanos=4000000)),
                cleanup=swarming_pb2.CleanupOverhead(
                    duration=duration_pb2.Duration(nanos=10000000)),
            ),
        ),
        exit_code=1,
        cas_output_root=swarming_pb2.CASReference(
            cas_instance=u'projects/test/instances/default',
            digest=swarming_pb2.Digest(hash='12345', size_bytes=1),
        ),
    )
    expected.request.create_time.FromDatetime(self.now +
                                              datetime.timedelta(seconds=2))
    expected.create_time.FromDatetime(self.now + datetime.timedelta(seconds=2))
    expected.start_time.FromDatetime(self.now + datetime.timedelta(seconds=20))
    expected.abandon_time.FromDatetime(self.now +
                                       datetime.timedelta(seconds=30))
    expected.end_time.FromDatetime(self.now + datetime.timedelta(seconds=40))

    actual = swarming_pb2.TaskResult()
    run_result.to_proto(actual)
    self.assertEqual(unicode(expected), unicode(actual))

    # Make sure the root task id is the grand parent.
    self.assertEqual(u'1d69b9f088008810', grand_parent.task_id)
    self.assertEqual(u'1d69b9f088008811', grand_parent_run_id)
    # Confirming that the parent and grand parent have different task ID.
    self.assertEqual(u'1d69b9f470008810', parent.task_id)
    self.assertEqual(expected.request.parent_task_id, parent.task_id)
    actual = swarming_pb2.TaskResult()
    expected.request.root_task_id = grand_parent.task_id
    expected.request.root_run_id = grand_parent_run_id
    run_result.to_proto(actual, append_root_ids=True)
    self.assertEqual(unicode(expected), unicode(actual))

  @parameterized.expand([
      (2**31 - 1,),
      (-2**31,),
      (3221225786,), # 0xc000013a, STATUS_CONTROL_C_EXIT on Windows
      (2**31,), # 0x80000000
      (2**32 - 1,), # 0xffffffff
      (2**32,), # 33bit
      (-2**31 - 1,), # 33bit
  ])
  def test_TaskRunResult_to_proto_exitcode(self, exit_code):
    actual = swarming_pb2.TaskResult()
    req = task_request.TaskRequest(id=1230)
    res = task_result.TaskResultSummary(parent=req.key)
    res._request_cache = req
    res.exit_code = exit_code
    res.to_proto(actual)
    self.assertEqual(actual.exit_code, exit_code)

  def test_TaskResultSummary_to_proto_empty(self):
    # Assert that it doesn't throw on empty entity.
    actual = swarming_pb2.TaskResult()
    # It's unreasonable to expect the entity key to be unset, which complicates
    # this test a bit.
    req = task_request.TaskRequest(id=1230)
    res = task_result.TaskResultSummary(parent=req.key)
    res._request_cache = req
    res.to_proto(actual)
    expected = swarming_pb2.TaskResult(
        request=swarming_pb2.TaskRequest(task_id='7ffffffffffffb310'),
        state=swarming_pb2.PENDING,
        state_category=swarming_pb2.CATEGORY_PENDING,
        task_id='7ffffffffffffb310')
    self.assertEqual(expected, actual)

  def test_TaskRunResult_to_proto_empty(self):
    # Assert that it doesn't throw on empty entity.
    actual = swarming_pb2.TaskResult()
    # It's unreasonable to expect the entity key to be unset, which complicates
    # this test a bit.
    req = task_request.TaskRequest(id=1230)
    res_sum = task_result.TaskResultSummary(parent=req.key, id=1)
    res_sum._request_cache = req
    res = task_result.TaskRunResult(parent=res_sum.key, id=1)
    res._request_cache = req
    res.to_proto(actual)
    expected = swarming_pb2.TaskResult(
        request=swarming_pb2.TaskRequest(task_id='7ffffffffffffb310'),
        state=swarming_pb2.RUNNING,
        state_category=swarming_pb2.CATEGORY_RUNNING,
        try_number=1,
        task_id='7ffffffffffffb310',
        run_id='7ffffffffffffb311')
    self.assertEqual(expected, actual)

  def test_performance_stats_pre_put_hook(self):
    with self.assertRaises(datastore_errors.BadValueError):
      task_result.PerformanceStats().put()

  def test_cron_update_tags(self):
    # > 1h
    self.mock_now(self.now, -3)
    summary1 = task_result.new_result_summary(_gen_request())
    summary1.modified_ts = self.now - datetime.timedelta(minutes=60)
    summary1.tags.append('mtime:60')
    summary1.put()

    # < 1h
    self.mock_now(self.now, -2)
    summary2 = task_result.new_result_summary(_gen_request())
    summary2.modified_ts = self.now - datetime.timedelta(minutes=59)
    summary2.tags.append('mtime:59')
    summary2.put()

    # >= 128 tags
    self.mock_now(self.now, -1)
    summary3 = task_result.new_result_summary(_gen_request())
    summary3.modified_ts = self.now
    summary3.tags.append('mtime:now')
    for n in range(0, 128):
        summary3.tags.append('n:' + str(n))
    summary3.put()

    self.mock_now(self.now, 0)

    expected = task_result.TagAggregation(
        key=task_result.TagAggregation.KEY,
        tags=[task_result.TagValues(
                tag=u'authenticated', values=[u'user:mocked@example.com']),
            task_result.TagValues(tag=u'mtime', values=[u'59', u'now']),
            task_result.TagValues(tag=u'n', values=[]), # >= 128
            task_result.TagValues(tag=u'pool', values=[u'default']),
            task_result.TagValues(tag=u'priority', values=[u'50']),
            task_result.TagValues(tag=u'realm', values=[u'none']),
            task_result.TagValues(tag=u'service_account', values=[u'none']),
            task_result.TagValues(
                tag=u'swarming.pool.template', values=[u'no_config']),
            task_result.TagValues(tag=u'tag', values=[u'1']),
            task_result.TagValues(tag=u'use_cas_1143123', values=[u'0']),
            task_result.TagValues(tag=u'use_isolate_1143123', values=[u'0']),
            task_result.TagValues(tag=u'user', values=[u'Jesus'])],
            ts=self.now)

    self.assertEqual(12, task_result.cron_update_tags())
    self.assertEqual(expected, task_result.TagAggregation.KEY.get())

  def _mock_send_to_bq(self, expected_table_name):
    payloads = []

    def send_to_bq(table_name, rows):
      self.assertEqual(expected_table_name, table_name)
      if rows:
        # When rows is empty, send_to_bq() can exit early.
        payloads.append(rows)

    self.mock(bq_state, 'send_to_bq', send_to_bq)
    return payloads

  def test_task_bq_run_empty(self):
    # Empty, nothing is done.
    start = utils.utcnow()
    end = start + datetime.timedelta(seconds=60)
    self.assertEqual(0, task_result.task_bq_run(start, end))

  def test_task_bq_run(self):
    payloads = self._mock_send_to_bq('task_results_run')

    # Generate 4 tasks results to test boundaries.
    self.mock_now(self.now, 10)
    run_result_1 = _gen_run_result()
    run_result_1.abandoned_ts = utils.utcnow()
    run_result_1.completed_ts = utils.utcnow()
    run_result_1.modified_ts = utils.utcnow()
    run_result_1.put()
    start = self.mock_now(self.now, 20)
    run_result_2 = _gen_run_result()
    run_result_2.completed_ts = utils.utcnow()
    run_result_2.modified_ts = utils.utcnow()
    run_result_2.put()
    end = self.mock_now(self.now, 30)
    run_result_3 = _gen_run_result()
    run_result_3.completed_ts = utils.utcnow()
    run_result_3.modified_ts = utils.utcnow()
    run_result_3.put()
    self.mock_now(self.now, 40)
    run_result_4 = _gen_run_result()
    run_result_4.completed_ts = utils.utcnow()
    run_result_4.modified_ts = utils.utcnow()
    run_result_4.put()

    self.assertEqual(2, task_result.task_bq_run(start, end))
    self.assertEqual(1, len(payloads), payloads)
    actual_rows = payloads[0]
    self.assertEqual(2, len(actual_rows))
    expected = [
        run_result_2.task_id,
        run_result_3.task_id,
    ]
    self.assertEqual(expected, [r[0] for r in actual_rows])

  def test_task_bq_run_running(self):
    payloads = self._mock_send_to_bq('task_results_run')
    self.now = datetime.datetime(2019, 1, 1)
    start = self.mock_now(self.now, 0)
    run_result = _gen_run_result()
    run_result.started_ts = utils.utcnow()
    run_result.modified_ts = utils.utcnow()
    run_result.put()
    end = self.mock_now(self.now, 60)

    self.assertEqual(0, task_result.task_bq_run(start, end))
    self.assertEqual(0, len(payloads), payloads)

  def test_task_bq_run_recent_abandoned_ts(self):
    # Confirm that a recent entity without completed_ts set is not found.
    payloads = self._mock_send_to_bq('task_results_run')
    start = self.now
    run_result = _gen_run_result()
    # Make sure started_ts is not caught.
    run_result.started_ts = datetime.datetime(2010, 1, 1)
    run_result.abandoned_ts = utils.utcnow()
    run_result.modified_ts = utils.utcnow()
    run_result.put()
    self.assertIsNone(run_result.key.get().completed_ts)
    end = self.mock_now(self.now, 60)

    self.assertEqual(0, task_result.task_bq_run(start, end))
    self.assertEqual(0, len(payloads), payloads)

  def test_task_bq_summary_empty(self):
    # Empty, nothing is done.
    start = utils.utcnow()
    end = start + datetime.timedelta(seconds=60)
    self.assertEqual(0, task_result.task_bq_summary(start, end))

  def test_task_bq_summary(self):
    payloads = self._mock_send_to_bq('task_results_summary')

    # Generate 4 tasks results to test boundaries.
    self.mock_now(self.now, 10)
    result_1 = _gen_summary_result()
    result_1.abandoned_ts = utils.utcnow()
    result_1.completed_ts = utils.utcnow()
    result_1.modified_ts = utils.utcnow()
    result_1.put()
    start = self.mock_now(self.now, 20)
    result_2 = _gen_summary_result()
    result_2.completed_ts = utils.utcnow()
    result_2.modified_ts = utils.utcnow()
    result_2.put()
    end = self.mock_now(self.now, 30)
    result_3 = _gen_summary_result()
    result_3.completed_ts = utils.utcnow()
    result_3.modified_ts = utils.utcnow()
    result_3.put()
    self.mock_now(self.now, 40)
    result_4 = _gen_summary_result()
    result_4.completed_ts = utils.utcnow()
    result_4.modified_ts = utils.utcnow()
    result_4.put()

    self.assertEqual(2, task_result.task_bq_summary(start, end))
    self.assertEqual(1, len(payloads), payloads)
    actual_rows = payloads[0]
    self.assertEqual(2, len(actual_rows))
    expected = [
        result_2.task_id,
        result_3.task_id,
    ]
    self.assertEqual(expected, [r[0] for r in actual_rows])

  def test_task_bq_summary_pending(self):
    payloads = self._mock_send_to_bq('task_results_summary')
    self.now = datetime.datetime(2019, 2, 28)
    start = self.mock_now(self.now, 0)
    result = _gen_summary_result()
    result.created_ts = utils.utcnow()
    result.modified_ts = utils.utcnow()
    result.put()
    end = self.mock_now(self.now, 60)

    self.assertEqual(0, task_result.task_bq_summary(start, end))
    self.assertEqual(0, len(payloads), payloads)

  def test_task_bq_summary_running(self):
    payloads = self._mock_send_to_bq('task_results_summary')
    self.now = datetime.datetime(2019, 2, 28)
    start = self.mock_now(self.now, 0)
    result = _gen_summary_result()
    result.started_ts = utils.utcnow()
    result.modified_ts = utils.utcnow()
    result.put()
    end = self.mock_now(self.now, 60)

    self.assertEqual(0, task_result.task_bq_summary(start, end))
    self.assertEqual(0, len(payloads), payloads)

  def test_task_bq_summary_recent_abandoned_ts(self):
    # Confirm that a recent entity without completed_ts set is not found.
    payloads = self._mock_send_to_bq('task_results_summary')
    start = self.now
    result = _gen_summary_result()
    # Make sure neither created_ts and started_ts is caught.
    result.created_ts = datetime.datetime(2010, 1, 1)
    result.started_ts = datetime.datetime(2010, 1, 1)
    result.abandoned_ts = utils.utcnow()
    result.modified_ts = utils.utcnow()
    result.put()
    self.assertIsNone(result.key.get().completed_ts)
    end = self.mock_now(self.now, 60)

    self.assertEqual(0, task_result.task_bq_summary(start, end))
    self.assertEqual(0, len(payloads), payloads)

  def test_get_result_summaries_query(self):
    # Indirectly tested by API.
    pass

  def test_get_run_results_query(self):
    # Indirectly tested by API.
    pass


class TestOutput(TestCase):

  def assertTaskOutputChunk(self, expected):
    q = task_result.TaskOutputChunk.query().order(
        task_result.TaskOutputChunk.key)
    self.assertEqual(expected, [t.to_dict() for t in q.fetch()])

  def test_append_output(self):
    # Force tedious chunking.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 2)
    run_result = _gen_run_result()

    # Test that one can stream output and it is returned fine.
    def run(*args):
      ndb.put_multi(run_result.append_output(*args))

    run('Part1\n', 0)
    run('Part2\n', len('Part1\n'))
    run('Part3\n', len('Part1P\n'))
    self.assertEqual('Part1\nPPart3\n', run_result.get_output(0, 0))

  def test_append_output_max_chunk(self):
    # Ensures that data is dropped.
    # Force tedious chunking.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 2)
    self.mock(task_result.TaskOutput, 'PUT_MAX_CHUNKS', 16)
    self.assertEqual(2 * 16, task_result.TaskOutput.PUT_MAX_CONTENT())

    run_result = _gen_run_result()

    calls = []
    self.mock(logging, 'warning', lambda *args: calls.append(args))
    max_chunk = 'x' * task_result.TaskOutput.PUT_MAX_CONTENT()
    entities = run_result.append_output(max_chunk, 0)
    self.assertEqual(task_result.TaskOutput.PUT_MAX_CHUNKS, len(entities))
    ndb.put_multi(entities)
    self.assertEqual([], calls)

    # Try with PUT_MAX_CONTENT + 1 bytes, so the last byte is discarded.
    entities = run_result.append_output(max_chunk + 'x', 0)
    self.assertEqual(task_result.TaskOutput.PUT_MAX_CHUNKS, len(entities))
    ndb.put_multi(entities)
    self.assertEqual(1, len(calls))
    self.assertTrue(calls[0][0].startswith('Dropping '), calls[0][0])
    self.assertEqual(1, calls[0][1])

  def test_append_output_partial(self):
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output('Foo', 10))
    expected_output = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Foo'
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    self.assertTaskOutputChunk([{'chunk': expected_output, 'gaps': [0, 10]}])

  def test_append_output_partial_hole(self):
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output('Foo', 0))
    ndb.put_multi(run_result.append_output('Bar', 10))
    expected_output = 'Foo\x00\x00\x00\x00\x00\x00\x00Bar'
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    self.assertTaskOutputChunk([{'chunk': expected_output, 'gaps': [3, 10]}])

  def test_append_output_partial_far(self):
    run_result = _gen_run_result()
    ndb.put_multi(
        run_result.append_output('Foo', 10 + task_result.TaskOutput.CHUNK_SIZE))
    expected_output = '\x00' * (task_result.TaskOutput.CHUNK_SIZE + 10) + 'Foo'
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    expected = [
        {
            'chunk': '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Foo',
            'gaps': [0, 10]
        },
    ]
    self.assertTaskOutputChunk(expected)

  def test_append_output_partial_far_split(self):
    # Missing, writing happens on two different TaskOutputChunk entities.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 16)
    run_result = _gen_run_result()
    ndb.put_multi(
        run_result.append_output('FooBar',
                                 2 * task_result.TaskOutput.CHUNK_SIZE - 3))
    expected_output = ('\x00' * (task_result.TaskOutput.CHUNK_SIZE * 2 - 3) +
                       'FooBar')
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    expected = [
        {
            'chunk': '\x00' * (task_result.TaskOutput.CHUNK_SIZE - 3) + 'Foo',
            'gaps': [0, 13],
        },
        {
            'chunk': 'Bar',
            'gaps': []
        },
    ]
    self.assertTaskOutputChunk(expected)

  def test_append_output_overwrite(self):
    # Overwrite previously written data.
    # Force tedious chunking.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 2)
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output('FooBar', 0))
    ndb.put_multi(run_result.append_output('X', 3))
    self.assertEqual('FooXar', run_result.get_output(0, 0))
    self.assertTaskOutputChunk([
        {
            'chunk': 'Fo',
            'gaps': []
        },
        {
            'chunk': 'oX',
            'gaps': []
        },
        {
            'chunk': 'ar',
            'gaps': []
        },
    ])

  def test_append_output_reverse_order(self):
    # Write the data in reverse order in multiple calls.
    # TODO(maruel): This isn't working perfectly, for example this fails if
    # CHUNK_SIZE is mocked to 8.
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output('Wow', 11))
    ndb.put_multi(run_result.append_output('Foo', 8))
    ndb.put_multi(run_result.append_output('Baz', 0))
    ndb.put_multi(run_result.append_output('Bar', 4))
    expected_output = 'Baz\x00Bar\x00FooWow'
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    self.assertTaskOutputChunk([{
        'chunk': expected_output,
        'gaps': [3, 4, 7, 8]
    }])

  def test_append_output_reverse_order_second_chunk(self):
    # Write the data in reverse order in multiple calls.
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 16)
    run_result = _gen_run_result()
    ndb.put_multi(
        run_result.append_output('Wow', task_result.TaskOutput.CHUNK_SIZE + 11))
    ndb.put_multi(
        run_result.append_output('Foo', task_result.TaskOutput.CHUNK_SIZE + 8))
    ndb.put_multi(
        run_result.append_output('Baz', task_result.TaskOutput.CHUNK_SIZE + 0))
    ndb.put_multi(
        run_result.append_output('Bar', task_result.TaskOutput.CHUNK_SIZE + 4))
    expected_output = (
        task_result.TaskOutput.CHUNK_SIZE * '\x00' + 'Baz\x00Bar\x00FooWow')
    self.assertEqual(expected_output, run_result.get_output(0, 0))
    self.assertTaskOutputChunk([{
        'chunk': 'Baz\x00Bar\x00FooWow',
        'gaps': [3, 4, 7, 8]
    }])

  def test_get_output_subset(self):
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 16)
    run_result = _gen_run_result()
    data = string.ascii_letters
    ndb.put_multi(run_result.append_output(data, 0))
    self.assertEqual(data[12:18], run_result.get_output(12, 6))
    self.assertEqual(data[12:], run_result.get_output(12, 0))

  def test_get_output_utf8(self):
    self.mock(task_result.TaskOutput, 'CHUNK_SIZE', 4)
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output(b'Foo🤠Bar', 0))
    self.assertEqual(b'Foo🤠Bar', run_result.get_output(0, 0))
    self.assertTaskOutputChunk([
        {
            'chunk': b'Foo\xf0',
            'gaps': []
        },
        {
            'chunk': b'\x9f\xa4\xa0B',
            'gaps': []
        },
        {
            'chunk': b'ar',
            'gaps': []
        },
    ])

  def test_get_output_utf8_range(self):
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output(b'Foo🤠Bar', 0))
    self.assertEqual(b'🤠', run_result.get_output(3, 4))

  def test_get_output_utf8_limit(self):
    run_result = _gen_run_result()
    ndb.put_multi(run_result.append_output(b'😀😃😄😁😆', 0))
    self.assertEqual(b'😀😃😄😁', run_result.get_output(0, 16))

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
