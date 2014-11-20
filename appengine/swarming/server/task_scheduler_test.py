#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import inspect
import logging
import os
import random
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.api import datastore_errors
from google.appengine.api import search
from google.appengine.ext import deferred
from google.appengine.ext import ndb

# From tools/third_party/
import webtest

from components import stats_framework
from components import utils
from server import stats
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_to_run
from support import test_case

from server.task_result import State

# pylint: disable=W0212,W0612


def _gen_request_data(name='Request name', properties=None, **kwargs):
  base_data = {
    'name': name,
    'user': 'Jesus',
    'properties': {
      'commands': [[u'command1']],
      'data': [],
      'dimensions': {},
      'env': {},
      'execution_timeout_secs': 24*60*60,
      'io_timeout_secs': None,
    },
    'priority': 50,
    'scheduling_expiration_secs': 60,
    'tags': [u'tag:1'],
  }
  base_data.update(kwargs)
  base_data['properties'].update(properties or {})
  return base_data


def get_results(request_key):
  """Fetches all task results for a specified TaskRequest ndb.Key.

  Returns:
    tuple(TaskResultSummary, list of TaskRunResult that exist).
  """
  result_summary_key = task_result.request_key_to_result_summary_key(
      request_key)
  result_summary = result_summary_key.get()
  # There's two way to look at it, either use a DB query or fetch all the
  # entities that could exist, at most 255. In general, there will be <3
  # entities so just fetching them by key would be faster. This function is
  # exclusively used in unit tests so it's not performance critical.
  q = task_result.TaskRunResult.query(ancestor=result_summary_key)
  q = q.order(task_result.TaskRunResult.key)
  return result_summary, q.fetch()


def _quick_reap():
  """Reaps a task."""
  data = _gen_request_data(
      properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
  request, _result_summary = task_scheduler.make_request(data)
  reaped_request, run_result = task_scheduler.bot_reap_task(
      {'OS': 'Windows-3.1.1'}, 'localhost', 'abc')
  return run_result


class TaskSchedulerApiTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(TaskSchedulerApiTest, self).setUp()
    self.testbed.init_search_stub()

    self.now = datetime.datetime(2014, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now)
    self.app = webtest.TestApp(
        deferred.application,
        extra_environ={
          'REMOTE_ADDR': '1.0.1.2',
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.mock(stats_framework, 'add_entry', self._parse_line)

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {}, {})
    self.assertIs(True, actual, line)

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
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    actual_request, run_result  = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)

  def test_exponential_backoff(self):
    self.mock(
        task_scheduler.random, 'random',
        lambda: task_scheduler._PROBABILITY_OF_QUICK_COMEBACK)
    self.mock(utils, 'is_canary', lambda: False)
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

  def test_task_idempotent(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    request_1, _result_summary = task_scheduler.make_request(
        _gen_request_data(
            properties=dict(
                dimensions={u'OS': u'Windows-3.1.1'}, idempotent=True)))
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    actual_request_1, run_result_1 = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(request_1, actual_request_1)
    self.assertEqual('localhost', run_result_1.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)
    # It's important to terminate the task with success.
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
            run_result_1.key, 'localhost', 0, 'Foo1', 0, 0, 0.1))

    # Create a second task, results are immediately returned.
    self.mock(random, 'getrandbits', lambda _: 0x77)
    original_ts = self.now
    new_ts = self.mock_now(original_ts, 10)
    request_2, _result_summary = task_scheduler.make_request(
        _gen_request_data(
            name='yay',
            user='Raoul',
            properties=dict(
                dimensions={u'OS': u'Windows-3.1.1'}, idempotent=True)))
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)
    actual_request_2, run_result_2 = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(None, actual_request_2)
    result_summary_duped, run_results_duped = get_results(request_2.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'created_ts': new_ts,
      'completed_ts': original_ts,
      'deduped_from': u'14350e868888801',
      'durations': [0.1],
      'exit_codes': [0],
      'failure': False,
      'id': '14350e88f987700',
      'internal_failure': False,
      # Only this value is updated to 'now', the rest uses the previous run
      # timestamps.
      'modified_ts': new_ts,
      'name': u'yay',
      'properties_hash': 'c5c77509f49b191689b056ad7c45c6d5fdcfea20',
      'server_versions': [u'default-version'],
      'started_ts': original_ts,
      'state': State.COMPLETED,
      'try_number': 0,
      'user': u'Raoul',
    }
    self.assertEqual(expected, result_summary_duped.to_dict())
    self.assertEqual([], run_results_duped)

  def test_get_results(self):
    # TODO(maruel): Split in more focused tests.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    created_ts = self.now
    self.mock_now(created_ts)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)

    # The TaskRequest was enqueued, the TaskResultSummary was created but no
    # TaskRunResult exist yet since the task was not scheduled on any bot.
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': None,
      'bot_version': None,
      'created_ts': created_ts,
      'completed_ts': None,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': created_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [],
      'started_ts': None,
      'state': State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual([], run_results)

    # A bot reaps the TaskToRun.
    reaped_ts = self.now + datetime.timedelta(seconds=60)
    self.mock_now(reaped_ts)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost', 'abc')
    self.assertEqual(request, reaped_request)
    self.assertTrue(run_result)
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'created_ts': created_ts,  # Time the TaskRequest was created.
      'completed_ts': None,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': reaped_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': reaped_ts,
      'state': State.RUNNING,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'bot_version': u'abc',
        'completed_ts': None,
        'durations': [],
        'exit_codes': [],
        'failure': False,
        'id': '14350e868888801',
        'internal_failure': False,
        'modified_ts': reaped_ts,
        'server_versions': [u'default-version'],
        'started_ts': reaped_ts,
        'state': State.RUNNING,
        'try_number': 1,
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in run_results])

    # The bot completes the task.
    done_ts = self.now + datetime.timedelta(seconds=120)
    self.mock_now(done_ts)
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'Foo1', 0, 0, 0.1))
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
        run_result.key, 'localhost', 1, 'Bar22', 0, 0, 0.2))
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'created_ts': created_ts,
      'completed_ts': done_ts,
      'deduped_from': None,
      'durations': [0.1, 0.2],
      'exit_codes': [0, 0],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': done_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': reaped_ts,
      'state': State.COMPLETED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'bot_version': u'abc',
        'completed_ts': done_ts,
        'durations': [0.1, 0.2],
        'exit_codes': [0, 0],
        'failure': False,
        'id': '14350e868888801',
        'internal_failure': False,
        'modified_ts': done_ts,
        'server_versions': [u'default-version'],
        'started_ts': reaped_ts,
        'state': State.COMPLETED,
        'try_number': 1,
      },
    ]
    self.assertEqual(expected, [t.to_dict() for t in run_results])

  def test_exit_code_failure(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost', 'abc')
    self.assertEqual(request, reaped_request)
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
        run_result.key, 'localhost', 0, 'Foo1', 0, 0, 0.1))
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
        run_result.key, 'localhost', 1, 'Bar22', 0, 1, 0.2))
    result_summary, run_results = get_results(request.key)

    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'created_ts': self.now,
      'completed_ts': self.now,
      'deduped_from': None,
      'durations': [0.1, 0.2],
      'exit_codes': [0, 1],
      'failure': True,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': self.now,
      'state': State.COMPLETED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())

    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'bot_version': u'abc',
        'completed_ts': self.now,
        'durations': [0.1, 0.2],
        'exit_codes': [0, 1],
        'failure': True,
        'id': '14350e868888801',
        'internal_failure': False,
        'modified_ts': self.now,
        'server_versions': [u'default-version'],
        'started_ts': self.now,
        'state': State.COMPLETED,
        'try_number': 1,
      },
    ]
    self.assertEqual(expected, [t.to_dict() for t in run_results])

  def test_make_request(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    # It is tested indirectly in the other functions.
    self.assertTrue(task_scheduler.make_request(data))

  def test_bot_update_task(self):
    run_result = _quick_reap()
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hi', 0, 0, 0.1))
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hey', 2, 0, 0.1))
    self.assertEqual(['hihey'], list(run_result.key.get().get_outputs()))

  def test_bot_update_task_new_two_commands(self):
    run_result = _quick_reap()
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hi', 0, 0, 0.1))
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 1, 'hey', 0, 0, 0.2))
    run_result = run_result.key.get()
    self.assertEqual(['hi', 'hey'], list(run_result.get_outputs()))
    self.assertEqual([0.1, 0.2], run_result.durations)

  def test_bot_update_task_new_overwrite(self):
    run_result = _quick_reap()
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hi', 0, None, None))
    self.assertEqual(
        (True, False),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hey', 1, None, None))
    self.assertEqual(['hhey'], list(run_result.key.get().get_outputs()))

  def test_bot_update_exception(self):
    run_result = _quick_reap()
    def r(*_):
      raise datastore_errors.TransactionFailedError('Sorry!')

    self.mock(ndb, 'put_multi', r)
    self.assertEqual(
        (False, False),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'hi', 0, 0, 0.1))

  def test_bot_kill_task(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, result_summary = task_scheduler.make_request(data)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost', 'abc')

    self.assertEqual(
        None, task_scheduler.bot_kill_task(run_result.key, 'localhost'))
    expected = {
      'abandoned_ts': self.now,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'created_ts': self.now,
      'completed_ts': None,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': True,
      'modified_ts': self.now,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': self.now,
      'state': State.BOT_DIED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.key.get().to_dict())
    expected = {
      'abandoned_ts': self.now,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888801',
      'internal_failure': True,
      'modified_ts': self.now,
      'server_versions': [u'default-version'],
      'started_ts': self.now,
      'state': State.BOT_DIED,
      'try_number': 1,
    }
    self.assertEqual(expected, run_result.key.get().to_dict())

  def test_bot_kill_task_wrong_bot(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, result_summary = task_scheduler.make_request(data)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost', 'abc')
    expected = (
      'Bot bot1 sent task kill for task 14350e868888801 owned by bot localhost')
    self.assertEqual(
        expected, task_scheduler.bot_kill_task(run_result.key, 'bot1'))

  def test_cancel_task(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _request, result_summary = task_scheduler.make_request(data)
    ok, was_running = task_scheduler.cancel_task(result_summary.key)
    self.assertEqual(True, ok)
    self.assertEqual(False, was_running)
    result_summary = result_summary.key.get()
    self.assertEqual(task_result.State.CANCELED, result_summary.state)

  def test_cron_abort_expired_task_to_run(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _request, result_summary = task_scheduler.make_request(data)
    abandoned_ts = self.mock_now(self.now, data['scheduling_expiration_secs']+1)
    self.assertEqual(1, task_scheduler.cron_abort_expired_task_to_run())
    self.assertEqual([], task_result.TaskRunResult.query().fetch())
    expected = {
      'abandoned_ts': abandoned_ts,
      'bot_id': None,
      'bot_version': None,
      'completed_ts': None,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': abandoned_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [],
      'started_ts': None,
      'state': task_result.State.EXPIRED,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.key.get().to_dict())

  def test_cron_abort_expired_task_to_run_retry(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, result_summary = task_scheduler.make_request(data)

    # Fake first try bot died.
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual((0, 1, 0), task_scheduler.cron_handle_bot_died())
    self.assertEqual(task_result.State.BOT_DIED, run_result.key.get().state)
    self.assertEqual(
        task_result.State.PENDING, run_result.result_summary_key.get().state)

    # BOT_DIED is kept instead of EXPIRED.
    abandoned_ts = self.mock_now(self.now, data['scheduling_expiration_secs']+1)
    self.assertEqual(1, task_scheduler.cron_abort_expired_task_to_run())
    self.assertEqual(1, len(task_result.TaskRunResult.query().fetch()))
    expected = {
      'abandoned_ts': abandoned_ts,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': None,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': True,
      'modified_ts': abandoned_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': self.now,
      'state': task_result.State.BOT_DIED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.key.get().to_dict())

  def test_cron_handle_bot_died(self):
    # Test first retry, then success.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}),
        scheduling_expiration_secs=600)
    task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual((0, 1, 0), task_scheduler.cron_handle_bot_died())

    # Refresh and compare:
    expected = {
      'abandoned_ts': now_1,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888801',
      'internal_failure': True,
      'modified_ts': now_1,
      'server_versions': [u'default-version'],
      'started_ts': self.now,
      'state': task_result.State.BOT_DIED,
      'try_number': 1,
    }
    self.assertEqual(expected, run_result.key.get().to_dict())
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': None,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': now_1,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': None,
      'state': task_result.State.PENDING,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())

    # Task was retried.
    now_2 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    logging.info('%s', [t.to_dict() for t in task_to_run.TaskToRun.query()])
    self.assertEqual(2, run_result.try_number)
    self.assertEqual(
        (True, True),
        task_scheduler.bot_update_task(
            run_result.key, 'localhost', 0, 'Foo1', 0, 0, 0.1))
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': now_2,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [0.1],
      'exit_codes': [0],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': now_2,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': now_2,
      'state': task_result.State.COMPLETED,
      'try_number': 2,
      'user': u'Jesus',
    }
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())

  def test_cron_handle_bot_died_second(self):
    # Test two retries leading to a BOT_DIED status.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}),
        scheduling_expiration_secs=600)
    task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual((0, 1, 0), task_scheduler.cron_handle_bot_died())
    now_1 = self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    now_2 = self.mock_now(self.now + 2 * task_result.BOT_PING_TOLERANCE, 3)
    self.assertEqual((1, 0, 0), task_scheduler.cron_handle_bot_died())
    self.assertEqual((0, 0, 0), task_scheduler.cron_handle_bot_died())
    expected = {
      'abandoned_ts': now_2,
      'bot_id': u'localhost',
      'bot_version': u'abc',
      'completed_ts': None,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': True,
      'modified_ts': now_2,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': now_1,
      'state': task_result.State.BOT_DIED,
      'try_number': 2,
      'user': u'Jesus',
    }
    self.assertEqual(expected, run_result.result_summary_key.get().to_dict())

  def test_cron_handle_bot_died_ignored(self):
    # Test two retries.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}),
        scheduling_expiration_secs=600)
    task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.assertEqual(1, run_result.try_number)
    self.assertEqual(task_result.State.RUNNING, run_result.state)
    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual((0, 1, 0), task_scheduler.cron_handle_bot_died())
    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 2)
    _request, run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')
    self.mock_now(self.now + 2 * task_result.BOT_PING_TOLERANCE, 3)
    self.assertEqual((1, 0, 0), task_scheduler.cron_handle_bot_died())
    self.assertEqual((0, 0, 0), task_scheduler.cron_handle_bot_died())

  def test_search_by_name(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _, result_summary = task_scheduler.make_request(data)

    # Assert that search is not case-sensitive by using unexpected casing.
    actual, _cursor = task_result.search_by_name('requEST', None, 10)
    self.assertEqual([result_summary], actual)
    actual, _cursor = task_result.search_by_name('name', None, 10)
    self.assertEqual([result_summary], actual)

  def test_search_by_name_failures(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _, result_summary = task_scheduler.make_request(data)

    actual, _cursor = task_result.search_by_name('foo', None, 10)
    self.assertEqual([], actual)
    # Partial match doesn't work.
    actual, _cursor = task_result.search_by_name('nam', None, 10)
    self.assertEqual([], actual)

  def test_search_by_name_broken_tasks(self):
    # Create tasks where task_scheduler_make_request() fails in the middle. This
    # is done by mocking the functions to fail every SKIP call and running it in
    # a loop.
    class RandomFailure(Exception):
      pass

    # First call fails ndb.put_multi(), second call fails search.Index.put(),
    # third call work.
    index = [0]
    SKIP = 3
    def put_multi(*args, **kwargs):
      callers = [i[3] for i in inspect.stack()]
      self.assertIn('make_request', callers)
      if (index[0] % SKIP) == 1:
        raise RandomFailure()
      return old_put_multi(*args, **kwargs)

    def put(*args, **kwargs):
      callers = [i[3] for i in inspect.stack()]
      self.assertIn('make_request', callers)
      if (index[0] % SKIP) == 2:
        raise RandomFailure()
      return old_put(*args, **kwargs)

    old_put_multi = self.mock(ndb, 'put_multi', put_multi)
    old_put = self.mock(search.Index, 'put', put)

    saved = []

    for i in xrange(100):
      index[0] = i
      data = _gen_request_data(
          name='Request %d' % i,
          properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
      try:
        _, result_summary = task_scheduler.make_request(data)
        saved.append(result_summary)
      except RandomFailure:
        pass

    self.assertEqual(34, len(saved))
    self.assertEqual(67, task_request.TaskRequest.query().count())
    self.assertEqual(34, task_result.TaskResultSummary.query().count())

    # Now the DB is full of half-corrupted entities.
    cursor = None
    actual, cursor = task_result.search_by_name('Request', cursor, 31)
    self.assertEqual(31, len(actual))
    actual, cursor = task_result.search_by_name('Request', cursor, 31)
    self.assertEqual(3, len(actual))
    actual, cursor = task_result.search_by_name('Request', cursor, 31)
    self.assertEqual(0, len(actual))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
