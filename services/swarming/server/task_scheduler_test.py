#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import deferred
from google.appengine.ext import ndb

# From tools/third_party/
import webtest

from components import stats_framework
from server import result_helper
from server import stats_new as stats
from server import task_scheduler
from server import test_helper
from support import test_case

from server.task_result import State

# pylint: disable=W0212,W0612


def _gen_request_data(properties=None, **kwargs):
  base_data = {
    'name': 'Request name',
    'user': 'Jesus',
    'properties': {
      'commands': [[u'command1']],
      'data': [],
      'dimensions': {},
      'env': {},
      'number_shards': 1,
      'execution_timeout_secs': 24*60*60,
      'io_timeout_secs': None,
    },
    'priority': 50,
    'scheduling_expiration_secs': 60,
  }
  base_data.update(kwargs)
  base_data['properties'].update(properties or {})
  return base_data


def get_shard_results(request_key):
  """Fetches all TaskShardResult's for a specified TaskRequest ndb.Key.

  Returns:
    tuple(TaskResultSummary, list of TaskShardResult that exist).
  """
  result_summary = task_scheduler.task_result.request_key_to_result_summary_key(
      request_key).get()
  if not result_summary:
    return result_summary, []
  keys = (
    result_summary.shard_result_key(i+1)
    for i in xrange(result_summary.number_shards)
  )
  keys = [k for k in keys if k]
  return result_summary, ndb.get_multi(keys) if keys else []


class TaskSchedulerApiTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(TaskSchedulerApiTest, self).setUp()
    self.now = datetime.datetime(2014, 1, 2, 3, 4, 5, 6)
    test_helper.mock_now(self, self.now)
    self.app = webtest.TestApp(
        deferred.application,
        extra_environ={
          'REMOTE_ADDR': '1.0.1.2',
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.mock(stats_framework, 'add_entry', self._parse_line)

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {})
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
        properties=dict(number_shards=2, dimensions={u'OS': u'Windows-3.1.1'}))
    request_1, shard_runs = task_scheduler.new_request(data)
    self.assertEqual(2, len(shard_runs))
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    request_2, shard_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost')
    self.assertEqual(request_1, request_2)
    self.assertEqual('localhost', shard_result.bot_id)
    self.assertEqual(None, shard_result.key.parent().get().queue_number)
    self.assertEqual(1, self.execute_tasks())

  def test_exponential_backoff(self):
    self.mock(
        task_scheduler.random, 'random',
        lambda: task_scheduler._PROBABILITY_OF_QUICK_COMEBACK)
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

  def test_get_shard_results(self):
    # TODO(maruel): Split in more focused tests.
    created_ts = self.now
    test_helper.mock_now(self, created_ts)
    data = _gen_request_data(
        properties=dict(number_shards=2, dimensions={u'OS': u'Windows-3.1.1'}))
    request, _ = task_scheduler.new_request(data)

    reaped_ts = self.now + datetime.timedelta(seconds=60)
    test_helper.mock_now(self, reaped_ts)
    self.assertEquals(0, self.execute_tasks())

    _, shard_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost')
    self.assertTrue(shard_result)
    result_summary, shard_results = get_shard_results(request.key)
    # Results are stale until the task queue is executed.
    expected = {
      'done_ts': None,
      'internal_failure': False,
      'modified_ts': None,
      'shards': [
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': None,
          'started_ts': None,
          'task_failure': False,
          'task_state': State.PENDING,
          'try_number': None,
        },
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': None,
          'started_ts': None,
          'task_failure': False,
          'task_state': State.PENDING,
          'try_number': None,
        },
      ],
      'task_failure': False,
      'task_state': State.PENDING,
    }
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual([], shard_results)

    # task_result._task_update_result_summary() was enqueued by
    # task_result.put_shard_result() via bot_reap_task().
    self.assertEquals(1, self.execute_tasks())
    result_summary, shard_results = get_shard_results(request.key)
    expected = {
      'done_ts': None,
      'internal_failure': False,
      'modified_ts': reaped_ts,
      'shards': [
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': reaped_ts,
          'started_ts': reaped_ts,
          'task_failure': False,
          'task_state': State.RUNNING,
          'try_number': 1,
        },
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': None,
          'started_ts': None,
          'task_failure': False,
          'task_state': State.PENDING,
          'try_number': None,
        },
      ],
      'task_failure': False,
      'task_state': State.RUNNING,
    }
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'completed_ts': None,
        'exit_codes': [],
        'internal_failure': False,
        'modified_ts': reaped_ts,
        'outputs': [],
        'started_ts': reaped_ts,
        'task_failure': False,
        'task_state': State.RUNNING,
      },
    ]
    self.assertEqual(expected, [t.to_dict() for t in shard_results])

    done_ts = self.now + datetime.timedelta(seconds=120)
    test_helper.mock_now(self, done_ts)
    output_keys = [
      result_helper.StoreResults('foo').key,
      result_helper.StoreResults('bar').key,
    ]
    data = {
      'exit_codes': [0, 0],
      'outputs': output_keys,
    }
    task_scheduler.bot_update_task(shard_results[0].key, data, 'localhost')
    self.assertEquals(1, self.execute_tasks())
    result_summary, shard_results = get_shard_results(request.key)
    expected = {
      'done_ts': None,
      'internal_failure': False,
      'modified_ts': done_ts,
      'shards': [
        {
          'abandoned_ts': None,
          'completed_ts': done_ts,
          'internal_failure': False,
          'modified_ts': done_ts,
          'started_ts': reaped_ts,
          'task_failure': False,
          'task_state': State.COMPLETED,
          'try_number': 1,
        },
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': None,
          'started_ts': None,
          'task_failure': False,
          'task_state': State.PENDING,
          'try_number': None,
        },
      ],
      'task_failure': False,
      'task_state': State.PENDING,
    }
    self.assertEqual(expected, result_summary.to_dict())
    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'completed_ts': done_ts,
        'exit_codes': [0, 0],
        'internal_failure': False,
        'modified_ts': done_ts,
        'outputs': output_keys,
        'started_ts': reaped_ts,
        'task_failure': False,
        'task_state': State.COMPLETED,
      },
    ]
    self.assertEqual(expected, [t.to_dict() for t in shard_results])

  def test_new_request(self):
    data = _gen_request_data(
        properties=dict(number_shards=2, dimensions={u'OS': u'Windows-3.1.1'}))
    # It is tested indirectly in the other functions.
    self.assertTrue(task_scheduler.new_request(data))

  def test_pack_shard_result_key(self):
    def getrandbits(i):
      self.assertEqual(i, 8)
      return 0x02
    self.mock(task_scheduler.random, 'getrandbits', getrandbits)
    test_helper.mock_now(
      self,
      task_scheduler.task_common.UNIX_EPOCH + datetime.timedelta(seconds=3))

    _, shards = task_scheduler.new_request(_gen_request_data())
    bot_dimensions = {'hostname': 'localhost'}
    _, shard_result = task_scheduler.bot_reap_task(bot_dimensions, None)
    actual = task_scheduler.pack_shard_result_key(shard_result.key)
    # 0xbb8 = 3000ms = 3 secs; 0x02 = random;  0x01 = shard;  -1 = try_number.
    self.assertEqual('bb80201-1', actual)
    self.assertEqual(1, self.execute_tasks())

  def test_unpack_shard_result_key(self):
    actual = task_scheduler.unpack_shard_result_key('bb80201-1')
    expected = (
        "Key('TaskShardToRunShard', '5d82f', 'TaskShardToRun', 196608513, "
        "'TaskShardResult', 1)")
    self.assertEqual(expected, str(actual))

  def test_bot_update_task(self):
    # See test_get_shard_results.
    pass

  def test_cron_abort_expired_shard_to_run(self):
    # Create two shards, one is properly reaped, the other is expired.
    data = _gen_request_data(
        properties=dict(number_shards=2, dimensions={u'OS': u'Windows-3.1.1'}))
    task_scheduler.new_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _, shard_result = task_scheduler.bot_reap_task(bot_dimensions, None)
    expiration = data['scheduling_expiration_secs']
    after_delay = self.now + datetime.timedelta(seconds=expiration+1)
    test_helper.mock_now(self, after_delay)
    self.assertEqual(1, task_scheduler.cron_abort_expired_shard_to_run())
    self.assertEqual(2, self.execute_tasks())

  def test_cron_abort_bot_died(self):
    data = _gen_request_data(
        properties=dict(number_shards=2, dimensions={u'OS': u'Windows-3.1.1'}))
    task_scheduler.new_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _, shard_result = task_scheduler.bot_reap_task(bot_dimensions, None)
    after_delay = (
        self.now + task_scheduler.task_common.BOT_PING_TOLERANCE +
        datetime.timedelta(seconds=1))
    test_helper.mock_now(self, after_delay)
    self.assertEqual(1, task_scheduler.cron_abort_bot_died())
    self.assertEqual(2, self.execute_tasks())

  def test_cron_sync_all_result_summary(self):
    ran = []
    self.mock(
        task_scheduler.task_result, 'sync_all_result_summary',
        lambda *args: ran.append(args))
    task_scheduler.cron_sync_all_result_summary()
    self.assertEqual([()], ran)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
