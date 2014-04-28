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

# From tools/third_party/
import webtest

from google.appengine.ext import deferred
from google.appengine.ext import ndb

from components import utils
from server import result_helper
from server import task_request
from server import task_shard_to_run
from server import task_result
from server import test_helper
from support import test_case

# pylint: disable=W0212


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


def _safe_cmp(a, b):
  # cmp(datetime.datetime.utcnow(), None) throws TypeError. Workaround.
  return cmp(utils.encode_to_json(a), utils.encode_to_json(b))


def get_entities(entity_model):
  return sorted(
      (i.to_dict() for i in entity_model.query().fetch()), cmp=_safe_cmp)


class TaskResultApiTest(test_case.TestCase):
  def setUp(self):
    super(TaskResultApiTest, self).setUp()
    self.now = datetime.datetime(2014, 1, 2, 3, 4, 5, 6)
    test_helper.mock_now(self, self.now)
    self.mock(task_request.random, 'getrandbits', lambda _: 0x88)
    self.app = webtest.TestApp(
        deferred.application,
        extra_environ={
          'REMOTE_ADDR': '1.0.1.2',
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.enable_task_queue(ROOT_DIR)

  def tearDown(self):
    try:
      if not self.has_failed():
        self.assertEqual(0, self.execute_tasks())
    finally:
      super(TaskResultApiTest, self).tearDown()

  def assertEntities(self, expected, entity_model):
    self.assertEqual(expected, get_entities(entity_model))

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
      task_result.State.to_string(task_state=0)

    self.assertEqual(
        set(task_result.State._NAMES), set(task_result.State.STATES))
    items = (
      task_result.State.STATES_RUNNING + task_result.State.STATES_DONE +
      task_result.State.STATES_ABANDONED)
    self.assertEqual(set(items), set(task_result.State.STATES))
    self.assertEqual(len(items), len(set(items)))
    self.assertEqual(
        task_result.State.STATES_RUNNING + task_result.State.STATES_NOT_RUNNING,
        task_result.State.STATES)

  def test_state_to_string(self):
    # Same code as State.to_string() except that it works for
    # TaskResultSummary too.
    class Foo(ndb.Model):
      task_state = task_result.StateProperty()
      task_failure = ndb.BooleanProperty(default=False)
      internal_failure = ndb.BooleanProperty(default=False)

    for i in task_result.State.STATES:
      self.assertTrue(task_result.State.to_string(i))
    for i in task_result.State.STATES:
      self.assertTrue(task_result.state_to_string(Foo(task_state=i)))

  def test_request_key_to_result_summary_key(self):
    request_key = task_request.id_to_request_key(256)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    expected = (
        "Key('TaskRequestShard', 'f7184', 'TaskRequest', 256, "
        "'TaskResultSummary', 1)")
    self.assertEqual(expected, str(result_summary_key))

  def test_shard_to_run_key_to_shard_result_key(self):
    shard_to_run_key = task_shard_to_run.shard_id_to_key(257)
    shard_result_key = task_result.shard_to_run_key_to_shard_result_key(
        shard_to_run_key, 1)
    expected = (
        "Key('TaskShardToRunShard', 'd9640', 'TaskShardToRun', 257, "
        "'TaskShardResult', 1)")
    self.assertEqual(expected, str(shard_result_key))

  def test_shard_result_key_to_request_key(self):
    request_key = task_request.id_to_request_key(0x100)
    shard_to_run_key = task_shard_to_run.shard_id_to_key(0x101)
    shard_result_key = task_result.shard_to_run_key_to_shard_result_key(
        shard_to_run_key, 1)
    actual = task_result.shard_result_key_to_request_key(shard_result_key)
    self.assertEqual(request_key, actual)

  def test_new_result_summary(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    actual = task_result.new_result_summary(request)
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
          'task_state': task_result.State.PENDING,
          'try_number': None,
        },
      ],
      'task_failure': False,
      'task_state': task_result.State.PENDING,
    }
    self.assertEqual(expected, actual.to_dict())

  def test_new_shard_result(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    self.assertEqual(1, len(shards))
    shard_to_run = shards[0]
    actual = task_result.new_shard_result(shard_to_run.key, 1, 'localhost')
    expected = {
      'abandoned_ts': None,
      'bot_id': 'localhost',
      'completed_ts': None,
      'exit_codes': [],
      'internal_failure': False,
      'modified_ts': None,
      'outputs': [],
      'started_ts': None,
      'task_failure': False,
      'task_state': task_result.State.RUNNING,
    }
    self.assertEqual(expected, actual.to_dict())

  def test_task_update_result_summary_end_to_end(self):
    # Creates 2 shards, ensure they are all synchronized properly.
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=2)))
    result_summary = task_result.new_result_summary(request)
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    ndb.put_multi([result_summary] + shards)
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
          'task_state': task_result.State.PENDING,
          'try_number': None,
        },
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': False,
          'modified_ts': None,
          'started_ts': None,
          'task_failure': False,
          'task_state': task_result.State.PENDING,
          'try_number': None,
        },
      ],
      'task_failure': False,
      'task_state': task_result.State.PENDING,
    }
    self.assertEqual(expected, result_summary.to_dict())

    # Nothing changed 2 secs later except latency.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2))
    self.assertEqual(0, self.execute_tasks())
    self.assertEqual(expected, result_summary.to_dict())

    # Shard #0 is reaped after 2 seconds (4 secs total).
    # TODO(maruel): This code has potential to be refactored to reduce the code
    # duplication. Please do at first occasion.
    reap_ts = self.now + datetime.timedelta(seconds=4)
    test_helper.mock_now(self, reap_ts)
    self.assertEqual(True, task_shard_to_run.reap_shard_to_run(shards[0].key))
    shard_result_0 = task_result.new_shard_result(shards[0].key, 1, 'localhost')
    task_result.put_shard_result(shard_result_0)
    task_result._task_update_result_summary(request.key.integer_id())
    expected['modified_ts'] = reap_ts
    expected['shards'][0]['modified_ts'] = reap_ts
    expected['shards'][0]['started_ts'] = reap_ts
    expected['shards'][0]['task_state'] = task_result.State.RUNNING
    expected['shards'][0]['try_number'] = 1
    expected['task_state'] = task_result.State.RUNNING
    self.assertEqual(expected, result_summary.to_dict())

    # Shard #1 is reaped after 2 seconds (6 secs total).
    reap_ts = self.now + datetime.timedelta(seconds=6)
    test_helper.mock_now(self, reap_ts)
    self.assertEqual(True, task_shard_to_run.reap_shard_to_run(shards[1].key))
    shard_result_1 = task_result.new_shard_result(shards[1].key, 1, '127.0.0.1')
    task_result.put_shard_result(shard_result_1)
    task_result._task_update_result_summary(request.key.integer_id())
    expected['modified_ts'] = reap_ts
    expected['shards'][1]['modified_ts'] = reap_ts
    expected['shards'][1]['started_ts'] = reap_ts
    expected['shards'][1]['task_state'] = task_result.State.RUNNING
    expected['shards'][1]['try_number'] = 1
    self.assertEqual(expected, result_summary.to_dict())

    # Shard #0 is completed after 2 seconds (8 secs total).
    complete_ts = self.now + datetime.timedelta(seconds=8)
    test_helper.mock_now(self, complete_ts)
    shard_result_0.completed_ts = complete_ts
    shard_result_0.exit_codes.append(0)
    shard_result_0.task_state = task_result.State.COMPLETED
    results_key = result_helper.StoreResults('foo').key
    shard_result_0.outputs.append(results_key)
    task_result.put_shard_result(shard_result_0)
    task_result._task_update_result_summary(request.key.integer_id())
    expected['modified_ts'] = complete_ts
    expected['shards'][0]['completed_ts'] = complete_ts
    expected['shards'][0]['modified_ts'] = complete_ts
    expected['shards'][0]['task_state'] = task_result.State.COMPLETED
    self.assertEqual(expected, result_summary.to_dict())

    # Shard #1 is completed after 2 seconds (10 secs total).
    complete_ts = self.now + datetime.timedelta(seconds=10)
    test_helper.mock_now(self, complete_ts)
    shard_result_1.completed_ts = complete_ts
    shard_result_1.exit_codes.append(0)
    shard_result_1.task_state = task_result.State.COMPLETED
    results_key = result_helper.StoreResults('bar').key
    shard_result_1.outputs.append(results_key)
    task_result.put_shard_result(shard_result_1)
    task_result._task_update_result_summary(request.key.integer_id())
    expected['done_ts'] = complete_ts
    expected['modified_ts'] = complete_ts
    expected['shards'][1]['completed_ts'] = complete_ts
    expected['shards'][1]['modified_ts'] = complete_ts
    expected['shards'][1]['task_state'] = task_result.State.COMPLETED
    expected['task_state'] = task_result.State.COMPLETED
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual(datetime.timedelta(seconds=8), result_summary.duration())

    # They accumulated from put_shard_result() calls.
    self.assertEqual(4, self.execute_tasks())

  def test_task_update_result_summary_completed(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=2)))
    result_summary = task_result.new_result_summary(request)
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    shard_results = [
      task_result.new_shard_result(shards[0].key, 1, 'localhost1'),
      task_result.new_shard_result(shards[1].key, 1, 'localhost2'),
    ]
    ndb.put_multi([result_summary] + shards)
    for shard_result in shard_results:
      task_result.put_shard_result(shard_result)

    # Time must advance for the lookup to happen again.
    now_1 = self.now + datetime.timedelta(seconds=1)
    test_helper.mock_now(self, now_1)
    shard_results[0].abandoned_ts = self.now
    shard_results[0].task_state = task_result.State.BOT_DIED
    shard_results[0].internal_failure = True
    shard_results[1].completed_ts = self.now
    shard_results[1].task_state = task_result.State.COMPLETED
    shard_results[1].task_failure = True
    for shard_result in shard_results:
      task_result.put_shard_result(shard_result)
    task_result._task_update_result_summary(request.key.integer_id())

    expected = [
      {
        'done_ts': now_1,
        'internal_failure': True,
        'modified_ts': now_1,
        'shards': [
          {
            'abandoned_ts': self.now,
            'completed_ts': None,
            'internal_failure': True,
            'modified_ts': now_1,
            'started_ts': self.now,
            'task_failure': False,
            'task_state': task_result.State.BOT_DIED,
            'try_number': 1,
          },
          {
            'abandoned_ts': None,
            'completed_ts': self.now,
            'internal_failure': False,
            'modified_ts': now_1,
            'started_ts': self.now,
            'task_failure': True,
            'task_state': task_result.State.COMPLETED,
            'try_number': 1,
          },
        ],
        'task_failure': True,
        'task_state': task_result.State.BOT_DIED,
      },
    ]
    self.assertEntities(expected, task_result.TaskResultSummary)
    expected = [
      {
        'abandoned_ts': self.now,
        'bot_id': u'localhost1',
        'completed_ts': None,
        'exit_codes': [],
        'internal_failure': True,
        'modified_ts': now_1,
        'outputs': [],
        'started_ts': self.now,
        'task_failure': False,
        'task_state': task_result.State.BOT_DIED,
      },
      {
        'abandoned_ts': None,
        'bot_id': u'localhost2',
        'completed_ts': self.now,
        'exit_codes': [],
        'internal_failure': False,
        'modified_ts': now_1,
        'outputs': [],
        'started_ts': self.now,
        'task_failure': True,
        'task_state': task_result.State.COMPLETED,
      },
    ]
    self.assertEntities(expected, task_result.TaskShardResult)
    self.assertEqual(
        'Bot died while running the task. Either the task killed the bot or '
        'the bot suicided (Internal failure)',
        shard_results[0].to_string())

    # They accumulated from put_shard_result() calls.
    self.assertEqual(4, self.execute_tasks())

  def test_task_update_result_summary(self):
    # Tests task_result.sync_all_result_summary(). It is basically a
    # wrapper around _task_update_result_summary() which is tested above.
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    result_summary = task_result.new_result_summary(request)
    result_summary.put()

    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    self.assertEqual(1, len(shards))
    shard_to_run = shards[0]
    shard_to_run.put()
    shard_result = task_result.new_shard_result(
        shard_to_run.key, 1, 'localhost')
    task_result.put_shard_result(shard_result)
    task_result._task_update_result_summary(request.key.integer_id())
    self.assertEqual(0, task_result.sync_all_result_summary())
    expected = [
      {
        'abandoned_ts': None,
        'bot_id': u'localhost',
        'completed_ts': None,
        'exit_codes': [],
        'internal_failure': False,
        'modified_ts': self.now,
        'outputs': [],
        'started_ts': self.now,
        'task_failure': False,
        'task_state': task_result.State.RUNNING,
      },
    ]
    self.assertEntities(expected, task_result.TaskShardResult)
    expected = [
      {
        'done_ts': None,
        'internal_failure': False,
        'modified_ts': self.now,
        'shards': [
          {
            'abandoned_ts': None,
            'completed_ts': None,
            'internal_failure': False,
            'modified_ts': self.now,
            'started_ts': self.now,
            'task_failure': False,
            'task_state': task_result.State.RUNNING,
            'try_number': 1,
          },
        ],
        'task_failure': False,
        'task_state': task_result.State.RUNNING,
      },
    ]
    self.assertEntities(expected, task_result.TaskResultSummary)
    self.assertEqual(1, self.execute_tasks())

  def test_terminate_shard_result(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    shard_to_run = shards[0]
    shard_result = task_result.new_shard_result(
        shard_to_run.key, 1, 'localhost')
    task_result.terminate_shard_result(shard_result, task_result.State.BOT_DIED)
    self.assertEqual(1, self.execute_tasks())
    expected = {
      'abandoned_ts': self.now,
      'bot_id': u'localhost',
      'completed_ts': None,
      'exit_codes': [],
      'internal_failure': False,
      'modified_ts': self.now,
      'outputs': [],
      'started_ts': self.now,
      'task_failure': False,
      'task_state': task_result.State.BOT_DIED,
    }
    self.assertEqual(expected, shard_result.key.get().to_dict())

  def test_yield_shard_results_without_update(self):
    # One is completed, one died.
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=2)))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    shard_result_0 = task_result.new_shard_result(shards[0].key, 1, 'localhost')
    shard_result_1 = task_result.new_shard_result(shards[1].key, 1, 'localhost')
    shard_result_0.task_state = task_result.State.COMPLETED
    shard_result_0.completed_ts = self.now
    task_result.put_shard_result(shard_result_0)
    task_result.put_shard_result(shard_result_1)
    self.assertEqual(2, self.execute_tasks())

    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=5*60+1))
    self.assertEqual(
        [shard_result_1],
        list(task_result.yield_shard_results_without_update()))

  def run_sync_all_result_summary(self, request):
    self.assertEqual(0, task_result.sync_all_result_summary())
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=1))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    self.assertEqual(1, len(shards))
    shard_to_run = shards[0]
    shard_to_run.put()
    shard_result = task_result.new_shard_result(
        shard_to_run.key, 1, 'localhost')
    task_result.put_shard_result(shard_result)

    # The cron job is run before the taskqueue but has no effect.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2*60-1))
    self.assertEqual(0, task_result.sync_all_result_summary())

    # Get over the 2 minutes tolerance for the cron job. This means the task
    # queue has trouble running.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2*60+1))
    self.assertEqual(1, task_result.sync_all_result_summary())
    expected = get_entities(task_result.TaskResultSummary)

    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2))
    # The taskqueue has no effect.
    self.assertEqual(1, self.execute_tasks())
    actual = get_entities(task_result.TaskResultSummary)
    self.assertEqual(expected, actual)

  def test_sync_all_result_summary(self):
    # It is basically a wrapper around _task_update_result_summary() which is
    # tested above.
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    result_summary = task_result.new_result_summary(request)
    result_summary.put()
    self.run_sync_all_result_summary(request)

  def test_task_update_result_summary_skip_task_missing_summary(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    self.run_sync_all_result_summary(request)

  def test_task_update_result_summary_fine(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    result_summary = task_result.new_result_summary(request)
    result_summary.put()

    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=1))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    self.assertEqual(1, len(shards))
    shard_to_run = shards[0]
    shard_to_run.put()
    shard_result = task_result.new_shard_result(
        shard_to_run.key, 1, 'localhost')
    task_result.put_shard_result(shard_result)
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2))
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, task_result.sync_all_result_summary())

  def test_enqueue_update_result_summary(self):
    request = task_request.new_request(
        _gen_request_data(properties=dict(number_shards=1)))
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    self.assertEqual(1, len(shards))
    shard_to_run = shards[0]
    shard_to_run.put()
    shard_result = task_result.new_shard_result(
        shard_to_run.key, 1, 'localhost')
    shard_result.task_state = task_result.State.COMPLETED
    shard_result.internal_failure = True
    task_result.put_shard_result(shard_result)
    result_summary = task_result.request_key_to_result_summary_key(
        request.key).get()
    self.assertEqual(None, result_summary)

    self.assertEqual(1, self.execute_tasks())
    result_summary = task_result.request_key_to_result_summary_key(
        request.key).get()
    expected = {
      'done_ts': self.now,
      'internal_failure': True,
      'modified_ts': self.now,
      'shards': [
        {
          'abandoned_ts': None,
          'completed_ts': None,
          'internal_failure': True,
          'modified_ts': self.now,
          'started_ts': self.now,
          'task_failure': False,
          'task_state': task_result.State.COMPLETED,
          'try_number': 1,
        },
      ],
      'task_failure': False,
      'task_state': task_result.State.COMPLETED,
    }
    self.assertEqual(expected, result_summary.to_dict())

  def test_put_shard_result(self):
    # Tested indirectly.
    pass


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
