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
from server import task_common
from server import task_request
from server import task_result
from server import task_to_run
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
  APP_DIR = ROOT_DIR

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
      task_result.State.to_string(0)

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
      state = task_result.StateProperty()
      failure = ndb.BooleanProperty(default=False)
      internal_failure = ndb.BooleanProperty(default=False)

    for i in task_result.State.STATES:
      self.assertTrue(task_result.State.to_string(i))
    for i in task_result.State.STATES:
      self.assertTrue(task_result.state_to_string(Foo(state=i)))

  def test_request_key_to_result_summary_key(self):
    request_key = task_request.id_to_request_key(256)
    result_key = task_result.request_key_to_result_summary_key(
        request_key)
    expected = (
        "Key('TaskRequestShard', 'f71849', 'TaskRequest', 256, "
        "'TaskResultSummary', 1)")
    self.assertEqual(expected, str(result_key))

  def test_result_summary_key_to_request_key(self):
    request_key = task_request.id_to_request_key(0x100)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    actual = task_result.result_summary_key_to_request_key(result_summary_key)
    self.assertEqual(request_key, actual)

  def test_result_summary_key_to_run_result_key(self):
    request_key = task_request.id_to_request_key(0x100)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 1)
    expected = (
        "Key('TaskRequestShard', 'f71849', 'TaskRequest', 256, "
        "'TaskResultSummary', 1, 'TaskRunResult', 1)")
    self.assertEqual(expected, str(run_result_key))

    with self.assertRaises(ValueError):
      task_result.result_summary_key_to_run_result_key(result_summary_key, 0)
    with self.assertRaises(NotImplementedError):
      task_result.result_summary_key_to_run_result_key(result_summary_key, 2)

  def test_run_result_key_to_result_summary_key(self):
    request_key = task_request.id_to_request_key(0x100)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 1)
    self.assertEqual(
        result_summary_key,
        task_result.run_result_key_to_result_summary_key(run_result_key))

  def test_new_result_summary(self):
    request = task_request.make_request(_gen_request_data())
    actual = task_result.new_result_summary(request)
    expected = {
      'abandoned_ts': None,
      'bot_id': None,
      'completed_ts': None,
      'created_ts': self.now,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': None,
      'name': u'Request name',
      'outputs': [],
      'started_ts': None,
      'state': task_result.State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, actual.to_dict())

  def test_new_run_result(self):
    request = task_request.make_request(_gen_request_data())
    actual = task_result.new_run_result(request, 1, 'localhost')
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'completed_ts': None,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': None,
      'outputs': [],
      'started_ts': self.now,
      'state': task_result.State.RUNNING,
      'try_number': 1,
    }
    self.assertEqual(expected, actual.to_dict())

  def test_integration(self):
    # Creates a TaskRequest, along its TaskResultSummary and TaskToRun. Have a
    # bot reap the task, and complete the task. Ensure the resulting
    # TaskResultSummary and TaskRunResult are properly updated.
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    task = task_to_run.new_task_to_run(request)
    ndb.put_multi([result_summary, task])
    expected = {
      'abandoned_ts': None,
      'bot_id': None,
      'exit_codes': [],
      'completed_ts': None,
      'created_ts': self.now,
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'Request name',
      'outputs': [],
      'started_ts': None,
      'failure': False,
      'state': task_result.State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())

    # Nothing changed 2 secs later except latency.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=2))
    self.assertEqual(expected, result_summary.to_dict())

    # Task is reaped after 2 seconds (4 secs total).
    reap_ts = self.now + datetime.timedelta(seconds=4)
    test_helper.mock_now(self, reap_ts)
    self.assertEqual(
        True,
        ndb.transaction(lambda: task_to_run.reap_task_to_run(task.key)))
    run_result = task_result.new_run_result(request, 1, 'localhost')
    task_result.put_run_result(run_result)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'completed_ts': None,
      'created_ts': self.now,
      'exit_codes': [],
      'internal_failure': False,
      'modified_ts': reap_ts,
      'name': u'Request name',
      'outputs': [],
      'started_ts': reap_ts,
      'failure': False,
      'state': task_result.State.RUNNING,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())

    # Task completed after 2 seconds (6 secs total), the task has been running
    # for 2 seconds.
    complete_ts = self.now + datetime.timedelta(seconds=6)
    test_helper.mock_now(self, complete_ts)
    run_result.completed_ts = complete_ts
    run_result.exit_codes.append(0)
    run_result.state = task_result.State.COMPLETED
    results_key = result_helper.StoreResults('foo').key
    run_result.outputs.append(results_key)
    task_result.put_run_result(run_result)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'completed_ts': complete_ts,
      'created_ts': self.now,
      'exit_codes': [0],
      'failure': False,
      'internal_failure': False,
      'modified_ts': complete_ts,
      'name': u'Request name',
      'outputs': [results_key],
      'started_ts': reap_ts,
      'state': task_result.State.COMPLETED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual(datetime.timedelta(seconds=2), result_summary.duration())

  def test_terminate_result(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    result_summary.put()
    run_result = task_result.new_run_result(request, 1, 'localhost')
    task_result.terminate_result(run_result, task_result.State.BOT_DIED)
    expected = {
      'abandoned_ts': self.now,
      'bot_id': u'localhost',
      'completed_ts': None,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': self.now,
      'outputs': [],
      'started_ts': self.now,
      'state': task_result.State.BOT_DIED,
      'try_number': 1,
    }
    self.assertEqual(expected, run_result.key.get().to_dict())

    expected = {
      'abandoned_ts': self.now,
      'bot_id': u'localhost',
      'completed_ts': None,
      'created_ts': self.now,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'Request name',
      'outputs': [],
      'started_ts': self.now,
      'state': task_result.State.BOT_DIED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.key.get().to_dict())

  def test_yield_run_results_with_dead_bot(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    result_summary.put()
    run_result = task_result.new_run_result(request, 1, 'localhost')
    run_result.completed_ts = self.now
    task_result.put_run_result(run_result)

    just_before = self.now + task_common.BOT_PING_TOLERANCE
    test_helper.mock_now(self, just_before)
    self.assertEqual([], list(task_result.yield_run_results_with_dead_bot()))

    late = (
        self.now + task_common.BOT_PING_TOLERANCE +
        datetime.timedelta(seconds=1))
    test_helper.mock_now(self, late)
    self.assertEqual(
        [run_result], list(task_result.yield_run_results_with_dead_bot()))

  def test_put_run_result(self):
    # Tested indirectly.
    pass


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
