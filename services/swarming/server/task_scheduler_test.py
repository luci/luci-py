#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import inspect
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.api import search
from google.appengine.ext import deferred
from google.appengine.ext import ndb

# From tools/third_party/
import webtest

from components import stats_framework
from server import result_helper
from server import stats
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_to_run
from server import test_helper
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


class TaskSchedulerApiTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(TaskSchedulerApiTest, self).setUp()
    self.testbed.init_search_stub()

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
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    actual_request, run_result  = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost')
    self.assertEqual(request, actual_request)
    self.assertEqual('localhost', run_result.bot_id)
    self.assertEqual(None, task_to_run.TaskToRun.query().get().queue_number)

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

  def test_get_results(self):
    # TODO(maruel): Split in more focused tests.
    created_ts = self.now
    test_helper.mock_now(self, created_ts)
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)

    # The TaskRequest was enqueued, the TaskResultSummary was created but no
    # TaskRunResult exist yet since the task was not scheduled on any bot.
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': None,
      'created_ts': created_ts,
      'completed_ts': None,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': created_ts,
      'name': u'Request name',
      'outputs': [],
      'started_ts': None,
      'state': State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual([], run_results)

    # A bot reaps the TaskToRun.
    reaped_ts = self.now + datetime.timedelta(seconds=60)
    test_helper.mock_now(self, reaped_ts)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost')
    self.assertEqual(request, reaped_request)
    self.assertTrue(run_result)
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'created_ts': created_ts,  # Time the TaskRequest was created.
      'completed_ts': None,
      'exit_codes': [],
      'failure': False,
      'internal_failure': False,
      'modified_ts': reaped_ts,
      'name': u'Request name',
      'outputs': [],
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
        'completed_ts': None,
        'exit_codes': [],
        'internal_failure': False,
        'modified_ts': reaped_ts,
        'outputs': [],
        'started_ts': reaped_ts,
        'failure': False,
        'state': State.RUNNING,
        'try_number': 1,
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in run_results])

    # The bot completes the task.
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
    task_scheduler.bot_update_task(run_result.key, data, 'localhost')
    result_summary, run_results = get_results(request.key)
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'created_ts': created_ts,
      'completed_ts': done_ts,
      'exit_codes': [0, 0],
      'failure': False,
      'internal_failure': False,
      'modified_ts': done_ts,
      'name': u'Request name',
      'outputs': output_keys,
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
        'completed_ts': done_ts,
        'exit_codes': [0, 0],
        'failure': False,
        'internal_failure': False,
        'modified_ts': done_ts,
        'outputs': output_keys,
        'started_ts': reaped_ts,
        'state': State.COMPLETED,
        'try_number': 1,
      },
    ]
    self.assertEqual(expected, [t.to_dict() for t in run_results])

  def test_exit_code_failure(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    request, _result_summary = task_scheduler.make_request(data)
    reaped_request, run_result = task_scheduler.bot_reap_task(
        {'OS': 'Windows-3.1.1'}, 'localhost')
    self.assertEqual(request, reaped_request)
    data = {
      'exit_codes': [0, 1],
      'outputs': [],
    }
    task_scheduler.bot_update_task(run_result.key, data, 'localhost')
    result_summary, run_results = get_results(request.key)

    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'created_ts': self.now,
      'completed_ts': self.now,
      'exit_codes': [0, 1],
      'failure': True,
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'Request name',
      'outputs': [],
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
        'completed_ts': self.now,
        'exit_codes': [0, 1],
        'failure': True,
        'internal_failure': False,
        'modified_ts': self.now,
        'outputs': [],
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

  def test_unpack_result_summary_key(self):
    actual = task_scheduler.unpack_result_summary_key('bb80200')
    expected = (
        "Key('TaskRequestShard', '6f4236', 'TaskRequest', 196608512, "
        "'TaskResultSummary', 1)")
    self.assertEqual(expected, str(actual))

    with self.assertRaises(ValueError):
      task_scheduler.unpack_result_summary_key('0')
    with self.assertRaises(ValueError):
      task_scheduler.unpack_result_summary_key('g')
    with self.assertRaises(ValueError):
      task_scheduler.unpack_result_summary_key('bb80201')

  def test_unpack_run_result_key(self):
    actual = task_scheduler.unpack_run_result_key('bb80201')
    expected = (
        "Key('TaskRequestShard', '6f4236', 'TaskRequest', 196608512, "
        "'TaskResultSummary', 1, 'TaskRunResult', 1)")
    self.assertEqual(expected, str(actual))

    with self.assertRaises(ValueError):
      task_scheduler.unpack_run_result_key('1')
    with self.assertRaises(ValueError):
      task_scheduler.unpack_run_result_key('g')
    with self.assertRaises(ValueError):
      task_scheduler.unpack_run_result_key('bb80200')
    with self.assertRaises(NotImplementedError):
      task_scheduler.unpack_run_result_key('bb80202')

  def test_bot_update_task(self):
    # See test_get_results.
    pass

  def test_cron_abort_expired_task_to_run(self):
    # Create two shards, one is properly reaped, the other is expired.
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    expiration = data['scheduling_expiration_secs']
    after_delay = self.now + datetime.timedelta(seconds=expiration+1)
    test_helper.mock_now(self, after_delay)
    self.assertEqual(1, task_scheduler.cron_abort_expired_task_to_run())

  def test_cron_abort_bot_died(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    task_scheduler.make_request(data)
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    _request, _run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost')
    after_delay = (
        self.now + task_scheduler.task_common.BOT_PING_TOLERANCE +
        datetime.timedelta(seconds=1))
    test_helper.mock_now(self, after_delay)
    self.assertEqual(1, task_scheduler.cron_abort_bot_died())

  def test_search_by_name(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _, result_summary = task_scheduler.make_request(data)

    # Assert that search is not case-sensitive by using unexpected casing.
    actual, _cursor = task_scheduler.search_by_name('requEST', None, 10)
    self.assertEqual([result_summary], actual)
    actual, _cursor = task_scheduler.search_by_name('name', None, 10)
    self.assertEqual([result_summary], actual)

  def test_search_by_name_failures(self):
    data = _gen_request_data(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    _, result_summary = task_scheduler.make_request(data)

    actual, _cursor = task_scheduler.search_by_name('foo', None, 10)
    self.assertEqual([], actual)
    # Partial match doesn't work.
    actual, _cursor = task_scheduler.search_by_name('nam', None, 10)
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
      self.assertEqual('make_request', inspect.stack()[1][3])
      if (index[0] % SKIP) == 1:
        raise RandomFailure()
      return old_put_multi(*args, **kwargs)

    def put(*args, **kwargs):
      self.assertEqual('make_request', inspect.stack()[1][3])
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
    # The mocking doesn't affect TaskRequest since it uses
    # datastore_utils.insert() which uses ndb.Model.put().
    self.assertEqual(100, task_request.TaskRequest.query().count())
    self.assertEqual(34, task_result.TaskResultSummary.query().count())

    # Now the DB is full of half-corrupted entities.
    cursor = None
    actual, cursor = task_scheduler.search_by_name('Request', cursor, 31)
    self.assertEqual(31, len(actual))
    actual, cursor = task_scheduler.search_by_name('Request', cursor, 31)
    self.assertEqual(3, len(actual))
    actual, cursor = task_scheduler.search_by_name('Request', cursor, 31)
    self.assertEqual(0, len(actual))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
