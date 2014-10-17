#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import random
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
from server import task_request
from server import task_result
from server import task_to_run
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
    'tags': [u'tag1'],
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
    self.mock_now(self.now)
    self.mock(random, 'getrandbits', lambda _: 0x88)
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
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 2)
    expected = (
        "Key('TaskRequestShard', 'f71849', 'TaskRequest', 256, "
        "'TaskResultSummary', 1, 'TaskRunResult', 2)")
    self.assertEqual(expected, str(run_result_key))

    with self.assertRaises(ValueError):
      task_result.result_summary_key_to_run_result_key(result_summary_key, 0)
    with self.assertRaises(NotImplementedError):
      task_result.result_summary_key_to_run_result_key(result_summary_key, 3)

  def test_run_result_key_to_result_summary_key(self):
    request_key = task_request.id_to_request_key(0x100)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 1)
    self.assertEqual(
        result_summary_key,
        task_result.run_result_key_to_result_summary_key(run_result_key))

  def test_pack_result_summary_key(self):
    request_key = task_request.id_to_request_key(0xbb80200)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 1)

    actual = task_result.pack_result_summary_key(result_summary_key)
    # 0xbb8 = 3000ms = 3 secs; 0x02 = random;  0x00 = try_number, e.g. it is a
    # TaskResultSummary.
    self.assertEqual('bb80200', actual)

    with self.assertRaises(AssertionError):
      task_result.pack_result_summary_key(run_result_key)

  def test_pack_run_result_key(self):
    request_key = task_request.id_to_request_key(0xbb80200)
    result_summary_key = task_result.request_key_to_result_summary_key(
        request_key)
    run_result_key = task_result.result_summary_key_to_run_result_key(
        result_summary_key, 1)

    actual = task_result.pack_run_result_key(run_result_key)
    # 0xbb8 = 3000ms = 3 secs; 0x02 = random;  0x01 = try_number, e.g. it is a
    # TaskRunResult.
    self.assertEqual('bb80201', actual)

    with self.assertRaises(AssertionError):
      task_result.pack_run_result_key(result_summary_key)

  def test_unpack_result_summary_key(self):
    actual = task_result.unpack_result_summary_key('bb80200')
    expected = (
        "Key('TaskRequestShard', '6f4236', 'TaskRequest', 196608512, "
        "'TaskResultSummary', 1)")
    self.assertEqual(expected, str(actual))

    with self.assertRaises(ValueError):
      task_result.unpack_result_summary_key('0')
    with self.assertRaises(ValueError):
      task_result.unpack_result_summary_key('g')
    with self.assertRaises(ValueError):
      task_result.unpack_result_summary_key('bb80201')

  def test_unpack_run_result_key(self):
    for i in ('1', '2'):
      actual = task_result.unpack_run_result_key('bb8020' + i)
      expected = (
          "Key('TaskRequestShard', '6f4236', 'TaskRequest', 196608512, "
          "'TaskResultSummary', 1, 'TaskRunResult', " + i + ")")
      self.assertEqual(expected, str(actual))

    with self.assertRaises(ValueError):
      task_result.unpack_run_result_key('1')
    with self.assertRaises(ValueError):
      task_result.unpack_run_result_key('g')
    with self.assertRaises(ValueError):
      task_result.unpack_run_result_key('bb80200')
    with self.assertRaises(NotImplementedError):
      task_result.unpack_run_result_key('bb80203')

  def test_new_result_summary(self):
    request = task_request.make_request(_gen_request_data())
    actual = task_result.new_result_summary(request)
    expected = {
      'abandoned_ts': None,
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
      'modified_ts': None,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [],
      'started_ts': None,
      'state': task_result.State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, actual.to_dict())
    self.assertEqual(50, actual.priority)
    self.assertEqual(True, actual.can_be_canceled)
    actual.state = task_result.State.RUNNING
    self.assertEqual(False, actual.can_be_canceled)

  def test_new_run_result(self):
    request = task_request.make_request(_gen_request_data())
    actual = task_result.new_run_result(request, 1, 'localhost', 'abc')
    expected = {
      'abandoned_ts': None,
      'bot_id': 'localhost',
      'bot_version': 'abc',
      'completed_ts': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888801',
      'internal_failure': False,
      'modified_ts': None,
      'server_versions': ['default-version'],
      'started_ts': self.now,
      'state': task_result.State.RUNNING,
      'try_number': 1,
    }
    self.assertEqual(expected, actual.to_dict())
    self.assertEqual(50, actual.priority)
    self.assertEqual(False, actual.can_be_canceled)

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
      'bot_version': None,
      'completed_ts': None,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': self.now,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [],
      'started_ts': None,
      'state': task_result.State.PENDING,
      'try_number': None,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())

    # Nothing changed 2 secs later except latency.
    self.mock_now(self.now, 2)
    self.assertEqual(expected, result_summary.to_dict())

    # Task is reaped after 2 seconds (4 secs total).
    reap_ts = self.now + datetime.timedelta(seconds=4)
    self.mock_now(reap_ts)
    task = task_to_run.is_task_reapable(task.key, None)
    task.queue_number = None
    task.put()
    run_result = task_result.new_run_result(request, 1, 'localhost', 'abc')
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))
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
      'modified_ts': reap_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': reap_ts,
      'state': task_result.State.RUNNING,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())

    # Task completed after 2 seconds (6 secs total), the task has been running
    # for 2 seconds.
    complete_ts = self.now + datetime.timedelta(seconds=6)
    self.mock_now(complete_ts)
    run_result.completed_ts = complete_ts
    run_result.exit_codes.append(0)
    run_result.state = task_result.State.COMPLETED
    ndb.put_multi(run_result.append_output(0, 'foo', 0))
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))
    expected = {
      'abandoned_ts': None,
      'bot_id': u'localhost',
      'bot_version': 'abc',
      'completed_ts': complete_ts,
      'created_ts': self.now,
      'deduped_from': None,
      'durations': [],
      'exit_codes': [0],
      'failure': False,
      'id': '14350e868888800',
      'internal_failure': False,
      'modified_ts': complete_ts,
      'name': u'Request name',
      'properties_hash': None,
      'server_versions': [u'default-version'],
      'started_ts': reap_ts,
      'state': task_result.State.COMPLETED,
      'try_number': 1,
      'user': u'Jesus',
    }
    self.assertEqual(expected, result_summary.to_dict())
    self.assertEqual(['foo'], result_summary.get_outputs())
    self.assertEqual(datetime.timedelta(seconds=2), result_summary.duration)
    self.assertEqual(
        datetime.timedelta(seconds=2), result_summary.duration_now())
    self.assertEqual(
        datetime.timedelta(seconds=4), result_summary.pending)
    self.assertEqual(
        datetime.timedelta(seconds=4), result_summary.pending_now())

    self.assertEqual(
        task_result.pack_result_summary_key(result_summary.key),
        result_summary.key_string)
    self.assertEqual(complete_ts, result_summary.ended_ts)
    self.assertEqual(
        task_result.pack_run_result_key(run_result.key),
        run_result.key_string)
    self.assertEqual(complete_ts, run_result.ended_ts)

  def test_yield_run_results_with_dead_bot(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    result_summary.put()
    run_result = task_result.new_run_result(request, 1, 'localhost', 'abc')
    run_result.completed_ts = self.now
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))

    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE)
    self.assertEqual([], list(task_result.yield_run_results_with_dead_bot()))

    self.mock_now(self.now + task_result.BOT_PING_TOLERANCE, 1)
    self.assertEqual(
        [run_result], list(task_result.yield_run_results_with_dead_bot()))

  def test_prepare_put_run_result(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    run_result = task_result.new_run_result(request, 1, 'localhost', 'abc')
    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.put_multi((result_summary, run_result))

    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))

    self.assertFalse(result_summary.need_update_from_run_result(run_result))

  def test_prepare_put_run_result_two_server_versions(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    run_result = task_result.new_run_result(request, 1, 'localhost', 'abc')
    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.put_multi((result_summary, run_result))

    self.assertTrue(result_summary.need_update_from_run_result(run_result))
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))

    self.mock(utils, 'get_app_version', lambda: 'new-version')
    ndb.put_multi(task_result.prepare_put_run_result(run_result, request))
    self.assertEqual(
        ['default-version', 'new-version'], run_result.server_versions)
    self.assertEqual(
        ['default-version', 'new-version'],
        result_summary.key.get().server_versions)

  def test_prepare_put_run_result_two_tries(self):
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    run_result_1 = task_result.new_run_result(request, 1, 'localhost', 'abc')
    run_result_2 = task_result.new_run_result(request, 2, 'localhost', 'abc')
    self.assertTrue(result_summary.need_update_from_run_result(run_result_1))
    ndb.put_multi((result_summary, run_result_2))

    self.assertTrue(result_summary.need_update_from_run_result(run_result_1))
    ndb.put_multi(task_result.prepare_put_run_result(run_result_1, request))

    self.assertFalse(result_summary.need_update_from_run_result(run_result_1))

    self.assertTrue(result_summary.need_update_from_run_result(run_result_2))
    ndb.put_multi(task_result.prepare_put_run_result(run_result_2, request))
    result_summary = result_summary.key.get()

    self.assertEqual(2, result_summary.try_number)
    self.assertFalse(result_summary.need_update_from_run_result(run_result_1))
    self.assertEqual(
        1, len(task_result.prepare_put_run_result(run_result_1, request)))

  def test_run_result_duration(self):
    run_result = task_result.TaskRunResult(
        started_ts=datetime.datetime(2010, 1, 1, 0, 0, 0),
        completed_ts=datetime.datetime(2010, 1, 1, 0, 2, 0))
    self.assertEqual(datetime.timedelta(seconds=120), run_result.duration)
    self.assertEqual(datetime.timedelta(seconds=120), run_result.duration_now())

    run_result = task_result.TaskRunResult(
        started_ts=datetime.datetime(2010, 1, 1, 0, 0, 0),
        abandoned_ts=datetime.datetime(2010, 1, 1, 0, 1, 0))
    self.assertEqual(None, run_result.duration)
    self.assertEqual(None, run_result.duration_now())


class TestOutput(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(TestOutput, self).setUp()
    request = task_request.make_request(_gen_request_data())
    result_summary = task_result.new_result_summary(request)
    result_summary.put()
    self.run_result = task_result.new_run_result(request, 1, 'localhost', 'abc')
    ndb.put_multi(task_result.prepare_put_run_result(self.run_result, request))

  def assertTaskOutputChunk(self, expected):
    q = task_result.TaskOutputChunk.query().order(
        task_result.TaskOutputChunk.key)
    self.assertEqual(expected, [t.to_dict() for t in q.fetch()])

  def test_append_output(self):
    # Test that one can stream output and it is returned fine.
    def run(*args):
      entities = self.run_result.append_output(*args)
      self.assertEqual(1, len(entities))
      ndb.put_multi(entities)
    run(0, 'Part1\n', 0)
    run(1, 'Part2\n', 0)
    run(1, 'Part3\n', len('Part2\n'))
    self.assertEqual(
        ['Part1\n', 'Part2\nPart3\n'], self.run_result.get_outputs())
    self.assertEqual(
        'Part1\n',
        self.run_result.get_command_output_async(0).get_result())
    self.assertEqual(
        'Part2\nPart3\n',
        self.run_result.get_command_output_async(1).get_result())
    self.assertEqual(
        None, self.run_result.get_command_output_async(2).get_result())

  def test_append_output_large(self):
    self.mock(logging, 'error', lambda *_: None)
    one_mb = '<3Google' * (1024*1024/8)

    def run(*args):
      entities = self.run_result.append_output(*args)
      # Asserts at least one entity was created.
      self.assertTrue(entities)
      ndb.put_multi(entities)

    for i in xrange(16):
      run(0, one_mb, i*len(one_mb))

    self.assertEqual(
        task_result.TaskOutput.FETCH_MAX_CONTENT,
        len(self.run_result.get_command_output_async(0).get_result()))

  def test_append_output_max_chunk(self):
    # This test case is very slow (1m25s locally) if running with the default
    # values, so scale it down a bit which results in ~2.5s.
    self.mock(
        task_result.TaskOutput, 'PUT_MAX_CONTENT',
        task_result.TaskOutput.PUT_MAX_CONTENT / 8)
    self.mock(
        task_result.TaskOutput, 'PUT_MAX_CHUNKS',
        task_result.TaskOutput.PUT_MAX_CHUNKS / 8)
    self.assertFalse(
        task_result.TaskOutput.PUT_MAX_CONTENT %
            task_result.TaskOutput.CHUNK_SIZE)

    calls = []
    self.mock(logging, 'error', lambda *args: calls.append(args))
    max_chunk = 'x' * task_result.TaskOutput.PUT_MAX_CONTENT
    entities = self.run_result.append_output(0, max_chunk, 0)
    self.assertEqual(task_result.TaskOutput.PUT_MAX_CHUNKS, len(entities))
    ndb.put_multi(entities)
    self.assertEqual([], calls)

    # Try with PUT_MAX_CONTENT + 1 bytes, so the last byte is discarded.
    entities = self.run_result.append_output(1, max_chunk + 'x', 0)
    self.assertEqual(task_result.TaskOutput.PUT_MAX_CHUNKS, len(entities))
    ndb.put_multi(entities)
    self.assertEqual(1, len(calls))
    self.assertTrue(calls[0][0].startswith('Dropping '), calls[0][0])
    self.assertEqual(1, calls[0][1])

  def test_append_output_partial(self):
    ndb.put_multi(self.run_result.append_output(0, 'Foo', 10))
    expected_output = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Foo'
    self.assertEqual(
        expected_output,
        self.run_result.get_command_output_async(0).get_result())
    self.assertTaskOutputChunk([{'chunk': expected_output, 'gaps': [0, 10]}])

  def test_append_output_partial_hole(self):
    ndb.put_multi(self.run_result.append_output(0, 'Foo', 0))
    ndb.put_multi(self.run_result.append_output(0, 'Bar', 10))
    expected_output = 'Foo\x00\x00\x00\x00\x00\x00\x00Bar'
    self.assertEqual(
        expected_output,
        self.run_result.get_command_output_async(0).get_result())
    self.assertTaskOutputChunk([{'chunk': expected_output, 'gaps': [3, 10]}])

  def test_append_output_partial_far(self):
    ndb.put_multi(self.run_result.append_output(
        0, 'Foo', 10 + task_result.TaskOutput.CHUNK_SIZE))
    self.assertEqual(
        '\x00' * (task_result.TaskOutput.CHUNK_SIZE + 10) + 'Foo',
        self.run_result.get_command_output_async(0).get_result())
    expected = [
      {'chunk': '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Foo', 'gaps': [0, 10]},
    ]
    self.assertTaskOutputChunk(expected)

  def test_append_output_partial_far_split(self):
    # Missing, writing happens on two different TaskOutputChunk entities.
    ndb.put_multi(self.run_result.append_output(
        0, 'FooBar', 2 * task_result.TaskOutput.CHUNK_SIZE - 3))
    self.assertEqual(
        '\x00' * (task_result.TaskOutput.CHUNK_SIZE * 2 - 3) + 'FooBar',
        self.run_result.get_command_output_async(0).get_result())
    expected = [
      {
        'chunk': '\x00' * (task_result.TaskOutput.CHUNK_SIZE - 3) + 'Foo',
        'gaps': [0, 102397],
      },
      {'chunk': 'Bar', 'gaps': []},
    ]
    self.assertTaskOutputChunk(expected)

  def test_append_output_overwrite(self):
    # Overwrite previously written data.
    ndb.put_multi(self.run_result.append_output(0, 'FooBar', 0))
    ndb.put_multi(self.run_result.append_output(0, 'X', 3))
    self.assertEqual(
        'FooXar', self.run_result.get_command_output_async(0).get_result())
    self.assertTaskOutputChunk([{'chunk': 'FooXar', 'gaps': []}])

  def test_append_output_reverse_order(self):
    # Write the data in reverse order in multiple calls.
    ndb.put_multi(self.run_result.append_output(0, 'Wow', 11))
    ndb.put_multi(self.run_result.append_output(0, 'Foo', 8))
    ndb.put_multi(self.run_result.append_output(0, 'Baz', 0))
    ndb.put_multi(self.run_result.append_output(0, 'Bar', 4))
    expected_output = 'Baz\x00Bar\x00FooWow'
    self.assertEqual(
        expected_output,
        self.run_result.get_command_output_async(0).get_result())
    self.assertTaskOutputChunk(
        [{'chunk': expected_output, 'gaps': [3, 4, 7, 8]}])

  def test_append_output_reverse_order_second_chunk(self):
    # Write the data in reverse order in multiple calls.
    ndb.put_multi(self.run_result.append_output(
        0, 'Wow', task_result.TaskOutput.CHUNK_SIZE + 11))
    ndb.put_multi(self.run_result.append_output(
        0, 'Foo', task_result.TaskOutput.CHUNK_SIZE + 8))
    ndb.put_multi(self.run_result.append_output(
        0, 'Baz', task_result.TaskOutput.CHUNK_SIZE + 0))
    ndb.put_multi(self.run_result.append_output(
        0, 'Bar', task_result.TaskOutput.CHUNK_SIZE + 4))
    self.assertEqual(
        task_result.TaskOutput.CHUNK_SIZE * '\x00' + 'Baz\x00Bar\x00FooWow',
        self.run_result.get_command_output_async(0).get_result())
    self.assertTaskOutputChunk(
        [{'chunk': 'Baz\x00Bar\x00FooWow', 'gaps': [3, 4, 7, 8]}])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
