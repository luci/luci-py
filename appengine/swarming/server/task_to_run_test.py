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

import webtest

from google.appengine.ext import ndb

import handlers_backend

from components import auth_testing
from components import utils
from test_support import test_case

from server import bot_management
from server import task_queues
from server import task_request
from server import task_to_run


# pylint: disable=W0212
# Method could be a function - pylint: disable=R0201


def _gen_request(properties=None, **kwargs):
  """Creates a TaskRequest."""
  props = {
    'command': [u'command1'],
    'dimensions': {u'pool': u'default'},
    'env': {},
    'execution_timeout_secs': 24*60*60,
    'io_timeout_secs': None,
  }
  props.update(properties or {})
  now = utils.utcnow()
  args = {
    'created_ts': now,
    'name': 'Request name',
    'priority': 50,
    'properties': task_request.TaskProperties(**props),
    'expiration_ts': now + datetime.timedelta(seconds=60),
    'tags': [u'tag:1'],
    'user': 'Jesus',
  }
  args.update(kwargs)
  return task_request.TaskRequest(**args)


def _task_to_run_to_dict(i):
  """Converts the queue_number to hex for easier testing."""
  out = i.to_dict()
  # Consistent formatting makes it easier to reason about.
  out['queue_number'] = '0x%016x' % out['queue_number']
  return out


def _yield_next_available_task_to_dispatch(bot_dimensions, deadline):
  bot_management.bot_event(
      'bot_connected', bot_dimensions[u'id'][0], '1.2.3.4', 'joe@localhost',
      bot_dimensions, {'state': 'real'}, '1234', False, None, None)
  task_queues.assert_bot_async(bot_dimensions).get_result()
  return [
    _task_to_run_to_dict(to_run)
    for _request, to_run in
        task_to_run.yield_next_available_task_to_dispatch(
            bot_dimensions, deadline)
  ]


def _hash_dimensions(dimensions):
  return task_queues.hash_dimensions(dimensions)



class TaskToRunApiTest(test_env_handlers.AppTestBase):
  def setUp(self):
    super(TaskToRunApiTest, self).setUp()
    self.now = datetime.datetime(2014, 01, 02, 03, 04, 05, 06)
    self.mock_now(self.now)
    auth_testing.mock_get_current_identity(self)
    # The default expiration_secs for _gen_request().
    self.expiration_ts = self.now + datetime.timedelta(seconds=60)
    # Setup the backend to handle task queues for 'task-dimensions'.
    self.app = webtest.TestApp(
        handlers_backend.create_application(True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self._enqueue_orig = self.mock(utils, 'enqueue_task', self._enqueue)

  def _enqueue(self, *args, **kwargs):
    return self._enqueue_orig(*args, use_dedicated_module=False, **kwargs)

  def mkreq(self, req, nb_task=1):
    """Stores a new initialized TaskRequest.

    nb_task is 1 or 0. It represents the number of GAE task queue
    rebuild-task-cache enqueued. It is 1 when the request.properties.dimensions
    is new (unseen before) and a GAE task queue was enqueued to process it, 0
    otherwise.
    """
    task_request.init_new_request(req, True, None)
    task_queues.assert_task(req)
    self.assertEqual(nb_task, self.execute_tasks())
    req.key = task_request.new_request_key()
    req.put()
    return req

  def _gen_new_task_to_run(self, nb_task=1, **kwargs):
    """Returns a TaskToRun saved in the DB."""
    request = self.mkreq(_gen_request(**kwargs), nb_task=nb_task)
    to_run = task_to_run.new_task_to_run(request)
    to_run.put()
    return to_run

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(task_to_run)
        if i[0] != '_' and hasattr(getattr(task_to_run, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_task_to_run_key_to_request_key(self):
    request = self.mkreq(_gen_request())
    task_key = task_to_run.request_to_task_to_run_key(request)
    actual = task_to_run.task_to_run_key_to_request_key(task_key)
    self.assertEqual(request.key, actual)

  def test_request_to_task_to_run_key(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    request = self.mkreq(_gen_request())
    # Ensures that the hash value is constant for the same input.
    self.assertEqual(
        ndb.Key('TaskRequest', 0x7e296460f77ff77e, 'TaskToRun', 1260396639),
        task_to_run.request_to_task_to_run_key(request))

  def test_validate_to_run_key(self):
    request = self.mkreq(_gen_request())
    task_key = task_to_run.request_to_task_to_run_key(request)
    task_to_run.validate_to_run_key(task_key)
    with self.assertRaises(ValueError):
      task_to_run.validate_to_run_key(ndb.Key('TaskRequest', 1, 'TaskToRun', 1))

  def test_gen_queue_number(self):
    # tuples of (input, expected).
    # 0x3fc00000 is the priority mask.
    data = [
      # Priorities.
      ((1, '1970-01-01 00:00:00.000',   0), (0x80000000,   0)),
      ((1, '1970-01-01 00:00:00.000',   1), (0x80400000,   1)),
      ((1, '1970-01-01 00:00:00.000',   2), (0x80800000,   2)),
      ((1, '1970-01-01 00:00:00.000',   3), (0x80c00000,   3)),
      ((1, '1970-01-01 00:00:00.000', 255), (0xbfc00000, 255)),
      # Largest hash.
      ((0xffffffff, '1970-01-01 00:00:00.000', 255), (0x7fffffffbfc00000, 255)),
      # Time resolution.
      ((1, '1970-01-01 00:00:00.040',   0), (0x80000000,   0)),
      ((1, '1970-01-01 00:00:00.050',   0), (0x80000001,   0)),
      ((1, '1970-01-01 00:00:00.100',   0), (0x80000001,   0)),
      ((1, '1970-01-01 00:00:00.900',   0), (0x80000009,   0)),
      ((1, '1970-01-01 00:00:01.000',   0), (0x8000000a,   0)),  # 10
      ((1, '2010-01-02 03:04:05.060',   0), (0x800ede73,   0)),
      ((1, '2010-01-02 03:04:05.060',   1), (0x804ede73,   1)),
      ((1, '2010-12-31 23:59:59.999',   0), (0x92cc0300,  75)),
      ((1, '2010-12-31 23:59:59.999',   1), (0x930c0300,  76)),
      ((1, '2010-12-31 23:59:59.999',   2), (0x934c0300,  77)),
      ((1, '2010-12-31 23:59:59.999', 255), (0xd28c0300, 330)),
      # It's the end of the world as we know it...
      ((1, '9999-12-31 23:59:59.999',   0), (0x92cc0300,  75)),
      ((1, '9999-12-31 23:59:59.999',   1), (0x930c0300,  76)),
      ((1, '9999-12-31 23:59:59.999', 255), (0xd28c0300, 330)),
      ((1, '9999-12-31 23:59:59.999', 255), (0xd28c0300, 330)),
    ]
    for i, (
      (dimensions_hash, timestamp, priority),
      (expected_v, expected_p)) in enumerate(data):
      d = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
      actual = task_to_run._gen_queue_number(
          dimensions_hash, d, priority)
      self.assertEqual((i, '0x%016x' % expected_v), (i, '0x%016x' % actual))
      # Ensure we can extract the priority back. That said, it is corrupted by
      # time.
      v = task_to_run.TaskToRun(queue_number=actual)
      self.assertEqual(
          (i, expected_p), (i, task_to_run._queue_number_priority(v)))

  def test_new_task_to_run(self):
    self.mock(random, 'getrandbits', lambda _: 0x12)
    request_dimensions = {u'os': u'Windows-3.1.1', u'pool': u'default'}
    now = utils.utcnow()
    data = _gen_request(
        properties={
          'command': [u'command1', u'arg1'],
          'dimensions': request_dimensions,
          'env': {u'foo': u'bar'},
          'execution_timeout_secs': 30,
        },
        priority=20,
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=31))
    task_to_run.new_task_to_run(self.mkreq(data)).put()

    # Create a second with higher priority.
    self.mock(random, 'getrandbits', lambda _: 0x23)
    data = _gen_request(
        properties={
          'command': [u'command1', u'arg1'],
          'dimensions': request_dimensions,
          'env': {u'foo': u'bar'},
          'execution_timeout_secs': 30,
        },
        priority=10,
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=31))
    task_to_run.new_task_to_run(self.mkreq(data, nb_task=0)).put()

    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.now + datetime.timedelta(seconds=31),
        'request_key': '0x7e296460f77ffdce',
        # Lower priority value means higher priority.
        'queue_number': '0x1a3aa663028ede72',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.now + datetime.timedelta(seconds=31),
        'request_key': '0x7e296460f77ffede',
        'queue_number': '0x1a3aa663050ede72',
      },
    ]

    def flatten(i):
      out = _task_to_run_to_dict(i)
      out['request_key'] = '0x%016x' % i.request_key.integer_id()
      return out

    # Warning: Ordering by key doesn't work because of TaskToRunShard; e.g.
    # the entity key ordering DOES NOT correlate with .queue_number
    # Ensure they come out in expected order.
    q = task_to_run.TaskToRun.query().order(task_to_run.TaskToRun.queue_number)
    self.assertEqual(expected, map(flatten, q.fetch()))

  def test_match_dimensions(self):
    data_true = (
      ({}, {}),
      ({}, {'a': 'b'}),
      ({'a': 'b'}, {'a': 'b'}),
      ({'os': 'amiga'}, {'os': ['amiga', 'amiga-3.1']}),
      ( {'os': 'amiga', 'foo': 'bar'},
        {'os': ['amiga', 'amiga-3.1'], 'a': 'b', 'foo': 'bar'}),
    )

    for request_dimensions, bot_dimensions in data_true:
      self.assertEqual(
          True,
          task_to_run.match_dimensions(request_dimensions, bot_dimensions))

    data_false = (
      ({'os': 'amiga'}, {'os': ['Win', 'Win-3.1']}),
    )
    for request_dimensions, bot_dimensions in data_false:
      self.assertEqual(
          False,
          task_to_run.match_dimensions(request_dimensions, bot_dimensions))

  def test_yield_next_available_task_to_dispatch_none(self):
    self._gen_new_task_to_run(
        properties={
          'dimensions': {u'os': u'Windows-3.1.1', u'pool': u'default'},
        })
    # Bot declares no dimensions, so it will fail to match.
    bot_dimensions = {u'id': [u'bot1'], u'pool': [u'default']}
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    self.assertEqual([], actual)

  def test_yield_next_available_task_to_dispatch_none_mismatch(self):
    self._gen_new_task_to_run(
        properties={
          'dimensions': {u'os': u'Windows-3.1.1', u'pool': u'default'},
        })
    # Bot declares other dimensions, so it will fail to match.
    bot_dimensions = {
      u'id': [u'bot1'],
      u'os': [u'Windows-3.0'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    self.assertEqual([], actual)

  def test_yield_next_available_task_to_dispatch(self):
    request_dimensions = {
      u'foo': u'bar',
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = {k: [v] for k, v in request_dimensions.iteritems()}
    bot_dimensions[u'id'] = [u'bot1']
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x613fbb330c8ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_subset(self):
    request_dimensions = {
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed, this is fine and it matches.
    bot_dimensions = {
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x1a3aa6630c8ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_shard(self):
    request_dimensions = {
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(properties=dict(dimensions=request_dimensions))
    bot_dimensions = {k: [v] for k, v in request_dimensions.iteritems()}
    bot_dimensions[u'id'] = [u'bot1']
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x1a3aa6630c8ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_subset_multivalue(self):
    request_dimensions = {
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed.
    bot_dimensions = {
      u'id': [u'localhost'],
      u'os': [u'Windows', u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x1a3aa6630c8ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_multi_normal(self):
    # Task added one after the other, normal case.
    request_dimensions_1 = {
      u'foo': u'bar',
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(properties=dict(dimensions=request_dimensions_1))

    # It's normally time ordered.
    self.mock_now(self.now, 1)
    request_dimensions_2 = {u'id': u'localhost', u'pool': u'default'}
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions_2), nb_task=0)

    bot_dimensions = {
      u'foo': [u'bar'],
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x613fbb330c8ede72',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': self.expiration_ts + datetime.timedelta(seconds=1),
        'queue_number': '0x5385bf748c8ede7c',
      },
    ]
    # There is a significant risk of non-determinism.
    self.assertEqual(sorted(expected), sorted(actual))

  def test_yield_next_available_task_to_dispatch_clock_skew(self):
    # Asserts that a TaskToRun added later in the DB (with a Key with an higher
    # value) but with a timestamp sooner (for example, time desynchronization
    # between machines) is still returned in the timestamp order, e.g. priority
    # is done based on timestamps and priority only.
    request_dimensions_1 = {
      u'foo': u'bar',
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(properties=dict(dimensions=request_dimensions_1))

    # The second shard is added before the first, potentially because of a
    # desynchronized clock. It'll have higher priority.
    self.mock_now(self.now, -1)
    request_dimensions_2 = {u'id': u'localhost', u'pool': u'default'}
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions_2), nb_task=0)

    bot_dimensions = {
      u'foo': [u'bar'],
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x613fbb330c8ede72',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        # Due to time being late on the second requester frontend.
        'expiration_ts': self.expiration_ts - datetime.timedelta(seconds=1),
        'queue_number': '0x5385bf748c8ede68',
      },
    ]
    # There is a significant risk of non-determinism.
    self.assertEqual(sorted(expected), sorted(actual))

  def test_yield_next_available_task_to_dispatch_priority(self):
    # Tasks added later but with higher priority are returned first.
    request_dimensions = {u'os': u'Windows-3.1.1', u'pool': u'default'}
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions), priority=50)

    # This one is later but has higher priority.
    self.mock_now(self.now, 60)
    request = self.mkreq(
        _gen_request(
            properties=dict(dimensions=request_dimensions), priority=10),
        nb_task=0)
    task_to_run.new_task_to_run(request).put()

    # It should return them all, in the expected order: lowest priority first.
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 6, 5, 6),
        'queue_number': '0x1a3aa663028ee0ca',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 5, 5, 6),
        'queue_number': '0x1a3aa6630c8ede72',
      },
    ]
    bot_dimensions = {
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_multi_priority(self):
    # High priority tasks added later with other dimensions are returned first.
    request_dimensions_1 = {u'os': u'Windows-3.1.1', u'pool': u'default'}
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions_1), priority=50)

    # This one is later but has higher priority.
    self.mock_now(self.now, 60)
    request_dimensions_2 = {u'id': u'localhost', u'pool': u'default'}
    request = self.mkreq(
        _gen_request(
            properties=dict(dimensions=request_dimensions_2), priority=10),
        nb_task=0)
    task_to_run.new_task_to_run(request).put()

    # It should return them all, in the expected order: lowest priority first.
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 6, 5, 6),
        'queue_number': '0x5385bf74828ee0ca',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 5, 5, 6),
        'queue_number': '0x1a3aa6630c8ede72',
      },
    ]
    bot_dimensions = {
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, None)
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_run_task_exceeds_deadline(self):
    request_dimensions = {
      u'foo': u'bar',
      u'id': u'localhost',
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions), nb_task=0)
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = {k: [v] for k, v in request_dimensions.iteritems()}
    actual = _yield_next_available_task_to_dispatch(
        bot_dimensions, datetime.datetime(1969, 1, 1))
    self.failIf(actual)

  def test_yield_next_available_task_to_run_task_meets_deadline(self):
    request_dimensions = {
      u'foo': u'bar',
      u'id': u'localhost',
      u'os': u'Windows-3.1.1',
      u'pool': u'default',
    }
    self._gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions), nb_task=0)
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = {k: [v] for k, v in request_dimensions.iteritems()}
    actual = _yield_next_available_task_to_dispatch(
        bot_dimensions, datetime.datetime(3000, 1, 1))
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x3f6b0f050c8ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_run_task_terminate(self):
    request_dimensions = {
      u'id': u'fake-id',
    }
    task = self._gen_new_task_to_run(
        priority=0,
        properties=dict(
            command=[], dimensions=request_dimensions, execution_timeout_secs=0,
            grace_period_secs=0),
        nb_task=0)
    self.assertTrue(task.key.parent().get().properties.is_terminate)
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = {k: [v] for k, v in request_dimensions.iteritems()}
    bot_dimensions[u'pool'] = [u'default']
    actual = _yield_next_available_task_to_dispatch(bot_dimensions, 0)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x54795e3c800ede72',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_expired_task_to_run(self):
    now = utils.utcnow()
    self._gen_new_task_to_run(
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=60))
    bot_dimensions = {u'id': [u'bot1'], u'pool': [u'default']}
    self.assertEqual(
        1,
        len(_yield_next_available_task_to_dispatch(bot_dimensions, None)))
    self.assertEqual(
        0, len(list(task_to_run.yield_expired_task_to_run())))

    # All tasks are now expired. Note that even if they still have .queue_number
    # set because the cron job wasn't run, they are still not yielded by
    # yield_next_available_task_to_dispatch()
    self.mock_now(self.now, 61)
    self.assertEqual(
        0, len(_yield_next_available_task_to_dispatch(bot_dimensions, None)))
    self.assertEqual(
        1, len(list(task_to_run.yield_expired_task_to_run())))

  def test_is_reapable(self):
    req_dimensions = {u'os': u'Windows-3.1.1', u'pool': u'default'}
    to_run = self._gen_new_task_to_run(
        properties=dict(dimensions=req_dimensions))
    bot_dimensions = {
      u'id': [u'localhost'],
      u'os': [u'Windows-3.1.1'],
      u'pool': [u'default'],
    }
    self.assertEqual(
        1, len(_yield_next_available_task_to_dispatch(bot_dimensions, None)))

    self.assertEqual(True, to_run.is_reapable)
    to_run.queue_number = None
    to_run.put()
    self.assertEqual(False, to_run.is_reapable)

  def test_set_lookup_cache(self):
    to_run = self._gen_new_task_to_run(
        properties={
          'dimensions': {u'os': u'Windows-3.1.1', u'pool': u'default'},
        })
    lookup = lambda k: task_to_run._lookup_cache_is_taken_async(k).get_result()
    self.assertEqual(False, lookup(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, True)
    self.assertEqual(False, lookup(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, False)
    self.assertEqual(True, lookup(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, True)
    self.assertEqual(False, lookup(to_run.key))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
