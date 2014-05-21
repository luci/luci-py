#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import hashlib
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from server import task_request
from server import task_shard_to_run
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


def _convert(i):
  """Converts the queue_number to hex for easier testing."""
  out = i.to_dict()
  # Consistent formatting makes it easier to reason about.
  out['queue_number'] = '0x%016x' % out['queue_number']
  return out


def _yield_next_available_shard_to_dispatch(bot_dimensions):
  return [
    _convert(shard_to_run)
    for _request, shard_to_run in
        task_shard_to_run.yield_next_available_shard_to_dispatch(bot_dimensions)
  ]


def _gen_new_shards_to_run(**kwargs):
  data = _gen_request_data(**kwargs)
  shards = task_shard_to_run.new_shards_to_run_for_request(
      task_request.make_request(data))
  ndb.put_multi(shards)
  return shards


def _hash_dimensions(dimensions):
  return task_shard_to_run._hash_dimensions(utils.encode_to_json(dimensions))


class TaskShardToRunPrivateTest(test_case.TestCase):
  def test_gen_queue_number_key(self):
    # tuples of (input, expected).
    # 2**47 / 365 / 24 / 60 / 60 / 1000. = 4462.756
    data = [
      (('1970-01-01 00:00:00.000',   0,   1), '0x0000000000000001'),
      (('1970-01-01 00:00:00.000', 255,   1), '0x7f80000000000001'),
      (('1970-01-01 00:00:00.000',   0, 255), '0x00000000000000ff'),
      (('1970-01-01 00:00:00.040',   0,   1), '0x0000000000002801'),
      (('1970-01-01 00:00:00.050',   0,   1), '0x0000000000003201'),
      (('1970-01-01 00:00:00.100',   0,   1), '0x0000000000006401'),
      (('1970-01-01 00:00:00.900',   0,   1), '0x0000000000038401'),
      (('1970-01-01 00:00:01.000',   0,   1), '0x000000000003e801'),
      (('1970-01-01 00:00:00.000',   1,   1), '0x0080000000000001'),
      (('1970-01-01 00:00:00.000',   2,   1), '0x0100000000000001'),
      (('2010-01-02 03:04:05.060',   0,   1), '0x000125ecfd5cc401'),
      (('2010-01-02 03:04:05.060',   1,   1), '0x008125ecfd5cc401'),
      # It's the end of the world as we know it...
      (('6429-10-17 02:45:55.327',   0,   1), '0x007fffffffffff01'),
      (('6429-10-17 02:45:55.327', 255,   1), '0x7fffffffffffff01'),
      (('6429-10-17 02:45:55.327', 255, 255), '0x7fffffffffffffff'),
    ]
    for (timestamp, priority, shard_id), expected in data:
      d = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
      actual = '0x%016x' % task_shard_to_run._gen_queue_number_key(
          d, priority, shard_id)
      self.assertEquals(expected, actual)

  def test_powerset(self):
    # tuples of (input, expected).
    # TODO(maruel): We'd want the code to deterministically try 'Windows-6.1'
    # before 'Windows'. Probably do a reverse() on the values?
    data = [
      ({'OS': 'Windows'}, [{'OS': 'Windows'}, {}]),
      (
        {'OS': ['Windows', 'Windows-6.1']},
        [{'OS': 'Windows'}, {'OS': 'Windows-6.1'}, {}],
      ),
      (
        {'OS': ['Windows', 'Windows-6.1'], 'hostname': 'foo'},
        [
          {'OS': 'Windows', 'hostname': 'foo'},
          {'OS': 'Windows-6.1', 'hostname': 'foo'},
          {'OS': 'Windows'},
          {'OS': 'Windows-6.1'},
          {'hostname': 'foo'},
          {},
        ],
      ),
      (
        {'OS': ['Windows', 'Windows-6.1'], 'hostname': 'foo', 'bar': [2, 3]},
        [
         {'OS': 'Windows', 'bar': 2, 'hostname': 'foo'},
          {'OS': 'Windows', 'bar': 3, 'hostname': 'foo'},
          {'OS': 'Windows-6.1', 'bar': 2, 'hostname': 'foo'},
          {'OS': 'Windows-6.1', 'bar': 3, 'hostname': 'foo'},
          {'OS': 'Windows', 'bar': 2},
          {'OS': 'Windows', 'bar': 3},
          {'OS': 'Windows-6.1', 'bar': 2},
          {'OS': 'Windows-6.1', 'bar': 3},
          {'OS': 'Windows', 'hostname': 'foo'},
          {'OS': 'Windows-6.1', 'hostname': 'foo'},
          {'bar': 2, 'hostname': 'foo'},
          {'bar': 3, 'hostname': 'foo'},
          {'OS': 'Windows'},
          {'OS': 'Windows-6.1'},
          {'bar': 2},
          {'bar': 3},
          {'hostname': 'foo'},
          {},
        ],
      ),
    ]
    for inputs, expected in data:
      actual = list(task_shard_to_run._powerset(inputs))
      self.assertEquals(expected, actual)

  def test_hash_dimensions(self):
    dimensions = 'this is not json'
    as_hex = hashlib.md5(dimensions).digest()[:4].encode('hex')
    actual = task_shard_to_run._hash_dimensions(dimensions)
    # It is exactly the same bytes reversed (little endian). It's positive even
    # with bit 31 set because python stores it internally as a int64.
    self.assertEqual('711d0bf1', as_hex)
    self.assertEqual(0xf10b1d71, actual)


class TaskShardToRunApiTest(test_case.TestCase):
  def setUp(self):
    super(TaskShardToRunApiTest, self).setUp()
    self.now = datetime.datetime(2014, 01, 02, 03, 04, 05, 06)
    test_helper.mock_now(self, self.now)
    # The default scheduling_expiration_secs for _gen_request_data().
    self.expiration_ts = self.now + datetime.timedelta(seconds=60)

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(task_shard_to_run)
        if i[0] != '_' and hasattr(getattr(task_shard_to_run, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_request_key_to_shard_to_run_key(self):
    parent = task_request.id_to_request_key(0x100)
    expected = (
        "Key('TaskShardToRunShard', 'd9640', 'TaskShardToRun', 257)")
    self.assertEqual(
        expected,
        str(task_shard_to_run.request_key_to_shard_to_run_key(parent, 1)))
    with self.assertRaises(ValueError):
      task_shard_to_run.request_key_to_shard_to_run_key(parent, 0)
    with self.assertRaises(ValueError):
      task_shard_to_run.request_key_to_shard_to_run_key(parent, 2)
    # The acceptable range is [1, 1] since sharding support is being removed:
    task_shard_to_run.request_key_to_shard_to_run_key(parent, 1)

  def test_shard_id_to_key(self):
    actual = task_shard_to_run.shard_id_to_key(257)
    expected = "Key('TaskShardToRunShard', 'd9640', 'TaskShardToRun', 257)"
    self.assertEqual(expected, str(actual))

  def test_new_shards_to_run_for_request(self):
    self.mock(task_request.random, 'getrandbits', lambda _: 0x88)
    request_dimensions = {u'OS': u'Windows-3.1.1'}
    data = _gen_request_data(
        properties={
          'commands': [[u'command1', u'arg1']],
          'data': [[u'http://localhost/foo', u'foo.zip']],
          'dimensions': request_dimensions,
          'env': {u'foo': u'bar'},
          'execution_timeout_secs': 30,
        },
        scheduling_expiration_secs=31)
    request = task_request.make_request(data)
    shards = task_shard_to_run.new_shards_to_run_for_request(request)
    ndb.put_multi(shards)

    dimensions_hash = _hash_dimensions(request_dimensions)
    expected = [
      {
        'dimensions_hash': dimensions_hash,
        'expiration_ts': self.now + datetime.timedelta(seconds=31),
        'key': '0x014350e868888801',
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]

    def flatten(i):
      out = _convert(i)
      out['key'] = '0x%016x' % i.key.integer_id()
      return out

    # Warning: Ordering by key doesn't work because of TaskShardToRunShard; e.g.
    # the entity key ordering DOES NOT correlate with .queue_number and
    # .shard_number
    # Ensure they come out in expected order.
    q = task_shard_to_run.TaskShardToRun.query().order(
        task_shard_to_run.TaskShardToRun.queue_number)
    actual = [flatten(i) for i in q.fetch()]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_none(self):
    _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    # Bot declares no dimensions, so it will fail to match.
    bot_dimensions = {}
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    self.assertEqual([], actual)

  def test_yield_next_available_shard_to_dispatch_none_mismatch(self):
    _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    # Bot declares other dimensions, so it will fail to match.
    bot_dimensions = {'OS': 'Windows-3.0'}
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    self.assertEqual([], actual)

  def test_yield_next_available_shard_to_dispatch(self):
    request_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = request_dimensions
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_subset(self):
    request_dimensions = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed, this is fine and it matches.
    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_subset_multivalue(self):
    request_dimensions = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed.
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_multi_normal(self):
    # Task added one after the other, normal case.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_1))

    # It's normally time ordered.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=1))
    request_dimensions_2 = {u'hostname': u'localhost'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_2))

    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': self.expiration_ts + datetime.timedelta(seconds=1),
        'queue_number': '0x19014350e86c7001',
        'shard_number': 0,
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_clock_skew(self):
    # Asserts that a TaskShardToRun added later in the DB (with a Key with an
    # higher value) added but with a timestamp sooner (for example, time
    # desynchronization between machines) still returns the items in the
    # timestamp order, e.g. priority is done based on timestamps and priority
    # only.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_1))

    # The second shard is added before the first, potentially because of a
    # desynchronized clock. It'll have higher priority.
    test_helper.mock_now(self, self.now - datetime.timedelta(seconds=1))
    request_dimensions_2 = {u'hostname': u'localhost'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_2))

    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        # Due to time being late on the second requester frontend.
        'expiration_ts': self.expiration_ts - datetime.timedelta(seconds=1),
        'queue_number': '0x19014350e864a001',
        'shard_number': 0,
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_shard_to_dispatch_priority(self):
    # Task added later but with higher priority are returned first.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_1))

    # This one is later but has higher priority.
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=60))
    request_dimensions_2 = {u'OS': u'Windows-3.1.1'}
    _gen_new_shards_to_run(
        properties=dict(dimensions=request_dimensions_2),
        priority=10)

    # It should return them all, in the expected order.
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 6, 5, 6),
        'queue_number': '0x05014350e952e801',
        'shard_number': 0,
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 5, 5, 6),
        'queue_number': '0x19014350e8688801',
        'shard_number': 0,
      },
    ]
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    actual = _yield_next_available_shard_to_dispatch(bot_dimensions)
    self.assertEqual(expected, actual)

  def test_yield_expired_shard_to_run(self):
    _gen_new_shards_to_run(scheduling_expiration_secs=60)
    self.assertEqual(1, len(_yield_next_available_shard_to_dispatch({})))
    self.assertEqual(
        0, len(list(task_shard_to_run.yield_expired_shard_to_run())))

    # All tasks are now expired. Note that even if they still have .queue_number
    # set because the cron job wasn't run, they are still not yielded by
    # yield_next_available_shard_to_dispatch()
    test_helper.mock_now(self, self.now + datetime.timedelta(seconds=61))
    self.assertEqual(0, len(_yield_next_available_shard_to_dispatch({})))
    self.assertEqual(
        1, len(list(task_shard_to_run.yield_expired_shard_to_run())))

  def test_reap_shard_to_run(self):
    shard_0 = _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))[0]
    shard_1 = _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))[0]
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    self.assertEqual(
        2, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

    # A bot is assigned a task shard.
    self.assertEqual(
        True, task_shard_to_run.reap_shard_to_run(shard_0.key))
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

    # This task shard cannot be assigned anymore.
    self.assertEqual(
        False, task_shard_to_run.reap_shard_to_run(shard_0.key))
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))
    self.assertEqual(
        True, task_shard_to_run.reap_shard_to_run(shard_1.key))
    self.assertEqual(
        0, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

  def test_retry_shard_to_run(self):
    shard_0 = _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))[0]
    shard_1 = _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))[0]
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    # Grabs the 2 available shards so no shard is available afterward.
    task_shard_to_run.reap_shard_to_run(shard_0.key)
    task_shard_to_run.reap_shard_to_run(shard_1.key)

    # Calling retry_shard_to_run() puts the shard back as available for
    # dispatch.
    self.assertEqual(
        True, task_shard_to_run.retry_shard_to_run(shard_0.key))
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

    # Can't retry a shard already available to be dispatched to a bot.
    self.assertEqual(
        False, task_shard_to_run.retry_shard_to_run(shard_0.key))
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

    # Retry the second shard too.
    self.assertEqual(
        True, task_shard_to_run.retry_shard_to_run(shard_1.key))
    self.assertEqual(
        2, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

  def test_abort_shard_to_run(self):
    shard_0 = _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))[0]
    _gen_new_shards_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    self.assertEqual(
        2, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))
    task_shard_to_run.abort_shard_to_run(shard_0)
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

    # Aborting an aborted task shard is just fine.
    task_shard_to_run.abort_shard_to_run(shard_0)
    self.assertEqual(
        1, len(_yield_next_available_shard_to_dispatch(bot_dimensions)))

  def test_shard_to_run_key_to_request_key(self):
    request_key = task_request.id_to_request_key(0x100)
    shard_to_run_key = task_shard_to_run.request_key_to_shard_to_run_key(
        request_key, 1)
    key_id = 0x100 + 1
    expected = (
        "Key('TaskShardToRunShard', 'd9640', 'TaskShardToRun', %s)" % key_id)
    self.assertEqual(expected, str(shard_to_run_key))

  def test_shard_to_run_key_to_shard_number(self):
    shard_0 = _gen_new_shards_to_run()[0]
    shard_1 = _gen_new_shards_to_run()[0]
    self.assertEqual(
        0, task_shard_to_run.shard_to_run_key_to_shard_number(shard_0.key))
    self.assertEqual(
        0, task_shard_to_run.shard_to_run_key_to_shard_number(shard_1.key))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
