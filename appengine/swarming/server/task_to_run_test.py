#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import hashlib
import os
import random
import sys
import timeit
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import auth_testing
from components import utils
from test_support import test_case

from server import task_request
from server import task_to_run


# pylint: disable=W0212
# Method could be a function - pylint: disable=R0201


def _gen_request(properties=None, **kwargs):
  """Creates a TaskRequest."""
  props = {
    'commands': [[u'command1']],
    'data': [],
    'dimensions': {},
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


def _yield_next_available_task_to_dispatch(bot_dimensions):
  return [
    _task_to_run_to_dict(to_run)
    for _request, to_run in
        task_to_run.yield_next_available_task_to_dispatch(bot_dimensions)
  ]


def _gen_new_task_to_run(**kwargs):
  """Returns a TaskToRun saved in the DB."""
  request = task_request.make_request(_gen_request(**kwargs), True)
  to_run = task_to_run.new_task_to_run(request)
  to_run.put()
  return to_run


def _hash_dimensions(dimensions):
  return task_to_run._hash_dimensions(utils.encode_to_json(dimensions))


class TestCase(test_case.TestCase):
  def setUp(self):
    super(TestCase, self).setUp()
    auth_testing.mock_get_current_identity(self)


class TaskToRunPrivateTest(TestCase):
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
      actual = list(task_to_run._powerset(inputs))
      self.assertEquals(expected, actual)

  def test_timeit_generation(self):
    # The hash table generation is done once per poll request, so it's a cost
    # latency wise and CPU wise.
    setup = (
      "import task_to_run\n"
      "from components import utils\n"
      # Test with 1024 combinations.
      "dimensions = {str(k): '01234567890123456789' for k in xrange(10)}")

    statement = (
      "items = tuple(sorted("
      "  task_to_run._hash_dimensions(utils.encode_to_json(i))"
      "  for i in task_to_run._powerset(dimensions)))\n"
      "del items")
    _perf_tuple = timeit.timeit(statement, setup, number=10)

    statement = (
      "items = frozenset("
      "  task_to_run._hash_dimensions(utils.encode_to_json(i))"
      "  for i in task_to_run._powerset(dimensions))\n"
      "del items")
    _perf_frozenset = timeit.timeit(statement, setup, number=10)

    # Creating the frozenset is fairly consistently faster by ~5%. Because the
    # difference is so low, the following assert could fail under load because
    # timeit is not very accurate.
    #self.assertGreater(perf_tuple, perf_frozenset)
    # For reference, numbers locally were: tuple: 0.6295s  frozenset:  0.5753s
    # Enable to get actual numbers on your workstation:
    #print('\ntuple: %.4fs  frozenset: %.4fs' % (perf_tuple, perf_frozenset))

  def test_timeit_lookup(self):
    # Lookups are done much more often than generation under normal utilization.
    # That's what we want to optimize for.
    setup = (
      "import task_to_run\n"
      "from components import utils\n"
      # Test with 1024 combinations.
      "dimensions = {str(k): '01234567890123456789' for k in xrange(10)}\n"
      "items = tuple(sorted("
      "  task_to_run._hash_dimensions(utils.encode_to_json(i))"
      "  for i in task_to_run._powerset(dimensions)))\n")

    statement = (
      # Simulating 10000 pending tasks. Normally, a single poll will not search
      # this many items, simply because the DB is not fast enough.
      "for _ in xrange(10000):"
      "  1 in items")
    # TODO(maruel): This is a linear search instead of a binary search, so this
    # test is a tad unfair. But it's very unlikely that even a binary search can
    # beat the frozenset.
    perf_tuple = timeit.timeit(statement, setup, number=10)
    setup = (
      "import task_to_run\n"
      "from components import utils\n"
      # Test with 1024 combinations.
      "dimensions = {str(k): '01234567890123456789' for k in xrange(10)}\n"
      "items = frozenset("
      "  task_to_run._hash_dimensions(utils.encode_to_json(i))"
      "  for i in task_to_run._powerset(dimensions))\n")
    perf_frozenset = timeit.timeit(statement, setup, number=10)

    # Creating the frozenset is fairly consistently faster by 333% (really).
    self.assertGreater(perf_tuple, perf_frozenset)
    # For reference, numbers locally were: tuple: 1.4612s  frozenset: 0.0043s
    # Enable to get actual numbers on your workstation:
    #print('\ntuple: %.4fs  frozenset: %.4fs' % (perf_tuple, perf_frozenset))

  def test_hash_dimensions(self):
    dimensions = 'this is not json'
    as_hex = hashlib.md5(dimensions).digest()[:4].encode('hex')
    actual = task_to_run._hash_dimensions(dimensions)
    # It is exactly the same bytes reversed (little endian). It's positive even
    # with bit 31 set because python stores it internally as a int64.
    self.assertEqual('711d0bf1', as_hex)
    self.assertEqual(0xf10b1d71, actual)

  def test_dimensions_search_sizing_10_1(self):
    dimensions = {str(k): '01234567890123456789' for k in xrange(10)}
    items = tuple(sorted(
        task_to_run._hash_dimensions(utils.encode_to_json(i))
        for i in task_to_run._powerset(dimensions)))
    self.assertEqual(1024, len(items))

  def test_dimensions_search_sizing_1_20(self):
    # Multi-value dimensions must *always* be prefered to split variables. They
    # are much quicker to search.
    dimensions = {'0': ['01234567890123456789' * i for i in xrange(1, 20)]}
    items = tuple(sorted(
        task_to_run._hash_dimensions(utils.encode_to_json(i))
        for i in task_to_run._powerset(dimensions)))
    self.assertEqual(20, len(items))

  def test_dimensions_search_sizing_7_4(self):
    # Likely maximum permitted; 7 keys of 4 items each.
    dimensions = {
      str(k): ['01234567890123456789' * i for i in xrange(1, 4)]
      for k in xrange(7)
    }
    items = tuple(sorted(
        task_to_run._hash_dimensions(utils.encode_to_json(i))
        for i in task_to_run._powerset(dimensions)))
    self.assertEqual(16384, len(items))

  def test_dimensions_search_sizing_14_1(self):
    dimensions = {str(k): '01234567890123456789' for k in xrange(14)}
    items = tuple(sorted(
        task_to_run._hash_dimensions(utils.encode_to_json(i))
        for i in task_to_run._powerset(dimensions)))
    self.assertEqual(16384, len(items))


class TaskToRunApiTest(TestCase):
  def setUp(self):
    super(TaskToRunApiTest, self).setUp()
    self.now = datetime.datetime(2014, 01, 02, 03, 04, 05, 06)
    self.mock_now(self.now)
    # The default expiration_secs for _gen_request().
    self.expiration_ts = self.now + datetime.timedelta(seconds=60)

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(task_to_run)
        if i[0] != '_' and hasattr(getattr(task_to_run, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_task_to_run_key_to_request_key(self):
    request = task_request.make_request(_gen_request(), True)
    task_key = task_to_run.request_to_task_to_run_key(request)
    actual = task_to_run.task_to_run_key_to_request_key(task_key)
    self.assertEqual(request.key, actual)

  def test_request_to_task_to_run_key(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    request = task_request.make_request(_gen_request(), True)
    self.assertEqual(
        ndb.Key('TaskRequest', 0x7e296460f77ff77e, 'TaskToRun', 2471203225),
        task_to_run.request_to_task_to_run_key(request))

  def test_validate_to_run_key(self):
    request = task_request.make_request(_gen_request(), True)
    task_key = task_to_run.request_to_task_to_run_key(request)
    task_to_run.validate_to_run_key(task_key)
    with self.assertRaises(ValueError):
      task_to_run.validate_to_run_key(ndb.Key('TaskRequest', 1, 'TaskToRun', 1))

  def test_gen_queue_number(self):
    # tuples of (input, expected).
    # 2**47 / 365 / 24 / 60 / 60 / 1000. = 4462.756
    data = [
      (('1970-01-01 00:00:00.000',   0), '0x0000000000000000'),
      (('1970-01-01 00:00:00.000', 255), '0x001c91dd87cc2000'),
      (('1970-01-01 00:00:00.040',   0), '0x0000000000009c40'),
      (('1970-01-01 00:00:00.050',   0), '0x000000000000c350'),
      (('1970-01-01 00:00:00.100',   0), '0x00000000000186a0'),
      (('1970-01-01 00:00:00.900',   0), '0x00000000000dbba0'),
      (('1970-01-01 00:00:01.000',   0), '0x00000000000f4240'),
      (('1970-01-01 00:00:00.000',   1), '0x00001cae8c13e000'),
      (('1970-01-01 00:00:00.000',   2), '0x0000395d1827c000'),
      (('2010-01-02 03:04:05.060',   0), '0x00047c25bdb25da0'),
      (('2010-01-02 03:04:05.060',   1), '0x000498d449c63da0'),
      # It's the end of the world as we know it...
      (('9999-12-31 23:59:59.999',   0), '0x0384440ccc735c20'),
      (('9999-12-31 23:59:59.999', 255), '0x03a0d5ea543f7c20'),
      (('9999-12-31 23:59:59.999', 255), '0x03a0d5ea543f7c20'),
    ]
    for i, ((timestamp, priority), expected) in enumerate(data):
      d = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
      actual = '0x%016x' % task_to_run._gen_queue_number(d, priority)
      self.assertEquals((i, expected), (i, actual))

  def test_new_task_to_run(self):
    self.mock(random, 'getrandbits', lambda _: 0x12)
    request_dimensions = {u'OS': u'Windows-3.1.1'}
    now = utils.utcnow()
    data = _gen_request(
        properties={
          'commands': [[u'command1', u'arg1']],
          'data': [[u'http://localhost/foo', u'foo.zip']],
          'dimensions': request_dimensions,
          'env': {u'foo': u'bar'},
          'execution_timeout_secs': 30,
        },
        priority=20,
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=31))
    task_to_run.new_task_to_run(task_request.make_request(data, True)).put()

    # Create a second with higher priority.
    self.mock(random, 'getrandbits', lambda _: 0x23)
    data = _gen_request(
        properties={
          'commands': [[u'command1', u'arg1']],
          'data': [[u'http://localhost/foo', u'foo.zip']],
          'dimensions': request_dimensions,
          'env': {u'foo': u'bar'},
          'execution_timeout_secs': 30,
        },
        priority=10,
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=31))
    task_to_run.new_task_to_run(task_request.make_request(data, True)).put()

    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.now + datetime.timedelta(seconds=31),
        'request_key': '0x7e296460f77ffdce',
        # Lower priority value means higher priority.
        'queue_number': '0x00060dc5849f1346',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.now + datetime.timedelta(seconds=31),
        'request_key': '0x7e296460f77ffede',
        'queue_number': '0x00072c96fd65d346',
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

  def test_dimensions_powerset_count(self):
    dimensions = {
      'a': ['1', '2'],
      'b': 'code',
      'd': ['3', '4'],
    }
    self.assertEqual(
        task_to_run.dimensions_powerset_count(dimensions),
        len(list(task_to_run._powerset(dimensions))))

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
    _gen_new_task_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    # Bot declares no dimensions, so it will fail to match.
    bot_dimensions = {}
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    self.assertEqual([], actual)

  def test_yield_next_available_task_to_dispatch_none_mismatch(self):
    _gen_new_task_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    # Bot declares other dimensions, so it will fail to match.
    bot_dimensions = {'OS': 'Windows-3.0'}
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    self.assertEqual([], actual)

  def test_yield_next_available_task_to_dispatch(self):
    request_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    _gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares exactly same dimensions so it matches.
    bot_dimensions = request_dimensions
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_subset(self):
    request_dimensions = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed, this is fine and it matches.
    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_shard(self):
    request_dimensions = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions))
    bot_dimensions = request_dimensions
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_subset_multivalue(self):
    request_dimensions = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions))
    # Bot declares more dimensions than needed.
    bot_dimensions = {
      u'OS': [u'Windows', u'Windows-3.1.1'],
      u'hostname': u'localhost',
      u'foo': u'bar',
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_multi_normal(self):
    # Task added one after the other, normal case.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions_1))

    # It's normally time ordered.
    self.mock_now(self.now, 1)
    request_dimensions_2 = {u'hostname': u'localhost'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions_2))

    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': self.expiration_ts + datetime.timedelta(seconds=1),
        'queue_number': '0x000a890b67c95586',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_clock_skew(self):
    # Asserts that a TaskToRun added later in the DB (with a Key with an higher
    # value) but with a timestamp sooner (for example, time desynchronization
    # between machines) is still returned in the timestamp order, e.g. priority
    # is done based on timestamps and priority only.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1', u'foo': u'bar'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions_1))

    # The second shard is added before the first, potentially because of a
    # desynchronized clock. It'll have higher priority.
    self.mock_now(self.now, -1)
    request_dimensions_2 = {u'hostname': u'localhost'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions_2))

    bot_dimensions = {
      u'OS': u'Windows-3.1.1', u'hostname': u'localhost', u'foo': u'bar',
    }
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        # Due to time being late on the second requester frontend.
        'expiration_ts': self.expiration_ts - datetime.timedelta(seconds=1),
        'queue_number': '0x000a890b67aad106',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': self.expiration_ts,
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    self.assertEqual(expected, actual)

  def test_yield_next_available_task_to_dispatch_priority(self):
    # Task added later but with higher priority are returned first.
    request_dimensions_1 = {u'OS': u'Windows-3.1.1'}
    _gen_new_task_to_run(properties=dict(dimensions=request_dimensions_1))

    # This one is later but has higher priority.
    self.mock_now(self.now, 60)
    request_dimensions_2 = {u'OS': u'Windows-3.1.1'}
    _gen_new_task_to_run(
        properties=dict(dimensions=request_dimensions_2), priority=10)

    # It should return them all, in the expected order.
    expected = [
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_1),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 6, 5, 6),
        'queue_number': '0x00060dc588329a46',
      },
      {
        'dimensions_hash': _hash_dimensions(request_dimensions_2),
        'expiration_ts': datetime.datetime(2014, 1, 2, 3, 5, 5, 6),
        'queue_number': '0x000a890b67ba1346',
      },
    ]
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    actual = _yield_next_available_task_to_dispatch(bot_dimensions)
    self.assertEqual(expected, actual)

  def test_yield_expired_task_to_run(self):
    now = utils.utcnow()
    _gen_new_task_to_run(
        created_ts=now,
        expiration_ts=now+datetime.timedelta(seconds=60))
    self.assertEqual(1, len(_yield_next_available_task_to_dispatch({})))
    self.assertEqual(
        0, len(list(task_to_run.yield_expired_task_to_run())))

    # All tasks are now expired. Note that even if they still have .queue_number
    # set because the cron job wasn't run, they are still not yielded by
    # yield_next_available_task_to_dispatch()
    self.mock_now(self.now, 61)
    self.assertEqual(0, len(_yield_next_available_task_to_dispatch({})))
    self.assertEqual(
        1, len(list(task_to_run.yield_expired_task_to_run())))

  def test_is_reapable(self):
    req_dimensions = {u'OS': u'Windows-3.1.1'}
    to_run = _gen_new_task_to_run(properties=dict(dimensions=req_dimensions))
    bot_dimensions = {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'}
    self.assertEqual(
        1, len(_yield_next_available_task_to_dispatch(bot_dimensions)))

    self.assertEqual(True, to_run.is_reapable)
    to_run.queue_number = None
    to_run.put()
    self.assertEqual(False, to_run.is_reapable)

  def test_set_lookup_cache(self):
    to_run = _gen_new_task_to_run(
        properties=dict(dimensions={u'OS': u'Windows-3.1.1'}))
    self.assertEqual(False, task_to_run._lookup_cache_is_taken(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, True)
    self.assertEqual(False, task_to_run._lookup_cache_is_taken(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, False)
    self.assertEqual(True, task_to_run._lookup_cache_is_taken(to_run.key))
    task_to_run.set_lookup_cache(to_run.key, True)
    self.assertEqual(False, task_to_run._lookup_cache_is_taken(to_run.key))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
