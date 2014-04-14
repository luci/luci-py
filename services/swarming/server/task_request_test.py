#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

import test_case
from server import task_common
from server import task_request


def _gen_request_data(properties=None, **kwargs):
  base_data = {
    'name': 'Request name',
    'user': 'Jesus',
    'properties': {
      'commands': [
        ['command1', 'arg1'],
        ['command2', 'arg2'],
      ],
      'data': [
        ['http://localhost/foo', 'foo.zip'],
        ['http://localhost/bar', 'bar.zip'],
      ],
      'dimensions': {'OS': 'Windows-3.1.1', 'hostname': 'localhost'},
      'env': {'foo': 'bar', 'joe': '2'},
      'number_shards': 1,
      'execution_timeout_secs': 30,
      'io_timeout_secs': None,
    },
    'priority': 50,
    'scheduling_expiration_secs': 30,
  }
  base_data.update(kwargs)
  base_data['properties'].update(properties or {})
  return base_data


class TaskRequestPrivateTest(test_case.TestCase):
  def test_new_request_key(self):
    for _ in xrange(3):
      now = task_common.milliseconds_since_epoch(None)
      key = task_request._new_request_key()
      key_id = key.integer_id()
      timestamp = key_id >> 16
      randomness = key_id & 0xFFFF
      diff = abs(timestamp - now)
      self.assertTrue(diff < 1000, diff)
      if randomness:
        break
    else:
      self.fail('Failed to find randomness')

  def test_new_request_key_no_random(self):
    def getrandbits(i):
      self.assertEqual(i, 8)
      return 0x77
    self.mock(task_request.random, 'getrandbits', getrandbits)
    days_until_end_of_the_world = 2**47 / 24. / 60. / 60. / 1000.
    num_days = int(days_until_end_of_the_world)
    # Remove 1ms to not overflow.
    num_seconds = (
        (days_until_end_of_the_world - num_days) * 24. * 60. * 60. - 0.001)
    self.assertEqual(1628906, num_days)
    now = task_common.UNIX_EPOCH + datetime.timedelta(
        days=num_days, seconds=num_seconds)
    self.mock(task_common, 'utcnow', lambda: now)
    key = task_request._new_request_key()
    # Last 0x00 is reserved for shard numbers.
    # Next to last 0x77 is the random bits.
    self.assertEqual('0x7fffffffffff7700', '0x%016x' % key.integer_id())


class TaskRequestApiTest(test_case.TestCase):
  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    module = task_request
    expected = frozenset(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - frozenset(
        i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing)

  def test_id_to_request_key(self):
    self.assertEqual(
        "Key('TaskRequestShard', 'f7184', 'TaskRequest', 256)",
        str(task_request.id_to_request_key(0x100)))
    with self.assertRaises(ValueError):
      task_request.id_to_request_key(1)

  def test_new_request(self):
    deadline_to_run = 31
    data = _gen_request_data(scheduling_expiration_secs=deadline_to_run)
    request = task_request.new_request(data)
    expected_properties = {
      'commands': [
        [u'command1', u'arg1'],
        [u'command2', u'arg2'],
      ],
      'data': [
        # Items were sorted.
        [u'http://localhost/bar', u'bar.zip'],
        [u'http://localhost/foo', u'foo.zip'],
      ],
      'dimensions': {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'},
      'env': {u'foo': u'bar', u'joe': u'2'},
      'execution_timeout_secs': 30,
      'io_timeout_secs': None,
      'number_shards': 1,
    }
    expected_request = {
      'name': u'Request name',
      'priority': 50,
      'properties': expected_properties,
      'properties_hash': '6d049340771e2197a4ab3f7202bb569e5fa248c3',
      'user': u'Jesus',
    }
    actual = request.to_dict()
    # expiration_ts - created_ts == deadline_to_run.
    created = actual.pop('created_ts')
    expiration = actual.pop('expiration_ts')
    self.assertEqual(
        int(round((expiration - created).total_seconds())), deadline_to_run)
    self.assertEqual(expected_request, actual)

  def test_duped(self):
    # Two TestRequest with the same properties.
    request_1 = task_request.new_request(_gen_request_data())
    request_2 = task_request.new_request(
        _gen_request_data(
            name='Other', user='Other', priority=201,
            scheduling_expiration_secs=129))
    self.assertEqual(request_1.properties_hash, request_2.properties_hash)

  def test_different(self):
    # Two TestRequest with different properties.
    request_1 = task_request.new_request(
        _gen_request_data(properties=dict(execution_timeout_secs=30)))
    request_2 = task_request.new_request(
        _gen_request_data(properties=dict(execution_timeout_secs=129)))
    self.assertNotEqual(request_1.properties_hash, request_2.properties_hash)

  def test_bad_values(self):
    # commands are required and must be a list.
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(properties=dict(commands=None)))
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(properties=dict(commands=[])))

    # .priority
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(priority=task_common.MAXIMUM_PRIORITY+1))
    task_request.new_request(
        _gen_request_data(priority=task_common.MAXIMUM_PRIORITY))

    # .number_shards
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(
              properties=dict(number_shards=task_common.MAXIMUM_SHARDS+1)))
    task_request.new_request(
        _gen_request_data(
            properties=dict(number_shards=task_common.MAXIMUM_SHARDS)))

    # .scheduling_expiration_secs
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(
              scheduling_expiration_secs=task_request._MIN_TIMEOUT_SECS-1))
    task_request.new_request(
        _gen_request_data(
            scheduling_expiration_secs=task_request._MIN_TIMEOUT_SECS))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
