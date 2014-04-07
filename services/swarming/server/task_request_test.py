#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

import test_case
from server import task_request


def _gen_request_data(**kwargs):
  base_data = {
    'name': 'Request name',
    'user': 'Jesus',
    'commands': [
      [u'command1', u'arg1'],
      [u'command2', u'arg2'],
    ],
    'data': [
      ['http://localhost/foo', 'foo.zip'],
      ['http://localhost/bar', 'bar.zip'],
    ],
    'dimensions':{'OS': 'Windows-3.1.1', 'hostname': 'localhost'},
    'env': {'foo': 'bar', 'joe': '2'},
    'shards': 1,
    'priority': 50,
    'scheduling_expiration': 30,
    'execution_timeout': 30,
    'io_timeout': None,
  }
  base_data.update(kwargs)
  return base_data


class TaskRequestPrivateTest(test_case.TestCase):
  def test_new_task_request_key(self):
    for _ in xrange(3):
      epoch = task_request.utcnow() - task_request._EPOCH
      now = int(round(epoch.total_seconds() * 1000))
      key = task_request._new_task_request_key()
      key_id = key.integer_id()
      timestamp = key_id >> 16
      randomness = key_id & 0xFFFF
      diff = abs(timestamp - now)
      self.assertTrue(diff < 1000, diff)
      if randomness:
        break
    else:
      self.fail('Failed to find randomness')

  def test_new_task_request_key_no_random(self):
    self.mock(task_request.random, 'getrandbits', lambda _: 0x8877)
    now = task_request._EPOCH + datetime.timedelta(seconds=100)
    self.mock(task_request, 'utcnow', lambda: now)
    key = task_request._new_task_request_key()
    self.assertEqual(0x186a08877, key.integer_id())


class TaskRequestApiTest(test_case.TestCase):
  def test_all_apis_are_tested(self):
    actual = set(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = set(
        i for i in dir(task_request)
        if i[0] != '_' and hasattr(getattr(task_request, i), 'func_name'))
    missing = expected - actual - set(['utcnow'])
    self.assertFalse(missing)

  def test_validate_priority(self):
    with self.assertRaises(TypeError):
      task_request.validate_priority('1')
    with self.assertRaises(ValueError):
      task_request.validate_priority(-1)
    with self.assertRaises(ValueError):
      task_request.validate_priority(task_request._MAXIMUM_PRIORITY)
    task_request.validate_priority(0)
    task_request.validate_priority(1)
    task_request.validate_priority(task_request._MAXIMUM_PRIORITY-1)

  def test_task_request_key(self):
    self.assertEqual(
        "Key('TaskRequestShard', 'c4ca4', 'TaskRequest', 1)",
        str(task_request.task_request_key(1)))

  def test_new_request(self):
    deadline_to_run = 31
    data = _gen_request_data(scheduling_expiration=deadline_to_run)
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
      'sharding': 1,
    }
    expected_request = {
      'name': u'Request name',
      'priority': 50,
      'properties': expected_properties,
      'properties_hash': '6ddbca38241daa62e660fb5e9188d04f164d0add',
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
            scheduling_expiration=129))
    self.assertEqual(request_1.properties_hash, request_2.properties_hash)

  def test_different(self):
    # Two TestRequest with different properties.
    request_1 = task_request.new_request(
        _gen_request_data(execution_timeout=30))
    request_2 = task_request.new_request(
        _gen_request_data(execution_timeout=129))
    self.assertNotEqual(request_1.properties_hash, request_2.properties_hash)

  def test_bad_values(self):
    with self.assertRaises(ValueError):
      task_request.new_request(_gen_request_data(commands=None))
    with self.assertRaises(ValueError):
      task_request.new_request(
          _gen_request_data(priority=task_request._MAXIMUM_PRIORITY))
    with self.assertRaises(ValueError):
      task_request.new_request(_gen_request_data(shards=51))
    with self.assertRaises(ValueError):
      task_request.new_request(_gen_request_data(scheduling_expiration=29))

  def test_new_request_old_api(self):
    deadline_to_run = 63
    data = {
      'configurations': [
        {
          'config_name': 'ignored',
          'deadline_to_run': deadline_to_run,
          'dimensions': {
            'OS': 'Windows-3.1.1',
            'hostname': 'localhost',
          },
          'num_instances': 23,
          'priority': 50,
        },
      ],
      'data': [
        ('http://localhost/foo', 'foo.zip'),
        ('http://localhost/bar', 'bar.zip'),
      ],
      'env_vars': {
        'foo': 'bar',
        'joe': '2',
      },
      'requestor': 'Jesus',
      'test_case_name': 'Request name',
      'tests': [
        {
          'action': ['command1', 'arg1'],
          'hard_time_out': 66.1,
          'io_time_out': 68.1,
          'test_name': 'very ignored',
        },
        {
          'action': ['command2', 'arg2'],
          'test_name': 'very ignored but must be different',
          'hard_time_out': 60000000,
          'io_time_out': 60000000,
        },
      ],
    }
    request = task_request.new_request_old_api(
        json.dumps(data))
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
      'execution_timeout_secs': 66,
      'io_timeout_secs': 68,
      'sharding': 23,
    }
    expected_request = {
      'name': u'Request name',
      'priority': 50,
      'properties': expected_properties,
      'properties_hash': 'c80f64ebd5f08f14738209f9456c1e4277b356be',
      'user': u'Jesus',
    }
    actual = request.to_dict()
    # expiration_ts - created_ts == deadline_to_run.
    created = actual.pop('created_ts')
    expiration = actual.pop('expiration_ts')
    self.assertEqual(
        int(round((expiration - created).total_seconds())), deadline_to_run)
    self.assertEqual(expected_request, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
