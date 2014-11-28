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

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import utils
from server import task_request
from support import test_case

# pylint: disable=W0212


def _gen_request_data(properties=None, **kwargs):
  base_data = {
    'name': 'Request name',
    'user': 'Jesus',
    'properties': {
      'commands': [
        [u'command1', u'arg1'],
        [u'command2', u'arg2'],
      ],
      'data': [
        [u'http://localhost/foo', u'foo.zip'],
        [u'http://localhost/bar', u'bar.zip'],
      ],
      'dimensions': {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'},
      'env': {u'foo': u'bar', u'joe': u'2'},
      'execution_timeout_secs': 30,
      'idempotent': False,
      'io_timeout_secs': None,
    },
    'priority': 50,
    'scheduling_expiration_secs': 30,
    'tags': [u'tag:1'],
  }
  base_data.update(kwargs)
  base_data['properties'].update(properties or {})
  return base_data


class TaskRequestPrivateTest(test_case.TestCase):
  def test_new_request_key(self):
    for _ in xrange(3):
      delta = utils.utcnow() - task_request._BEGINING_OF_THE_WORLD
      now = int(round(delta.total_seconds() * 1000.))
      key = task_request._new_request_key()
      # Remove the XOR.
      key_id = key.integer_id() ^ task_request._TASK_REQUEST_KEY_ID_MASK
      timestamp = key_id >> 20
      randomness = (key_id >> 4) & 0xFFFF
      version = key_id & 0xF
      self.assertLess(abs(timestamp - now), 1000)
      self.assertEqual(1, version)
      if randomness:
        break
    else:
      self.fail('Failed to find randomness')

  def test_new_request_key_zero(self):
    def getrandbits(i):
      self.assertEqual(i, 16)
      return 0x7766
    self.mock(random, 'getrandbits', getrandbits)
    self.mock_now(task_request._BEGINING_OF_THE_WORLD)
    key = task_request._new_request_key()
    # Remove the XOR.
    key_id = key.integer_id() ^ task_request._TASK_REQUEST_KEY_ID_MASK
    #   00000000000 7766 1
    #     ^          ^   ^
    #     |          |   |
    #  since 2010    | schema version
    #                |
    #               rand
    self.assertEqual('0x0000000000077661', '0x%016x' % key_id)

  def test_new_request_key_end(self):
    def getrandbits(i):
      self.assertEqual(i, 16)
      return 0x7766
    self.mock(random, 'getrandbits', getrandbits)
    days_until_end_of_the_world = 2**43 / 24. / 60. / 60. / 1000.
    num_days = int(days_until_end_of_the_world)
    # Remove 1ms to not overflow.
    num_seconds = (
        (days_until_end_of_the_world - num_days) * 24. * 60. * 60. - 0.001)
    self.assertEqual(101806, num_days)
    self.assertEqual(278, int(num_days / 365.3))
    now = (task_request._BEGINING_OF_THE_WORLD +
        datetime.timedelta(days=num_days, seconds=num_seconds))
    self.mock_now(now)
    key = task_request._new_request_key()
    # Remove the XOR.
    key_id = key.integer_id() ^ task_request._TASK_REQUEST_KEY_ID_MASK
    #   7ffffffffff 7766 1
    #     ^          ^   ^
    #     |          |   |
    #  since 2010    | schema version
    #                |
    #               rand
    self.assertEqual('0x7ffffffffff77661', '0x%016x' % key_id)


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

  def test_request_id_to_key(self):
    # Old style keys.
    self.assertEqual(
        ndb.Key('TaskRequestShard', 'f71849', 'TaskRequest', 256),
        task_request.request_id_to_key('10'))
    # New style key.
    self.assertEqual(
        ndb.Key('TaskRequest', 0x7fffffffffffffee),
        task_request.request_id_to_key('11'))
    with self.assertRaises(ValueError):
      task_request.request_id_to_key('2')

  def test_request_key_to_id(self):
    # Old style keys.
    self.assertEqual(
        '10',
       task_request.request_key_to_id(
           ndb.Key('TaskRequestShard', 'f71849', 'TaskRequest', 256)))
    # New style key.
    self.assertEqual(
        '11',
       task_request.request_key_to_id(
           ndb.Key('TaskRequest', 0x7fffffffffffffee)))

  def test_validate_request_key(self):
    task_request.validate_request_key(task_request.request_id_to_key('10'))
    task_request.validate_request_key(
        ndb.Key(
            'TaskRequestShard', 'a' * task_request._SHARDING_LEVEL,
            'TaskRequest', 0x100))
    with self.assertRaises(ValueError):
      task_request.validate_request_key(ndb.Key('TaskRequest', 1))
    with self.assertRaises(ValueError):
      task_request.validate_request_key(
          ndb.Key(
              'TaskRequestShard', 'a' * (task_request._SHARDING_LEVEL + 1),
              'TaskRequest', 0x100))

  def test_make_request(self):
    deadline_to_run = 31
    data = _gen_request_data(scheduling_expiration_secs=deadline_to_run)
    request = task_request.make_request(data)
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
      'idempotent': False,
      'io_timeout_secs': None,
    }
    expected_request = {
      'name': u'Request name',
      'priority': 50,
      'properties': expected_properties,
      'properties_hash': None,
      'user': u'Jesus',
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'priority:50',
        u'tag:1',
        u'user:Jesus',
      ],
    }
    actual = request.to_dict()
    # expiration_ts - created_ts == deadline_to_run.
    created = actual.pop('created_ts')
    expiration = actual.pop('expiration_ts')
    self.assertEqual(
        int(round((expiration - created).total_seconds())), deadline_to_run)
    self.assertEqual(expected_request, actual)
    self.assertEqual(31., request.scheduling_expiration_secs)

  def test_make_request_idempotent(self):
    request = task_request.make_request(
        _gen_request_data(properties=dict(idempotent=True)))
    as_dict = request.to_dict()
    self.assertEqual(True, as_dict['properties']['idempotent'])
    # Ensure the algorithm is deterministic.
    self.assertEqual(
        '264479359746dd42a6c7154af1bc244061f63170', as_dict['properties_hash'])

  def test_duped(self):
    # Two TestRequest with the same properties.
    request_1 = task_request.make_request(
        _gen_request_data(properties=dict(idempotent=True)))
    request_2 = task_request.make_request(
        _gen_request_data(
            name='Other', user='Other', priority=201,
            scheduling_expiration_secs=129, tags=['tag:2'],
            properties=dict(idempotent=True)))
    self.assertEqual(
        request_1.properties.properties_hash,
        request_2.properties.properties_hash)
    self.assertTrue(request_1.properties.properties_hash)

  def test_different(self):
    # Two TestRequest with different properties.
    request_1 = task_request.make_request(
        _gen_request_data(
          properties=dict(execution_timeout_secs=30, idempotent=True)))
    request_2 = task_request.make_request(
        _gen_request_data(
          properties=dict(execution_timeout_secs=129, idempotent=True)))
    self.assertNotEqual(
        request_1.properties.properties_hash,
        request_2.properties.properties_hash)

  def test_bad_values(self):
    with self.assertRaises(ValueError):
      task_request.make_request({})
    with self.assertRaises(ValueError):
      task_request.make_request(_gen_request_data(properties={'foo': 'bar'}))
    task_request.make_request(_gen_request_data())

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request_data(properties=dict(commands=None)))
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request_data(properties=dict(commands=[])))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(commands={})))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(commands=['python'])))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(commands=[['python']])))
    task_request.make_request(
        _gen_request_data(properties=dict(commands=[[u'python']])))

    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(env=[])))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(env={u'a': 1})))
    task_request.make_request(_gen_request_data(properties=dict(env={})))

    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(data=[['a',]])))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(data=[('a', '1')])))
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request_data(properties=dict(data=[(u'a', u'1')])))
    task_request.make_request(
        _gen_request_data(properties=dict(data=[[u'a', u'1']])))

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request_data(priority=task_request.MAXIMUM_PRIORITY+1))
    task_request.make_request(
        _gen_request_data(priority=task_request.MAXIMUM_PRIORITY))

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request_data(
              properties=dict(
                  execution_timeout_secs=task_request._ONE_DAY_SECS+1)))
    task_request.make_request(
        _gen_request_data(
            properties=dict(execution_timeout_secs=task_request._ONE_DAY_SECS)))

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request_data(
              scheduling_expiration_secs=task_request._MIN_TIMEOUT_SECS-1))
    task_request.make_request(
        _gen_request_data(
            scheduling_expiration_secs=task_request._MIN_TIMEOUT_SECS))

  def test_validate_priority(self):
    with self.assertRaises(TypeError):
      task_request.validate_priority('1')
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.validate_priority(-1)
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.validate_priority(task_request.MAXIMUM_PRIORITY+1)
    task_request.validate_priority(0)
    task_request.validate_priority(1)
    task_request.validate_priority(task_request.MAXIMUM_PRIORITY)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
