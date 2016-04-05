#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import random
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import auth_testing
from components import utils
from test_support import test_case

from server import task_pack
from server import task_request


# pylint: disable=W0212


def _gen_request(properties=None, **kwargs):
  """Creates a TaskRequest."""
  props = {
    'commands': [[u'command1', u'arg1']],
    'dimensions': {
      u'OS': u'Windows-3.1.1',
      u'hostname': u'localhost',
      u'pool': u'default',
    },
    'env': {u'foo': u'bar', u'joe': u'2'},
    'execution_timeout_secs': 30,
    'grace_period_secs': 30,
    'idempotent': False,
    'io_timeout_secs': None,
  }
  props.update(properties or {})
  now = utils.utcnow()
  args = {
    'created_ts': now,
    'name': 'Request name',
    'priority': 50,
    'properties': task_request.TaskProperties(**props),
    'expiration_ts': now + datetime.timedelta(seconds=30),
    'tags': [u'tag:1'],
    'user': 'Jesus',
  }
  args.update(kwargs)
  return task_request.TaskRequest(**args)


class Prop(object):
  _name = 'foo'


class TestCase(test_case.TestCase):
  def setUp(self):
    super(TestCase, self).setUp()
    auth_testing.mock_get_current_identity(self)


class TaskRequestPrivateTest(TestCase):
  def test_new_request_key(self):
    for _ in xrange(3):
      delta = utils.utcnow() - task_request._BEGINING_OF_THE_WORLD
      now = int(round(delta.total_seconds() * 1000.))
      key = task_request._new_request_key()
      # Remove the XOR.
      key_id = key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
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
    key_id = key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
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
    key_id = key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
    #   7ffffffffff 7766 1
    #     ^          ^   ^
    #     |          |   |
    #  since 2010    | schema version
    #                |
    #               rand
    self.assertEqual('0x7ffffffffff77661', '0x%016x' % key_id)

  def test_validate_task_run_id(self):
    self.assertEqual(
        '1d69b9f088008811',
        task_request._validate_task_run_id(Prop(), '1d69b9f088008811'))
    self.assertEqual(None, task_request._validate_task_run_id(Prop(), ''))
    with self.assertRaises(ValueError):
      task_request._validate_task_run_id(Prop(), '1')

  def test_validate_command(self):
    task_request._validate_command(Prop(), [[u'a', u'b']])
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_command(Prop(), [[u'a', u'b'], [u'c']])

  def test_validate_isolated(self):
    task_request._validate_isolated(
        Prop(), '0123456789012345678901234567890123456789')
    task_request._validate_isolated(
        Prop(), '0123456789012345678901234567890123abcdef')
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), '123456789012345678901234567890123456789')
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), 'g123456789012345678901234567890123456789')


class TaskRequestApiTest(TestCase):
  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    module = task_request
    expected = frozenset(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - frozenset(
        i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing)

  def test_validate_request_key(self):
    task_request.validate_request_key(task_pack.unpack_request_key('10'))
    task_request.validate_request_key(
        ndb.Key(
            'TaskRequestShard', 'a' * task_pack.DEPRECATED_SHARDING_LEVEL,
            'TaskRequest', 0x100))
    with self.assertRaises(ValueError):
      task_request.validate_request_key(ndb.Key('TaskRequest', 1))
    with self.assertRaises(ValueError):
      key = ndb.Key(
          'TaskRequestShard', 'a' * (task_pack.DEPRECATED_SHARDING_LEVEL + 1),
          'TaskRequest', 0x100)
      task_request.validate_request_key(key)

  def test_make_request(self):
    # Compare with test_make_request_clone().
    parent = task_request.make_request(_gen_request(), True)
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    r = _gen_request(
        properties=dict(idempotent=True), parent_task_id=parent_id)
    request = task_request.make_request(r, True)
    expected_properties = {
      'commands': [[u'command1', u'arg1']],
      'dimensions': {
        u'OS': u'Windows-3.1.1',
        u'hostname': u'localhost',
        u'pool': u'default',
      },
      'env': {u'foo': u'bar', u'joe': u'2'},
      'extra_args': [],
      'execution_timeout_secs': 30,
      'grace_period_secs': 30,
      'idempotent': True,
      'inputs_ref': None,
      'io_timeout_secs': None,
    }
    expected_request = {
      'authenticated': auth_testing.DEFAULT_MOCKED_IDENTITY,
      'name': u'Request name',
      'parent_task_id': unicode(parent_id),
      'priority': 49,
      'properties': expected_properties,
      # Intentionally hard code the hash value since it has to be deterministic.
      # Other unit tests should use the calculated value.
      'properties_hash': 'cad6addd80cb545042968d267f7561c87323b45a',
      'pubsub_topic': None,
      'pubsub_userdata': None,
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'pool:default',
        u'priority:49',
        u'tag:1',
        u'user:Jesus',
      ],
      'user': u'Jesus',
    }
    actual = request.to_dict()
    actual.pop('created_ts')
    actual.pop('expiration_ts')
    self.assertEqual(expected_request, actual)
    self.assertEqual(30, request.expiration_secs)

  def test_make_request_isolated(self):
    parent = task_request.make_request(
        _gen_request(
            properties={
              'commands': None,
              'inputs_ref': {
                'isolated': '0123456789012345678901234567890123456789',
                'isolatedserver': 'http://localhost:1',
                'namespace': 'default-gzip',
              },
            }),
        True)
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    request = task_request.make_request(
        _gen_request(properties={'idempotent':True}, parent_task_id=parent_id),
        True)
    expected_properties = {
      'commands': [[u'command1', u'arg1']],
      'dimensions': {
        u'OS': u'Windows-3.1.1',
        u'hostname': u'localhost',
        u'pool': u'default',
      },
      'env': {u'foo': u'bar', u'joe': u'2'},
      'extra_args': [],
      'execution_timeout_secs': 30,
      'grace_period_secs': 30,
      'idempotent': True,
      'inputs_ref': None,
      'io_timeout_secs': None,
    }
    expected_request = {
      'authenticated': auth_testing.DEFAULT_MOCKED_IDENTITY,
      'name': u'Request name',
      'parent_task_id': unicode(parent_id),
      'priority': 49,
      'properties': expected_properties,
      # Intentionally hard code the hash value since it has to be deterministic.
      # Other unit tests should use the calculated value.
      'properties_hash': 'cad6addd80cb545042968d267f7561c87323b45a',
      'pubsub_topic': None,
      'pubsub_userdata': None,
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'pool:default',
        u'priority:49',
        u'tag:1',
        u'user:Jesus',
      ],
      'user': u'Jesus',
    }
    actual = request.to_dict()
    # expiration_ts - created_ts == scheduling_expiration_secs.
    actual.pop('created_ts')
    actual.pop('expiration_ts')
    self.assertEqual(expected_request, actual)
    self.assertEqual(30, request.expiration_secs)

  def test_make_request_parent(self):
    parent = task_request.make_request(_gen_request(), True)
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    child = task_request.make_request(
        _gen_request(parent_task_id=parent_id), True)
    self.assertEqual(parent_id, child.parent_task_id)

  def test_make_request_invalid_parent_id(self):
    # Must ends with '1' or '2', not '0'
    with self.assertRaises(ValueError):
      _gen_request(parent_task_id='1d69b9f088008810')

  def test_make_request_idempotent(self):
    request = task_request.make_request(
        _gen_request(properties=dict(idempotent=True)), True)
    as_dict = request.to_dict()
    self.assertEqual(True, as_dict['properties']['idempotent'])
    # Intentionally hard code the hash value since it has to be deterministic.
    # Other unit tests should use the calculated value.
    # Ensure the algorithm is deterministic.
    self.assertEqual(
        'cad6addd80cb545042968d267f7561c87323b45a', as_dict['properties_hash'])

  def test_duped(self):
    # Two TestRequest with the same properties.
    request_1 = task_request.make_request(
        _gen_request(properties=dict(idempotent=True)), True)
    now = utils.utcnow()
    request_2 = task_request.make_request(
        _gen_request(
            name='Other',
            user='Other',
            priority=201,
            created_ts=now,
            expiration_ts=now + datetime.timedelta(seconds=129),
            tags=['tag:2'],
            properties=dict(idempotent=True)),
        True)
    self.assertEqual(
        request_1.properties.properties_hash,
        request_2.properties.properties_hash)
    self.assertTrue(request_1.properties.properties_hash)

  def test_different(self):
    # Two TestRequest with different properties.
    request_1 = task_request.make_request(
        _gen_request(
          properties=dict(execution_timeout_secs=30, idempotent=True)), True)
    request_2 = task_request.make_request(
        _gen_request(
          properties=dict(execution_timeout_secs=129, idempotent=True)), True)
    self.assertNotEqual(
        request_1.properties.properties_hash,
        request_2.properties.properties_hash)

  def test_bad_values(self):
    with self.assertRaises(AssertionError):
      task_request.make_request(None, True)
    with self.assertRaises(AssertionError):
      task_request.make_request({}, True)
    with self.assertRaises(AttributeError):
      task_request.make_request(_gen_request(properties={'foo': 'bar'}), True)
    task_request.make_request(_gen_request(), True)

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request(properties=dict(commands=[])), True)
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request(properties=dict(commands={'a': 'b'})), True)
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request(properties=dict(commands=['python'])), True)
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request(properties=dict(commands=[['python']])), True)
    task_request.make_request(
        _gen_request(properties=dict(commands=[[u'python']])), True)

    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request(properties=dict(env=[])), True)
    with self.assertRaises(TypeError):
      task_request.make_request(
          _gen_request(properties=dict(env={u'a': 1})), True)
    task_request.make_request(_gen_request(properties=dict(env={})), True)

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request(priority=task_request.MAXIMUM_PRIORITY+1), True)
    task_request.make_request(
        _gen_request(priority=task_request.MAXIMUM_PRIORITY), True)

    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request(
              properties=dict(
                  execution_timeout_secs=task_request._ONE_DAY_SECS+1)),
              True)
    task_request.make_request(
        _gen_request(
            properties=dict(execution_timeout_secs=task_request._ONE_DAY_SECS)),
        True)

    now = utils.utcnow()
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request(
              created_ts=now,
              expiration_ts=
                  now+datetime.timedelta(
                      seconds=task_request._MIN_TIMEOUT_SECS-1)),
          True)
    task_request.make_request(
        _gen_request(
              created_ts=now,
              expiration_ts=
                  now+datetime.timedelta(
                      seconds=task_request._MIN_TIMEOUT_SECS)),
        True)

    # Try with isolated/isolatedserver/namespace.
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.make_request(
          _gen_request(properties=dict(
              commands=['see', 'spot', 'run'], isolated='something.isolated')),
          True)

  def test_make_request_clone(self):
    # Compare with test_make_request().
    parent = task_request.make_request(_gen_request(), True)
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    data = _gen_request(
        properties=dict(idempotent=True), parent_task_id=parent_id)
    request = task_request.make_request_clone(
        task_request.make_request(data, True))
    # Differences from make_request() are:
    # - idempotent was reset to False.
    # - parent_task_id was reset to None.
    expected_properties = {
      'commands': [[u'command1', u'arg1']],
      'dimensions': {
        u'OS': u'Windows-3.1.1',
        u'hostname': u'localhost',
        u'pool': u'default',
      },
      'env': {u'foo': u'bar', u'joe': u'2'},
      'execution_timeout_secs': 30,
      'extra_args': [],
      'grace_period_secs': 30,
      'idempotent': False,
      'inputs_ref': None,
      'io_timeout_secs': None,
    }
    # Differences from make_request() are:
    # - parent_task_id was reset to None.
    # - tag 'user:' was replaced
    # - user was replaced.
    expected_request = {
      'authenticated': auth_testing.DEFAULT_MOCKED_IDENTITY,
      'name': u'Request name (Retry #1)',
      'parent_task_id': None,
      'priority': 49,
      'properties': expected_properties,
      'properties_hash': None,
      'pubsub_topic': None,
      'pubsub_userdata': None,
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'pool:default',
        u'priority:49',
        u'tag:1',
        u'user:mocked@example.com',
      ],
      'user': u'mocked@example.com',
    }
    actual = request.to_dict()
    # expiration_ts - created_ts == deadline_to_run.
    actual.pop('created_ts')
    actual.pop('expiration_ts')
    self.assertEqual(expected_request, actual)
    self.assertEqual(30, request.expiration_secs)

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

  def test_datetime_to_request_base_id(self):
    now = datetime.datetime(2012, 1, 2, 3, 4, 5, 123456)
    self.assertEqual(
        0xeb5313d0300000, task_request.datetime_to_request_base_id(now))

  def test_convert_to_request_key(self):
    """Indirectly tested by API."""
    now = datetime.datetime(2012, 1, 2, 3, 4, 5, 123456)
    key = task_request.convert_to_request_key(now)
    self.assertEqual(9157134072765480958, key.id())

  def test_request_key_to_datetime(self):
    key = ndb.Key(task_request.TaskRequest, 0x7f14acec2fcfffff)
    # Resolution is only kept at millisecond level compared to
    # datetime_to_request_base_id() by design.
    self.assertEqual(
        datetime.datetime(2012, 1, 2, 3, 4, 5, 123000),
        task_request.request_key_to_datetime(key))

  def test_request_id_to_key(self):
    # Simple XOR.
    self.assertEqual(
        ndb.Key(task_request.TaskRequest, 0x7f14acec2fcfffff),
        task_request.request_id_to_key(0xeb5313d0300000))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
