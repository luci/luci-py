#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
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


def mkreq(req, secret_bytes=None):
  # This function fits the old style where TaskRequest was stored first, before
  # TaskToRun and TaskResultSummary.
  sb = None
  if secret_bytes is not None:
    req.properties.has_secret_bytes = True
    sb = task_request.SecretBytes(secret_bytes=secret_bytes)
  task_request.init_new_request(req, True, sb)
  req.key = task_request.new_request_key()
  req.put()
  if sb:
    sb.key = req.secret_bytes_key
    sb.put()
  return req


def _gen_request(properties=None, **kwargs):
  """Creates a TaskRequest."""
  properties = properties or {}

  def merge(override, defaults):
    if override is None:
      return None
    result = defaults.copy()
    result.update(override)
    return result

  cipd_input = properties.pop('cipd_input', {})
  cipd_input = merge(cipd_input, {
    'client_package': merge(cipd_input.pop('client_package', {}), {
      'package_name': 'infra/tools/cipd/${platform}',
      'version': 'git_revision:deadbeef',
    }),
    'packages': [{
      'package_name': 'rm',
      'path': 'bin',
      'version': 'git_revision:deadbeef',
    }],
    'server': 'https://chrome-infra-packages.appspot.com'
  })

  inputs_ref = properties.pop('inputs_ref', {
    'isolatedserver': 'https://isolateserver.appspot.com',
    'namespace': 'default-gzip',
  })

  properties = merge(properties, {
    'cipd_input': cipd_input,
    'command': [u'command1', u'arg1'],
    'dimensions': {
      u'OS': [u'Windows-3.1.1'],
      u'hostname': [u'localhost'],
      u'pool': [u'default'],
    },
    'env': {u'foo': u'bar', u'joe': u'2'},
    'env_prefixes': {u'PATH': [u'local/path']},
    'execution_timeout_secs': 30,
    'grace_period_secs': 30,
    'idempotent': False,
    'inputs_ref': inputs_ref,
    'io_timeout_secs': None,
    'has_secret_bytes': 'secret_bytes' in kwargs,
  })
  properties['dimensions_data'] = properties.pop('dimensions')
  now = utils.utcnow()
  args = {
    'created_ts': now,
    'name': 'Request name',
    'priority': 50,
    'properties': properties,
    'expiration_ts': now + datetime.timedelta(seconds=30),
    'tags': [u'tag:1'],
    'user': 'Jesus',
  }
  _sb = args.pop('secret_bytes', None)
  args.update(kwargs)
  # Note that ndb model constructor accepts dicts for structured properties.
  return task_request.TaskRequest(**args)


class Prop(object):
  _name = 'foo'


class TestCase(test_case.TestCase):
  def setUp(self):
    super(TestCase, self).setUp()
    auth_testing.mock_get_current_identity(self)


class TaskRequestPrivateTest(TestCase):
  def test_validate_task_run_id(self):
    self.assertEqual(
        '1d69b9f088008811',
        task_request._validate_task_run_id(Prop(), '1d69b9f088008811'))
    self.assertEqual(None, task_request._validate_task_run_id(Prop(), ''))
    with self.assertRaises(ValueError):
      task_request._validate_task_run_id(Prop(), '1')

  def test_validate_isolated(self):
    task_request._validate_isolated(
        Prop(), '0123456789012345678901234567890123456789')
    task_request._validate_isolated(
        Prop(), '0123456789012345678901234567890123abcdef')
    task_request._validate_isolated(
        Prop(), '0123456789abcdef'*4)
    task_request._validate_isolated(
        Prop(), '0123456789abcdef'*8)
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), '123456789012345678901234567890123456789')
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), 'g123456789012345678901234567890123456789')
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), '0123456789abcdef'*3)
    with self.assertRaises(datastore_errors.BadValueError):
      task_request._validate_isolated(
          Prop(), '0123456789abcdef'*9)


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

  def test_create_termination_task(self):
    request = task_request.create_termination_task(u'some-bot')
    self.assertTrue(request.properties.is_terminate)

  def test_new_request_key(self):
    for _ in xrange(3):
      delta = utils.utcnow() - task_request._BEGINING_OF_THE_WORLD
      now = int(round(delta.total_seconds() * 1000.))
      key = task_request.new_request_key()
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
    key = task_request.new_request_key()
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
    key = task_request.new_request_key()
    # Remove the XOR.
    key_id = key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
    #   7ffffffffff 7766 1
    #     ^          ^   ^
    #     |          |   |
    #  since 2010    | schema version
    #                |
    #               rand
    self.assertEqual('0x7ffffffffff77661', '0x%016x' % key_id)

  def test_validate_request_key(self):
    task_request.validate_request_key(task_pack.unpack_request_key('11'))
    with self.assertRaises(ValueError):
      task_request.validate_request_key(ndb.Key('TaskRequest', 1))

  def test_init_new_request(self):
    parent = mkreq(_gen_request())
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    r = _gen_request(
      properties=dict(idempotent=True, relative_cwd=u'deeep'),
      parent_task_id=parent_id)
    request = mkreq(r, 'I am a banana')
    expected_properties = {
      'caches': [],
      'cipd_input': {
        'client_package': {
          'package_name': u'infra/tools/cipd/${platform}',
          'path': None,
          'version': u'git_revision:deadbeef',
        },
        'packages': [{
          'package_name': u'rm',
          'path': u'bin',
          'version': u'git_revision:deadbeef',
        }],
        'server': u'https://chrome-infra-packages.appspot.com'
      },
      'command': [u'command1', u'arg1'],
      'relative_cwd': u'deeep',
      'dimensions': {
        u'OS': [u'Windows-3.1.1'],
        u'hostname': [u'localhost'],
        u'pool': [u'default'],
      },
      'env': {u'foo': u'bar', u'joe': u'2'},
      'env_prefixes': {u'PATH': [u'local/path']},
      'extra_args': [],
      'execution_timeout_secs': 30,
      'grace_period_secs': 30,
      'idempotent': True,
      'inputs_ref': {
        'isolated': None,
        'isolatedserver': u'https://isolateserver.appspot.com',
        'namespace': u'default-gzip',
      },
      'io_timeout_secs': None,
      'outputs': [],
      'has_secret_bytes': True,
    }
    expected_request = {
      'authenticated': auth_testing.DEFAULT_MOCKED_IDENTITY,
      'name': u'Request name',
      'parent_task_id': unicode(parent_id),
      'priority': 49,
      'properties': expected_properties,
      # Intentionally hard code the hash value since it has to be deterministic.
      # Other unit tests should use the calculated value.
      'properties_hash':
          'aa33c679b3ee30e37b9724d79a9d20bc767475c00e7f659b6191508f6b16f1ab',
      'pubsub_topic': None,
      'pubsub_userdata': None,
      'service_account': u'none',
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'pool:default',
        u'priority:49',
        u'service_account:none',
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

  def test_init_new_request_isolated(self):
    parent = mkreq(_gen_request(properties={
      'command': [],
      'inputs_ref': {
        'isolated': '0123456789012345678901234567890123456789',
        'isolatedserver': 'http://localhost:1',
        'namespace': 'default-gzip',
      },
    }))
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    request = mkreq(_gen_request(
      properties={'idempotent':True},
      parent_task_id=parent_id), 'I am not a banana')
    expected_properties = {
      'caches': [],
      'cipd_input': {
        'client_package': {
          'package_name': u'infra/tools/cipd/${platform}',
          'path': None,
          'version': u'git_revision:deadbeef',
        },
        'packages': [{
          'package_name': u'rm',
          'path': u'bin',
          'version': u'git_revision:deadbeef',
        }],
        'server': u'https://chrome-infra-packages.appspot.com'
      },
      'command': [u'command1', u'arg1'],
      'relative_cwd': None,
      'dimensions': {
        u'OS': [u'Windows-3.1.1'],
        u'hostname': [u'localhost'],
        u'pool': [u'default'],
      },
      'env': {u'foo': u'bar', u'joe': u'2'},
      'env_prefixes': {u'PATH': [u'local/path']},
      'extra_args': [],
      'execution_timeout_secs': 30,
      'grace_period_secs': 30,
      'idempotent': True,
      'inputs_ref': {
        'isolated': None,
        'isolatedserver': u'https://isolateserver.appspot.com',
        'namespace': u'default-gzip',
      },
      'io_timeout_secs': None,
      'outputs': [],
      'has_secret_bytes': True,
    }
    expected_request = {
      'authenticated': auth_testing.DEFAULT_MOCKED_IDENTITY,
      'name': u'Request name',
      'parent_task_id': unicode(parent_id),
      'priority': 49,
      'properties': expected_properties,
      # Intentionally hard code the hash value since it has to be deterministic.
      # Other unit tests should use the calculated value.
      'properties_hash':
          '121c6bd6216a4cc9c4302a52da6292e5a240807ef13ace6f7f36a0c83aec6f55',
      'pubsub_topic': None,
      'pubsub_userdata': None,
      'service_account': u'none',
      'tags': [
        u'OS:Windows-3.1.1',
        u'hostname:localhost',
        u'pool:default',
        u'priority:49',
        u'service_account:none',
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

  def test_init_new_request_parent(self):
    parent = mkreq(_gen_request())
    # Hack: Would need to know about TaskResultSummary.
    parent_id = task_pack.pack_request_key(parent.key) + '1'
    child = mkreq(_gen_request(parent_task_id=parent_id))
    self.assertEqual(parent_id, child.parent_task_id)

  def test_init_new_request_invalid_parent_id(self):
    # Must ends with '1' or '2', not '0'
    with self.assertRaises(ValueError):
      _gen_request(parent_task_id='1d69b9f088008810')

  def test_init_new_request_idempotent(self):
    request = mkreq(_gen_request(properties=dict(idempotent=True)))
    as_dict = request.to_dict()
    self.assertEqual(True, as_dict['properties']['idempotent'])
    # Intentionally hard code the hash value since it has to be deterministic.
    # Other unit tests should use the calculated value.
    # Ensure the algorithm is deterministic.
    self.assertEqual(
        '58b6b8966199b901406b82ed15b23b7070cbf6ea8cba237838911939b387b4c6',
        as_dict['properties_hash'])

  def test_init_new_request_bot_service_account(self):
    request = mkreq(_gen_request(service_account='bot'))
    as_dict = request.to_dict()
    self.assertEqual('bot', as_dict['service_account'])
    self.assertIn(u'service_account:bot', as_dict['tags'])

  def test_duped(self):
    # Two TestRequest with the same properties.
    request_1 = mkreq(_gen_request(properties=dict(idempotent=True)))
    now = utils.utcnow()
    request_2 = mkreq(_gen_request(
        name='Other',
        user='Other',
        priority=201,
        created_ts=now,
        expiration_ts=now + datetime.timedelta(seconds=129),
        tags=['tag:2'],
        properties=dict(idempotent=True)))
    self.assertEqual(
        request_1.properties_hash,
        request_2.properties_hash)
    self.assertTrue(request_1.properties_hash)

  def test_different(self):
    # Two TestRequest with different properties.
    request_1 = mkreq(_gen_request(
        properties=dict(execution_timeout_secs=30, idempotent=True)))
    request_2 = mkreq(_gen_request(
        properties=dict(execution_timeout_secs=129, idempotent=True)))
    self.assertNotEqual(
        request_1.properties_hash,
        request_2.properties_hash)

  def test_bad_values(self):
    with self.assertRaises(AssertionError):
      mkreq(None)
    with self.assertRaises(AssertionError):
      mkreq({})
    with self.assertRaises(AttributeError):
      mkreq(_gen_request(properties={'foo': 'bar'}))
    mkreq(_gen_request())

    # Command.
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(properties=dict(command=[])))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(properties=dict(command={'a': 'b'})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(properties=dict(command='python')))
    mkreq(_gen_request(properties=dict(command=['python'])))
    mkreq(_gen_request(properties=dict(command=[u'python'])))
    # command and inputs_ref.
    mkreq(_gen_request(properties=dict(
        command=['python'],
        inputs_ref=task_request.FilesRef(
            isolated='deadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
            isolatedserver='http://localhost:1',
            namespace='default-gzip'))))

    # CIPD.
    def mkcipdreq(idempotent=False, **cipd_input):
      mkreq(_gen_request(
          properties=dict(idempotent=idempotent, cipd_input=cipd_input)))

    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[{}])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[
        dict(package_name='infra|rm', path='.', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[dict(package_name='rm', path='.')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[dict(package_name='rm', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[dict(package_name='rm', path='/', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[dict(package_name='rm', path='/a', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[
        dict(package_name='rm', path='a/..', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[
        dict(package_name='rm', path='a/./b', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(packages=[
        dict(package_name='rm', path='.', version='latest'),
        dict(package_name='rm', path='.', version='canary'),
      ])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(
          idempotent=True,
          packages=[dict(package_name='rm', path='.', version='latest')])
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(server='abc')
    with self.assertRaises(datastore_errors.BadValueError):
      mkcipdreq(client_package=dict(package_name='--bad package--'))
    mkcipdreq()
    mkcipdreq(packages=[dict(package_name='rm', path='.', version='latest')])
    mkcipdreq(
        client_package=dict(
            package_name='infra/tools/cipd/${platform}',
            version='git_revision:daedbeef',
        ),
        packages=[dict(package_name='rm', path='.', version='latest')],
        server='https://chrome-infra-packages.appspot.com',
    )

    # Named caches.
    mkcachereq = lambda *c: mkreq(_gen_request(properties=dict(caches=c)))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='', path='git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path=''))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(
          dict(name='git_chromium', path='git_cache'),
          dict(name='git_v8', path='git_cache'),
      )
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(
          dict(name='git_chromium', path='git_cache'),
          dict(name='git_chromium', path='git_cache2'),
      )
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='/git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='../git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='git_cache/../../a'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='../git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='git_cache//a'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='git_chromium', path='a/./git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='has space', path='git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      mkcachereq(dict(name='CAPITAL', path='git_cache'))
    with self.assertRaises(datastore_errors.BadValueError):
      # A CIPD package and named caches cannot be mapped to the same path.
      mkreq(_gen_request(properties=dict(
          caches=[dict(name='git_chromium', path='git_cache')],
          cipd_input=dict(packages=[
            dict(package_name='foo', path='git_cache', version='latest')]))))
    mkcachereq()
    mkcachereq(dict(name='git_chromium', path='git_cache'))
    mkcachereq(
        dict(name='git_chromium', path='git_cache'),
        dict(name='build_chromium', path='out'))

    # Dimensions.
    with self.assertRaises(TypeError):
      mkreq(_gen_request(properties=dict(dimensions=[])))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(properties=dict(dimensions={})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'id': u'b', u'a:': u'b'})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'id': u'b', u'a.': u'b'})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'id': u'b', u'a': [u'b']})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'id': [u'a', u'b']})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'id': u'b', u'pool': u'b'})))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(dimensions={u'pool': [u'b', u'b']})))
    mkreq(_gen_request(
        properties=dict(dimensions={u'id': [u'b'], u'pool': [u'b']})))
    mkreq(_gen_request(
        properties=dict(
            dimensions={u'id': [u'b'], u'pool': [u'b'], u'a.': [u'c']})))
    mkreq(_gen_request(
        properties=dict(
            dimensions={u'pool': [u'b'], u'a.': [u'b', u'c']})))

    # Environment.
    with self.assertRaises(TypeError):
      mkreq(_gen_request(properties=dict(env=[])))
    with self.assertRaises(TypeError):
      mkreq(_gen_request(properties=dict(env={u'a': 1})))
    mkreq(_gen_request(properties=dict(env={})))

    # Priority.
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(priority=task_request.MAXIMUM_PRIORITY+1))
    mkreq(_gen_request(priority=task_request.MAXIMUM_PRIORITY))

    # Execution timeout.
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          properties=dict(
              execution_timeout_secs=task_request._THREE_DAY_SECS+1)))
    mkreq(_gen_request(
        properties=dict(execution_timeout_secs=task_request._THREE_DAY_SECS)))

    # Expiration.
    now = utils.utcnow()
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          created_ts=now,
          expiration_ts=now + datetime.timedelta(
              seconds=task_request._MIN_TIMEOUT_SECS-1)))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(
          created_ts=now,
          expiration_ts=
              now+datetime.timedelta(seconds=task_request._SEVEN_DAYS_SECS+1)))
    mkreq(_gen_request(
        created_ts=now,
        expiration_ts=
            now+datetime.timedelta(seconds=task_request._MIN_TIMEOUT_SECS)))
    mkreq(_gen_request(
        created_ts=now,
        expiration_ts=
            now + datetime.timedelta(seconds=task_request._SEVEN_DAYS_SECS)))

    # Try with isolated/isolatedserver/namespace.
    with self.assertRaises(datastore_errors.BadValueError):
      # Both command and inputs_ref.isolated.
      mkreq(_gen_request(properties=dict(
          command=['see', 'spot', 'run'],
          inputs_ref=task_request.FilesRef(
              isolated='deadbeef',
              isolatedserver='http://localhost:1',
              namespace='default-gzip'))))
    with self.assertRaises(datastore_errors.BadValueError):
      # inputs_ref without server/namespace.
      mkreq(_gen_request(properties=dict(inputs_ref=task_request.FilesRef())))
    with self.assertRaises(datastore_errors.BadValueError):
      mkreq(_gen_request(properties=dict(
          command=[],
          inputs_ref=task_request.FilesRef(
              isolatedserver='https://isolateserver.appspot.com',
              namespace='default-gzip^^^',
          ))))
    mkreq(_gen_request(properties=dict(
        command=[],
        inputs_ref=task_request.FilesRef(
            isolated='deadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
            isolatedserver='http://localhost:1',
            namespace='default-gzip'))))

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

  def test_secret_bytes(self):
    task_request.SecretBytes(secret_bytes='a'*(20*1024)).put()
    with self.assertRaises(datastore_errors.BadValueError):
      task_request.SecretBytes(secret_bytes='a'*(20*1024+1)).put()


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
