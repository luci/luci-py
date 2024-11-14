#!/usr/bin/env vpython3
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

from parameterized import parameterized

from depot_tools import auto_stub

import remote_client


class TestRemoteClient(auto_stub.TestCase):

  def setUp(self):
    super(TestRemoteClient, self).setUp()
    self.slept = 0
    def sleep_mock(t):
      self.slept += t

    self.mock(time, 'sleep', sleep_mock)

  def test_initialize_success(self):
    headers = {'A': 'a'}
    exp_ts = time.time() + 3600
    c = remote_client.RemoteClientNative(
        'http://localhost:1', lambda: (headers, exp_ts), 'localhost', '/')
    c.initialize(threading.Event())
    self.assertEqual(0, self.slept)
    self.assertTrue(c.uses_auth)
    self.assertEqual(headers, c.get_authentication_headers())

  def test_initialize_retries(self):
    headers = {'A': 'a'}
    exp_ts = time.time() + 3600
    attempt = [0]
    def callback():
      attempt[0] += 1
      if attempt[0] == 10:
        return headers, exp_ts
      raise Exception('fail')
    c = remote_client.RemoteClientNative('http://localhost:1', callback,
                                         'localhost', '/')
    c.initialize(threading.Event())
    self.assertEqual(9*2, self.slept)
    self.assertTrue(c.uses_auth)
    self.assertEqual(headers, c.get_authentication_headers())

  def test_initialize_gives_up(self):

    def callback():
      raise Exception('fail')
    c = remote_client.RemoteClientNative('http://localhost:1', callback,
                                         'localhost', '/')
    with self.assertRaises(remote_client.InitializationError):
      c.initialize(threading.Event())
    self.assertEqual(29*2, self.slept)
    self.assertFalse(c.uses_auth)
    self.assertEqual({}, c.get_authentication_headers())

  def test_get_headers(self):
    today = datetime.datetime(2018, 2, 16, 1, 19, 45, 130574)
    self.mock(remote_client, 'utcnow', lambda: today)

    auth_headers = {'A': 'a'}
    auth_exp_ts = time.time() + 3600

    c = remote_client.RemoteClientNative(
        'http://localhost:1',
        lambda: (auth_headers, auth_exp_ts),
        'localhost',
        '/')
    self.assertTrue(c.uses_auth)

    self.assertEqual({'Cookie': 'GOOGAPPUID=899'}, c.get_headers())
    self.assertEqual({
        'A': 'a',
        'Cookie': 'GOOGAPPUID=899'
    }, c.get_headers(include_auth=True))

  def test_get_authentication_headers(self):
    self.mock(time, 'time', lambda: 100000)
    c = remote_client.RemoteClientNative(
        'http://localhost:1',
        lambda: ({'Now': str(time.time())}, time.time() + 3600),
        'localhost',
        '/')

    # Grab initial headers.
    self.assertEqual({'Now': '100000'}, c.get_authentication_headers())

    # A bit later still using same cached headers.
    self.mock(time, 'time', lambda: 102000)
    self.assertEqual({'Now': '100000'}, c.get_authentication_headers())

    # Close to expiration => refreshed.
    self.mock(time, 'time', lambda: 103500)
    self.assertEqual({'Now': '103500'}, c.get_authentication_headers())

  def test_mint_oauth_token_ok(self):
    fake_resp = {
        'service_account': 'blah@example.com',
        'access_token': 'abc',
        'expiry': 12345,
    }

    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')
    c.bot_id = 'bot_id'

    def mocked_call(url_path, data, expected_error_codes, retry_transient=True):
      self.assertEqual('/swarming/api/v1/bot/oauth_token', url_path)
      self.assertEqual({
          'account_id': 'account_id',
          'id': 'bot_id',
          'scopes': ['a', 'b'],
          'task_id': 'task_id',
      }, data)
      self.assertEqual((400,), expected_error_codes)
      self.assertTrue(retry_transient)
      return fake_resp
    self.mock(c, '_url_read_json', mocked_call)

    resp = c.mint_oauth_token('task_id', 'account_id', ['a', 'b'])
    self.assertEqual(fake_resp, resp)

  def test_mint_id_token_ok(self):
    fake_resp = {
        'service_account': 'blah@example.com',
        'id_token': 'abc',
        'expiry': 12345,
    }

    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')
    c.bot_id = 'bot_id'

    def mocked_call(url_path, data, expected_error_codes):
      self.assertEqual('/swarming/api/v1/bot/id_token', url_path)
      self.assertEqual({
          'account_id': 'account_id',
          'audience': 'https://example.com',
          'id': 'bot_id',
          'task_id': 'task_id',
      }, data)
      self.assertEqual((400,), expected_error_codes)
      return fake_resp
    self.mock(c, '_url_read_json', mocked_call)

    resp = c.mint_id_token('task_id', 'account_id', 'https://example.com')
    self.assertEqual(fake_resp, resp)

  @parameterized.expand([
      ('mint_oauth_token', ['scope-a', 'scope-b']),
      ('mint_id_token', 'https://audience.example.com'),
  ])
  def test_mint_token_transient_err(self, method, arg):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(*_args, **_kwargs):
      return None  # that's how net.url_read_json indicates HTTP 500 :-/
    self.mock(c, '_url_read_json', mocked_call)
    with self.assertRaises(remote_client.InternalError):
      getattr(c, method)('task_id', 'account_id', arg)

  @parameterized.expand([
      ('mint_oauth_token', ['scope-a', 'scope-b']),
      ('mint_id_token', 'https://audience.example.com'),
  ])
  def test_mint_token_fatal_err(self, method, arg):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(*_args, **_kwargs):
      return {'error': 'blah'}

    self.mock(c, '_url_read_json', mocked_call)
    with self.assertRaises(remote_client.MintTokenError):
      getattr(c, method)('task_id', 'account_id', arg)

  def test_rbe_create_session_ok(self):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(_url, data, retry_transient):
      self.assertEqual(
          {
              'dimensions': {
                  'dim': ['v1', 'v2']
              },
              'bot_version': 'bot_version',
              'poll_token': 'poll_tok',
              'session_token': 'session_tok',
          }, data)
      self.assertFalse(retry_transient)
      return {'session_token': 'session_tok', 'session_id': 'sid'}

    self.mock(c, '_url_read_json', mocked_call)

    resp = c.rbe_create_session({'dim': ['v1', 'v2']}, 'bot_version', None,
                                'poll_tok', 'session_tok', False)

    self.assertEqual('session_tok', resp.session_token)
    self.assertEqual('sid', resp.session_id)

  @parameterized.expand([
      (None, ),
      ('not a dict', ),
      ({
          'session_id': 'sid',
      }, ),
      ({
          'session_token': None,
          'session_id': 'sid',
      }, ),
      ({
          'session_token': '',
          'session_id': 'sid',
      }, ),
      ({
          'session_token': 123,
          'session_id': 'sid',
      }, ),
      ({
          'session_token': 'tok',
      }, ),
      ({
          'session_token': 'tok',
          'session_id': None,
      }, ),
      ({
          'session_token': 'tok',
          'session_id': '',
      }, ),
      ({
          'session_token': 'tok',
          'session_id': 123,
      }, ),
  ])
  def test_rbe_create_session_bad_resp(self, dct):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    self.mock(c, '_url_read_json', lambda *_args, **_kwargs: dct)

    with self.assertRaises(remote_client.RBEServerError):
      c.rbe_create_session({'dim': ['v1', 'v2']}, 'bot_version', None,
                           'poll_tok', None, False)

  def test_rbe_update_session_full_ok(self):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(_url, data, retry_transient):
      self.assertEqual(
          {
              'session_token': 'session_tok',
              'status': 'OK',
              'dimensions': {
                  'dim': ['v1', 'v2']
              },
              'bot_version': 'bot_version',
              'poll_token': 'poll_tok',
              'lease': {
                  'id': 'lease-id',
                  'state': 'ACTIVE'
              },
              'nonblocking': True,
          }, data)
      self.assertFalse(retry_transient)
      return {
          'session_token': 'new_session_tok',
          'status': 'BOT_TERMINATING',
          'lease': {
              'id': 'another-lease-id',
              'state': 'PENDING',
          },
      }

    self.mock(c, '_url_read_json', mocked_call)

    resp = c.rbe_update_session(
        'session_tok',
        remote_client.RBESessionStatus.OK,
        {'dim': ['v1', 'v2']},
        'bot_version',
        None,
        remote_client.RBELease('lease-id', remote_client.RBELeaseState.ACTIVE),
        'poll_tok',
        False,
        False,
    )

    self.assertEqual('new_session_tok', resp.session_token)
    self.assertEqual(remote_client.RBESessionStatus.BOT_TERMINATING,
                     resp.status)
    self.assertIsInstance(resp.lease, remote_client.RBELease)
    self.assertEqual('another-lease-id', resp.lease.id)
    self.assertEqual(remote_client.RBELeaseState.PENDING, resp.lease.state)

  def test_rbe_update_session_minimal_ok(self):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(_url, data, retry_transient):
      self.assertEqual(
          {
              'session_token': 'session_tok',
              'status': 'OK',
              'dimensions': {
                  'dim': ['v1', 'v2']
              },
          }, data)
      self.assertFalse(retry_transient)
      return {
          'session_token': 'new_session_tok',
          'status': 'BOT_TERMINATING',
      }

    self.mock(c, '_url_read_json', mocked_call)

    resp = c.rbe_update_session(
        'session_tok',
        remote_client.RBESessionStatus.OK,
        {'dim': ['v1', 'v2']},
        None,
        None,
        None,
        None,
        True,
        False,
    )

    self.assertEqual('new_session_tok', resp.session_token)
    self.assertEqual(remote_client.RBESessionStatus.BOT_TERMINATING,
                     resp.status)
    self.assertIsNone(resp.lease)

  def test_rbe_update_session_expired_session(self):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    def mocked_call(_url, data, retry_transient):
      self.assertEqual(
          {
              'session_token': 'session_tok',
              'status': 'OK',
              'dimensions': {
                  'dim': ['v1', 'v2']
              },
          }, data)
      self.assertFalse(retry_transient)
      return {
          'status': 'BOT_TERMINATING',
      }

    self.mock(c, '_url_read_json', mocked_call)

    resp = c.rbe_update_session(
        'session_tok',
        remote_client.RBESessionStatus.OK,
        {'dim': ['v1', 'v2']},
        None,
        None,
        None,
        None,
        True,
        False,
    )

    self.assertIsNone(resp.session_token)
    self.assertEqual(remote_client.RBESessionStatus.BOT_TERMINATING,
                     resp.status)
    self.assertIsNone(resp.lease)

  @parameterized.expand([
      (None, ),
      ('not a dict', ),
      ({
          'session_token': 123,
          'status': 'OK',
      }, ),
      ({
          'session_token': '',
          'status': 'OK',
      }, ),
      ({
          'session_token': 'tok',
          'status': 'WRONG_ENUM',
      }, ),
      ({
          'session_token': 'tok',
          'status': 'OK',
          'lease': 'not-a-dict',
      }, ),
      ({
          'session_token': 'tok',
          'status': 'OK',
          'lease': {
              'id': None
          },
      }, ),
  ])
  def test_rbe_update_session_bad_resp(self, dct):
    c = remote_client.RemoteClientNative('http://localhost:1', None,
                                         'localhost', '/')

    self.mock(c, '_url_read_json', lambda *_args, **_kwargs: dct)

    with self.assertRaises(remote_client.RBEServerError):
      c.rbe_update_session(
          'session_tok',
          remote_client.RBESessionStatus.OK,
          {'dim': ['v1', 'v2']},
          None,
          None,
          None,
          None,
          True,
          False,
      )


class TestRBELease(unittest.TestCase):
  def test_to_dict_simple(self):
    lease = remote_client.RBELease('some-id',
                                   remote_client.RBELeaseState.PENDING)
    self.assertEqual({'id': 'some-id', 'state': 'PENDING'}, lease.to_dict())

  def test_to_dict_result_payload(self):
    lease = remote_client.RBELease('some-id',
                                   remote_client.RBELeaseState.ACTIVE,
                                   {'payload': '123'}, {'result': '456'})
    self.assertEqual(
        {
            'id': 'some-id',
            'state': 'ACTIVE',
            'payload': {
                'payload': '123'
            },
            'result': {
                'result': '456'
            },
        }, lease.to_dict())
    self.assertEqual(
        {
            'id': 'some-id',
            'state': 'ACTIVE',
            'result': {
                'result': '456'
            },
        }, lease.to_dict(omit_payload=True))

  @parameterized.expand([
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
          'payload': {
              'payload': '123'
          },
          'result': {
              'result': '456'
          },
      }, ),
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
          'payload': {},
          'result': {},
      }, ),
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
          'payload': None,
          'result': None,
      }, ),
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
      }, ),
  ])
  def test_from_dict_ok(self, dct):
    expected = dct.copy()
    if expected.get('result') is None:
      expected.pop('result', None)
    if expected.get('payload') is None:
      expected.pop('payload', None)
    self.assertEqual(expected, remote_client.RBELease.from_dict(dct).to_dict())

  @parameterized.expand([
      ('not a dict', ),
      ({
          'id': 123,
          'state': 'ACTIVE',
      }, ),
      ({
          'id': 'some-id',
          'state': 123,
      }, ),
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
          'payload': 123,
      }, ),
      ({
          'id': 'some-id',
          'state': 'ACTIVE',
          'result': 123,
      }, ),
  ])
  def test_from_dict_type_error(self, dct):
    with self.assertRaises(TypeError):
      remote_client.RBELease.from_dict(dct)

  @parameterized.expand([
      ({
          'state': 'ACTIVE',
      }, ),
      ({
          'id': '',
          'state': 'ACTIVE',
      }, ),
      ({
          'id': 'some-id',
      }, ),
      ({
          'id': 'some-id',
          'state': '',
      }, ),
      ({
          'id': 'some-id',
          'state': 'WRONG_ENUM',
      }, ),
  ])
  def test_from_dict_value_error(self, dct):
    with self.assertRaises(ValueError):
      remote_client.RBELease.from_dict(dct)


class MockedRBERemote:
  def __init__(self):
    self.last_dimensions = None
    self.last_bot_version = None
    self.last_worker_properties = None
    self.last_poll_token = None
    self.last_blocking = None
    self.last_retry_transient = None
    self.last_session_token = None
    self.last_status = None
    self.last_lease = None
    self.next_session_id = 'mocked_session_id'
    self.next_status = None
    self.next_lease = None

  def mock_next_response(self, status, lease):
    self.next_status = status
    self.next_lease = lease

  def rbe_create_session(self,
                         dimensions,
                         bot_version,
                         worker_properties,
                         poll_token,
                         session_token=None,
                         retry_transient=False):
    self.last_dimensions = dimensions.copy()
    self.last_bot_version = bot_version
    self.last_worker_properties = worker_properties
    self.last_poll_token = poll_token
    self.last_session_token = session_token
    self.last_retry_transient = retry_transient
    return remote_client.RBECreateSessionResponse('session_tok:0',
                                                  self.next_session_id)

  def rbe_update_session(self,
                         session_token,
                         status,
                         dimensions,
                         bot_version,
                         worker_properties,
                         lease=None,
                         poll_token=None,
                         blocking=True,
                         retry_transient=False):
    if self.next_status is None:
      raise AssertionError('Unexpected rbe_update_session call')

    self.last_session_token = session_token
    self.last_status = status
    self.last_dimensions = dimensions.copy()
    self.last_bot_version = bot_version
    self.last_worker_properties = worker_properties
    self.last_lease = lease.clone() if lease else None
    self.last_poll_token = poll_token
    self.last_blocking = blocking
    self.last_retry_transient = retry_transient

    status, self.next_status = self.next_status, None
    lease, self.next_lease = self.next_lease, None

    assert session_token.startswith('session_tok:')
    num = int(session_token.split(':')[1])

    return remote_client.RBEUpdateSessionResponse(
        session_token='session_tok:%d' % (num + 1),
        status=status,
        lease=lease,
    )


class TestRBESession(unittest.TestCase):
  def setUp(self):
    super().setUp()
    self.temp_dir = tempfile.mkdtemp(prefix='swarming')

  def tearDown(self):
    shutil.rmtree(self.temp_dir)
    super().tearDown()

  def check_serialization_works(self, session):
    dump = session.to_dict()
    loaded = remote_client.RBESession.from_dict(session._remote, dump)
    self.assertEqual(session._instance, loaded._instance)
    self.assertEqual(session._dimensions, loaded._dimensions)
    self.assertEqual(session._bot_version, loaded._bot_version)
    self.assertEqual(session._worker_properties, loaded._worker_properties)
    self.assertEqual(session._poll_token, loaded._poll_token)
    self.assertEqual(session._session_token, loaded._session_token)
    self.assertEqual(session._session_id, loaded._session_id)
    self.assertEqual(session._last_acked_status, loaded._last_acked_status)
    self.assertEqual(session._active_lease, loaded._active_lease)
    self.assertEqual(session._finished_lease, loaded._finished_lease)
    # Just confirm `restore` doesn't crash.
    loaded.restore(dump)

  def test_full_flow(self):
    remote = MockedRBERemote()
    dims = lambda x: {'dim': ['v1', str(x)]}
    wp = remote_client.WorkerProperties('rbe-pool-id', 'rbe-pool-version')

    s = remote_client.RBESession(remote, 'some-instance', dims(0),
                                 'bot_version', wp, 'poll_tok:0')

    # In the initial state.
    self.assertEqual('some-instance', s.instance)
    self.assertEqual('mocked_session_id', s.session_id)
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)
    # Called `rbe_create_session`.
    self.assertEqual(dims(0), remote.last_dimensions)
    self.assertEqual('bot_version', remote.last_bot_version)
    self.assertEqual(wp, remote.last_worker_properties)
    self.assertEqual('poll_tok:0', remote.last_poll_token)

    # Can be serialized/restored in this state.
    self.check_serialization_works(s)

    # Calling ping_active_lease without a lease is not allowed.
    with self.assertRaises(remote_client.RBESessionException):
      s.ping_active_lease()

    # Wait for a lease, get none.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    lease = s.update(remote_client.RBESessionStatus.OK, dims(1), 'poll_tok:1')
    self.assertIsNone(lease)
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)
    # Called `rbe_update_session`.
    self.assertEqual('session_tok:0', remote.last_session_token)
    self.assertEqual(remote_client.RBESessionStatus.OK, remote.last_status)
    self.assertEqual(dims(1), remote.last_dimensions)
    self.assertEqual('bot_version', remote.last_bot_version)
    self.assertEqual(wp, remote.last_worker_properties)
    self.assertIsNone(remote.last_lease)
    self.assertEqual('poll_tok:1', remote.last_poll_token)
    self.assertTrue(remote.last_blocking)
    self.assertFalse(remote.last_retry_transient)

    # Wait for a lease and get some!
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'some-lease',
            remote_client.RBELeaseState.PENDING,
            {'payload': '123'},
            None,
        ),
    )
    lease = s.update(remote_client.RBESessionStatus.OK, dims(2), 'poll_tok:2')
    self.assertIsNotNone(lease)
    self.assertIs(lease, s.active_lease)
    self.assertEqual(
        {
            'id': 'some-lease',
            'payload': {
                'payload': '123'
            },
            'state': 'PENDING'
        }, lease.to_dict())
    self.assertTrue(s.alive)

    # Can be serialized/restored in this state.
    self.check_serialization_works(s)

    # Calling update while holding onto a lease is not allowed.
    with self.assertRaises(remote_client.RBESessionException):
      s.update(remote_client.RBESessionStatus.OK, dims(2), 'poll_tok:2')

    # Ping the active lease, keep it ACTIVE to keep working on it.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'some-lease',
            remote_client.RBELeaseState.ACTIVE,
            None,
            None,
        ),
    )
    self.assertTrue(s.ping_active_lease())
    self.assertIsNotNone(s.active_lease)
    self.assertTrue(s.alive)
    # Called `rbe_update_session` correctly.
    self.assertEqual('session_tok:2', remote.last_session_token)
    self.assertEqual(remote_client.RBESessionStatus.OK, remote.last_status)
    self.assertEqual(dims(2), remote.last_dimensions)
    self.assertIsNone(remote.last_poll_token)
    self.assertFalse(remote.last_retry_transient)
    self.assertEqual('some-lease', remote.last_lease.id)
    self.assertEqual(remote_client.RBELeaseState.ACTIVE,
                     remote.last_lease.state)

    # Mark the lease as done.
    s.finish_active_lease({'result': 'xxx'})
    self.assertIsNone(s.active_lease)
    self.assertTrue(s.alive)

    # Can be serialized/restored in this state.
    self.check_serialization_works(s)

    # Finishing the least twice is not allowed.
    with self.assertRaises(remote_client.RBESessionException):
      s.finish_active_lease({'result': 'zzz'})

    # Report the lease result, discover there's no new leases.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    lease = s.update(remote_client.RBESessionStatus.OK, dims(3), 'poll_tok:3')
    self.assertIsNone(lease)
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)
    # Passed the lease result to `rbe_update_session`.
    self.assertEqual(
        {
            'id': 'some-lease',
            'payload': {
                'payload': '123'
            },
            'result': {
                'result': 'xxx'
            },
            'state': 'COMPLETED'
        }, remote.last_lease.to_dict())

    # One more idle cycle. Doesn't report the finished lease result anymore.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    lease = s.update(remote_client.RBESessionStatus.OK, dims(4), 'poll_tok:4')
    self.assertIsNone(lease)
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)
    # Passed no leases to `rbe_update_session`.
    self.assertIsNone(remote.last_lease)

    # Setting non-OK status effectively closes the session. Note that the
    # backend still replies with OK status even if the bot wants the session
    # gone!
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    lease = s.update(remote_client.RBESessionStatus.BOT_TERMINATING, dims(5),
                     'poll_tok:5')
    self.assertIsNone(lease)
    self.assertFalse(s.alive)
    # Passed correct status to `rbe_update_session`.
    self.assertEqual(remote_client.RBESessionStatus.BOT_TERMINATING,
                     remote.last_status)

    # Calling update with dead session is not allowed.
    with self.assertRaises(remote_client.RBESessionException):
      s.update(remote_client.RBESessionStatus.OK, dims(5), 'poll_tok:5')

    # Can be serialized/restored in this state.
    self.check_serialization_works(s)

    # This doesn't actually do much, since the session is already closed.
    # There's a separate test for it.
    s.terminate()

    # Verify a session can be recreated.
    remote.next_session_id = 'new_session_id'
    remote.last_session_token = None
    remote.last_worker_properties = None
    s.recreate()
    self.assertEqual('session_tok:6', remote.last_session_token)
    self.assertEqual(wp, remote.last_worker_properties)
    self.assertEqual('some-instance', s.instance)
    self.assertEqual('new_session_id', s.session_id)
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)

  def test_no_idle_time(self):
    remote = MockedRBERemote()
    dims = lambda x: {'dim': ['v1', str(x)]}

    s = remote_client.RBESession(remote, 'some-instance', dims(0),
                                 'bot_version', None, 'poll_tok')

    # Get a task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims(1), 'poll_tok')
    self.assertEqual('lease-0', s.active_lease.id)

    # Finish it right away.
    s.finish_active_lease({'result': 0})
    self.assertIsNone(s.active_lease)

    # Report the result, get the next task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-1',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims(2), 'poll_tok')
    self.assertEqual('lease-1', s.active_lease.id)
    # Reported the task result.
    self.assertEqual(
        {
            'id': 'lease-0',
            'payload': {},
            'result': {
                'result': 0
            },
            'state': 'COMPLETED'
        }, remote.last_lease.to_dict())

    # Finish it right away.
    s.finish_active_lease({'result': 1})
    self.assertIsNone(s.active_lease)

    # Terminate the session. This reports the last task result.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    s.terminate()
    self.assertFalse(s.alive)
    # Called `rbe_update_session` correctly.
    self.assertEqual('session_tok:2', remote.last_session_token)
    self.assertEqual(remote_client.RBESessionStatus.BOT_TERMINATING,
                     remote.last_status)
    self.assertEqual(dims(2), remote.last_dimensions)
    self.assertIsNone(remote.last_poll_token)
    # Reported the task result.
    self.assertEqual(
        {
            'id': 'lease-1',
            'payload': {},
            'result': {
                'result': 1
            },
            'state': 'COMPLETED'
        }, remote.last_lease.to_dict())

  def test_server_side_session_expiry(self):
    remote = MockedRBERemote()
    dims = {'dim': ['v1', 'v2']}

    s = remote_client.RBESession(remote, 'some-instance', dims, 'bot_version',
                                 None, 'poll_tok')

    # The session is marked as being terminated.
    remote.mock_next_response(remote_client.RBESessionStatus.BOT_TERMINATING,
                              None)
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertIsNone(s.active_lease)
    self.assertTrue(s.alive)
    self.assertTrue(s.terminating)

    # Terminate closes the session for good.
    remote.mock_next_response(remote_client.RBESessionStatus.BOT_TERMINATING,
                              None)
    s.terminate()
    self.assertFalse(s.alive)

  def test_server_side_session_expiry_with_active_lease(self):
    remote = MockedRBERemote()
    dims = {'dim': ['v1', 'v2']}

    s = remote_client.RBESession(remote, 'some-instance', dims, 'bot_version',
                                 None, 'poll_tok')

    # Get a task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertEqual('lease-0', s.active_lease.id)

    # Send a ping and discover the session is gone and the lease is lost.
    remote.mock_next_response(remote_client.RBESessionStatus.BOT_TERMINATING,
                              None)
    self.assertFalse(s.ping_active_lease())
    self.assertTrue(s.alive)
    self.assertTrue(s.terminating)
    self.assertIsNone(s.active_lease)

    # Terminate closes the session for good.
    remote.mock_next_response(remote_client.RBESessionStatus.BOT_TERMINATING,
                              None)
    s.terminate()
    self.assertFalse(s.alive)

  def test_lease_server_cancellation(self):
    remote = MockedRBERemote()
    dims = {'dim': ['v1', 'v2']}

    s = remote_client.RBESession(remote, 'some-instance', dims, 'bot_version',
                                 None, 'poll_tok')

    # Get a task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertEqual('lease-0', s.active_lease.id)

    # Send a ping and discover the lease is canceled.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.CANCELLED,
            None,
            None,
        ),
    )
    self.assertFalse(s.ping_active_lease())
    self.assertTrue(s.alive)
    self.assertIsNotNone(s.active_lease)

    # Canceled leases still needs to be reported as finished though, perhaps
    # with empty result.
    s.finish_active_lease(None)
    self.assertIsNone(s.active_lease)

    # The Next update reports the lease as done.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertEqual({
        'id': 'lease-0',
        'payload': {},
        'state': 'COMPLETED'
    }, remote.last_lease.to_dict())

  def test_lease_lost(self):
    remote = MockedRBERemote()
    dims = {'dim': ['v1', 'v2']}

    s = remote_client.RBESession(remote, 'some-instance', dims, 'bot_version',
                                 None, 'poll_tok')

    # Get a task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertEqual('lease-0', s.active_lease.id)

    # Send a ping and discover the lease is lost.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        None,
    )
    self.assertFalse(s.ping_active_lease())
    self.assertTrue(s.alive)
    self.assertIsNone(s.active_lease)

    # The next update doesn't report the lost lease.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertIsNone(remote.last_lease)

  def test_finish_active_lease_flush(self):
    remote = MockedRBERemote()
    dims = {'dim': ['v1', 'v2']}

    s = remote_client.RBESession(remote, 'some-instance', dims, 'bot_version',
                                 None, 'poll_tok')

    # Get a task.
    remote.mock_next_response(
        remote_client.RBESessionStatus.OK,
        remote_client.RBELease(
            'lease-0',
            remote_client.RBELeaseState.PENDING,
            {},
            None,
        ),
    )
    s.update(remote_client.RBESessionStatus.OK, dims, 'poll_tok')
    self.assertEqual('lease-0', s.active_lease.id)

    # Finish it and flush the result right away.
    remote.mock_next_response(remote_client.RBESessionStatus.OK, None)
    s.finish_active_lease({}, flush=True)
    self.assertIsNone(s.active_lease)
    self.assertTrue(s.alive)

    # Check the call arguments.
    self.assertEqual(remote_client.RBESessionStatus.MAINTENANCE,
                     remote.last_status)
    self.assertEqual(dims, remote.last_dimensions)
    self.assertIsNone(remote.last_poll_token)
    self.assertEqual('lease-0', remote.last_lease.id)
    self.assertEqual(remote_client.RBELeaseState.COMPLETED,
                     remote.last_lease.state)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
