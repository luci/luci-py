#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import collections
import datetime
import functools
import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from components import utils
from components.auth import service_account
from test_support import test_case


EMPTY_SECRET_KEY = service_account.ServiceAccountKey(None, None, None)
FAKE_SECRET_KEY = service_account.ServiceAccountKey('email', 'pkey', 'pkey_id')


class GetAccessTokenTest(test_case.TestCase):
  def setUp(self):
    super(GetAccessTokenTest, self).setUp()
    self.mock(service_account.logging, 'error', lambda *_: None)

  def mock_methods(self):
    calls = []
    def get_token(method, *args):
      calls.append((method, args))
      return 'token', 0
    self.mock(
        service_account, '_get_jwt_based_token',
        functools.partial(get_token, 'jwt_based'))
    self.mock(
        service_account.app_identity, 'get_access_token',
        functools.partial(get_token, 'gae_api'))
    return calls

  ## Verify what token generation method are used when running on real GAE.

  def test_on_gae_no_key_uses_gae_api(self):
    """Uses GAE api if secret key is not used and running on GAE."""
    self.mock(service_account.utils, 'is_local_dev_server', lambda: False)
    calls = self.mock_methods()
    self.assertEqual(('token', 0), service_account.get_access_token('scope'))
    self.assertEqual([('gae_api', ('scope',))], calls)

  def test_on_gae_empty_key_fails(self):
    """If empty key is passed on GAE, dies with error."""
    self.mock(service_account.utils, 'is_local_dev_server', lambda: False)
    calls = self.mock_methods()
    with self.assertRaises(service_account.AccessTokenError):
      service_account.get_access_token('scope', EMPTY_SECRET_KEY)
    self.assertFalse(calls)

  def test_on_gae_good_key_is_used(self):
    """If good key is passed on GAE, invokes JWT based fetch."""
    self.mock(service_account.utils, 'is_local_dev_server', lambda: False)
    calls = self.mock_methods()
    self.assertEqual(
        ('token', 0),
        service_account.get_access_token('scope', FAKE_SECRET_KEY))
    self.assertEqual([('jwt_based', ('scope', FAKE_SECRET_KEY))], calls)

  ## Tests for individual token generation methods.

  def test_get_jwt_based_token_memcache(self):
    now = datetime.datetime(2015, 1, 2, 3)

    # Fake memcache, dev server's one doesn't know about mocked time.
    memcache = {}
    def fake_get(key):
      if key not in memcache or memcache[key][1] < utils.time_time():
        return None
      return memcache[key][0]
    def fake_set(key, value, exp):
      memcache[key] = (value, exp)
    self.mock(service_account.memcache, 'get', fake_get)
    self.mock(service_account.memcache, 'set', fake_set)

    # Stub calls to real minting method.
    calls = []
    def fake_mint_token(*args):
      calls.append(args)
      return {
        'access_token': 'token@%d' % utils.time_time(),
        'exp_ts': utils.time_time() + 3600,
      }
    self.mock(service_account, '_mint_jwt_based_token', fake_mint_token)

    # Cold cache -> mint a new token, put in cache.
    self.mock_now(now, 0)
    self.assertEqual(
        ('token@1420167600', 1420171200.0),
        service_account._get_jwt_based_token('http://scope', FAKE_SECRET_KEY))
    self.assertEqual([(['http://scope'], FAKE_SECRET_KEY)], calls)
    self.assertEqual(['access_token@http://scope@pkey_id'], memcache.keys())
    del calls[:]

    # Uses cached copy while it is valid.
    self.mock_now(now, 3000)
    self.assertEqual(
        ('token@1420167600', 1420171200.0),
        service_account._get_jwt_based_token('http://scope', FAKE_SECRET_KEY))
    self.assertFalse(calls)

    # 5 min before expiration it is considered unusable, and new one is minted.
    self.mock_now(now, 3600 - 5 * 60 + 1)
    self.assertEqual(
        ('token@1420170901', 1420174501.0),
        service_account._get_jwt_based_token('http://scope', FAKE_SECRET_KEY))
    self.assertEqual([(['http://scope'], FAKE_SECRET_KEY)], calls)

  def test_mint_jwt_based_token(self):
    self.mock_now(datetime.datetime(2015, 1, 2, 3))

    rsa_sign_calls = []
    def mocked_rsa_sign(*args):
      rsa_sign_calls.append(args)
      return '\x00signature\x00'
    self.mock(service_account, '_rsa_sign', mocked_rsa_sign)

    fetch_calls = []
    def mocked_fetch(**kwargs):
      fetch_calls.append(kwargs)
      response = collections.namedtuple('Response', 'status_code content')
      return response(
          200, json.dumps({'access_token': 'token', 'expires_in': 3600}))
    self.mock(service_account.urlfetch, 'fetch', mocked_fetch)

    token = service_account._mint_jwt_based_token(
        ['scope1', 'scope2'], FAKE_SECRET_KEY)
    self.assertEqual({'access_token': 'token', 'exp_ts': 1420171200.0}, token)

    self.assertEqual(
        [('eyJhbGciOiJSUzI1NiIsImtpZCI6InBrZXlfaWQiLCJ0eXAiOiJKV1QifQ.'
          'eyJhdWQiOiJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjM'
          'vdG9rZW4iLCJleHAiOjE0MjAxNzEyMDAsImlhdCI6MTQyMDE2NzYwMCwiaX'
          'NzIjoiZW1haWwiLCJzY29wZSI6InNjb3BlMSBzY29wZTIifQ', 'pkey')],
        rsa_sign_calls)

    self.assertEqual([
      {
        'url': 'https://www.googleapis.com/oauth2/v3/token',
        'follow_redirects': False,
        'method': 'POST',
        'headers': {'Content-Type': 'application/x-www-form-urlencoded'},
        'deadline': 10,
        'validate_certificate': True,
        'payload':
            'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&'
            'assertion=eyJhbGciOiJSUzI1NiIsImtpZCI6InBrZXlfaWQiLCJ0eXAiOiJKV1Q'
            'ifQ.eyJhdWQiOiJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjMvd'
            'G9rZW4iLCJleHAiOjE0MjAxNzEyMDAsImlhdCI6MTQyMDE2NzYwMCwiaXNzIjoiZW'
            '1haWwiLCJzY29wZSI6InNjb3BlMSBzY29wZTIifQ.AHNpZ25hdHVyZQA',
      }], fetch_calls)

  def test_mint_jwt_based_token_failure(self):
    rsa_sign_calls = []
    def mocked_rsa_sign(*args):
      rsa_sign_calls.append(args)
      return '\x00signature\x00'
    self.mock(service_account, '_rsa_sign', mocked_rsa_sign)

    fetch_calls = []
    def mocked_fetch(**kwargs):
      fetch_calls.append(kwargs)
      response = collections.namedtuple('Response', 'status_code content')
      return response(500, 'error')
    self.mock(service_account.urlfetch, 'fetch', mocked_fetch)

    with self.assertRaises(service_account.AccessTokenError):
      service_account._mint_jwt_based_token(
          ['scope1', 'scope2'], FAKE_SECRET_KEY)

    # Sign once, try to send request N times.
    self.assertEqual(1, len(rsa_sign_calls))
    self.assertEqual(5, len(fetch_calls))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
