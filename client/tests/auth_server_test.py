#!/usr/bin/env vpython3
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import contextlib
import json
import socket
import time

# Mutates sys.path.
import test_env

# third_party/
from depot_tools import auto_stub
import requests

from libs import luci_context
from utils import authenticators
from utils import auth_server
from utils import net
from utils import oauth

import net_utils


def call_rpc(account_id, scopes=None, audience=None):
  ctx = luci_context.read('local_auth')

  if audience is None:
    assert scopes
    method = 'GetOAuthToken'
    body = {
      'account_id': account_id,
      'scopes': scopes,
      'secret': ctx['secret'],
    }
  else:
    assert scopes is None
    method = 'GetIDToken'
    body = {
      'account_id': account_id,
      'audience': audience,
      'secret': ctx['secret'],
    }

  r = requests.post(
      url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.%s' % (
          ctx['rpc_port'], method),
      data=json.dumps(body),
      headers={'Content-Type': 'application/json'})

  return r.json()


@contextlib.contextmanager
def local_auth_server(token_cb, default_account_id, **overrides):
  class MockedProvider:
    def generate_access_token(self, account_id, scopes):
      return token_cb(account_id, scopes=scopes)
    def generate_id_token(self, account_id, audience):
      return token_cb(account_id, audience=audience)

  acc = lambda aid: auth_server.Account(id=aid, email=aid+'@example.com')

  s = auth_server.LocalAuthServer()
  try:
    local_auth = s.start(
        token_provider=MockedProvider(),
        accounts=(acc('acc_1'), acc('acc_2'), acc('acc_3')),
        default_account_id=default_account_id)
    local_auth.update(overrides)
    with luci_context.write(local_auth=local_auth):
      yield
  finally:
    s.stop()


class LocalAuthServerTest(auto_stub.TestCase):
  epoch = 12345678

  def setUp(self):
    super(LocalAuthServerTest, self).setUp()
    self.mock_time(0)

  def mock_time(self, delta):
    self.mock(time, 'time', lambda: self.epoch + delta)

  def test_accounts_in_ctx(self):
    def token_gen(_account_id, **_kwargs):
      self.fail('must not be called')
    with local_auth_server(token_gen, 'acc_1'):
      ctx = luci_context.read('local_auth')
      ctx.pop('rpc_port')
      ctx.pop('secret')
      self.assertEqual({
          'accounts': [
              {'email': 'acc_1@example.com', 'id': 'acc_1'},
              {'email': 'acc_2@example.com', 'id': 'acc_2'},
              {'email': 'acc_3@example.com', 'id': 'acc_3'},
          ],
         'default_account_id': 'acc_1',
      }, ctx)

  def test_access_tokens(self):
    calls = []
    def token_gen(account_id, scopes=None, audience=None):
      assert audience is None
      calls.append((account_id, scopes))
      return auth_server.AccessToken('tok_%s' % account_id, time.time() + 300)

    with local_auth_server(token_gen, 'acc_1'):
      # Grab initial token.
      resp = call_rpc('acc_1', scopes=['B', 'B', 'A', 'C'])
      self.assertEqual({
          'access_token': 'tok_acc_1',
          'expiry': self.epoch + 300
      }, resp)
      self.assertEqual([('acc_1', ('A', 'B', 'C'))], calls)
      del calls[:]

      # Reuses cached token until it is close to expiration.
      self.mock_time(60)
      resp = call_rpc('acc_1', scopes=['B', 'A', 'C'])
      self.assertEqual({
          'access_token': 'tok_acc_1',
          'expiry': self.epoch + 300
      }, resp)
      self.assertFalse(calls)

      # Asking for different account gives another token.
      resp = call_rpc('acc_2', scopes=['B', 'B', 'A', 'C'])
      self.assertEqual({
          'access_token': 'tok_acc_2',
          'expiry': self.epoch + 360
      }, resp)
      self.assertEqual([('acc_2', ('A', 'B', 'C'))], calls)
      del calls[:]

      # First token has expired. Generated new one.
      self.mock_time(300)
      resp = call_rpc('acc_1', scopes=['A', 'B', 'C'])
      self.assertEqual({
          'access_token': 'tok_acc_1',
          'expiry': self.epoch + 600
      }, resp)
      self.assertEqual([('acc_1', ('A', 'B', 'C'))], calls)

  def test_id_tokens(self):
    calls = []
    def token_gen(account_id, scopes=None, audience=None):
      assert scopes is None
      calls.append((account_id, audience))
      return auth_server.AccessToken('tok_%s' % account_id, time.time() + 300)

    with local_auth_server(token_gen, 'acc_1'):
      # Grab initial token.
      resp = call_rpc('acc_1', audience='some-audience')
      self.assertEqual({
          'id_token': 'tok_acc_1',
          'expiry': self.epoch + 300
      }, resp)
      self.assertEqual([('acc_1', 'some-audience')], calls)
      del calls[:]

      # Reuses cached token until it is close to expiration.
      self.mock_time(60)
      resp = call_rpc('acc_1', audience='some-audience')
      self.assertEqual({
          'id_token': 'tok_acc_1',
          'expiry': self.epoch + 300
      }, resp)
      self.assertFalse(calls)

      # Asking for different audience gives another token.
      resp = call_rpc('acc_1', audience='another-audience')
      self.assertEqual({
          'id_token': 'tok_acc_1',
          'expiry': self.epoch + 360
      }, resp)
      self.assertEqual([('acc_1', 'another-audience')], calls)
      del calls[:]

      # Asking for different account gives another token.
      resp = call_rpc('acc_2', audience='some-audience')
      self.assertEqual({
          'id_token': 'tok_acc_2',
          'expiry': self.epoch + 360
      }, resp)
      self.assertEqual([('acc_2', 'some-audience')], calls)
      del calls[:]

      # First token has expired. Generated new one.
      self.mock_time(300)
      resp = call_rpc('acc_1', audience='some-audience')
      self.assertEqual({
          'id_token': 'tok_acc_1',
          'expiry': self.epoch + 600
      }, resp)
      self.assertEqual([('acc_1', 'some-audience')], calls)

  def test_handles_token_errors(self):
    calls = []
    def token_gen(_account_id, **_kwargs):
      calls.append(1)
      raise auth_server.TokenError(123, 'error message')

    with local_auth_server(token_gen, 'acc_1'):
      self.assertEqual({
          'error_code': 123,
          'error_message': 'error message'
      }, call_rpc('acc_1', scopes=['B', 'B', 'A', 'C']))
      self.assertEqual(1, len(calls))

      # Errors are cached. Same error is returned.
      self.assertEqual({
          'error_code': 123,
          'error_message': 'error message'
      }, call_rpc('acc_1', scopes=['B', 'B', 'A', 'C']))
      self.assertEqual(1, len(calls))

  def test_http_level_errors(self):
    def token_gen(_account_id, **_kwargs):
      self.fail('must not be called')

    with local_auth_server(token_gen, 'acc_1'):
      # Wrong URL.
      ctx = luci_context.read('local_auth')
      r = requests.post(
          url='http://127.0.0.1:%d/blah/LuciLocalAuthService.GetOAuthToken' %
              ctx['rpc_port'],
          data=json.dumps({
            'account_id': 'acc_1',
            'scopes': ['A', 'B', 'C'],
            'secret': ctx['secret'],
          }),
          headers={'Content-Type': 'application/json'})
      self.assertEqual(404, r.status_code)

      # Wrong HTTP method.
      r = requests.get(
          url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.GetOAuthToken' %
              ctx['rpc_port'],
          data=json.dumps({
            'account_id': 'acc_1',
            'scopes': ['A', 'B', 'C'],
            'secret': ctx['secret'],
          }),
          headers={'Content-Type': 'application/json'})
      self.assertEqual(501, r.status_code)

      # Wrong content type.
      r = requests.post(
          url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.GetOAuthToken' %
              ctx['rpc_port'],
          data=json.dumps({
            'account_id': 'acc_1',
            'scopes': ['A', 'B', 'C'],
            'secret': ctx['secret'],
          }),
          headers={'Content-Type': 'application/xml'})
      self.assertEqual(400, r.status_code)

      # Bad JSON.
      r = requests.post(
          url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.GetOAuthToken' %
              ctx['rpc_port'],
          data='not a json',
          headers={'Content-Type': 'application/json'})
      self.assertEqual(400, r.status_code)

  def test_validation(self):
    def token_gen(_account_id, **_kwargs):
      self.fail('must not be called')

    with local_auth_server(token_gen, 'acc_1'):
      ctx = luci_context.read('local_auth')

      def must_fail(method, body, err, code):
        for m in ['GetOAuthToken', 'GetIDToken'] if method == '*' else [method]:
          r = requests.post(
              url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.%s' % (
                  ctx['rpc_port'], m),
              data=json.dumps(body),
              headers={'Content-Type': 'application/json'})
          self.assertEqual(code, r.status_code)
          self.assertIn(err, r.text)

      cases = [
        # account_id
        (
          '*',
          {},
          '"account_id" is required',
          400,
        ),
        (
          '*',
          {'account_id': 123},
          '"account_id" must be a string',
          400,
        ),

        # secret
        (
          '*',
          {'account_id': 'acc_1', 'scopes': ['a']},
          '"secret" is required',
          400,
        ),
        (
          '*',
          {'account_id': 'acc_1', 'scopes': ['a'], 'secret': 123},
          '"secret" must be a string',
          400,
        ),
        (
          '*',
          {'account_id': 'acc_1', 'scopes': ['a'], 'secret': 'abc'},
          'Invalid "secret"',
          403,
        ),

        # The account is known.
        (
          '*',
          {'account_id': 'zzz', 'scopes': ['a'], 'secret': ctx['secret']},
          'Unrecognized account ID',
          404,
        ),

        # scopes
        (
          'GetOAuthToken',
          {'account_id': 'acc_1', 'secret': ctx['secret']},
          '"scopes" is required',
          400,
        ),
        (
          'GetOAuthToken',
          {'account_id': 'acc_1', 'secret': ctx['secret'], 'scopes': []},
          '"scopes" is required',
          400,
        ),
        (
          'GetOAuthToken',
          {'account_id': 'acc_1', 'secret': ctx['secret'], 'scopes': 'abc'},
          '"scopes" must be a list of strings',
          400,
        ),
        (
          'GetOAuthToken',
          {'account_id': 'acc_1', 'secret': ctx['secret'], 'scopes': [1]},
          '"scopes" must be a list of strings',
          400,
        ),

        # audience
        (
          'GetIDToken',
          {'account_id': 'acc_1', 'secret': ctx['secret']},
          '"audience" is required',
          400,
        ),
        (
          'GetIDToken',
          {'account_id': 'acc_1', 'secret': ctx['secret'], 'audience': ''},
          '"audience" is required',
          400,
        ),
        (
          'GetIDToken',
          {'account_id': 'acc_1', 'secret': ctx['secret'], 'audience': 123},
          '"audience" must be a string',
          400,
        ),
      ]
      for method, body, err, code in cases:
        must_fail(method, body, err, code)


class LocalAuthHttpServiceTest(auto_stub.TestCase):
  """Tests for LocalAuthServer and LuciContextAuthenticator."""
  epoch = 12345678

  def setUp(self):
    super(LocalAuthHttpServiceTest, self).setUp()
    self.mock_time(0)

  def mock_time(self, delta):
    self.mock(time, 'time', lambda: self.epoch + delta)

  @staticmethod
  def mocked_http_service(
      url='http://example.com',
      perform_request=None):

    class MockedRequestEngine:
      def perform_request(self, request):
        return perform_request(request) if perform_request else None

    return net.HttpService(
        url,
        authenticator=authenticators.LuciContextAuthenticator(),
        engine=MockedRequestEngine())

  def test_works(self):
    service_url = 'http://example.com'
    request_url = '/some_request'
    response = b'True'
    token = 'notasecret'

    def token_gen(account_id, scopes=None, audience=None):
      self.assertEqual('acc_1', account_id)
      self.assertEqual(1, len(scopes))
      self.assertEqual(oauth.OAUTH_SCOPES, scopes[0])
      self.assertIsNone(audience)
      return auth_server.AccessToken(token, time.time() + 300)

    def handle_request(request):
      self.assertTrue(
          request.get_full_url().startswith(service_url + request_url))
      self.assertEqual(b'', request.body)
      self.assertEqual('Bearer %s' % token, request.headers['Authorization'])
      return net_utils.make_fake_response(response, request.get_full_url())

    with local_auth_server(token_gen, 'acc_1'):
      service = self.mocked_http_service(perform_request=handle_request)
      self.assertEqual(service.request(request_url, data={}).read(), response)

  def test_bad_secret(self):
    service_url = 'http://example.com'
    request_url = '/some_request'
    response = b'False'

    def token_gen(_account_id, **_kwargs):
      self.fail('must not be called')

    def handle_request(request):
      self.assertTrue(
          request.get_full_url().startswith(service_url + request_url))
      self.assertEqual(b'', request.body)
      self.assertIsNone(request.headers.get('Authorization'))
      return net_utils.make_fake_response(response, request.get_full_url())

    with local_auth_server(token_gen, 'acc_1', secret='invalid'):
      service = self.mocked_http_service(perform_request=handle_request)
      self.assertEqual(service.request(request_url, data={}).read(), response)

  def test_bad_port(self):
    request_url = '/some_request'

    def token_gen(_account_id, **_kwargs):
      self.fail('must not be called')

    def handle_request(_request):
      self.fail('must not be called')

    # This little dance should pick an unused port, bind it and then close it,
    # trusting that the OS will not reallocate it between now and when the http
    # client attempts to use it as a local_auth service. This is better than
    # picking a static port number, as there's at least some guarantee that the
    # port WASN'T in use before this test ran.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    sock.close()
    with local_auth_server(token_gen, 'acc_1', rpc_port=port):
      service = self.mocked_http_service(perform_request=handle_request)
      with self.assertRaises(ConnectionRefusedError):
        self.assertRaises(service.request(request_url, data={}).read())

  def test_expired_token(self):
    service_url = 'http://example.com'
    request_url = '/some_request'
    response = b'False'
    token = 'notasecret'

    def token_gen(account_id, scopes=None, audience=None):
      self.assertEqual('acc_1', account_id)
      self.assertEqual(1, len(scopes))
      self.assertEqual(oauth.OAUTH_SCOPES, scopes[0])
      self.assertIsNone(audience)
      return auth_server.AccessToken(token, time.time())

    def handle_request(request):
      self.assertTrue(
          request.get_full_url().startswith(service_url + request_url))
      self.assertEqual(b'', request.body)
      self.assertIsNone(request.headers.get('Authorization'))
      return net_utils.make_fake_response(response, request.get_full_url())

    with local_auth_server(token_gen, 'acc_1'):
      service = self.mocked_http_service(perform_request=handle_request)
      self.assertEqual(service.request(request_url, data={}).read(), response)


if __name__ == '__main__':
  # Terminate HTTP server in tests 50x faster. Impacts performance though so
  # do it only in tests.
  auth_server._HTTPServer.poll_interval = 0.01
  test_env.main()
