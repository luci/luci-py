#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import utils
from components.auth import api
from components.auth import delegation
from components.auth import endpoints_support
from components.auth import ipaddr
from components.auth import model
from components.auth import tokens
from components.auth.proto import delegation_pb2
from test_support import test_case


class EndpointsAuthTest(test_case.TestCase):
  """Tests for auth.endpoints_support.initialize_request_auth function."""

  # pylint: disable=unused-argument

  def setUp(self):
    super(EndpointsAuthTest, self).setUp()
    self.mock(logging, 'error', lambda *_args: None)
    self.mock(logging, 'warning', lambda *_args: None)
    self.mock(delegation, 'get_trusted_signers', self.mock_get_trusted_signers)

  def mock_get_trusted_signers(self):
    return {'user:token-server@example.com': self}

  # Implements CertificateBundle interface, as used in mock_get_trusted_signers.
  def check_signature(self, blob, key_name, signature):
    return True

  def call(self, remote_address, email, headers=None):
    """Mocks current user in initialize_request_auth."""
    headers = (headers or {}).copy()
    if email:
      headers['Authorization'] = 'Bearer %s' % email

    # Mock ours auth.
    ident = model.Anonymous
    if email:
      ident = model.Identity(model.IDENTITY_USER, email)
    self.mock(api, 'check_oauth_access_token', lambda _: (ident, None))

    # Mock auth implemented by the Cloud Endpoints.
    class User(object):
      def email(self):
        return email
    self.mock(
        endpoints_support.endpoints, 'get_current_user',
        lambda: User() if email else None)

    api.reset_local_state()
    endpoints_support.initialize_request_auth(remote_address, headers)
    return api.get_current_identity().to_bytes()

  def test_ip_whitelist_bot(self):
    """Requests from client in bots IP whitelist are authenticated as bot."""
    model.bootstrap_ip_whitelist(
        model.bots_ip_whitelist(), ['192.168.1.100/32'])
    self.assertEqual('bot:whitelisted-ip', self.call('192.168.1.100', None))
    self.assertEqual('anonymous:anonymous', self.call('127.0.0.1', None))

  def test_ip_whitelist_whitelisted(self):
    """Per-account IP whitelist works."""
    model.bootstrap_ip_whitelist('whitelist', ['192.168.1.100/32'])
    model.bootstrap_ip_whitelist_assignment(
        model.Identity(model.IDENTITY_USER, 'a@example.com'), 'whitelist')
    self.assertEqual(
        'user:a@example.com',
        self.call('192.168.1.100', 'a@example.com'))

  def test_ip_whitelist_not_whitelisted(self):
    """Per-account IP whitelist works."""
    model.bootstrap_ip_whitelist('whitelist', ['192.168.1.100/32'])
    model.bootstrap_ip_whitelist_assignment(
        model.Identity(model.IDENTITY_USER, 'a@example.com'), 'whitelist')
    with self.assertRaises(api.AuthorizationError):
      self.call('127.0.0.1', 'a@example.com')

  def test_ip_whitelist_not_used(self):
    """Per-account IP whitelist works."""
    model.bootstrap_ip_whitelist('whitelist', ['192.168.1.100/32'])
    model.bootstrap_ip_whitelist_assignment(
        model.Identity(model.IDENTITY_USER, 'a@example.com'), 'whitelist')
    self.assertEqual(
        'user:another_user@example.com',
        self.call('127.0.0.1', 'another_user@example.com'))

  def test_get_peer_ip(self):
    """IP address is stored in auth context."""
    self.call('1.2.3.4', 'user@example.com')
    self.assertEqual(ipaddr.ip_from_string('1.2.3.4'), api.get_peer_ip())

  def test_delegation_token(self):
    def call(tok=None):
      headers = {'X-Delegation-Token-V1': tok} if tok else None
      self.call('127.0.0.1', 'peer@a.com', headers)
      return {
        'cur_id': api.get_current_identity().to_bytes(),
        'peer_id': api.get_current_identity().to_bytes(),
      }

    # No delegation.
    self.assertEqual(
        {'cur_id': 'user:peer@a.com', 'peer_id': 'user:peer@a.com'}, call())

    # Grab a fake-signed delegation token.
    subtoken = delegation_pb2.Subtoken(
        delegated_identity='user:delegated@a.com',
        kind=delegation_pb2.Subtoken.BEARER_DELEGATION_TOKEN,
        audience=['*'],
        services=['*'],
        creation_time=int(utils.time_time()),
        validity_duration=3600)
    tok_pb = delegation_pb2.DelegationToken(
      serialized_subtoken=subtoken.SerializeToString(),
      signer_id='user:token-server@example.com',
      signing_key_id='signing-key',
      pkcs1_sha256_sig='fake-signature')
    tok = tokens.base64_encode(tok_pb.SerializeToString())

    # Valid delegation token.
    self.assertEqual(
        {'cur_id': 'user:delegated@a.com', 'peer_id': 'user:delegated@a.com'},
        call(tok))

    # Invalid delegation token.
    with self.assertRaises(api.AuthorizationError):
      call(tok + 'blah')


@endpoints.api(name='testing', version='v1')
class TestingServiceApi(remote.Service):
  """Used as an example Endpoints service below."""

  Requests = endpoints.ResourceContainer(
      message_types.VoidMessage,
      param1=messages.StringField(1),
      param2=messages.StringField(2),
      raise_error=messages.BooleanField(3))

  class Response(messages.Message):
    param1 = messages.StringField(1)
    param2 = messages.StringField(2)

  @endpoints.method(
      Requests,
      Response,
      name='public_method_name',
      http_method='GET')
  def real_method_name(self, request):
    if request.raise_error:
      raise endpoints.BadRequestException()
    return self.Response(param1=request.param1, param2=request.param2)


class EndpointsTestCaseTest(test_case.EndpointsTestCase):
  api_service_cls = TestingServiceApi

  def test_ok(self):
    response = self.call_api(
        method='real_method_name',
        body={'param1': 'a', 'param2': 'b', 'raise_error': False})
    self.assertEqual({'param1': 'a', 'param2': 'b'}, response.json_body)

  def test_fail(self):
    with self.call_should_fail(400):
      self.call_api(
          method='real_method_name',
          body={'param1': 'a', 'param2': 'b', 'raise_error': True})


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
