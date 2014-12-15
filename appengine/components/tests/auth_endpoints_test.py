#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

import endpoints
from protorpc import messages
from protorpc import remote

from support import test_case

from components.auth import api
from components.auth import endpoints_support
from components.auth import model


class EndpointsAuthTest(test_case.TestCase):
  """Tests for auth.endpoints_support.initialize_request_auth function."""

  def setUp(self):
    super(EndpointsAuthTest, self).setUp()
    self.mock(endpoints_support.logging, 'error', lambda *_args: None)

  def call(self, remote_address, email):
    """Mocks current user calls initialize_request_auth."""

    class User(object):
      def email(self):
        return email
    self.mock(
        endpoints_support.endpoints, 'get_current_user',
        lambda: User() if email else None)

    api.reset_local_state()
    endpoints_support.initialize_request_auth(remote_address)
    return api.get_current_identity().to_bytes()

  def test_ip_whitelist_bot(self):
    """Requests from client in "bots" IP whitelist are authenticated as bot."""
    model.bootstrap_ip_whitelist('bots', ['192.168.1.100/32'])
    self.assertEqual('bot:192.168.1.100', self.call('192.168.1.100', None))
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


@endpoints.api(name='testing', version='v1')
class TestingServiceApi(remote.Service):
  """Used as an example Endpoints service below."""

  class Requests(messages.Message):
    param1 = messages.StringField(1)
    param2 = messages.StringField(2)
    raise_error = messages.BooleanField(3)

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
