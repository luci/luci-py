#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Disable 'Unused variable', 'Unused argument' and 'Method could be a function'.
# pylint: disable=W0612,W0613,R0201

import os
import sys
import unittest

import test_env
test_env.setup_test_env()

import test_case
import webapp2
import webtest

from google.appengine.api import oauth
from google.appengine.api import users

from components.auth import api
from components.auth import handler
from components.auth import model


class AuthenticatingHandlerMetaclassTest(test_case.TestCase):
  """Tests for AuthenticatingHandlerMetaclass."""

  def test_good(self):
    # No request handling methods defined at all.
    class TestHandler1(handler.AuthenticatingHandler):
      def some_other_method(self):
        pass

    # @public is used.
    class TestHandler2(handler.AuthenticatingHandler):
      @api.public
      def get(self):
        pass

    # @require is used.
    class TestHandler3(handler.AuthenticatingHandler):
      @api.require(model.READ, 'some')
      def get(self):
        pass

  def test_bad(self):
    # @public or @require is missing.
    with self.assertRaises(TypeError):
      class TestHandler1(handler.AuthenticatingHandler):
        def get(self):
          pass


class AuthenticatingHandlerTest(test_case.TestCase):
  """Tests for AuthenticatingHandler class."""

  def setUp(self):
    super(AuthenticatingHandlerTest, self).setUp()
    # Reset global config of auth library before each test.
    handler.configure([])
    # Capture error log messages.
    self.logged_errors = []
    self.mock(
        handler.logging, 'error', lambda *args: self.logged_errors.append(args))

  def make_test_app(self, path, request_handler):
    """Returns webtest.TestApp with single route."""
    return webtest.TestApp(webapp2.WSGIApplication([(path, request_handler)]))

  def test_anonymous(self):
    """If all auth methods are not applicable, identity is set to Anonymous."""
    test = self

    class Handler(handler.AuthenticatingHandler):
      @api.public
      def get(self):
        test.assertEqual(model.Anonymous, api.get_current_identity())
        self.response.write('OK')

    app = self.make_test_app('/request', Handler)
    self.assertEqual('OK', app.get('/request').body)

  def test_auth_method_order(self):
    """Registered auth methods are tested in order."""
    test = self
    calls = []
    ident = model.Identity(model.IDENTITY_USER, 'joe@example.com')

    def not_applicable(request):
      self.assertEqual('/request', request.path)
      calls.append('not_applicable')
      return None

    def applicable(request):
      self.assertEqual('/request', request.path)
      calls.append('applicable')
      return ident

    class Handler(handler.AuthenticatingHandler):
      @api.public
      def get(self):
        test.assertEqual(ident, api.get_current_identity())
        self.response.write('OK')

    handler.configure([not_applicable, applicable])
    app = self.make_test_app('/request', Handler)
    self.assertEqual('OK', app.get('/request').body)

    # Both methods should be tried.
    expected_calls = [
      'not_applicable',
      'applicable',
    ]
    self.assertEqual(expected_calls, calls)

  def test_authentication_error(self):
    """AuthenticationError in auth method stops request processing."""
    test = self
    calls = []

    def failing(request):
      raise api.AuthenticationError('Too bad')

    def skipped(request):
      self.fail('authenticate should not be called')

    class Handler(handler.AuthenticatingHandler):
      @api.public
      def get(self):
        test.fail('Handler code should not be called')

      def authentication_error(self, err):
        test.assertEqual('Too bad', err.message)
        calls.append('authentication_error')
        super(Handler, self).authentication_error(err)

    handler.configure([failing, skipped])
    app = self.make_test_app('/request', Handler)
    response = app.get('/request', expect_errors=True)

    # Custom error handler is called and returned HTTP 401.
    self.assertEqual(['authentication_error'], calls)
    self.assertEqual(401, response.status_int)

    # Authentication error is logged.
    self.assertEqual(1, len(self.logged_errors))

  def test_authorization_error(self):
    """AuthorizationError in auth method is handled."""
    test = self
    calls = []

    # Forbid all access.
    self.mock(handler.api, 'has_permission', lambda *_args: False)

    class Handler(handler.AuthenticatingHandler):
      @api.require(model.READ, 'some')
      def get(self):
        test.fail('Handler code should not be called')

      def authorization_error(self, err):
        calls.append('authorization_error')
        super(Handler, self).authorization_error(err)

    app = self.make_test_app('/request', Handler)
    response = app.get('/request', expect_errors=True)

    # Custom error handler is called and returned HTTP 403.
    self.assertEqual(['authorization_error'], calls)
    self.assertEqual(403, response.status_int)

    # Authorization error is logged.
    self.assertEqual(1, len(self.logged_errors))


class CookieAuthenticationTest(test_case.TestCase):
  """Tests for cookie_authentication function."""

  def test_non_applicable(self):
    self.assertIsNone(handler.cookie_authentication(webapp2.Request({})))

  def test_applicable(self):
    os.environ.update({
      'USER_EMAIL': 'joe@example.com',
      'USER_ID': '123',
      'USER_IS_ADMIN': '0',
    })
    # Actual request is not used by CookieAuthentication.
    self.assertEqual(
        model.Identity(model.IDENTITY_USER, 'joe@example.com'),
        handler.cookie_authentication(webapp2.Request({})))


class OAuthAuthenticationTest(test_case.TestCase):
  """Tests for oauth_authentication."""

  @staticmethod
  def make_request():
    return webapp2.Request({'HTTP_AUTHORIZATION': 'Bearer 123'})

  def mock_allowed_client_id(self, client_id):
    auth_db = api.AuthDB(
        global_config=model.AuthGlobalConfig(oauth_client_id=client_id))
    self.mock(handler.api, 'get_request_auth_db', lambda: auth_db)

  def test_non_applicable(self):
    self.assertIsNone(handler.oauth_authentication(webapp2.Request({})))

  def test_applicable(self):
    self.mock_allowed_client_id('allowed-client-id')
    self.mock(handler.oauth, 'get_client_id', lambda _arg: 'allowed-client-id')
    self.mock(handler.oauth, 'get_current_user',
        lambda _arg: users.User(email='joe@example.com'))
    self.assertEqual(
        model.Identity(model.IDENTITY_USER, 'joe@example.com'),
        handler.oauth_authentication(self.make_request()))

  def test_bad_token(self):
    def mocked_get_client_id(_arg):
      raise oauth.NotAllowedError()
    self.mock(handler.oauth, 'get_client_id', mocked_get_client_id)

    with self.assertRaises(api.AuthenticationError):
      handler.oauth_authentication(self.make_request())

  def test_not_allowed_client_id(self):
    self.mock_allowed_client_id('good-client-id')
    self.mock(handler.oauth, 'get_client_id', lambda _arg: 'bad-client-id')
    with self.assertRaises(api.AuthenticationError):
      handler.oauth_authentication(self.make_request())


class ServiceToServiceAuthenticationTest(test_case.TestCase):
  """Tests for service_to_service_authentication."""

  def test_non_applicable(self):
    request = webapp2.Request({})
    self.assertIsNone(
        handler.service_to_service_authentication(request))

  def test_applicable(self):
    request = webapp2.Request({
      'HTTP_X_APPENGINE_INBOUND_APPID': 'some-app',
    })
    self.assertEqual(
      model.Identity(model.IDENTITY_SERVICE, 'some-app'),
      handler.service_to_service_authentication(request))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
