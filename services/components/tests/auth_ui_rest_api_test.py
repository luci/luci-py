#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Disable 'Method could be a function.'
# pylint: disable=R0201

import json
import sys
import unittest

import test_env
test_env.setup_test_env()

import test_case
import webapp2
import webtest

from components.auth import api
from components.auth import handler
from components.auth import model

from components.auth_ui import rest_api


def call_get(request_handler, expect_errors=False):
  """Calls request_handler's 'get' in a context of webtest app."""
  path = '/dummy_path'
  app = webtest.TestApp(webapp2.WSGIApplication([(path, request_handler)]))
  return app.get(path, expect_errors=expect_errors)


def call_post(request_handler, body, content_type, expect_errors=False):
  """Calls request_handler's 'post' in a context of webtest app."""
  path = '/dummy_path'
  app = webtest.TestApp(webapp2.WSGIApplication([(path, request_handler)]))
  return app.post(
      path, body, content_type=content_type, expect_errors=expect_errors)


class ApiHandlerClassTest(test_case.TestCase):
  """Tests for ApiHandler base class itself."""

  def setUp(self):
    super(ApiHandlerClassTest, self).setUp()
    # Reset global config.
    handler.configure([])
    # Catch errors in log.
    self.errors = []
    self.mock(handler.logging, 'error',
        lambda *args, **kwargs: self.errors.append((args, kwargs)))

  def test_authentication_error(self):
    """AuthenticationErrors are returned as JSON with status 401."""
    test = self

    def failing_auth(_request):
      raise api.AuthenticationError('Boom!')
    handler.configure([failing_auth])

    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        test.fail('Should not be called')

    response = call_get(Handler, expect_errors=True)
    self.assertEqual(401, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual({'text': 'Boom!'}, json.loads(response.body))

  def test_authorization_error(self):
    """AuthorizationErrors are returned as JSON with status 403."""
    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        raise api.AuthorizationError('Boom!')

    response = call_get(Handler, expect_errors=True)
    self.assertEqual(403, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual({'text': 'Boom!'}, json.loads(response.body))

  def test_send_response_simple(self):
    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        self.send_response({'some': 'response'})

    response = call_get(Handler)
    self.assertEqual(200, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual({'some': 'response'}, json.loads(response.body))

  def test_send_response_custom_status_code(self):
    """Non 200 status codes in 'send_response' work."""
    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        self.send_response({'some': 'response'}, http_code=302)

    response = call_get(Handler)
    self.assertEqual(302, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual({'some': 'response'}, json.loads(response.body))

  def test_send_response_custom_header(self):
    """Response headers in 'send_response' work."""
    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        self.send_response({'some': 'response'}, headers={'Some-Header': '123'})

    response = call_get(Handler)
    self.assertEqual(200, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual(
        '123', response.headers.get('Some-Header'))
    self.assertEqual({'some': 'response'}, json.loads(response.body))

  def test_abort_with_error(self):
    """'abort_with_error' aborts execution and returns error as JSON."""
    test = self

    class Handler(rest_api.ApiHandler):
      @api.public
      def get(self):
        self.abort_with_error(http_code=404, text='abc', stuff=123)
        test.fail('Should not be called')

    response = call_get(Handler, expect_errors=True)
    self.assertEqual(404, response.status_int)
    self.assertEqual(
        'application/json; charset=UTF-8', response.headers.get('Content-Type'))
    self.assertEqual({'text': 'abc', 'stuff': 123}, json.loads(response.body))

  def test_parse_body_success(self):
    """'parse_body' successfully decodes json-encoded dict in the body."""
    test = self

    class Handler(rest_api.ApiHandler):
      xsrf_token_enforce_on = ()
      @api.public
      def post(self):
        test.assertEqual({'abc': 123}, self.parse_body())

    response = call_post(
        Handler,
        json.dumps({'abc': 123}),
        'application/json; charset=UTF-8')
    self.assertEqual(200, response.status_int)

  def test_parse_body_bad_content_type(self):
    """'parse_body' checks Content-Type header."""
    test = self

    class Handler(rest_api.ApiHandler):
      xsrf_token_enforce_on = ()
      @api.public
      def post(self):
        self.parse_body()
        test.fail('Request should have been aborted')

    response = call_post(
        Handler,
        json.dumps({'abc': 123}),
        'application/xml; charset=UTF-8',
        expect_errors=True)
    self.assertEqual(400, response.status_int)

  def test_parse_body_bad_json(self):
    """'parse_body' returns HTTP 400 if body is not a valid json."""
    test = self

    class Handler(rest_api.ApiHandler):
      xsrf_token_enforce_on = ()
      @api.public
      def post(self):
        self.parse_body()
        test.fail('Request should have been aborted')

    response = call_post(
        Handler,
        'not-json',
        'application/json; charset=UTF-8',
        expect_errors=True)
    self.assertEqual(400, response.status_int)

  def test_parse_body_not_dict(self):
    """'parse_body' returns HTTP 400 if body is not a json dict."""
    test = self

    class Handler(rest_api.ApiHandler):
      xsrf_token_enforce_on = ()
      @api.public
      def post(self):
        self.parse_body()
        test.fail('Request should have been aborted')

    response = call_post(
        Handler,
        '[]',
        'application/json; charset=UTF-8',
        expect_errors=True)
    self.assertEqual(400, response.status_int)


class RestAPITestCase(test_case.TestCase):
  """Test case for some concrete Auth REST API handler.

  Handler should be defined in get_rest_api_routes().
  """

  def setUp(self):
    super(RestAPITestCase, self).setUp()
    # Make webtest app that can execute REST API requests.
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(rest_api.get_rest_api_routes()))
    # Reset global config.
    handler.configure([])
    # Catch errors in log.
    self.errors = []
    self.mock(handler.logging, 'error',
        lambda *args, **kwargs: self.errors.append((args, kwargs)))

  def _make_request(
      self, method, path, params=None, headers=None, expect_errors=False):
    response = getattr(self.app, method.lower())(
        path, params=params, headers=headers, expect_errors=expect_errors)
    # All REST API responses should be in JSON. Even errors.
    self.assertEqual(
        response.headers['Content-Type'],
        'application/json; charset=UTF-8')
    return response.status_int, json.loads(response.body)

  def get(self, path):
    return self._make_request('GET', path)

  def post(self, path, body=None, headers=None, expect_errors=False):
    if body:
      headers = dict(headers or {})
      headers['Content-Type'] = 'application/json; charset=UTF-8'
    return self._make_request(
        'POST',
        path,
        params=json.dumps(body) if body else '',
        headers=headers,
        expect_errors=expect_errors)


class OAuthConfigHandlerTest(RestAPITestCase):
  def test_non_configured_works(self):
    status, body = self.get('/auth/api/v1/server/oauth_config')
    self.assertEqual(200, status)
    self.assertEqual({'client_id': None, 'client_not_so_secret': None}, body)

  def test_configured_works(self):
    # Mock auth_db.get_oauth_config().
    fake_config = model.AuthGlobalConfig(
        oauth_client_id='some-client-id',
        oauth_client_secret='some-secret')
    self.mock(rest_api.api, 'get_request_auth_db',
        lambda: api.AuthDB(global_config=fake_config))
    # Call should return this data.
    expected = {
      'client_id': 'some-client-id',
      'client_not_so_secret': 'some-secret',
    }
    status, body = self.get('/auth/api/v1/server/oauth_config')
    self.assertEqual(200, status)
    self.assertEqual(expected, body)


class SelfHandlerTest(RestAPITestCase):
  def test_anonymous(self):
    status, body = self.get('/auth/api/v1/accounts/self')
    self.assertEqual(200, status)
    self.assertEqual({'identity': 'anonymous:anonymous'}, body)

  def test_non_anonymous(self):
    # Add fake authenticator.
    handler.configure([
        lambda _req: model.Identity(model.IDENTITY_USER, 'joe@example.com')])
    status, body = self.get('/auth/api/v1/accounts/self')
    self.assertEqual(200, status)
    self.assertEqual({'identity': 'user:joe@example.com'}, body)


class XSRFHandlerTest(RestAPITestCase):
  def test_works(self):
    status, body = self.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'})
    self.assertEqual(200, status)
    self.assertTrue(isinstance(body.get('xsrf_token'), basestring))

  def test_requires_header(self):
    status, body = self.post(
        '/auth/api/v1/accounts/self/xsrf_token', expect_errors=True)
    self.assertEqual(403, status)
    self.assertEqual({'text': 'Missing required XSRF request header'}, body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
