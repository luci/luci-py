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

import webapp2
import webtest

from support import test_case
from components import utils

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


def make_xsrf_token(identity=model.Anonymous, xsrf_token_data=None):
  """Returns XSRF token that can be used in tests."""
  # See handler.AuthenticatingHandler.generate
  return handler.XSRFToken.generate([identity.to_bytes()], xsrf_token_data)


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
        webapp2.WSGIApplication(rest_api.get_rest_api_routes(), debug=True))
    # Reset global config and cached state.
    handler.configure([])
    api.reset_local_state()
    self.mocked_identity = model.Anonymous
    # Catch errors in log.
    self.errors = []
    self.mock(handler.logging, 'error',
        lambda *args, **kwargs: self.errors.append((args, kwargs)))

  def mock_ndb_now(self, now):
    """Makes properties with |auto_now| and |auto_now_add| use mocked time."""
    self.mock(model.ndb.DateTimeProperty, '_now', lambda _: now)
    self.mock(model.ndb.DateProperty, '_now', lambda _: now.date())

  def mock_current_identity(self, identity):
    """Makes api.get_current_identity() return predefined value."""
    self.mocked_identity = identity
    self.mock(api, 'get_current_identity', lambda: identity)

  def make_request(
      self,
      method,
      path,
      params=None,
      headers=None,
      expect_errors=False,
      expect_xsrf_token_check=False,
      expected_auth_checks=()):
    """Sends a request to the app via webtest framework.

    Args:
      method: HTTP method of a request (like 'GET' or 'POST').
      path: request path.
      params: for GET, it's a query string, for POST/PUT its a body, etc. See
          webtest docs.
      headers: a dict with request headers.
      expect_errors: True if call is expected to end with some HTTP error code.
      expect_xsrf_token_check: if True, will append X-XSRF-Token header to the
          request and will verify that request handler checked it.
      expected_auth_checks: a list of pairs (<action>, <resource name>) that
          specifies expected ACL checks.

    Returns:
      Tuple (status code, deserialized JSON response, response headers).
    """
    # Catch calls to has_permissions and compare it to next expected check.
    expected_auth_checks = list(expected_auth_checks)
    def has_permission_mock(action, resource):
      self.assertTrue(expected_auth_checks)
      self.assertEqual(expected_auth_checks[0], (action, resource))
      expected_auth_checks.pop(0)
      return True
    self.mock(rest_api.api, 'has_permission', has_permission_mock)

    # Add XSRF header if required.
    headers = dict(headers or {})
    if expect_xsrf_token_check:
      assert 'X-XSRF-Token' not in headers
      headers['X-XSRF-Token'] = make_xsrf_token(self.mocked_identity)

    # Hook XSRF token check to verify it's called.
    original_validate = handler.XSRFToken.validate
    xsrf_token_validate_calls = []
    def mocked_validate(*args, **kwargs):
      xsrf_token_validate_calls.append((args, kwargs))
      return original_validate(*args, **kwargs)
    self.mock(handler.XSRFToken, 'validate', staticmethod(mocked_validate))

    # Do the call. Pass |params| only if not None, otherwise use whatever
    # webtest method uses by default (for 'DELETE' it's something called
    # utils.NoDefault and it's treated differently from None).
    kwargs = {'headers': headers, 'expect_errors': expect_errors}
    if params is not None:
      kwargs['params'] = params
    response = getattr(self.app, method.lower())(path, **kwargs)

    # Ensure XSRF token was checked.
    if expect_xsrf_token_check:
      self.assertEqual(1, len(xsrf_token_validate_calls))

    # Ensure all expected auth checks were executed.
    self.assertFalse(expected_auth_checks)

    # All REST API responses should be in JSON. Even errors. If content type
    # is not application/json, then response was generated by webapp2 itself,
    # show it in error message (it's usually some sort of error page).
    self.assertEqual(
        response.headers['Content-Type'],
        'application/json; charset=UTF-8',
        msg=response.body)
    return response.status_int, json.loads(response.body), response.headers

  def get(self, path, **kwargs):
    """Sends GET request to REST API endpoint.

    Returns tuple (status code, deserialized JSON response, response headers).
    """
    return self.make_request('GET', path, **kwargs)

  def post(self, path, body=None, **kwargs):
    """Sends POST request to REST API endpoint.

    Returns tuple (status code, deserialized JSON response, response headers).
    """
    assert 'params' not in kwargs
    headers = dict(kwargs.pop('headers', None) or {})
    if body:
      headers['Content-Type'] = 'application/json; charset=UTF-8'
    kwargs['headers'] = headers
    kwargs['params'] = json.dumps(body) if body is not None else ''
    return self.make_request('POST', path, **kwargs)

  def delete(self, path, **kwargs):
    """Sends DELETE request to REST API endpoint.

    Returns tuple (status code, deserialized JSON response, response headers).
    """
    return self.make_request('DELETE', path, **kwargs)


################################################################################
## Test cases for REST end points.


class SelfHandlerTest(RestAPITestCase):
  def test_anonymous(self):
    status, body, _ = self.get('/auth/api/v1/accounts/self')
    self.assertEqual(200, status)
    self.assertEqual({'identity': 'anonymous:anonymous'}, body)

  def test_non_anonymous(self):
    # Add fake authenticator.
    handler.configure([
        lambda _req: model.Identity(model.IDENTITY_USER, 'joe@example.com')])
    status, body, _ = self.get('/auth/api/v1/accounts/self')
    self.assertEqual(200, status)
    self.assertEqual({'identity': 'user:joe@example.com'}, body)


class XSRFHandlerTest(RestAPITestCase):
  def test_works(self):
    status, body, _ = self.post(
        path='/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'})
    self.assertEqual(200, status)
    self.assertTrue(isinstance(body.get('xsrf_token'), basestring))

  def test_requires_header(self):
    status, body, _ = self.post(
        path='/auth/api/v1/accounts/self/xsrf_token',
        expect_errors=True)
    self.assertEqual(403, status)
    self.assertEqual({'text': 'Missing required XSRF request header'}, body)


class GroupsHandlerTest(RestAPITestCase):
  def test_empty_list(self):
    status, body, _ = self.get(
        path='/auth/api/v1/groups',
        expected_auth_checks=[(model.READ, 'auth/management')])
    self.assertEqual(200, status)
    self.assertEqual({'groups': []}, body)

  def test_non_empty_list(self):
    # Freeze time in NDB's |auto_now| properties.
    self.mock_ndb_now(utils.timestamp_to_datetime(1300000000000000))

    # Create a bunch of groups with all kinds of members.
    for i in xrange(0, 5):
      group = model.AuthGroup(
          id='Test group %d' % i,
          parent=model.ROOT_KEY,
          created_by=model.Identity.from_bytes('user:creator@example.com'),
          description='Group for testing, #%d' % i,
          modified_by=model.Identity.from_bytes('user:modifier@example.com'),
          members=[model.Identity.from_bytes('user:joe@example.com')],
          globs=[model.IdentityGlob.from_bytes('user:*@example.com')])
      group.put()

    # Group Listing should return all groups. Member lists should be omitted.
    # Order is alphabetical by name,
    status, body, _ = self.get(
        path='/auth/api/v1/groups',
        expected_auth_checks=[(model.READ, 'auth/management')])
    self.assertEqual(200, status)
    self.assertEqual(
      {
        'groups': [
          {
            'created_by': 'user:creator@example.com',
            'created_ts': 1300000000000000,
            'description': 'Group for testing, #%d' % i,
            'modified_by': 'user:modifier@example.com',
            'modified_ts': 1300000000000000,
            'name': 'Test group %d' % i,
          } for i in xrange(0, 5)
        ],
      }, body)


class GroupHandlerTest(RestAPITestCase):
  def test_get_missing(self):
    status, body, _ = self.get(
        path='/auth/api/v1/groups/a%20group',
        expect_errors=True,
        expected_auth_checks=[(model.READ, 'auth/management/groups/a group')])
    self.assertEqual(404, status)
    self.assertEqual({'text': 'No such group'}, body)

  def test_get_existing(self):
    # Freeze time in NDB's |auto_now| properties.
    self.mock_ndb_now(utils.timestamp_to_datetime(1300000000000000))

    # Create a group with all kinds of members.
    group = model.AuthGroup(
        id='A Group',
        parent=model.ROOT_KEY,
        created_by=model.Identity.from_bytes('user:creator@example.com'),
        description='Group for testing',
        modified_by=model.Identity.from_bytes('user:modifier@example.com'),
        members=[model.Identity.from_bytes('user:joe@example.com')],
        globs=[model.IdentityGlob.from_bytes('user:*@example.com')])
    group.put()

    # Fetch it via API call.
    status, body, headers = self.get(
        path='/auth/api/v1/groups/A%20Group',
        expected_auth_checks=[(model.READ, 'auth/management/groups/A Group')])
    self.assertEqual(200, status)
    self.assertEqual(
      {
        'group': {
          'created_by': 'user:creator@example.com',
          'created_ts': 1300000000000000,
          'description': 'Group for testing',
          'globs': ['user:*@example.com'],
          'members': ['user:joe@example.com'],
          'modified_by': 'user:modifier@example.com',
          'modified_ts': 1300000000000000,
          'name': 'A Group',
          'nested': [],
        },
      }, body)
    self.assertEqual(
        'Sun, 13 Mar 2011 07:06:40 -0000',
        headers['Last-Modified'])

  def test_delete_existing(self):
    # Create a group.
    group = model.AuthGroup(id='A Group', parent=model.ROOT_KEY)
    group.put()

    # Delete it via API.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A%20Group',
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(200, status)
    self.assertEqual({'ok': True}, body)

    # It is gone.
    self.assertFalse(
        model.AuthGroup.get_by_id(id='A Group', parent=model.ROOT_KEY))

  def test_delete_existing_with_condition_ok(self):
    # Create a group.
    group = model.AuthGroup(id='A Group', parent=model.ROOT_KEY)
    group.put()

    # Delete it via API using passing If-Unmodified-Since condition.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A%20Group',
        headers={
          'If-Unmodified-Since': utils.datetime_to_rfc2822(group.modified_ts),
        },
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(200, status)
    self.assertEqual({'ok': True}, body)

    # It is gone.
    self.assertFalse(
        model.AuthGroup.get_by_id(id='A Group', parent=model.ROOT_KEY))

  def test_delete_existing_with_condition_fail(self):
    # Create a group.
    group = model.AuthGroup(id='A Group', parent=model.ROOT_KEY)
    group.put()

    # Try to delete it via API using failing If-Unmodified-Since condition.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A%20Group',
        headers={
          'If-Unmodified-Since': 'Sun, 1 Mar 1990 00:00:00 -0000',
        },
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(412, status)
    self.assertEqual({'text': 'Group was modified by someone else'}, body)

    # It is still there.
    self.assertTrue(
        model.AuthGroup.get_by_id(id='A Group', parent=model.ROOT_KEY))

  def test_delete_referenced_group(self):
    # Create a group.
    group = model.AuthGroup(id='A Group', parent=model.ROOT_KEY)
    group.put()

    # Create another group that includes first one as nested.
    another = model.AuthGroup(
        id='Another group',
        parent=model.ROOT_KEY,
        nested=['A Group'])
    another.put()

    # Create a service that uses the group in its ACL rules.
    service = model.AuthServiceConfig(
        id='Some service',
        parent=model.ROOT_KEY,
        rules=[
          model.AccessRule(model.ALLOW_RULE, 'A Group', [model.READ], '^.*$')
        ])
    service.put()

    # Try to delete it via API.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A%20Group',
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(409, status)
    self.assertEqual(
        {
          'text': 'Group is being referenced in rules or other groups',
          'details': {
            'groups': ['Another group'],
            'services': ['Some service'],
          },
        }, body)

    # It is still there.
    self.assertTrue(
        model.AuthGroup.get_by_id(id='A Group', parent=model.ROOT_KEY))

  def test_delete_missing(self):
    # Unconditionally deleting a group that's not there is ok.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A Group',
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(200, status)
    self.assertEqual({'ok': True}, body)

  def test_delete_missing_with_condition(self):
    # Deleting missing group with condition is a error.
    status, body, _ = self.delete(
        path='/auth/api/v1/groups/A%20Group',
        headers={
          'If-Unmodified-Since': 'Sun, 1 Mar 1990 00:00:00 -0000',
        },
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.DELETE, 'auth/management/groups/A Group')])
    self.assertEqual(412, status)
    self.assertEqual({'text': 'Group was deleted by someone else'}, body)

  def test_post_success(self):
    frozen_time = utils.timestamp_to_datetime(1300000000000000)
    creator_identity = model.Identity.from_bytes('user:creator@example.com')

    # Freeze time in NDB's |auto_now| properties.
    self.mock_ndb_now(frozen_time)
    # get_current_identity is used for 'created_by' and 'modified_by'.
    self.mock_current_identity(creator_identity)

    # Create a group to act as a nested one.
    group = model.AuthGroup(id='Nested Group', parent=model.ROOT_KEY)
    group.put()

    # Create the group using REST API.
    status, body, headers = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={
          'description': 'Test group',
          'globs': ['user:*@example.com'],
          'members': ['bot:some-bot', 'user:some@example.com'],
          'name': 'A Group',
          'nested': ['Nested Group'],
        },
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(201, status)
    self.assertEqual({'ok': True}, body)
    self.assertEqual(
        'Sun, 13 Mar 2011 07:06:40 -0000', headers['Last-Modified'])
    self.assertEqual(
        'http://localhost/auth/api/v1/groups/A%20Group', headers['Location'])

    # Ensure it's there and all fields are set.
    entity = model.AuthGroup.get_by_id('A Group', parent=model.ROOT_KEY)
    self.assertTrue(entity)
    expected = {
      'created_by': model.Identity(kind='user', name='creator@example.com'),
      'created_ts': frozen_time,
      'description': 'Test group',
      'globs': [model.IdentityGlob(kind='user', pattern='*@example.com')],
      'members': [
        model.Identity(kind='bot', name='some-bot'),
        model.Identity(kind='user', name='some@example.com'),
      ],
      'modified_by': model.Identity(kind='user', name='creator@example.com'),
      'modified_ts': frozen_time,
      'nested': ['Nested Group']
    }
    self.assertEqual(expected, entity.to_dict())

  def test_post_minimal_body(self):
    # Posting just a name is enough to create an empty group.
    status, body, _ = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={'name': 'A Group'},
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(201, status)
    self.assertEqual({'ok': True}, body)

  def test_post_mismatching_name(self):
    # 'name' key and name in URL should match.
    status, body, _ = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={'name': 'Another name here'},
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(400, status)
    self.assertEqual(
        {'text': 'Missing or mismatching group name in request body'}, body)

  def test_post_bad_body(self):
    # Posting invalid body ('members' should be a list, not a dict).
    status, body, _ = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={'name': 'A Group', 'members': {}},
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(400, status)
    self.assertEqual(
        {
          'text':
            'Expecting a list or tuple for \'members\', got \'dict\' instead',
        },
        body)

  def test_post_already_exists(self):
    # Make a group first.
    group = model.AuthGroup(id='A Group', parent=model.ROOT_KEY)
    group.put()

    # Now try to recreate it again via API. Should fail with HTTP 409.
    status, body, _ = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={'name': 'A Group'},
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(409, status)
    self.assertEqual({'text': 'Such group already exists'}, body)

  def test_post_missing_nested(self):
    # Try to create a group that references non-existing nested group.
    status, body, _ = self.post(
        path='/auth/api/v1/groups/A%20Group',
        body={'name': 'A Group', 'nested': ['Missing group']},
        expect_errors=True,
        expect_xsrf_token_check=True,
        expected_auth_checks=[(model.CREATE, 'auth/management/groups/A Group')])
    self.assertEqual(409, status)
    self.assertEqual(
      {
        'text': 'Referencing a nested group that doesn\'t exist',
        'details': {
          'missing': ['Missing group'],
        }
      }, body)


class OAuthConfigHandlerTest(RestAPITestCase):
  def test_non_configured_works(self):
    expected = {
      'additional_client_ids': [],
      'client_id': None,
      'client_not_so_secret': None,
    }
    status, body, _ = self.get('/auth/api/v1/server/oauth_config')
    self.assertEqual(200, status)
    self.assertEqual(expected, body)

  def test_configured_works(self):
    # Mock auth_db.get_oauth_config().
    fake_config = model.AuthGlobalConfig(
        oauth_client_id='some-client-id',
        oauth_client_secret='some-secret',
        oauth_additional_client_ids=['a', 'b', 'c'])
    self.mock(rest_api.api, 'get_request_auth_db',
        lambda: api.AuthDB(global_config=fake_config))
    # Call should return this data.
    expected = {
      'additional_client_ids': ['a', 'b', 'c'],
      'client_id': 'some-client-id',
      'client_not_so_secret': 'some-secret',
    }
    status, body, _ = self.get('/auth/api/v1/server/oauth_config')
    self.assertEqual(200, status)
    self.assertEqual(expected, body)

  def test_no_cache_works(self):
    # Put something into DB.
    config_in_db = model.AuthGlobalConfig(
        key=model.ROOT_KEY,
        oauth_client_id='config-from-db',
        oauth_client_secret='some-secret-db',
        oauth_additional_client_ids=['a', 'b'])
    config_in_db.put()

    # Put another version into auth DB cache.
    config_in_cache = model.AuthGlobalConfig(
        oauth_client_id='config-from-cache',
        oauth_client_secret='some-secret-cache',
        oauth_additional_client_ids=['c', 'd'])
    self.mock(rest_api.api, 'get_request_auth_db',
        lambda: api.AuthDB(global_config=config_in_cache))

    # Without cache control header a cached version is used.
    expected = {
      'additional_client_ids': ['c', 'd'],
      'client_id': 'config-from-cache',
      'client_not_so_secret': 'some-secret-cache',
    }
    status, body, _ = self.get('/auth/api/v1/server/oauth_config')
    self.assertEqual(200, status)
    self.assertEqual(expected, body)

    # With cache control header a version from DB is used.
    expected = {
      'additional_client_ids': ['a', 'b'],
      'client_id': 'config-from-db',
      'client_not_so_secret': 'some-secret-db',
    }
    status, body, _ = self.get(
        path='/auth/api/v1/server/oauth_config',
        headers={'Cache-Control': 'no-cache'})
    self.assertEqual(200, status)
    self.assertEqual(expected, body)

  def test_post_works(self):
    # Send POST.
    request_body = {
      'additional_client_ids': ['1', '2', '3'],
      'client_id': 'some-client-id',
      'client_not_so_secret': 'some-secret',
    }
    headers = {
      'X-XSRF-Token': make_xsrf_token(),
    }
    status, response, _ = self.post(
        path='/auth/api/v1/server/oauth_config',
        body=request_body,
        headers=headers,
        expected_auth_checks=[(model.UPDATE, 'auth/management')])
    self.assertEqual(200, status)
    self.assertEqual({'ok': True}, response)

    # Ensure it modified the state in DB.
    config = model.ROOT_KEY.get()
    self.assertEqual('some-client-id', config.oauth_client_id)
    self.assertEqual('some-secret', config.oauth_client_secret)
    self.assertEqual(['1', '2', '3'], config.oauth_additional_client_ids)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
