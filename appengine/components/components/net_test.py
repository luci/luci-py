#!/usr/bin/env vpython
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import collections
import logging
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.api import urlfetch
from google.appengine.ext import ndb

from components import auth
from components import net
from test_support import test_case

Response = collections.namedtuple('Response', 'status_code content headers')


class NetTest(test_case.TestCase):

  def setUp(self):
    super(NetTest, self).setUp()

    @ndb.tasklet
    def get_access_token(*_args):
      raise ndb.Return(('own-token', 0))
    self.mock(auth, 'get_access_token_async', get_access_token)

    @ndb.tasklet
    def get_project_access_token(project_id, _scopes):
      raise ndb.Return(('%s-token' % project_id, 0))
    self.mock(auth, 'get_project_access_token_async', get_project_access_token)

    self.mock(
        auth, 'is_internal_domain',
        lambda domain: domain == 'internal.example.com')

    self.mock(logging, 'warning', lambda *_args: None)
    self.mock(logging, 'error', lambda *_args: None)

  def mock_urlfetch(self, calls):

    @ndb.tasklet
    def mocked(**kwargs):
      if not calls:
        self.fail('Unexpected urlfetch call: %s' % kwargs)
      expected, response = calls.pop(0)
      defaults = {
          'deadline': 10,
          'follow_redirects': False,
          'headers': {},
          'method': 'GET',
          'payload': None,
          'validate_certificate': True,
      }
      defaults.update(expected)

      # Treat the 'Authorization' key especially, since JWT support makes this
      # complicated to do.
      expected_authorization = None
      if 'Authorization' in defaults['headers']:
        expected_authorization = defaults['headers']['Authorization']
        del defaults['headers']['Authorization']

      provided_authorization = None
      if 'Authorization' in kwargs.get('headers', {}):
        provided_authorization = kwargs.get('headers', {}).get('Authorization')
        del kwargs['headers']['Authorization']

      # If we find a pattern in the expected_authorization (introduced with a
      # '$' character) then we'll treat it as a regular expression.
      if expected_authorization:
        pattern_offset = expected_authorization.find('$')
        if pattern_offset != -1:
          self.assertRegexpMatches(provided_authorization,
                                   expected_authorization[pattern_offset + 1:])
        else:
          self.assertEqual(provided_authorization, expected_authorization)
      self.assertDictEqual(defaults, kwargs)

      if isinstance(response, Exception):
        raise response
      raise ndb.Return(response)

    self.mock(net, 'urlfetch_async', mocked)
    return calls

  def test_request_works(self):
    self.mock_urlfetch([
        ({
            'deadline': 123,
            'headers': {
                'Accept': 'text/plain',
                'Authorization': 'Bearer own-token'
            },
            'method': 'POST',
            'payload': 'post body',
            'url': 'http://localhost/123?a=%3D&b=%26',
        }, Response(200, 'response body', {'example-header': 'example-value'})),
    ])
    response_headers = {}
    response = net.request(
        url='http://localhost/123',
        method='POST',
        payload='post body',
        params={
            'a': '=',
            'b': '&'
        },
        headers={'Accept': 'text/plain'},
        scopes=['scope'],
        service_account_key=auth.ServiceAccountKey('a', 'b', 'c'),
        deadline=123,
        max_attempts=5,
        response_headers=response_headers,
    )
    self.assertEqual('response body', response)
    self.assertEqual('example-value', response_headers['example-header'])

  def test_project_tokens_external_service(self):
    self.mock_urlfetch([
        ({
            'deadline': 10,
            'headers': {
                'Authorization': 'Bearer project-id-token'
            },
            'method': 'GET',
            'url': 'https://external.example.com/123',
        }, Response(200, '', {})),
    ])
    net.request(url='https://external.example.com/123', project_id='project-id')

  def test_project_tokens_fallback(self):
    @ndb.tasklet
    def get_project_access_token(_project_id, _scopes):
      raise auth.NotFoundError('not found')
    self.mock(auth, 'get_project_access_token_async', get_project_access_token)

    self.mock_urlfetch([
        ({
            'deadline': 10,
            'headers': {
                # Switches to own token.
                'Authorization': 'Bearer own-token',
            },
            'method': 'GET',
            'url': 'https://external.example.com/123',
        }, Response(200, '', {})),
    ])
    net.request(url='https://external.example.com/123', project_id='project-id')

  def test_project_tokens_internal_service(self):
    self.mock_urlfetch([
        ({
            'deadline': 10,
            'headers': {
                'Authorization': 'Bearer own-token',
                'X-Luci-Project': 'project-id',
            },
            'method': 'GET',
            'url': 'https://internal.example.com/123',
        }, Response(200, '', {})),
    ])
    net.request(url='https://internal.example.com/123', project_id='project-id')

  def test_retries_transient_errors(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, urlfetch.Error()),
        ({
            'url': 'http://localhost/123'
        }, Response(408, 'clien timeout', {})),
        ({
            'url': 'http://localhost/123'
        }, Response(500, 'server error', {})),
        ({
            'url': 'http://localhost/123'
        }, Response(200, 'response body', {})),
    ])
    response = net.request('http://localhost/123', max_attempts=4)
    self.assertEqual('response body', response)

  def test_gives_up_retrying(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, Response(500, 'server error', {})),
        ({
            'url': 'http://localhost/123'
        }, Response(500, 'server error', {})),
        ({
            'url': 'http://localhost/123'
        }, Response(200, 'response body', {})),
    ])
    with self.assertRaises(net.Error):
      net.request('http://localhost/123', max_attempts=2)

  def test_404(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, Response(404, 'Not found', {})),
    ])
    with self.assertRaises(net.NotFoundError):
      net.request('http://localhost/123')

  def test_crappy_cloud_endpoints_404_is_retried(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/_ah/api/blah'
        }, Response(404, 'Not found', {})),
        ({
            'url': 'http://localhost/_ah/api/blah'
        }, Response(200, 'response body', {})),
    ])
    response = net.request('http://localhost/_ah/api/blah')
    self.assertEqual('response body', response)

  def test_legitimate_cloud_endpoints_404_is_not_retried(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/_ah/api/blah'
        }, Response(404, '{}', {'Content-Type': 'application/json'})),
    ])
    with self.assertRaises(net.NotFoundError):
      net.request('http://localhost/_ah/api/blah')

  def test_401(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, Response(401, 'Auth error', {})),
    ])
    with self.assertRaises(net.AuthError):
      net.request('http://localhost/123')

  def test_403(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, Response(403, 'Auth error', {})),
    ])
    with self.assertRaises(net.AuthError):
      net.request('http://localhost/123')

  def test_json_request_works(self):
    self.mock_urlfetch([
        ({
            'deadline': 123,
            'headers': {
                'Authorization': 'Bearer own-token',
                'Accept': 'application/json; charset=utf-8',
                'Content-Type': 'application/json; charset=utf-8',
                'Header': 'value',
            },
            'method': 'POST',
            'payload': '{"key":"value"}',
            'url': 'http://localhost/123?a=%3D&b=%26',
        }, Response(200, ')]}\'\n{"a":"b"}',
                    {'example-header': 'example-value'})),
    ])
    response_headers = {}
    response = net.json_request(
        url='http://localhost/123',
        method='POST',
        payload={'key': 'value'},
        params={
            'a': '=',
            'b': '&'
        },
        headers={'Header': 'value'},
        scopes=['scope'],
        service_account_key=auth.ServiceAccountKey('a', 'b', 'c'),
        deadline=123,
        max_attempts=5,
        response_headers=response_headers)
    self.assertEqual({'a': 'b'}, response)
    self.assertEqual(response_headers, {'example-header': 'example-value'})

  def test_json_with_jwt_auth_works(self):
    self.mock_urlfetch([
        ({
            'deadline': 123,
            'headers': {
                'Authorization': r'$^Bearer\ [a-zA-Z0-9_=-]+\.[a-zA-Z0-9_=-]+\.'
                                 '[a-zA-Z0-9_=-]+$',
                'Accept': 'application/json; charset=utf-8',
                'Content-Type': 'application/json; charset=utf-8',
                'Header': 'value',
            },
            'method': 'POST',
            'payload': '{"key":"value"}',
            'url': 'http://localhost/123?a=%3D&b=%26',
        }, Response(200, ')]}\'\n{"a":"b"}', {})),
    ])
    response = net.json_request(
        url='http://localhost/123',
        method='POST',
        payload={'key': 'value'},
        params={
            'a': '=',
            'b': '&'
        },
        headers={'Header': 'value'},
        deadline=123,
        max_attempts=5,
        use_jwt_auth=True,
        audience='my-service.appspot.com')
    self.assertEqual({'a': 'b'}, response)

  def test_json_with_jwt_auth_and_scopes_fail(self):
    with self.assertRaises(ValueError):
      net.json_request(
          url='http://localhost/123',
          method='POST',
          payload={'key': 'value'},
          params={
              'a': '=',
              'b': '&'
          },
          headers={'Header': 'value'},
          deadline=123,
          max_attempts=5,
          scopes=['scope'],
          use_jwt_auth=True)

  def test_json_with_jwt_auth_and_project_id_fail(self):
    with self.assertRaises(ValueError):
      net.json_request(
          url='http://localhost/123',
          method='POST',
          payload={'key': 'value'},
          params={
              'a': '=',
              'b': '&'
          },
          headers={'Header': 'value'},
          deadline=123,
          max_attempts=5,
          project_id='some-id',
          scopes=['scope'],
          use_jwt_auth=True)

  def test_json_with_jwt_auth_audience_and_scopes_fail(self):
    with self.assertRaises(ValueError):
      net.json_request(
          url='http://localhost/123',
          method='POST',
          payload={'key': 'value'},
          params={
              'a': '=',
              'b': '&'
          },
          headers={'Header': 'value'},
          deadline=123,
          max_attempts=5,
          scopes=['scope'],
          audience='my-service.appspot.com')

  def test_json_with_jwt_auth_audience_and_project_id_fail(self):
    with self.assertRaises(ValueError):
      net.json_request(
          url='http://localhost/123',
          method='POST',
          payload={'key': 'value'},
          params={
              'a': '=',
              'b': '&'
          },
          headers={'Header': 'value'},
          deadline=123,
          max_attempts=5,
          project_id='some-id',
          scopes=['scope'],
          audience='my-service.appspot.com')

  def test_json_bad_response(self):
    self.mock_urlfetch([
        ({
            'url': 'http://localhost/123'
        }, Response(200, 'not a json', {})),
    ])
    with self.assertRaises(net.Error):
      net.json_request('http://localhost/123')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
