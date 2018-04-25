#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from protorpc import message_types
from protorpc import messages
from protorpc import remote
import endpoints

from test_support import test_case
import discovery_webapp2


class Enum(messages.Enum):
  """An enum to test with."""
  UNKNOWN = 0
  KNOWN = 1


class Message(messages.Message):
  """A message to test with."""
  boolean = messages.BooleanField(1)
  integer = messages.IntegerField(2)
  string = messages.StringField(3)


class Child(messages.Message):
  """A child message to test recursion with."""
  enum = messages.EnumField(Enum, 1, default=Enum.UNKNOWN)


class Parent(messages.Message):
  """A parent message to test recursion with."""
  child = messages.MessageField(Child, 1)


@endpoints.api('service', 'v1')
class Service(remote.Service):
  """A service to test with."""

  @endpoints.method(message_types.VoidMessage, Message, http_method='GET')
  def get_method(self, _):
    """An HTTP GET method."""
    return Message(value='value')

  @endpoints.method(Message, Message)
  def post_method(self, message):
    """An HTTP POST method.

    Has a multi-line description.
    """
    return Message(value=message.Value)


class DiscoveryWebapp2TestCase(test_case.TestCase):
  """Tests for discovery_webapp2.py"""

  def test_get_methods(self):
    """Tests discovery_webapp2._get_methods."""
    expected = {
      'methods': {
        'get_method': {
          'description': 'An HTTP GET method.',
          'httpMethod': 'GET',
          'id': 'service.get_method',
          'path': 'get_method',
          'response': {
            '$ref': 'discovery_webapp2_test.Message',
          },
          'scopes': [
            'https://www.googleapis.com/auth/userinfo.email',
          ],
        },
        'post_method': {
          'description': 'An HTTP POST method. Has a multi-line description.',
          'httpMethod': 'POST',
          'id': 'service.post_method',
          'path': 'post_method',
          'request': {
            '$ref': 'discovery_webapp2_test.Message',
            'parameterName': 'resource',
          },
          'response': {
            '$ref': 'discovery_webapp2_test.Message',
          },
          'scopes': [
            'https://www.googleapis.com/auth/userinfo.email',
          ],
        },
      },
      'schemas': {
        'discovery_webapp2_test.Message': {
          'description': 'A message to test with.',
          'id': 'discovery_webapp2_test.Message',
          'properties': {
            'boolean': {
              'type': 'boolean',
            },
            'integer': {
              'format': 'int64',
              'type': 'string',
            },
            'string': {
              'type': 'string',
            },
          },
          'type': 'object',
        },
      },
    }
    self.assertEqual(discovery_webapp2._get_methods(Service), expected)

  def test_get_schemas(self):
    """Tests for discovery_webapp2._get_schemas."""
    expected = {
      'discovery_webapp2_test.Child': {
        'description': 'A child message to test recursion with.',
        'id': 'discovery_webapp2_test.Child',
        'properties': {
          'enum': {
            'default': 'UNKNOWN',
            'enum': [
              'UNKNOWN',
              'KNOWN',
            ],
            'type': 'string',
          },
        },
        'type': 'object',
      },
      'discovery_webapp2_test.Parent': {
        'description': 'A parent message to test recursion with.',
        'id': 'discovery_webapp2_test.Parent',
        'properties': {
          'child': {
            '$ref': 'discovery_webapp2_test.Child',
            'description': 'A child message to test recursion with.',
          },
        },
        'type': 'object',
      },
    }
    self.assertEqual(discovery_webapp2._get_schemas([Parent]), expected)

  def test_generate(self):
    """Tests for discovery_webapp2.generate."""
    expected = {
      'auth': {
        'oauth2': {
          'scopes': {
            'https://www.googleapis.com/auth/userinfo.email': {
              'description': 'https://www.googleapis.com/auth/userinfo.email',
            },
          },
        },
      },
      'basePath': '/api/service/v1',
      'baseUrl': 'https://None/api/service/v1',
      'batchPath': 'batch',
      'description': 'A service to test with.',
      'discoveryVersion': 'v1',
      'icons': {
        'x16': 'https://www.google.com/images/icons/product/search-16.gif',
        'x32': 'https://www.google.com/images/icons/product/search-32.gif',
      },
      'id': 'service:v1',
      'kind': 'discovery#restDescription',
      'methods': {
        'get_method': {
          'description': 'An HTTP GET method.',
          'httpMethod': 'GET',
          'id': 'service.get_method',
          'path': 'get_method',
          'response': {
            '$ref': 'discovery_webapp2_test.Message',
          },
          'scopes': [
            'https://www.googleapis.com/auth/userinfo.email',
          ],
        },
        'post_method': {
          'description': 'An HTTP POST method. Has a multi-line description.',
          'httpMethod': 'POST',
          'id': 'service.post_method',
          'path': 'post_method',
          'request': {
            '$ref': 'discovery_webapp2_test.Message',
            'parameterName': 'resource',
          },
          'response': {
            '$ref': 'discovery_webapp2_test.Message',
          },
          'scopes': [
            'https://www.googleapis.com/auth/userinfo.email',
          ],
        },
      },
      'name': 'service',
      'parameters': {
        'alt': {
          'default': 'json',
          'description': 'Data format for the response.',
          'enum': ['json'],
          'enumDescriptions': [
            'Responses with Content-Type of application/json',
          ],
          'location': 'query',
          'type': 'string',
        },
        'fields': {
          'description': (
              'Selector specifying which fields to include in a partial'
              ' response.'),
          'location': 'query',
          'type': 'string',
        },
        'key': {
          'description': (
              'API key. Your API key identifies your project and provides you'
              ' with API access, quota, and reports. Required unless you'
              ' provide an OAuth 2.0 token.'),
          'location': 'query',
          'type': 'string',
        },
        'oauth_token': {
          'description': 'OAuth 2.0 token for the current user.',
          'location': 'query',
          'type': 'string',
        },
        'prettyPrint': {
          'default': 'true',
          'description': 'Returns response with indentations and line breaks.',
          'location': 'query',
          'type': 'boolean',
        },
        'quotaUser': {
          'description': (
              'Available to use for quota purposes for server-side'
              ' applications. Can be any arbitrary string assigned to a user,'
              ' but should not exceed 40 characters. Overrides userIp if both'
              ' are provided.'),
          'location': 'query',
          'type': 'string',
        },
        'userIp': {
          'description': (
              'IP address of the site where the request originates. Use this if'
              ' you want to enforce per-user limits.'),
          'location': 'query',
          'type': 'string',
        },
      },
      'protocol': 'rest',
      'rootUrl': 'https://None/api/',
      'servicePath': 'service/v1/',
      'schemas': {
        'discovery_webapp2_test.Message': {
          'description': 'A message to test with.',
          'id': 'discovery_webapp2_test.Message',
          'properties': {
            'boolean': {
              'type': 'boolean',
            },
            'integer': {
              'format': 'int64',
              'type': 'string',
            },
            'string': {
              'type': 'string',
            },
          },
          'type': 'object',
        },
      },
      'version': 'v1',
    }
    self.assertEqual(discovery_webapp2.generate(Service), expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
