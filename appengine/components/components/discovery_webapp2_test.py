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
  required = messages.BooleanField(4, required=True)


class Child(messages.Message):
  """A child message to test recursion with."""
  enum = messages.EnumField(Enum, 1, default=Enum.UNKNOWN)


class Parent(messages.Message):
  """A parent message to test recursion with."""
  child = messages.MessageField(Child, 1)
  datetime = message_types.DateTimeField(2)


@endpoints.api('service', 'v1')
class Service(remote.Service):
  """A service to test with."""

  @endpoints.method(message_types.VoidMessage, Message, http_method='GET')
  def get_method(self, _):
    """An HTTP GET method."""
    return Message()

  @endpoints.method(Message, Message)
  def post_method(self, _):
    """An HTTP POST method.

    Has a multi-line description.
    """
    return Message()

  @endpoints.method(endpoints.ResourceContainer(
      Message, string=messages.StringField(1)), Message)
  def query_string_method(self, _):
    """A method supporting query strings."""
    return Message()

  @endpoints.method(endpoints.ResourceContainer(
      Message, path=messages.StringField(1)), Message, path='{path}/method')
  def path_parameter_method(self, _):
    """A method supporting path parameters."""
    return Message()


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
        'path_parameter_method': {
          'description': 'A method supporting path parameters.',
          'httpMethod': 'POST',
          'id': 'service.path_parameter_method',
          'parameterOrder': [
            'path',
          ],
          'parameters': {
            'path': {
              'location': 'path',
              'type': 'string',
            },
          },
          'path': '{path}/method',
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
        'query_string_method': {
          'description': 'A method supporting query strings.',
          'httpMethod': 'POST',
          'id': 'service.query_string_method',
          'parameters': {
            'string': {
              'location': 'query',
              'type': 'string',
            },
          },
          'path': 'query_string_method',
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
            'required': {
              'required': True,
              'type': 'boolean',
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

  def test_get_parameters(self):
    """Tests for discovery_webapp2._get_parameters."""
    expected = {
      'parameterOrder': [
        'boolean',
        'string',
        'required',
      ],
      'parameters': {
        'boolean': {
          'location': 'path',
          'type': 'boolean',
        },
        'integer': {
          'format': 'int64',
          'location': 'query',
          'type': 'string',
        },
        'required': {
          'location': 'query',
          'required': True,
          'type': 'boolean',
        },
        'string': {
          'location': 'path',
          'type': 'string',
        },
      },
    }
    self.assertEqual(discovery_webapp2._get_parameters(
        Message, 'path/{boolean}/with/{string}/parameters'), expected)

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
          'datetime': {
            'format': 'date-time',
            'type': 'string',
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
        'path_parameter_method': {
          'description': 'A method supporting path parameters.',
          'httpMethod': 'POST',
          'id': 'service.path_parameter_method',
          'parameterOrder': [
            'path',
          ],
          'parameters': {
            'path': {
              'location': 'path',
              'type': 'string',
            },
          },
          'path': '{path}/method',
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
        'query_string_method': {
          'description': 'A method supporting query strings.',
          'httpMethod': 'POST',
          'id': 'service.query_string_method',
          'parameters': {
            'string': {
              'location': 'query',
              'type': 'string',
            },
          },
          'path': 'query_string_method',
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
            'required': {
              'required': True,
              'type': 'boolean',
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
    self.assertEqual(discovery_webapp2.generate(Service, '/api'), expected)

  def test_directory(self):
    """Tests for discovery_webapp2.directory."""
    expected = {
      'discoveryVersion': 'v1',
      'items': [
        {
          'description': 'A service to test with.',
          'discoveryLink': './apis/service/v1/rest',
          'discoveryRestUrl':
              'https://None/api/discovery/v1/apis/service/v1/rest',
          'icons': {
            'x16': 'https://www.google.com/images/icons/product/search-16.gif',
            'x32': 'https://www.google.com/images/icons/product/search-32.gif',
          },
          'id': 'service:v1',
          'kind': 'discovery#directoryItem',
          'name': 'service',
          'preferred': True,
          'version': 'v1',
        },
      ],
      'kind': 'discovery#directoryList',
    }
    self.assertEqual(discovery_webapp2.directory([Service], '/api'), expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
