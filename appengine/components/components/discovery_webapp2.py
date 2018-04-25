# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Discovery document generator for an Endpoints v1 over webapp2 service."""

from protorpc import message_types
from protorpc import messages


def _normalize_whitespace(s):
  """Replaces consecutive whitespace characters with a single space.

  Args:
    s: The string to normalize, or None to return an empty string.

  Returns:
    A normalized version of the given string.
  """
  return ' '.join((s or '').split())


def _get_type_format(field):
  """Returns the schema type and format for the given message type.

  Args:
    field: The protorpc.messages.Field to get schema type and format for.

  Returns:
    (type, format) for use in the "schemas" section of a discovery document.
  """
  if isinstance(field, messages.BooleanField):
    return ('boolean', None)

  if isinstance(field, messages.BytesField):
    return ('string', 'byte')

  if isinstance(field, message_types.DateTimeField):
    return ('string', 'date-time')

  if isinstance(field, messages.EnumField):
    return ('string', None)

  if isinstance(field, messages.FloatField):
    if field.variant == messages.Variant.DOUBLE:
      return ('number', 'double')
    return ('number', 'float')

  if isinstance(field, messages.IntegerField):
    if field.variant in (messages.Variant.INT32, messages.Variant.SINT32):
      return ('integer', 'int32')

    if field.variant in (messages.Variant.INT64, messages.Variant.SINT64):
      # If the type requires int64 or uint64, specify string or JavaScript will
      # convert them to 32-bit.
      return ('string', 'int64')

    if field.variant == messages.Variant.UINT32:
      return ('integer', 'uint32')

    if field.variant == messages.Variant.UINT64:
      return ('string', 'uint64')

    # Despite the warning about JavaScript, Endpoints v2's discovery document
    # generator uses integer, int64 as the default here. Follow their choice.
    return ('integer', 'int64')

  if isinstance(field, messages.StringField):
    return ('string', None)

  return (None, None)


def _get_schemas(types):
  """Returns a schemas document for the given types.

  Args:
    types: The set of protorpc.messages.Messages subclasses to describe.

  Returns:
    A dict which can be written as JSON describing the types.
  """
  schemas = {}
  seen = set(types)
  types = list(types)
  # Messages may reference other messages whose schemas we need to add.
  # Keep a set of types we've already seen (but not necessarily processed) to
  # avoid repeatedly processing or queuing to process the same type.
  # Desired invariant: seen contains types which have ever been in types.
  # This invariant allows us to extend types mid-loop to add more types to
  # process without unnecessarily processing the same type twice. We achieve
  # this invariant by initializing seen to types and adding to seen every time
  # the loop adds to types.
  for message_type in types:
    # Endpoints v1 and v2 discovery documents "normalize" these names by
    # removing non-alphanumeric characters and putting the rest in PascalCase.
    # However, it's possible these names only need to match the $refs below and
    # exact formatting is irrelevant.
    # TODO(smut): Figure out if these names need to be normalized.
    name = message_type.definition_name()

    schemas[name] = {
      'id': name,
      'properties': {},
      'type': 'object',
    }

    desc = _normalize_whitespace(message_type.__doc__)
    if desc:
      schemas[name]['description'] = desc

    for field in message_type.all_fields():
      items = {}
      field_properties = {}

      if isinstance(field, messages.MessageField):
        field_type = field.type().__class__
        desc = _normalize_whitespace(field_type.__doc__)
        if desc:
          field_properties['description'] = desc
        # Queue new types to have their schema added in a future iteration.
        if field_type not in seen:
          types.append(field_type)
          # Maintain loop invariant.
          seen.add(field_type)
        items['$ref'] = field_type.definition_name()
      else:
        schema_type, schema_format = _get_type_format(field)
        items['type'] = schema_type
        if schema_format:
          items['format'] = schema_format

      if isinstance(field, messages.EnumField):
        if field.default:
          field_properties['default'] = str(field.default)
        items['enum'] = [enum.name for enum in field.type]
      elif field.default:
        field_properties['default'] = field.default

      if field.required:
        field_properties['required'] = True

      if field.repeated:
        field_properties['items'] = items
        field_properties['type'] = 'array'
      else:
        field_properties.update(items)

      schemas[name]['properties'][field.name] = field_properties

  return schemas


def _get_methods(service):
  """Returns a methods and schemas document for the given service.

  Args:
    service: The protorpc.remote.Service to describe.

  Returns:
    A dict which can be written as JSON describing the methods and types.
  """
  methods = {}
  types = set()

  for _, method in service.all_remote_methods().iteritems():
    # Only describe methods decorated with @method.
    info = getattr(method, 'method_info', None)
    if info is None:
      continue
    # info.method_id returns <service name>.<method>. Extract <method>.
    name = info.method_id(service.api_info).split('.')[-1]

    methods[name] = {
      'httpMethod': info.http_method,
      # <service name>.<method>.
      'id': info.method_id(service.api_info),
      'path': info.get_path(service.api_info),
      'scopes': [
        'https://www.googleapis.com/auth/userinfo.email',
      ],
    }

    desc = _normalize_whitespace(method.remote.method.__doc__)
    if desc:
      methods[name]['description'] = desc

    request = method.remote.request_type()
    if not isinstance(request, message_types.VoidMessage):
      if info.http_method not in ('GET', 'DELETE'):
        methods[name]['request'] = {
          # $refs are used to look up the schema elsewhere in the discovery doc.
          '$ref': request.__class__.definition_name(),
          'parameterName': 'resource',
        }
        types.add(request.__class__)

    response = method.remote.response_type()
    if not isinstance(response, message_types.VoidMessage):
      methods[name]['response'] = {
        '$ref': response.__class__.definition_name(),
      }
      types.add(response.__class__)

    # TODO(smut): Add parameters.

  document = {}
  if methods:
    document['methods'] = methods
  schemas = _get_schemas(types)
  if schemas:
    document['schemas'] = schemas
  return document


def generate(service):
  """Returns a discovery document for the given service.

  Args:
    service: The protorpc.remote.Service to describe.

  Returns:
    A dict which can be written as JSON describing the service.
  """
  document = {
    'discoveryVersion': 'v1',
    'auth': {
      'oauth2': {
        'scopes': {s: {'description': s} for s in service.api_info.scopes},
      },
    },
    # TODO(smut): Allow /api part of the path to be customized.
    'basePath': '/api/%s/%s' % (
        service.api_info.name, service.api_info.version),
    'baseUrl': 'https://%s/api/%s/%s' % (
        service.api_info.hostname, service.api_info.name,
        service.api_info.version),
    'batchPath': 'batch',
    'icons': {
      'x16': 'https://www.google.com/images/icons/product/search-16.gif',
      'x32': 'https://www.google.com/images/icons/product/search-32.gif',
    },
    'id': '%s:%s' % (service.api_info.name, service.api_info.version),
    'kind': 'discovery#restDescription',
    'name': service.api_info.name,
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
            ' with API access, quota, and reports. Required unless you provide'
            ' an OAuth 2.0 token.'),
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
            'Available to use for quota purposes for server-side applications.'
            ' Can be any arbitrary string assigned to a user, but should not'
            ' exceed 40 characters. Overrides userIp if both are provided.'),
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
    'rootUrl': 'https://%s/api/' % service.api_info.hostname,
    'servicePath': '%s/%s/' % (service.api_info.name, service.api_info.version),
    'version': service.api_info.version,
  }
  desc = _normalize_whitespace(service.api_info.description or service.__doc__)
  if desc:
    document['description'] = desc
  document.update(_get_methods(service))
  return document
