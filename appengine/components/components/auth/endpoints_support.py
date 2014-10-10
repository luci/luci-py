# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration with Cloud Endpoints.

This module is used only when 'endpoints' is importable (see auth/__init__.py).
"""

import functools

import endpoints
from protorpc import message_types
from protorpc import util

from . import api
from . import config
from . import model

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'endpoints_api',
  'endpoints_method',
]


@util.positional(2)
def endpoints_api(name, version, **kwargs):
  """Same as @endpoints.api but adds auth related properties."""
  auth_level = kwargs.get('auth_level') or endpoints.AUTH_LEVEL.OPTIONAL
  if auth_level == endpoints.AUTH_LEVEL.NONE:
    raise ValueError('Authentication is required')
  kwargs['auth_level'] = auth_level
  allowed_client_ids = set(kwargs.get('allowed_client_ids') or [])
  allowed_client_ids.add(endpoints.API_EXPLORER_CLIENT_ID)
  allowed_client_ids.update(api.fetch_allowed_client_ids())
  kwargs['allowed_client_ids'] = list(allowed_client_ids)
  return endpoints.api(name, version, **kwargs)


@util.positional(2)
def endpoints_method(
    request_message=message_types.VoidMessage,
    response_message=message_types.VoidMessage,
    **kwargs):
  """Same as @endpoints.method but also adds auth state initialization code."""
  orig_decorator = endpoints.method(request_message, response_message, **kwargs)
  def new_decorator(func):
    @functools.wraps(func)
    def wrapper(service, *args, **kwargs):
      try:
        initialize_auth(service)
        return func(service, *args, **kwargs)
      except api.AuthenticationError:
        raise endpoints.UnauthorizedException()
      except api.AuthorizationError:
        raise endpoints.ForbiddenException()
    return orig_decorator(wrapper)
  return new_decorator


def initialize_auth(_service):
  """Called before executing cloud endpoints method."""
  # With endpoints we support only single authentication method: the one that
  # endpoints library provides.
  # TODO(vadimsh): Add IP whitelist support.
  config.ensure_configured()
  current_user = endpoints.get_current_user()
  if not current_user:
    ident = model.Anonymous
  else:
    ident = model.Identity(model.IDENTITY_USER, current_user.email())
  api.get_request_cache().set_current_identity(ident)
