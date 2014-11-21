# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration with Cloud Endpoints.

This module is used only when 'endpoints' is importable (see auth/__init__.py).
"""

import functools
import logging
import os

import endpoints
from protorpc import message_types
from protorpc import util

from . import api
from . import config
from . import model

from components import utils

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'endpoints_api',
  'endpoints_method',
]


# TODO(vadimsh): id_token auth (used when talking to Cloud Endpoints from
# Android for example) is not supported yet, since this module talks to
# OAuth API directly to validate client_ids to simplify usage of Cloud Endpoints
# APIs by service accounts. Otherwise, each service account (or rather it's
# client_id) has to be hardcoded into the application source code.


# Cloud Endpoints auth library likes to spam logging.debug(...) messages: four
# messages per _every_ authenticated request. Monkey patch it.
from endpoints import users_id_token
users_id_token.logging = logging.getLogger('endpoints.users_id_token')
users_id_token.logging.setLevel(logging.INFO)


@util.positional(2)
def endpoints_api(
    name, version,
    auth_level=endpoints.AUTH_LEVEL.OPTIONAL,
    allowed_client_ids=None,
    **kwargs):
  """Same as @endpoints.api but tweaks default auth related properties.

  By default API marked with this decorator will use same authentication scheme
  as non-endpoints request handlers (i.e. fetch a whitelist of OAuth client_id's
  from the datastore, recognize service accounts, etc.), disabling client_id
  checks performed by Cloud Endpoints frontend (and doing them on the backend,
  see 'initialize_auth' below).

  Using service accounts with vanilla Cloud Endpoints auth is somewhat painful:
  every service account should be whitelisted in the 'allowed_client_ids' list
  in the source code of the application (when calling @endpoints.api). By moving
  client_id checks to the backend we can support saner logic.
  """
  # 'audiences' is used with id_token auth, it's not supported yet.
  assert 'audiences' not in kwargs, 'Not supported'

  # We love authentication.
  if auth_level == endpoints.AUTH_LEVEL.NONE:
    raise ValueError('Authentication is required')

  # We love API Explorer.
  if allowed_client_ids is None:
    allowed_client_ids = endpoints.SKIP_CLIENT_ID_CHECK
  if allowed_client_ids != endpoints.SKIP_CLIENT_ID_CHECK:
    allowed_client_ids = sorted(
        set(allowed_client_ids) | set([endpoints.API_EXPLORER_CLIENT_ID]))

  return endpoints.api(
      name, version,
      auth_level=auth_level,
      allowed_client_ids=allowed_client_ids,
      **kwargs)


def endpoints_method(
    request_message=message_types.VoidMessage,
    response_message=message_types.VoidMessage,
    **kwargs):
  """Same as @endpoints.method but also adds auth state initialization code.

  Also forbids changing changing auth parameters on per-method basis, since it
  unnecessary complicates authentication code. All methods inherit properties
  set on the service level.
  """
  assert 'audiences' not in kwargs, 'Not supported'
  assert 'allowed_client_ids' not in kwargs, 'Not supported'

  orig_decorator = endpoints.method(request_message, response_message, **kwargs)
  def new_decorator(func):
    @functools.wraps(func)
    def wrapper(service, *args, **kwargs):
      try:
        initialize_request_auth()
        return func(service, *args, **kwargs)
      except api.AuthenticationError:
        raise endpoints.UnauthorizedException()
      except api.AuthorizationError:
        raise endpoints.ForbiddenException()
    return orig_decorator(wrapper)
  return new_decorator


def initialize_request_auth():
  """Grabs caller identity and initializes request local authentication context.

  Called before executing a cloud endpoints method. May raise AuthorizationError
  or AuthenticationError exceptions.
  """
  # TODO(vadimsh): Add IP whitelist support in addition to OAuth.
  config.ensure_configured()

  # Endpoints library always does authentication before invoking a method. Just
  # grab the result of that authentication: it's doesn't make any RPCs.
  current_user = endpoints.get_current_user()

  # Cloud Endpoints auth on local dev server works much better compared to OAuth
  # library since endpoints is using real authentication backend, while oauth.*
  # API is mocked. It makes API Explorer work with local apps. Always use Cloud
  # Endpoints auth on the dev server. It has a side effect: client_id whitelist
  # is ignored, there's no way to get client_id on dev server via endpoints API.
  ident = None
  if utils.is_local_dev_server():
    if current_user:
      ident = model.Identity(model.IDENTITY_USER, current_user.email())
    else:
      ident = model.Anonymous
  else:
    # Use OAuth API directly to grab both client_id and email and validate them.
    # endpoints.get_current_user() itself is implemented in terms of OAuth API,
    # with some additional code to handle id_token that we currently skip (see
    # TODO at the top of this file). OAuth API calls below will just reuse
    # cached values without making any additional RPCs.
    if os.environ.get('HTTP_AUTHORIZATION'):
      # Raises error for forbidden client_id, never returns None or Anonymous.
      ident = api.extract_oauth_caller_identity(
          extra_client_ids=[endpoints.API_EXPLORER_CLIENT_ID])
      # Double check that we used same cached values as endpoints did.
      assert ident and not ident.is_anonymous, ident
      assert current_user is not None
      assert ident.name == current_user.email(), (
          ident.name, current_user.email())
    else:
      # 'Authorization' header is missing. Endpoints still could have found
      # id_token in GET request parameters. Ignore it for now, the code is
      # complicated without it already.
      if current_user is not None:
        raise api.AuthenticationError('Unsupported authentication method')
      ident = model.Anonymous

  assert ident is not None
  api.get_request_cache().set_current_identity(ident)
