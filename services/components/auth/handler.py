# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration with webapp2."""

import logging
import webapp2

from google.appengine.api import oauth
from google.appengine.api import users

from . import api
from . import model

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'AuthenticatingHandler',
  'configure',
  'cookie_authentication',
  'oauth_authentication',
  'service_to_service_authentication',
]


# Global list of authentication functions to use to authenticate all
# requests. Used by AuthenticatingHandler. Initialized in 'configure'.
_auth_methods = ()


class AuthenticatingHandlerMetaclass(type):
  """Ensures that 'get', 'post', etc. are marked with @require or @public."""

  def __new__(mcs, name, bases, attributes):
    for method in webapp2.WSGIApplication.allowed_methods:
      func = attributes.get(method.lower())
      if func and not api.is_decorated(func):
        raise TypeError(
            'Method \'%s\' of \'%s\' is not protected by @require or @public '
            'decorator' % (method.lower(), name))
    return type.__new__(mcs, name, bases, attributes)


class AuthenticatingHandler(webapp2.RequestHandler):
  """Base class for request handlers that use Auth system.

  Knows how to extract Identity from request data and how to initialize auth
  request context, so that get_current_identity() and has_permission() work.

  All request handling methods (like 'get', 'post', etc) should be marked by
  either @require or @public decorators.
  """

  # Checks that all 'get', 'post', etc. are marked with @require or @public.
  __metaclass__ = AuthenticatingHandlerMetaclass

  def dispatch(self):
    """Extracts and verifies Identity, sets up request auth context."""
    identity = model.Anonymous
    for method_func in _auth_methods:
      try:
        identity = method_func(self.request)
        if identity:
          break
      except api.AuthenticationError as err:
        logging.error('Authentication error.\n%s', err)
        self.authentication_error(err)
        return

    # Successfully extracted and validated an identity. Put it into request
    # cache. It's later used by 'get_current_identity()' and 'has_permission()'.
    assert isinstance(identity, model.Identity)
    api.get_request_cache().set_current_identity(identity)

    try:
      # TODO(vadimsh): Add XSRF token verification.

      # All other ACL checks will be performed by corresponding handlers
      # manually or via '@required' decorator. Failed ACL check raises
      # AuthorizationError.
      return super(AuthenticatingHandler, self).dispatch()
    except api.AuthorizationError as err:
      logging.error('Authorization error.\n%s\nIdentity: %s', err, identity)
      self.authorization_error(err)

  def authentication_error(self, error):
    """Called when authentication fails to report the error to requester.

    Authentication error means that some credentials are provided but they are
    invalid. If no credentials are provided at all, no authentication is
    attempted and current identity is just set to 'anonymous:anonymous'.

    Default behavior is to abort the request with HTTP 401 error (and human
    readable HTML body).

    Args:
      error: instance of AuthenticationError subclass.
    """
    self.abort(401, detail=str(error))

  def authorization_error(self, error):
    """Called when authentication succeeds, but access to a resource is denied.

    Called whenever request handler raises AuthorizationError exception.
    In particular this exception is raised by method decorated with @require if
    current identity doesn't have required permission.

    Default behavior is to abort the request with HTTP 403 error (and human
    readable HTML body).

    Args:
      error: instance of AuthorizationError subclass.
    """
    self.abort(403, detail=str(error))


def configure(auth_methods):
  """Sets a list of authentication methods to use for all requests.

  Its a global configuration that will be used by all request handlers inherited
  from AuthenticatingHandler. AuthenticatingHandler will try to apply auth
  methods sequentially one by one by until it finds one that works.

  Args:
    auth_methods: list of authentication functions to use to authenticate
        a request. Order is important. For example if both cookie_authentication
        and oauth_authentication methods are specified and a request has both
        cookies and 'Authorization' header, whatever method comes first in the
        list is used.

  Each auth method is a function that accepts webapp2 Request and can finish
  with 3 outcomes:

  * Return None: Authentication method is not applicable to that request
    and next method should be tried (for example cookie-based
    authentication is not applicable when there's no cookies).

  * Returns Identity associated with the request.
    Authentication method is applicable and request authenticity is confirmed.

  * Raises AuthenticationError: Authentication method is applicable, but
    request contains bad credentials or invalid token, etc. For example,
    OAuth2 token is given, but it is revoked.
  """
  global _auth_methods
  assert all(callable(func) for func in auth_methods)
  _auth_methods = tuple(auth_methods)


################################################################################
## Concrete implementations of authentication methods.


def cookie_authentication(_request):
  """AppEngine cookie based authentication via users.get_current_user()."""
  user = users.get_current_user()
  return model.Identity(model.IDENTITY_USER, user.email()) if user else None


def oauth_authentication(request):
  """OAuth2 based authentication via oauth.get_current_user()."""
  # Skip if 'Authorization' header is not set.
  if not request.headers.get('Authorization'):
    return None

  # OAuth2 scope token should have.
  oauth_scope = 'https://www.googleapis.com/auth/userinfo.email'

  # Extract client_id from access token. That also validates the token and
  # raises OAuthRequestError if token is revoked or otherwise not valid. It's
  # important to abort request with invalid token rather than fall back to
  # default Anonymous identity.
  try:
    client_id = oauth.get_client_id(oauth_scope)
  # Note: we intentionally do not handle OAuthServiceFailureError here.
  # If it happens client receives HTTP 500 error retries with existing token,
  # as it should.
  except oauth.OAuthRequestError:
    raise api.AuthenticationError('Invalid OAuth token')

  # Ensure given client_id is known.
  auth_db = api.get_request_auth_db()
  if not auth_db.is_allowed_oauth_client_id(client_id):
    raise api.AuthenticationError('Invalid OAuth client_id: %s' % client_id)

  # Extract email associated with the token.
  return model.Identity(
      model.IDENTITY_USER, oauth.get_current_user(oauth_scope).email())


def service_to_service_authentication(request):
  """Used for AppEngine <-> AppEngine communication.

  Relies on X-Appengine-Inbound-Appid header set by AppEngine itself. It can't
  be set by external users (with exception of admins).
  """
  app_id = request.headers.get('X-Appengine-Inbound-Appid')
  return model.Identity(model.IDENTITY_SERVICE, app_id) if app_id else None
