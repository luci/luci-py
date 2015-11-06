# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration with webapp2."""

# Disable 'Method could be a function.'
# pylint: disable=R0201

import functools
import json
import logging
import urllib
import webapp2

from google.appengine.api import urlfetch
from google.appengine.api import users

from components import utils

from . import api
from . import config
from . import delegation
from . import host_token
from . import ipaddr
from . import model
from . import openid
from . import tokens

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'ApiHandler',
  'AuthenticatingHandler',
  'gae_cookie_authentication',
  'get_authenticated_routes',
  'oauth_authentication',
  'openid_cookie_authentication',
  'require_xsrf_token_request',
  'service_to_service_authentication',
]


def require_xsrf_token_request(f):
  """Use for handshaking APIs."""
  @functools.wraps(f)
  def hook(self, *args, **kwargs):
    if not self.request.headers.get('X-XSRF-Token-Request'):
      raise api.AuthorizationError('Missing required XSRF request header')
    return f(self, *args, **kwargs)
  return hook


class XSRFToken(tokens.TokenKind):
  """XSRF token parameters."""
  expiration_sec = 4 * 3600
  secret_key = api.SecretKey('xsrf_token', scope='local')
  version = 1


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
  """Base class for webapp2 request handlers that use Auth system.

  Knows how to extract Identity from request data and how to initialize auth
  request context, so that get_current_identity() and is_group_member() work.

  All request handling methods (like 'get', 'post', etc) should be marked by
  either @require or @public decorators.
  """

  # Checks that all 'get', 'post', etc. are marked with @require or @public.
  __metaclass__ = AuthenticatingHandlerMetaclass

  # List of HTTP methods that trigger XSRF token validation.
  xsrf_token_enforce_on = ('DELETE', 'POST', 'PUT')
  # If not None, the header to search for XSRF token.
  xsrf_token_header = 'X-XSRF-Token'
  # If not None, the request parameter (GET or POST) to search for XSRF token.
  xsrf_token_request_param = 'xsrf_token'
  # Embedded data extracted from XSRF token of current request.
  xsrf_token_data = None
  # If not None, sets X_Frame-Options on all replies.
  frame_options = 'DENY'
  # A method used to authenticate this request, see get_auth_methods().
  auth_method = None

  def dispatch(self):
    """Extracts and verifies Identity, sets up request auth context."""
    # Ensure auth component is configured before executing any code.
    conf = config.ensure_configured()
    auth_context = api.reinitialize_request_cache()

    # http://www.html5rocks.com/en/tutorials/security/content-security-policy/
    # https://www.owasp.org/index.php/Content_Security_Policy
    # TODO(maruel): Remove 'unsafe-inline' once all inline style="foo:bar" in
    # all HTML tags were removed. Warning if seeing this post 2016, it could
    # take a while.
    # - https://www.google.com is due to Google Viz library.
    # - https://www.google-analytics.com due to Analytics.
    # - 'unsafe-eval' due to polymer.
    self.response.headers['Content-Security-Policy'] = (
        'default-src https: \'self\' \'unsafe-inline\' https://www.google.com '
        'https://www.google-analytics.com \'unsafe-eval\'')
    # Enforce HTTPS by adding the HSTS header; 365*24*60*60s.
    # https://www.owasp.org/index.php/HTTP_Strict_Transport_Security
    self.response.headers['Strict-Transport-Security'] = (
        'max-age=31536000; includeSubDomains; preload')
    # Disable frame support wholesale.
    # https://www.owasp.org/index.php/Clickjacking_Defense_Cheat_Sheet
    if self.frame_options:
      self.response.headers['X-Frame-Options'] = self.frame_options

    identity = None
    for method_func in self.get_auth_methods(conf):
      try:
        identity = method_func(self.request)
        if identity:
          break
      except api.AuthenticationError as err:
        self.authentication_error(err)
        return
      except api.AuthorizationError as err:
        self.authorization_error(err)
        return
    else:
      method_func = None
    self.auth_method = method_func

    # If no authentication method is applicable, default to anonymous identity.
    identity = identity or model.Anonymous

    # XSRF token is required only if using Cookie based or IP whitelist auth.
    # A browser doesn't send Authorization: 'Bearer ...' or any other headers
    # by itself. So XSRF check is not required if header based authentication
    # is used.
    using_headers_auth = method_func in (
        oauth_authentication, service_to_service_authentication)

    # Extract caller host name from host token header, if present and valid.
    host_tok = self.request.headers.get(host_token.HTTP_HEADER)
    if host_tok:
      validated_host = host_token.validate_host_token(host_tok)
      if validated_host:
        auth_context.peer_host = validated_host

    # Verify IP is whitelisted and authenticate requests from bots.
    assert self.request.remote_addr
    ip = ipaddr.ip_from_string(self.request.remote_addr)
    auth_context.peer_ip = ip
    try:
      # 'verify_ip_whitelisted' may change identity for bots, store new one.
      auth_context.peer_identity = api.verify_ip_whitelisted(
          identity, ip, self.request.headers)
    except api.AuthorizationError as err:
      self.authorization_error(err)
      return

    # Parse delegation token, if given, to deduce end-user identity.
    delegation_tok = self.request.headers.get(delegation.HTTP_HEADER)
    if delegation_tok:
      try:
        auth_context.current_identity = delegation.check_delegation_token(
            delegation_tok, auth_context.peer_identity)
      except delegation.BadTokenError as exc:
        self.authorization_error(
            api.AuthorizationError('Bad delegation token: %s' % exc))
      except delegation.TransientError as exc:
        msg = 'Transient error while validating delegation token.\n%s' % exc
        logging.error(msg)
        self.abort(500, detail=msg)
    else:
      auth_context.current_identity = auth_context.peer_identity

    try:
      # Fail if XSRF token is required, but not provided.
      need_xsrf_token = (
          not using_headers_auth and
          self.request.method in self.xsrf_token_enforce_on)
      if need_xsrf_token and self.xsrf_token is None:
        raise api.AuthorizationError('XSRF token is missing')

      # If XSRF token is present, verify it is valid and extract its payload.
      # Do it even if XSRF token is not strictly required, since some handlers
      # use it to store session state (it is similar to a signed cookie).
      self.xsrf_token_data = {}
      if self.xsrf_token is not None:
        # This raises AuthorizationError if token is invalid.
        self.xsrf_token_data = self.verify_xsrf_token()

      # All other ACL checks will be performed by corresponding handlers
      # manually or via '@required' decorator. Failed ACL check raises
      # AuthorizationError.
      super(AuthenticatingHandler, self).dispatch()
    except api.AuthorizationError as err:
      self.authorization_error(err)

  @classmethod
  def get_auth_methods(cls, conf):
    """Returns an enumerable of functions to use to authenticate request.

    The handler will try to apply auth methods sequentially one by one by until
    it finds one that works.

    Each auth method is a function that accepts webapp2.Request and can finish
    with 3 outcomes:

    * Return None: authentication method is not applicable to that request
      and next method should be tried (for example cookie-based
      authentication is not applicable when there's no cookies).

    * Returns Identity associated with the request. Means authentication method
      is applicable and request authenticity is confirmed.

    * Raises AuthenticationError: authentication method is applicable, but
      request contains bad credentials or invalid token, etc. For example,
      OAuth2 token is given, but it is revoked.

    A chosen auth method function will be stored in request's auth_method field.

    Args:
      conf: components.auth GAE config, see config.py.
    """
    if conf.USE_OPENID:
      cookie_auth = openid_cookie_authentication
    else:
      cookie_auth = gae_cookie_authentication
    return oauth_authentication, cookie_auth, service_to_service_authentication

  def generate_xsrf_token(self, xsrf_token_data=None):
    """Returns new XSRF token that embeds |xsrf_token_data|.

    The token is bound to current identity and is valid only when used by same
    identity.
    """
    return XSRFToken.generate(
        [api.get_current_identity().to_bytes()], xsrf_token_data)

  @property
  def xsrf_token(self):
    """Returns XSRF token passed with the request or None if missing.

    Doesn't do any validation. Use verify_xsrf_token() instead.
    """
    token = None
    if self.xsrf_token_header:
      token = self.request.headers.get(self.xsrf_token_header)
    if not token and self.xsrf_token_request_param:
      param = self.request.get_all(self.xsrf_token_request_param)
      token = param[0] if param else None
    return token

  def verify_xsrf_token(self):
    """Grabs a token from the request, validates it and extracts embedded data.

    Current identity must be the same as one used to generate the token.

    Returns:
      Whatever was passed as |xsrf_token_data| in 'generate_xsrf_token'
      method call used to generate the token.

    Raises:
      AuthorizationError if token is missing, invalid or expired.
    """
    token = self.xsrf_token
    if not token:
      raise api.AuthorizationError('XSRF token is missing')
    # Check that it was generated for the same identity.
    try:
      return XSRFToken.validate(token, [api.get_current_identity().to_bytes()])
    except tokens.InvalidTokenError as err:
      raise api.AuthorizationError(str(err))

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
    logging.warning('Authentication error.\n%s', error)
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
    logging.warning(
        'Authorization error.\n%s\nPeer: %s\nIP: %s',
        error, api.get_peer_identity().to_bytes(), self.request.remote_addr)
    self.abort(403, detail=str(error))

  ### Wrappers around Users API or its OpenID equivalent.

  def get_current_user(self):
    """When cookie auth is used returns instance of CurrentUser or None."""
    return self._get_users_api().get_current_user(self.request)

  def is_current_user_gae_admin(self):
    """When cookie auth is used returns True if current caller is GAE admin."""
    return self._get_users_api().is_current_user_gae_admin(self.request)

  def create_login_url(self, dest_url):
    """When cookie auth is used returns URL to redirect user to login."""
    return self._get_users_api().create_login_url(self.request, dest_url)

  def create_logout_url(self, dest_url):
    """When cookie auth is used returns URL to redirect user to logout."""
    return self._get_users_api().create_logout_url(self.request, dest_url)

  def _get_users_api(self):
    """Returns GAEUsersAPI, OpenIDAPI or raises NotImplementedError.

    Chooses based on what auth_method was used of what methods are available.
    """
    method = self.auth_method
    if not method:
      # Anonymous request -> pick first method that supports API.
      for method in self.get_auth_methods(config.ensure_configured()):
        if method in _METHOD_TO_USERS_API:
          break
      else:
        raise NotImplementedError('No methods support UsersAPI')
    elif method not in _METHOD_TO_USERS_API:
      raise NotImplementedError(
          '%s doesn\'t support UsersAPI' % method.__name__)
    return _METHOD_TO_USERS_API[method]


class ApiHandler(AuthenticatingHandler):
  """Parses JSON request body to a dict, serializes response to JSON."""
  CONTENT_TYPE_BASE = 'application/json'
  CONTENT_TYPE_FULL = 'application/json; charset=utf-8'
  _json_body = None
  # Clickjacking not applicable to APIs.
  frame_options = None

  def authentication_error(self, error):
    logging.warning('Authentication error.\n%s', error)
    self.abort_with_error(401, text=str(error))

  def authorization_error(self, error):
    logging.warning(
        'Authorization error.\n%s\nPeer: %s\nIP: %s',
        error, api.get_peer_identity().to_bytes(), self.request.remote_addr)
    self.abort_with_error(403, text=str(error))

  def send_response(self, response, http_code=200, headers=None):
    """Sends successful reply and continues execution."""
    self.response.set_status(http_code)
    self.response.headers.update(headers or {})
    self.response.headers['Content-Type'] = self.CONTENT_TYPE_FULL
    self.response.write(json.dumps(response))

  def abort_with_error(self, http_code, **kwargs):
    """Sends error reply and stops execution."""
    self.abort(
        http_code,
        json=kwargs,
        headers={'Content-Type': self.CONTENT_TYPE_FULL})

  def parse_body(self):
    """Parses JSON body and verifies it's a dict.

    webob.Request doesn't cache the decoded json body, this function does.
    """
    if self._json_body is None:
      if (self.CONTENT_TYPE_BASE and
          self.request.content_type != self.CONTENT_TYPE_BASE):
        msg = (
            'Expecting JSON body with content type \'%s\'' %
            self.CONTENT_TYPE_BASE)
        self.abort_with_error(400, text=msg)
      try:
        self._json_body = self.request.json
        if not isinstance(self._json_body, dict):
          raise ValueError()
      except (LookupError, ValueError):
        self.abort_with_error(400, text='Not a valid json dict body')
    return self._json_body.copy()


def get_authenticated_routes(app):
  """Given WSGIApplication returns list of routes that use authentication.

  Intended to be used only for testing.
  """
  # This code is adapted from router's __repr__ method (that enumerate
  # all routes for pretty-printing).
  routes = list(app.router.match_routes)
  routes.extend(
      v for k, v in app.router.build_routes.iteritems()
      if v not in app.router.match_routes)
  return [r for r in routes if issubclass(r.handler, AuthenticatingHandler)]


################################################################################
## All supported implementations of authentication methods for webapp2 handlers.


def gae_cookie_authentication(_request):
  """AppEngine cookie based authentication via users.get_current_user()."""
  user = users.get_current_user()
  try:
    return model.Identity(model.IDENTITY_USER, user.email()) if user else None
  except ValueError:
    raise api.AuthenticationError('Unsupported user email: %s' % user.email())


def openid_cookie_authentication(request):
  """Cookie based authentication that uses OpenID flow for login."""
  user = openid.get_current_user(request)
  try:
    return model.Identity(model.IDENTITY_USER, user.email) if user else None
  except ValueError:
    raise api.AuthenticationError('Unsupported user email: %s' % user.email)


def oauth_authentication(request):
  """OAuth2 based authentication via oauth.get_current_user()."""
  if not request.headers.get('Authorization'):
    return None
  if not utils.is_local_dev_server():
    return api.extract_oauth_caller_identity()

  # OAuth2 library is mocked on dev server to return some nonsense. Use (slow,
  # but real) OAuth2 API endpoint instead to validate access_token. It is also
  # what Cloud Endpoints do on a local server. For simplicity ignore client_id
  # on dev server.
  header = request.headers['Authorization'].split(' ', 1)
  if len(header) != 2 or header[0] not in ('OAuth', 'Bearer'):
    raise api.AuthenticationError('Invalid authorization header')

  # Adapted from endpoints/users_id_tokens.py, _set_bearer_user_vars_local.
  base_url = 'https://www.googleapis.com/oauth2/v1/tokeninfo'
  result = urlfetch.fetch(
      url='%s?%s' % (base_url, urllib.urlencode({'access_token': header[1]})),
      follow_redirects=False,
      validate_certificate=True)
  if result.status_code != 200:
    try:
      error = json.loads(result.content)['error_description']
    except (KeyError, ValueError):
      error = repr(result.content)
    raise api.AuthenticationError('Failed to validate the token: %s' % error)

  token_info = json.loads(result.content)
  if 'email' not in token_info:
    raise api.AuthenticationError('Token doesn\'t include an email address')
  if not token_info.get('verified_email'):
    raise api.AuthenticationError('Token email isn\'t verified')

  email = token_info['email']
  try:
    return model.Identity(model.IDENTITY_USER, email)
  except ValueError:
    raise api.AuthenticationError('Unsupported user email: %s' % email)


def service_to_service_authentication(request):
  """Used for AppEngine <-> AppEngine communication.

  Relies on X-Appengine-Inbound-Appid header set by AppEngine itself. It can't
  be set by external users (with exception of admins).
  """
  app_id = request.headers.get('X-Appengine-Inbound-Appid')
  try:
    return model.Identity(model.IDENTITY_SERVICE, app_id) if app_id else None
  except ValueError:
    raise api.AuthenticationError('Unsupported application ID: %s' % app_id)


################################################################################
## API wrapper on top of Users API and OpenID API to make them similar.


class CurrentUser(object):
  """Mimics subset of GAE users.User object for ease of transition.

  Also adds .picture().
  """

  def __init__(self, user_id, email, picture):
    self._user_id = user_id
    self._email = email
    self._picture = picture

  def nickname(self):
    return self._email

  def email(self):
    return self._email

  def user_id(self):
    return self._user_id

  def picture(self):
    return self._picture

  def __unicode__(self):
    return unicode(self.nickname())

  def __str__(self):
    return str(self.nickname())


class GAEUsersAPI(object):
  @staticmethod
  def get_current_user(request):  # pylint: disable=unused-argument
    user = users.get_current_user()
    return CurrentUser(user.user_id(), user.email(), None) if user else None

  @staticmethod
  def is_current_user_gae_admin(request):  # pylint: disable=unused-argument
    return users.is_current_user_admin()

  @staticmethod
  def create_login_url(request, dest_url):  # pylint: disable=unused-argument
    return users.create_login_url(dest_url)

  @staticmethod
  def create_logout_url(request, dest_url):  # pylint: disable=unused-argument
    return users.create_logout_url(dest_url)


class OpenIDAPI(object):
  @staticmethod
  def get_current_user(request):
    user = openid.get_current_user(request)
    return CurrentUser(user.sub, user.email, user.picture) if user else None

  @staticmethod
  def is_current_user_gae_admin(request):  # pylint: disable=unused-argument
    return False

  @staticmethod
  def create_login_url(request, dest_url):
    return openid.create_login_url(request, dest_url)

  @staticmethod
  def create_logout_url(request, dest_url):
    return openid.create_logout_url(request, dest_url)


# See AuthenticatingHandler._get_users_api().
_METHOD_TO_USERS_API = {
  gae_cookie_authentication: GAEUsersAPI,
  openid_cookie_authentication: OpenIDAPI,
}
