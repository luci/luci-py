# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Functions related to OpenID Connect protocol.

At least Google's implementation.

See https://developers.google.com/identity/protocols/OpenIDConnect.
"""

import collections
import datetime
import json
import logging
import urllib
import urlparse

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

import webapp2

from components import utils

from . import api
from . import tokens


### Low level API implementing OpenID protocol steps.


# Where to grab parameters of the protocol.
DISCOVERY_URL = 'https://accounts.google.com/.well-known/openid-configuration'


class OpenIDError(Exception):
  """Error when running OpenID flow."""
  def __init__(self, msg, transient=False):
    super(OpenIDError, self).__init__(msg)
    self.transient = transient


class OpenIDStateToken(tokens.TokenKind):
  """Used to HMAC-tag 'state' variable passed via OAuth flow."""
  expiration_sec = 30 * 60
  secret_key = api.SecretKey('openid_state_token', scope='local')
  version = 1


class AuthOpenIDConfig(ndb.Model):
  """Configuration for OpenID protocol.

  Managed via Cloud Endpoints API in ui/endpoints_api.py. It can't be fetched
  from central auth_service because OAuth2 client configuration includes
  redirect URL with full hostname. Thus each app has to have its own OAuth2
  client (at least until auth_service implements SSO).

  Note that components.datastore.config is using components.auth and thus can't
  be used here.
  """
  # OAuth2 Web client representing the application.
  client_id = ndb.StringProperty(indexed=False)
  # Associated secret. Must be secret for real.
  client_secret = ndb.StringProperty(indexed=False)
  # Redirect URI must be 'https://<apphost>/auth/openid/callback'. It is stored
  # in config explicitly to remind admin that OAuth2 client must be configured
  # accordingly.
  redirect_uri = ndb.StringProperty(indexed=False)


def get_config():
  """Fetches AuthOpenIDConfig from datastore or returns default instance."""
  conf = AuthOpenIDConfig.get_by_id(id='default')
  if not conf:
    uri = '%s://%s/auth/openid/callback' % (
        'http' if utils.is_local_dev_server() else 'https',
        app_identity.get_default_version_hostname())
    conf = AuthOpenIDConfig(id='default', redirect_uri=uri)
  return conf


@utils.cache_with_expiration(expiration_sec=300)
def get_cached_config():
  """Same as get_config but with in-memory cache."""
  return get_config()


@utils.cache_with_expiration(expiration_sec=3600)
def get_discovery_document():
  """Returns decoded discovery document with parameters for OpenID protocol.

  The keys we use (with example of values):
    'authorization_endpoint': 'https://accounts.google.com/o/oauth2/v2/auth',
    'token_endpoint': 'https://www.googleapis.com/oauth2/v4/token',
    'userinfo_endpoint': 'https://www.googleapis.com/oauth2/v3/userinfo',

  See https://developers.google.com/identity/protocols/OpenIDConnect#discovery.
  """
  key = 'openid_discovery_doc!%s' % DISCOVERY_URL
  doc = memcache.get(key, namespace='auth')
  if not doc:
    doc = _fetch_json('GET', DISCOVERY_URL)
    memcache.set(key, doc, time=24*3600, namespace='auth')
  return doc


def generate_authentication_uri(conf, state):
  """Returns URI to redirect a user to in order to authenticate via OpenID.

  This is step 1 of the authentication flow. Generate authentication URL and
  redirect user's browser to it. After consent screen, redirect_uri will be
  called (via user's browser) with state and authorization code passed to it,
  eventually resulting in a call to 'handle_authorization_code'.

  Args:
    conf: instance of AuthOpenIDConfig with OAuth client details.
    state: dict with string keys and values to sign and pass to redirect_uri.

  Returns:
    URI to redirect the user to.

  Raises:
    OpenIDError on fatal or transient errors.
  """
  params = {
    'client_id': conf.client_id,
    'response_type': 'code',
    'scope': 'openid email profile',
    'redirect_uri': conf.redirect_uri,
    'state': OpenIDStateToken.generate(embedded=state),
  }
  base_url = get_discovery_document()['authorization_endpoint'].encode('ascii')
  return base_url + '?' + urllib.urlencode(sorted(params.iteritems()))


def validate_state(state):
  """Validates 'state' token passed to redirect_uri.

  Returns:
    Original dict as passed to generate_authentication_uri(...)

  Raises:
    OpenIDError if invalid (e.g. corrupted HMAC).
  """
  try:
    return OpenIDStateToken.validate(state)
  except tokens.InvalidTokenError as e:
    raise OpenIDError('Bad state token: %s' % e)


def handle_authorization_code(conf, code):
  """Validates and decodes state token, gets access_token, fetches profile.

  It is steps 2 and 3 of the authentication flow. Converts authorization code
  into user info dict. See:
    https://developers.google.com/+/web/api/rest/openidconnect/getOpenIdConnect

  Args:
    conf: instance of AuthOpenIDConfig with OAuth client details.
    code: whatever is passed as 'code=...' to redirect URI.

  Returns:
    User info dict, see getOpenIdConnect response.

  Raises:
    OpenIDError on fatal or transient errors.
  """
  # Exchange 'code' for access token.
  post_data = {
    'code': code,
    'client_id': conf.client_id,
    'client_secret': conf.client_secret,
    'redirect_uri': conf.redirect_uri,
    'grant_type': 'authorization_code',
  }
  tok = _fetch_json(
      method='POST',
      url=get_discovery_document()['token_endpoint'],
      payload=urllib.urlencode(post_data),
      headers={'Content-Type': 'application/x-www-form-urlencoded'})

  # Use access token to grab profile information via API call. Note that we
  # want user name and profile picture. If we were only after email, we could
  # grab it from id_token: it is embedded there and can be extracted without
  # additional round trips.
  try:
    auth_header = '%s %s' % (tok['token_type'], tok['access_token'])
  except KeyError as e:
    raise OpenIDError('Missing field in access token dict: %s' % e)
  return _fetch_json(
      method='GET',
      url=get_discovery_document()['userinfo_endpoint'],
      headers={'Authorization': auth_header})


def _fetch_json(method, url, payload=None, headers=None):
  """Makes HTTP request converting errors to OpenIDError."""
  try:
    response = urlfetch.fetch(
        method=method,
        url=url,
        payload=payload,
        headers=headers or {},
        follow_redirects=False,
        deadline=10,
        validate_certificate=True)
  except (apiproxy_errors.DeadlineExceededError, urlfetch.Error) as e:
    raise OpenIDError('Failed to fetch %s: %s' % (url, e), transient=True)
  if response.status_code >= 300:
    raise OpenIDError(
        'Failed to fetch %s (HTTP %d): %r' %
        (url, response.status_code, response.content),
        transient=response.status_code>=500)
  try:
    val = json.loads(response.content)
    if not isinstance(val, dict):
      raise ValueError('not a dict')
    return val
  except ValueError as e:
    raise OpenIDError('Endpoint %s returned bad response: %s' % (url, e))


### API (and implementation) similar (though not identical) to Users API.


# Cookie that holds HMAC-protected id of AuthOpenIDUser for logged in user.
COOKIE_NAME = 'oid_user_id'


class AuthOpenIDUser(ndb.Model):
  """Holds profile information of some user.

  Root entity. ID is 'sub' string of UserInfo response (string with unique
  account id). Created or refreshed on sign in.
  """
  # Latest known email.
  email = ndb.StringProperty()
  # Latest known full name of the user.
  name = ndb.StringProperty(indexed=False)
  # Latest known profile picture URL.
  picture = ndb.StringProperty(indexed=False)


class UserProfile(object):
  """Information about a user, as fetched from OpenID userinfo endpoint.

  Args:
    sub: the unique ID of the authenticated user (ascii str).
    email: the user's email address (unicode).
    name: the user's full name (unicode).
    picture: the URL of the user's profile picture (ascii str).
  """
  def __init__(self, sub, email, name, picture):
    assert isinstance(sub, str)
    assert isinstance(email, unicode)
    assert isinstance(name, unicode)
    assert isinstance(picture, str)
    self.sub = sub
    self.email = email
    self.name = name
    self.picture = picture


class UserIdCookie(tokens.TokenKind):
  """Used to HMAC-tag 'oid_user_id' authentication cookie."""
  expiration_sec = 30 * 24 * 3600
  secret_key = api.SecretKey('oid_user_profile_cookie', scope='local')
  version = 1


class LoginHandler(webapp2.RequestHandler):
  """Redirects the user to OpenID login page."""
  def get(self):
    dest_url = self.request.get('r')
    try:
      dest_url = normalize_dest_url(self.request.host_url, dest_url)
    except ValueError as e:
      self.abort(400, detail='Bad redirect URL: %s' % e)
    try:
      state = {
        'dest_url': dest_url,
        'host_url': self.request.host_url,
      }
      # This will redirect to OpenID login page, then to CallbackHandler on
      # default version of the app (as specified in redirect_uri config).
      self.redirect(generate_authentication_uri(get_cached_config(), state))
    except OpenIDError as e:
      self.abort(500 if e.transient else 400, detail=str(e))


class LogoutHandler(webapp2.RequestHandler):
  """Logs out the user and redirect them to given URL."""
  def get(self):
    dest_url = self.request.get('r')
    try:
      dest_url = normalize_dest_url(self.request.host_url, dest_url)
    except ValueError as e:
      self.abort(400, detail='Bad redirect URL: %s' % e)
    self.response.delete_cookie(COOKIE_NAME)
    nuke_gae_cookies(self.response)
    self.redirect(dest_url)


class CallbackHandler(webapp2.RequestHandler):
  """Handles OAuth2 callback redirect."""

  def get(self):
    # TODO(vadimsh): Show some prettier page. This code path is hit when user
    # clicks "Deny" on consent page.
    error = self.request.get('error')
    if error:
      self.abort(400, detail='OpenID login error: %s' % error)

    # Validate inputs.
    code = self.request.get('code')
    if not code:
      self.abort(400, detail='Missing "code" parameter')
    state = self.request.get('state')
    if not state:
      self.abort(400, detai='Missing "state" parameter')
    try:
      state = validate_state(state)
    except OpenIDError as e:
      self.abort(400, detail=str(e))

    # Callback URI is hardcoded in OAuth2 client config and must always point
    # to default version. Yet we want to support logging to non-default versions
    # that have different hostnames. Do some redirect dance here to pass control
    # to required version if necessary (so that it can set cookie on
    # non-default version domain). Same handler with same params, just with
    # different hostname. For most common case of logging in into default
    # version this should not trigger.
    if self.request.host_url != state['host_url']:
      # Replace 'scheme' and 'netloc' of this_url with host_url values.
      host_url = urlparse.urlsplit(state['host_url'])
      this_url = urlparse.urlsplit(self.request.url)
      target_url = urlparse.urlunsplit(host_url[:2] + this_url[2:])
      self.redirect(target_url)
      return

    # Grab user profile from the code.
    try:
      userinfo = handle_authorization_code(get_cached_config(), code)
    except OpenIDError as e:
      self.abort(500 if e.transient else 400, detail=str(e))

    # Strictly speaking dest_url was already validated when put into state.
    # Double check this.
    dest_url = state['dest_url']
    try:
      dest_url = normalize_dest_url(self.request.host_url, dest_url)
    except ValueError as e:
      self.abort(400, detail='Bad redirect URL: %s' % e)

    # Ignore non https:// URLs for pictures. We serve all pages over HTTPS and
    # don't want to break this rule just for a pretty picture. Google userinfo
    # endpoint always returns https:// URL anyway.
    pic = userinfo.get('picture')
    if pic and not pic.startswith('https://'):
      pic = None
    # Google avatars sometimes look weird if used directly. Resized version
    # always looks fine. 's64' is documented, for example, here:
    # https://cloud.google.com/appengine/docs/python/images
    if pic and pic.endswith('/photo.jpg'):
      pic = pic.rstrip('/photo.jpg') + '/s64/photo.jpg'

    # Refresh datastore entry for logged in user.
    sub = userinfo['sub'].encode('ascii')
    AuthOpenIDUser(
        id=sub,
        email=userinfo['email'],
        name=userinfo['name'],
        picture=pic).put()

    # Make cookie expire a bit earlier than the token itself, to avoid
    # "bad token" errors in most common case.
    expires = utils.utcnow() + datetime.timedelta(
        seconds=UserIdCookie.expiration_sec-1800)
    self.response.set_cookie(
        key=COOKIE_NAME,
        value=UserIdCookie.generate(embedded={'sub': sub}),
        expires=expires,
        secure=not utils.is_local_dev_server(),
        httponly=True)

    nuke_gae_cookies(self.response)
    self.redirect(dest_url)


def get_ui_routes():
  """Returns webapp2.Routes under /auth/openid/*."""
  return [
    webapp2.Route(r'/auth/openid/login', LoginHandler),
    webapp2.Route(r'/auth/openid/logout', LogoutHandler),
    webapp2.Route(r'/auth/openid/callback', CallbackHandler),
  ]


def nuke_gae_cookies(response):
  """Removes GAE authentication related cookies.

  To reduce confusion when OpenID cookies are used. Having users to be logged
  in with two different methods at once is extremely weird.
  """
  response.delete_cookie('SACSID')
  if utils.is_local_dev_server():
    response.delete_cookie('dev_appserver_login')


def normalize_dest_url(host_url, dest_url):
  """Strips current hostname from url if it includes it.

  Passes path-only URLs through unchanged. Raises ValueError if URL point to
  other hostname or can not be used in create_*_url.
  """
  if not dest_url:
    raise ValueError('Destination URL must be provided')
  assert host_url[-1] != '/', host_url
  if dest_url.startswith(host_url + '/'):
    return dest_url[len(host_url):]
  if dest_url[0] != '/':
    raise ValueError(
        'Destination URL (%s) must be relative to the current host (%s)' %
        (dest_url, host_url))
  return dest_url


def get_current_user(request):
  """Returns UserProfile of the current user or None.

  Args:
    request: webapp2.Request object with the current request.
  """
  assert not ndb.in_transaction(), (
      'Do not call get_current_user() in a transaction')
  cookie = request.cookies.get(COOKIE_NAME)
  if not cookie:
    return None
  try:
    decoded = UserIdCookie.validate(cookie)
  except tokens.InvalidTokenError as e:
    return None
  # Relying on ndb in-process cache here to avoid refetches from datastore.
  e = AuthOpenIDUser.get_by_id(decoded['sub'])
  if not e:
    return None
  return UserProfile(
      sub=decoded['sub'],
      email=e.email,
      name=e.name,
      picture=e.picture.encode('ascii') if e.picture else None)


def create_login_url(request, dest_url):
  """Returns URL to a login page that redirects to dest_url on login.

  Args:
    request: webapp2.Request object with the current request.
    dest_url: URL to redirect to on successful login.
  """
  assert request.host_url[-1] != '/', request.host_url
  dest_url = normalize_dest_url(request.host_url, dest_url)
  return '%s/auth/openid/login?r=%s' % (
      request.host_url, urllib.quote_plus(dest_url))


def create_logout_url(request, dest_url):
  """Returns URL to a logout page that redirects to dest_url on logout.

  Args:
    request: webapp2.Request object with the current request.
    dest_url: URL to redirect to after logout.
  """
  assert request.host_url[-1] != '/', request.host_url
  dest_url = normalize_dest_url(request.host_url, dest_url)
  return '%s/auth/openid/logout?r=%s' % (
      request.host_url, urllib.quote_plus(dest_url))
