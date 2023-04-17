# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility library for reading user information from an id_token.

This is an experimental library that can temporarily be used to extract
a user from an id_token.  The functionality provided by this library
will be provided elsewhere in the future.
"""

from __future__ import absolute_import

import base64
import binascii
import hmac
import json
import logging
import os
import re
import six
import time
from six.moves import urllib
from collections import Container as _Container
from collections import Iterable as _Iterable
from collections import Mapping as _Mapping

from google.appengine.api import memcache
from google.appengine.api import oauth
from google.appengine.api import urlfetch
from google.appengine.api import users

from . import constants
from . import types as endpoints_types

try:
  # PyCrypto may not be installed for the import_aeta_test or in dev's
  # individual Python installations.  It is available on AppEngine in prod.

  # Disable "Import not at top of file" warning.
  # pylint: disable=g-import-not-at-top
  from Crypto.Hash import SHA256
  from Crypto.PublicKey import RSA
  # pylint: enable=g-import-not-at-top
  _CRYPTO_LOADED = True
except ImportError:
  _CRYPTO_LOADED = False


__all__ = [
    'convert_jwks_uri',
    'get_current_user',
    'get_verified_jwt',
    'InvalidGetUserCall',
    'SKIP_CLIENT_ID_CHECK',
]

_logger = logging.getLogger(__name__)

SKIP_CLIENT_ID_CHECK = ['*']  # This needs to be a list, for comparisons.
_CLOCK_SKEW_SECS = 300  # 5 minutes in seconds
_MAX_TOKEN_LIFETIME_SECS = 86400  # 1 day in seconds
_DEFAULT_CERT_URI = ('https://www.googleapis.com/service_accounts/v1/metadata/'
                     'raw/federated-signon@system.gserviceaccount.com')
_ENDPOINTS_USER_INFO = 'google.api.auth.user_info'
_ENV_USE_OAUTH_SCOPE = 'ENDPOINTS_USE_OAUTH_SCOPE'
_ENV_AUTH_EMAIL = 'ENDPOINTS_AUTH_EMAIL'
_ENV_AUTH_DOMAIN = 'ENDPOINTS_AUTH_DOMAIN'
_EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
_TOKENINFO_URL = 'https://www.googleapis.com/oauth2/v3/tokeninfo'
_MAX_AGE_REGEX = re.compile(r'\s*max-age\s*=\s*(\d+)\s*')
_CERT_NAMESPACE = '__verify_jwt'
_ISSUERS = ('accounts.google.com', 'https://accounts.google.com')
_DEFAULT_GOOGLE_ISSUER = {
    'google_id_token': endpoints_types.Issuer(_ISSUERS, _DEFAULT_CERT_URI)
}


class _AppIdentityError(Exception):
  pass


class InvalidGetUserCall(Exception):
  """Called get_current_user when the environment was not set up for it."""


# pylint: disable=g-bad-name
def get_current_user():
  """Get user information from the id_token or oauth token in the request.

  This should only be called from within an Endpoints request handler,
  decorated with an @endpoints.method decorator.  The decorator should include
  the https://www.googleapis.com/auth/userinfo.email scope.

  If `endpoints_management.control.wsgi.AuthenticationMiddleware` is enabled,
  this returns the user info decoded by the middleware. Otherwise, if the
  current request uses an id_token, this validates and parses the token against
  the info in the current request handler and returns the user.  Or, for an
  Oauth token, this call validates the token against the tokeninfo endpoint and
  oauth.get_current_user with the scopes provided in the method's decorator.

  Returns:
    None if there is no token or it's invalid.  If the token was valid, this
      returns a User.  Only the user's email field is guaranteed to be set.
      Other fields may be empty.

  Raises:
    InvalidGetUserCall: if the environment variables necessary to determine the
      endpoints user are not set. These are typically set when processing a
      request using an Endpoints handler. If they are not set, it likely
      indicates that this function was called from outside an Endpoints request
      handler.
  """
  if not _is_auth_info_available():
    raise InvalidGetUserCall('No valid endpoints user in environment.')

  if _ENDPOINTS_USER_INFO in os.environ:
    user_info = os.environ[_ENDPOINTS_USER_INFO]
    return users.User(user_info.email)

  if _ENV_USE_OAUTH_SCOPE in os.environ:
    # We can get more information from the oauth.get_current_user function,
    # as long as we know what scope to use.  Since that scope has been
    # cached, we can just return this:
    return oauth.get_current_user(os.environ[_ENV_USE_OAUTH_SCOPE].split())

  if (_ENV_AUTH_EMAIL in os.environ and
      _ENV_AUTH_DOMAIN in os.environ):
    if not os.environ[_ENV_AUTH_EMAIL]:
      # Either there was no id token or we were unable to validate it,
      # so there's no user.
      return None

    return users.User(os.environ[_ENV_AUTH_EMAIL],
                      os.environ[_ENV_AUTH_DOMAIN] or None)

  # Shouldn't hit this, because all the _is_auth_info_available cases were
  # checked, but just in case.
  return None


# pylint: disable=g-bad-name
def _is_auth_info_available():
  """Check if user auth info has been set in environment variables."""
  return (_ENDPOINTS_USER_INFO in os.environ or
          (_ENV_AUTH_EMAIL in os.environ and _ENV_AUTH_DOMAIN in os.environ) or
          _ENV_USE_OAUTH_SCOPE in os.environ)


def _maybe_set_current_user_vars(method, api_info=None, request=None):
  """Get user information from the id_token or oauth token in the request.

  Used internally by Endpoints to set up environment variables for user
  authentication.

  Args:
    method: The class method that's handling this request.  This method
      should be annotated with @endpoints.method.
    api_info: An api_config._ApiInfo instance. Optional. If None, will attempt
      to parse api_info from the implicit instance of the method.
    request: The current request, or None.
  """
  if _is_auth_info_available():
    return

  # By default, there's no user.
  os.environ[_ENV_AUTH_EMAIL] = ''
  os.environ[_ENV_AUTH_DOMAIN] = ''

  # Choose settings on the method, if specified.  Otherwise, choose settings
  # from the API.  Specifically check for None, so that methods can override
  # with empty lists.
  try:
    api_info = api_info or method.im_self.api_info
  except AttributeError:
    # The most common case for this is someone passing an unbound method
    # to this function, which most likely only happens in our unit tests.
    # We could propagate the exception, but this results in some really
    # difficult to debug behavior.  Better to log a warning and pretend
    # there are no API-level settings.
    _logger.warning('AttributeError when accessing %s.im_self.  An unbound '
                    'method was probably passed as an endpoints handler.',
                    method.__name__)
    scopes = method.method_info.scopes
    audiences = method.method_info.audiences
    allowed_client_ids = method.method_info.allowed_client_ids
  else:
    scopes = (method.method_info.scopes
              if method.method_info.scopes is not None
              else api_info.scopes)
    audiences = (method.method_info.audiences
                 if method.method_info.audiences is not None
                 else api_info.audiences)
    allowed_client_ids = (method.method_info.allowed_client_ids
                          if method.method_info.allowed_client_ids is not None
                          else api_info.allowed_client_ids)

  if not scopes and not audiences and not allowed_client_ids:
    # The user hasn't provided any information to allow us to parse either
    # an id_token or an Oauth token.  They appear not to be interested in
    # auth.
    return

  token = _get_token(request)
  if not token:
    return None

  if allowed_client_ids and _is_local_dev():
    allowed_client_ids = (constants.API_EXPLORER_CLIENT_ID,) + tuple(allowed_client_ids)

  # When every item in the acceptable scopes list is
  # "https://www.googleapis.com/auth/userinfo.email", and there is a non-empty
  # allowed_client_ids list, the API code will first attempt OAuth 2/OpenID
  # Connect ID token processing for any incoming bearer token.
  if ((scopes == [_EMAIL_SCOPE] or scopes == (_EMAIL_SCOPE,)) and
      allowed_client_ids):
    _logger.debug('Checking for id_token.')
    issuers = api_info.issuers
    if issuers is None:
      issuers = _DEFAULT_GOOGLE_ISSUER
    elif 'google_id_token' not in issuers:
      issuers.update(_DEFAULT_GOOGLE_ISSUER)
    time_now = int(time.time())
    user = _get_id_token_user(token, issuers, audiences, allowed_client_ids,
                              time_now, memcache)
    if user:
      os.environ[_ENV_AUTH_EMAIL] = user.email()
      os.environ[_ENV_AUTH_DOMAIN] = user.auth_domain()
      return

  # Check if the user is interested in an oauth token.
  if scopes:
    _logger.debug('Checking for oauth token.')
    if _is_local_dev():
      _set_bearer_user_vars_local(token, allowed_client_ids, scopes)
    else:
      _set_bearer_user_vars(allowed_client_ids, scopes)


def _get_token(
    request=None, allowed_auth_schemes=('OAuth', 'Bearer'),
    allowed_query_keys=('bearer_token', 'access_token')):
  """Get the auth token for this request.

  Auth token may be specified in either the Authorization header or
  as a query param (either access_token or bearer_token).  We'll check in
  this order:
    1. Authorization header.
    2. bearer_token query param.
    3. access_token query param.

  Args:
    request: The current request, or None.

  Returns:
    The token in the request or None.
  """
  allowed_auth_schemes = _listlike_guard(
      allowed_auth_schemes, 'allowed_auth_schemes', iterable_only=True)
  # Check if the token is in the Authorization header.
  auth_header = os.environ.get('HTTP_AUTHORIZATION')
  if auth_header:
    for auth_scheme in allowed_auth_schemes:
      if auth_header.startswith(auth_scheme):
        return auth_header[len(auth_scheme) + 1:]
    # If an auth header was specified, even if it's an invalid one, we won't
    # look for the token anywhere else.
    return None

  # Check if the token is in the query string.
  if request:
    allowed_query_keys = _listlike_guard(
        allowed_query_keys, 'allowed_query_keys', iterable_only=True)
    for key in allowed_query_keys:
      token, _ = request.get_unrecognized_field_info(key)
      if token:
        return token


def _get_id_token_user(token, issuers, audiences, allowed_client_ids, time_now, cache):
  """Get a User for the given id token, if the token is valid.

  Args:
    token: The id_token to check.
    issuers: dict of Issuers
    audiences: List of audiences that are acceptable.
    allowed_client_ids: List of client IDs that are acceptable.
    time_now: The current time as an int (eg. int(time.time())).
    cache: Cache to use (eg. the memcache module).

  Returns:
    A User if the token is valid, None otherwise.
  """
  # Verify that the token is valid before we try to extract anything from it.
  # This verifies the signature and some of the basic info in the token.
  for issuer_key, issuer in issuers.items():
    issuer_cert_uri = convert_jwks_uri(issuer.jwks_uri)
    try:
      parsed_token = _verify_signed_jwt_with_certs(
          token, time_now, cache, cert_uri=issuer_cert_uri)
    except Exception:  # pylint: disable=broad-except
      _logger.debug(
          'id_token verification failed for issuer %s', issuer_key, exc_info=True)
      continue

    issuer_values = _listlike_guard(issuer.issuer, 'issuer', log_warning=False)
    if isinstance(audiences, _Mapping):
      audiences = audiences[issuer_key]
    if _verify_parsed_token(
        parsed_token, issuer_values, audiences, allowed_client_ids,
        # There's some special handling we do for Google issuers.
        # ESP doesn't do this, and it's both unnecessary and invalid for other issuers.
        # So we'll turn it off except in the Google issuer case.
        is_legacy_google_auth=(issuer.issuer == _ISSUERS)):
      email = parsed_token['email']
      # The token might have an id, but it's a Gaia ID that's been
      # obfuscated with the Focus key, rather than the AppEngine (igoogle)
      # key.  If the developer ever put this email into the user DB
      # and retrieved the ID from that, it'd be different from the ID we'd
      # return here, so it's safer to not return the ID.
      # Instead, we'll only return the email.
      return users.User(email)


# pylint: disable=unused-argument
def _set_oauth_user_vars(token_info, audiences, allowed_client_ids, scopes,
                         local_dev):
  _logger.warning('_set_oauth_user_vars is deprecated and will be removed '
                  'soon.')
  return _set_bearer_user_vars(allowed_client_ids, scopes)
# pylint: enable=unused-argument


def _process_scopes(scopes):
  """Parse a scopes list into a set of all scopes and a set of sufficient scope sets.

     scopes: A list of strings, each of which is a space-separated list of scopes.
       Examples: ['scope1']
                 ['scope1', 'scope2']
                 ['scope1', 'scope2 scope3']

     Returns:
       all_scopes: a set of strings, each of which is one scope to check for
       sufficient_scopes: a set of sets of strings; each inner set is
         a set of scopes which are sufficient for access.
         Example: {{'scope1'}, {'scope2', 'scope3'}}
  """
  all_scopes = set()
  sufficient_scopes = set()
  for scope_set in scopes:
    scope_set_scopes = frozenset(scope_set.split())
    all_scopes.update(scope_set_scopes)
    sufficient_scopes.add(scope_set_scopes)
  return all_scopes, sufficient_scopes


def _are_scopes_sufficient(authorized_scopes, sufficient_scopes):
  """Check if a list of authorized scopes satisfies any set of sufficient scopes.

     Args:
       authorized_scopes: a list of strings, return value from oauth.get_authorized_scopes
       sufficient_scopes: a set of sets of strings, return value from _process_scopes
  """
  for sufficient_scope_set in sufficient_scopes:
    if sufficient_scope_set.issubset(authorized_scopes):
      return True
  return False



def _set_bearer_user_vars(allowed_client_ids, scopes):
  """Validate the oauth bearer token and set endpoints auth user variables.

  If the bearer token is valid, this sets ENDPOINTS_USE_OAUTH_SCOPE.  This
  provides enough information that our endpoints.get_current_user() function
  can get the user.

  Args:
    allowed_client_ids: List of client IDs that are acceptable.
    scopes: List of acceptable scopes.
  """
  all_scopes, sufficient_scopes = _process_scopes(scopes)
  try:
    authorized_scopes = oauth.get_authorized_scopes(sorted(all_scopes))
  except oauth.Error:
    _logger.debug('Unable to get authorized scopes.', exc_info=True)
    return
  if not _are_scopes_sufficient(authorized_scopes, sufficient_scopes):
    _logger.warning('Authorized scopes did not satisfy scope requirements.')
    return
  client_id = oauth.get_client_id(authorized_scopes)

  # The client ID must be in allowed_client_ids.  If allowed_client_ids is
  # empty, don't allow any client ID.  If allowed_client_ids is set to
  # SKIP_CLIENT_ID_CHECK, all client IDs will be allowed.
  if (list(allowed_client_ids) != SKIP_CLIENT_ID_CHECK and
      client_id not in allowed_client_ids):
    _logger.warning('Client ID is not allowed: %s', client_id)
    return

  os.environ[_ENV_USE_OAUTH_SCOPE] = ' '.join(authorized_scopes)
  _logger.debug('get_current_user() will return user from matched oauth_user.')


def _set_bearer_user_vars_local(token, allowed_client_ids, scopes):
  """Validate the oauth bearer token on the dev server.

  Since the functions in the oauth module return only example results in local
  development, this hits the tokeninfo endpoint and attempts to validate the
  token.  If it's valid, we'll set _ENV_AUTH_EMAIL and _ENV_AUTH_DOMAIN so we
  can get the user from the token.

  Args:
    token: String with the oauth token to validate.
    allowed_client_ids: List of client IDs that are acceptable.
    scopes: List of acceptable scopes.
  """
  # Get token info from the tokeninfo endpoint.
  result = urlfetch.fetch(
      '%s?%s' % (_TOKENINFO_URL, urllib.parse.urlencode({'access_token': token})))
  if result.status_code != 200:
    try:
      error_description = json.loads(result.content)['error_description']
    except (ValueError, KeyError):
      error_description = ''
    _logger.error('Token info endpoint returned status %s: %s',
                  result.status_code, error_description)
    return
  token_info = json.loads(result.content)

  # Validate email.
  if 'email' not in token_info:
    _logger.warning('Oauth token doesn\'t include an email address.')
    return
  if token_info.get('email_verified') != 'true':
    _logger.warning('Oauth token email isn\'t verified.')
    return

  # Validate client ID.
  client_id = token_info.get('azp')
  if (list(allowed_client_ids) != SKIP_CLIENT_ID_CHECK and
      client_id not in allowed_client_ids):
    _logger.warning('Client ID is not allowed: %s', client_id)
    return

  # Verify at least one of the scopes matches.
  _, sufficient_scopes = _process_scopes(scopes)
  authorized_scopes = token_info.get('scope', '').split(' ')
  if not _are_scopes_sufficient(authorized_scopes, sufficient_scopes):
    _logger.warning('Oauth token scopes don\'t match any acceptable scopes.')
    return

  os.environ[_ENV_AUTH_EMAIL] = token_info['email']
  os.environ[_ENV_AUTH_DOMAIN] = ''
  _logger.debug('Local dev returning user from token.')


def _is_local_dev():
  return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')


def _verify_parsed_token(parsed_token, issuers, audiences, allowed_client_ids, is_legacy_google_auth=True):
  """Verify a parsed user ID token.

  Args:
    parsed_token: The parsed token information.
    issuers: A list of allowed issuers
    audiences: The allowed audiences.
    allowed_client_ids: The allowed client IDs.

  Returns:
    True if the token is verified, False otherwise.
  """
  # Verify the issuer.
  if parsed_token.get('iss') not in issuers:
    _logger.warning('Issuer was not valid: %s', parsed_token.get('iss'))
    return False

  # Check audiences.
  aud = parsed_token.get('aud')
  if not aud:
    _logger.warning('No aud field in token')
    return False
  # Special legacy handling if aud == cid.  This occurs with iOS and browsers.
  # As long as audience == client_id and cid is allowed, we need to accept
  # the audience for compatibility.
  cid = parsed_token.get('azp')
  audience_allowed = (aud in audiences) or (is_legacy_google_auth and aud == cid)
  if not audience_allowed:
    _logger.warning('Audience not allowed: %s', aud)
    return False

  # Check allowed client IDs, for legacy auth.
  if is_legacy_google_auth:
    if list(allowed_client_ids) == SKIP_CLIENT_ID_CHECK:
      _logger.warning('Client ID check can\'t be skipped for ID tokens.  '
                      'Id_token cannot be verified.')
      return False
    elif not cid or cid not in allowed_client_ids:
      _logger.warning('Client ID is not allowed: %s', cid)
      return False

  if 'email' not in parsed_token:
    return False

  return True


def _urlsafe_b64decode(b64string):
  # Guard against unicode strings, which base64 can't handle.
  b64string = six.ensure_binary(b64string, 'ascii')
  padded = b64string + '=' * ((4 - len(b64string)) % 4)
  return base64.urlsafe_b64decode(padded)


def _get_cert_expiration_time(headers):
  """Get the expiration time for a cert, given the response headers.

  Get expiration time from the headers in the result.  If we can't get
  a time from the headers, this returns 0, indicating that the cert
  shouldn't be cached.

  Args:
    headers: A dict containing the response headers from the request to get
      certs.

  Returns:
    An integer with the number of seconds the cert should be cached.  This
    value is guaranteed to be >= 0.
  """
  # Check the max age of the cert.
  cache_control = headers.get('Cache-Control', '')
  # http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2 indicates only
  # a comma-separated header is valid, so it should be fine to split this on
  # commas.
  for entry in cache_control.split(','):
    match = _MAX_AGE_REGEX.match(entry)
    if match:
      cache_time_seconds = int(match.group(1))
      break
  else:
    return 0

  # Subtract the cert's age.
  age = headers.get('Age')
  if age is not None:
    try:
      age = int(age)
    except ValueError:
      age = 0
    cache_time_seconds -= age

  return max(0, cache_time_seconds)


def _get_cached_certs(cert_uri, cache):
  """Get certs from cache if present; otherwise, gets from URI and caches them.

  Args:
    cert_uri: URI from which to retrieve certs if cache is stale or empty.
    cache: Cache of pre-fetched certs.

  Returns:
    The retrieved certs.
  """
  certs = cache.get(cert_uri, namespace=_CERT_NAMESPACE)
  if certs is None:
    _logger.debug('Cert cache miss for %s', cert_uri)
    try:
      result = urlfetch.fetch(cert_uri)
    except AssertionError:
      # This happens in unit tests.  Act as if we couldn't get any certs.
      return None

    if result.status_code == 200:
      certs = json.loads(result.content)
      expiration_time_seconds = _get_cert_expiration_time(result.headers)
      if expiration_time_seconds:
        cache.set(cert_uri, certs, time=expiration_time_seconds,
                  namespace=_CERT_NAMESPACE)
    else:
      _logger.error(
          'Certs not available, HTTP request returned %d', result.status_code)

  return certs


def _b64_to_int(b):
  b = six.ensure_binary(b, 'ascii')
  b += b'=' * ((4 - len(b)) % 4)
  b = base64.b64decode(b)
  return int(binascii.hexlify(b), 16)


def _verify_signed_jwt_with_certs(
    jwt, time_now, cache,
    cert_uri=_DEFAULT_CERT_URI):
  """Verify a JWT against public certs.

  See http://self-issued.info/docs/draft-jones-json-web-token.html.

  The PyCrypto library included with Google App Engine is severely limited and
  so you have to use it very carefully to verify JWT signatures. The first
  issue is that the library can't read X.509 files, so we make a call to a
  special URI that has the public cert in modulus/exponent form in JSON.

  The second issue is that the RSA.verify method doesn't work, at least for
  how the JWT tokens are signed, so we have to manually verify the signature
  of the JWT, which means hashing the signed part of the JWT and comparing
  that to the signature that's been encrypted with the public key.

  Args:
    jwt: string, A JWT.
    time_now: The current time, as an int (eg. int(time.time())).
    cache: Cache to use (eg. the memcache module).
    cert_uri: string, URI to get cert modulus and exponent in JSON format.

  Returns:
    dict, The deserialized JSON payload in the JWT.

  Raises:
    _AppIdentityError: if any checks are failed.
  """

  segments = jwt.split('.')

  if len(segments) != 3:
    # Note that anywhere we print the jwt or its json body, we need to use
    # %r instead of %s, so that non-printable characters are escaped safely.
    raise _AppIdentityError('Token is not an id_token (Wrong number of '
                            'segments)')
  signed = '%s.%s' % (segments[0], segments[1])

  signature = _urlsafe_b64decode(segments[2])

  # pycrypto only deals in integers, so we have to convert the string of bytes
  # into an int.
  lsignature = int(binascii.hexlify(signature), 16)

  # Verify expected header.
  header_body = _urlsafe_b64decode(segments[0])
  try:
    header = json.loads(header_body)
  except:
    raise _AppIdentityError("Can't parse header")
  if header.get('alg') != 'RS256':
    raise _AppIdentityError('Unexpected encryption algorithm: %r' %
                            header.get('alg'))

  # Formerly we would parse the token body here.
  # However, it's not safe to do that without first checking the signature.

  certs = _get_cached_certs(cert_uri, cache)
  if certs is None:
    raise _AppIdentityError(
        'Unable to retrieve certs needed to verify the signed JWT')

  # Verify that we were able to load the Crypto libraries, before we try
  # to use them.
  if not _CRYPTO_LOADED:
    raise _AppIdentityError('Unable to load pycrypto library.  Can\'t verify '
                            'id_token signature.  See http://www.pycrypto.org '
                            'for more information on pycrypto.')

  # SHA256 hash of the already 'signed' segment from the JWT. Since a SHA256
  # hash, will always have length 64.
  local_hash = SHA256.new(signed).hexdigest()

  # Check signature.
  verified = False
  for keyvalue in certs['keyvalues']:
    try:
      modulus = _b64_to_int(keyvalue['modulus'])
      exponent = _b64_to_int(keyvalue['exponent'])
      key = RSA.construct((modulus, exponent))

      # Encrypt, and convert to a hex string.
      hexsig = '%064x' % key.encrypt(lsignature, '')[0]
      # Make sure we have only last 64 base64 chars
      hexsig = hexsig[-64:]

      # Check the signature on 'signed' by encrypting 'signature' with the
      # public key and confirming the result matches the SHA256 hash of
      # 'signed'. hmac.compare_digest(a, b) is used to avoid timing attacks.
      verified = hmac.compare_digest(hexsig, local_hash)
      if verified:
        break
    except Exception as e:  # pylint: disable=broad-except
      # Log the exception for debugging purpose.
      _logger.debug(
          'Signature verification error: %s; continuing with the next cert.', e)
      continue
  if not verified:
    raise _AppIdentityError('Invalid token signature')

  # Parse token.
  json_body = _urlsafe_b64decode(segments[1])
  try:
    parsed = json.loads(json_body)
  except:
    raise _AppIdentityError("Can't parse token body")

  # Check creation timestamp.
  iat = parsed.get('iat')
  if iat is None:
    raise _AppIdentityError('No iat field in token')
  earliest = iat - _CLOCK_SKEW_SECS

  # Check expiration timestamp.
  exp = parsed.get('exp')
  if exp is None:
    raise _AppIdentityError('No exp field in token')
  if exp >= time_now + _MAX_TOKEN_LIFETIME_SECS:
    raise _AppIdentityError('exp field too far in future')
  latest = exp + _CLOCK_SKEW_SECS

  if time_now < earliest:
    raise _AppIdentityError('Token used too early, %d < %d' %
                            (time_now, earliest))
  if time_now > latest:
    raise _AppIdentityError('Token used too late, %d > %d' %
                            (time_now, latest))

  return parsed


_TEXT_CERT_PREFIX = 'https://www.googleapis.com/robot/v1/metadata/x509/'
_JSON_CERT_PREFIX = 'https://www.googleapis.com/service_accounts/v1/metadata/raw/'


def convert_jwks_uri(jwks_uri):
  """
  The PyCrypto library included with Google App Engine is severely limited and
  can't read X.509 files, so we change the URI to a special URI that has the
  public cert in modulus/exponent form in JSON.
  """
  if not jwks_uri.startswith(_TEXT_CERT_PREFIX):
    return jwks_uri
  return jwks_uri.replace(_TEXT_CERT_PREFIX, _JSON_CERT_PREFIX)


def get_verified_jwt(
    providers, audiences,
    check_authorization_header=True, check_query_arg=True,
    request=None, cache=memcache):
  """
  This function will extract, verify, and parse a JWT token from the
  Authorization header or access_token query argument.

  The JWT is assumed to contain an issuer and audience claim, as well
  as issued-at and expiration timestamps. The signature will be
  cryptographically verified, the claims and timestamps will be
  checked, and the resulting parsed JWT body is returned.

  If at any point the JWT is missing or found to be invalid, the
  return result will be None.

  Arguments:
  providers - An iterable of dicts each containing 'issuer' and 'cert_uri' keys
  audiences - An iterable of valid audiences

  check_authorization_header - Boolean; check 'Authorization: Bearer' header
  check_query_arg - Boolean; check 'access_token' query arg

  request - Must be the request object if check_query_arg is true; otherwise ignored.
  cache - In testing, override the certificate cache
  """
  if not (check_authorization_header or check_query_arg):
      raise ValueError(
          'Either check_authorization_header or check_query_arg must be True.')
  if check_query_arg and request is None:
      raise ValueError(
          'Cannot check query arg without request object.')
  schemes = ('Bearer',) if check_authorization_header else ()
  keys = ('access_token',) if check_query_arg else ()
  token = _get_token(
      request=request, allowed_auth_schemes=schemes, allowed_query_keys=keys)
  if token is None:
    return None
  time_now = int(time.time())
  for provider in providers:
    parsed_token = _parse_and_verify_jwt(
        token, time_now, (provider['issuer'],), audiences, provider['cert_uri'], cache)
    if parsed_token is not None:
      return parsed_token
  return None


def _parse_and_verify_jwt(token, time_now, issuers, audiences, cert_uri, cache):
  try:
    parsed_token = _verify_signed_jwt_with_certs(token, time_now, cache, cert_uri)
  except (_AppIdentityError, TypeError) as e:
    _logger.debug('id_token verification failed: %s', e)
    return None

  issuers = _listlike_guard(issuers, 'issuers')
  audiences = _listlike_guard(audiences, 'audiences')
  # We can't use _verify_parsed_token because there's no client id (azp) or email in these JWTs
  # Verify the issuer.
  if parsed_token.get('iss') not in issuers:
    _logger.warning('Issuer was not valid: %s', parsed_token.get('iss'))
    return None

  # Check audiences.
  aud = parsed_token.get('aud')
  if not aud:
    _logger.warning('No aud field in token')
    return None
  if aud not in audiences:
    _logger.warning('Audience not allowed: %s', aud)
    return None

  return parsed_token


def _listlike_guard(obj, name, iterable_only=False, log_warning=True):
  """
  We frequently require passed objects to support iteration or
  containment expressions, but not be strings. (Of course, strings
  support iteration and containment, but not usefully.)  If the passed
  object is a string, we'll wrap it in a tuple and return it. If it's
  already an iterable, we'll return it as-is. Otherwise, we'll raise a
  TypeError.
  """
  required_type = (_Iterable,) if iterable_only else (_Container, _Iterable)
  required_type_name = ' or '.join(t.__name__ for t in required_type)

  if not isinstance(obj, required_type):
    raise ValueError('{} must be of type {}'.format(name, required_type_name))
  # at this point it is definitely the right type, but might be a string
  if isinstance(obj, six.string_types):
    if log_warning:
      _logger.warning('{} passed as a string; should be list-like'.format(name))
    return (obj,)
  return obj
