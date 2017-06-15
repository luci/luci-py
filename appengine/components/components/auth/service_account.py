# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Generation of OAuth2 token for a service account.

Supports two ways to generate OAuth2 tokens:
  * app_identity.get_access_token(...) to use native GAE service account.
  * OAuth flow with JWT token, for @*.iam.gserviceaccount.com service
    accounts (the one with a private key).
"""

import base64
import collections
import hashlib
import json
import logging
import random
import urllib

from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import utils


# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'get_access_token',
  'get_access_token_async',
  'AccessTokenError',
  'ServiceAccountKey',
]

# Information about @*.iam.gserviceaccount.com. Field values can be extracted
# from corresponding fields in JSON file produced by "Generate new JSON key"
# button in "Credentials" section of any Cloud Console project.
ServiceAccountKey = collections.namedtuple('ServiceAccountKey', [
  # Service account email.
  'client_email',
  # Service account PEM encoded private key.
  'private_key',
  # Service account key fingerprint, an unique identifier of this key.
  'private_key_id',
])


class AccessTokenError(Exception):
  """Raised by get_access_token() on fatal errors."""


# Do not log AccessTokenError exception raised from a tasklet.
ndb.add_flow_exception(AccessTokenError)


@ndb.tasklet
def get_access_token_async(scopes, service_account_key=None):
  """Returns an OAuth2 access token for a service account.

  If 'service_account_key' is specified, will use it to generate access token
  for corresponding @*iam.gserviceaccount.com account. Otherwise will invoke
  app_identity.get_access_token(...) to use app's @appspot.gserviceaccount.com
  account.

  Args:
    scopes: the requested API scope string, or a list of strings.
    service_account_key: optional instance of ServiceAccountKey.

  Returns:
    Tuple (access token, expiration time in seconds since the epoch). The token
    should be valid for at least 5 minutes. It will be cached across multiple
    calls using memcache (e.g. get_access_token call can be considered cheap).

  Raises:
    AccessTokenError on errors.
  """
  # Accept a single string to mimic app_identity.get_access_token behavior.
  if isinstance(scopes, basestring):
    scopes = [scopes]
  scopes = sorted(scopes)

  if service_account_key:
    # Empty private_key_id probably means that the app is not configured yet.
    if not service_account_key.private_key_id:
      raise AccessTokenError('Service account secret key is not initialized')
    cache_key = _memcache_key(
        method='pkey',
        email=service_account_key.client_email,
        scopes=scopes,
        key_id=service_account_key.private_key_id)
    t = yield _get_jwt_based_token_async(scopes, cache_key, service_account_key)
    raise ndb.Return(t)

  # TODO(vadimsh): Use app_identity.make_get_access_token_call to make it async.
  raise ndb.Return(app_identity.get_access_token(scopes))


def get_access_token(*args, **kwargs):
  """Blocking version of get_access_token_async."""
  return get_access_token_async(*args, **kwargs).get_result()


## Private stuff.


_MEMCACHE_NS = 'access_tokens'


def _memcache_key(method, email, scopes, key_id=None):
  """Returns a string to use as a memcache key for a token.

  Args:
    method: 'pkey' currently.
    email: service account email we are getting a token for.
    scopes: list of strings with scopes.
    key_id: private key ID used (if known).
  """
  blob = utils.encode_to_json({
    'method': method,
    'email': email,
    'scopes': scopes,
    'key_id': key_id,
  })
  return hashlib.sha256(blob).hexdigest()


@ndb.tasklet
def _get_jwt_based_token_async(scopes, cache_key, service_account_key):
  """Returns token for @*.iam.gserviceaccount.com service account.

  Randomizes refresh time to avoid thundering herd effect when token expires.
  """
  token_info = yield _memcache_get(cache_key, namespace=_MEMCACHE_NS)
  should_refresh = (
      not token_info or
      token_info['exp_ts'] - utils.time_time() < random.randint(300, 600))
  if should_refresh:
    logging.info('Refreshing the access token with scopes %s', scopes)
    token_info = yield _mint_jwt_based_token_async(scopes, service_account_key)
    yield _memcache_set(
        cache_key, token_info, token_info['exp_ts'], namespace=_MEMCACHE_NS)
  raise ndb.Return((token_info['access_token'], token_info['exp_ts']))


@ndb.tasklet
def _mint_jwt_based_token_async(scopes, service_account_key):
  """Creates new access token given service account private key."""
  # For more info see:
  # * https://developers.google.com/accounts/docs/OAuth2ServiceAccount.

  b64_encode = lambda data: base64.urlsafe_b64encode(data).rstrip('=')

  # JWT header.
  header_b64 = b64_encode(utils.encode_to_json({
    'alg': 'RS256',
    'kid': service_account_key.private_key_id,
    'typ': 'JWT',
  }))

  # Prepare a claim set to be signed by the service account key. Note that
  # Google backends seem to ignore 'exp' field and always give one-hour long
  # tokens, so we just always request 1h long token too.
  #
  # Also revert time back a tiny bit, for the sake of machines whose time is not
  # perfectly in sync with global time. If client machine's time is in the
  # future according to Google server clock, the access token request will be
  # denied. It doesn't complain about slightly late clock though.
  now = int(utils.time_time()) - 5
  claimset_b64 = b64_encode(utils.encode_to_json({
    'aud': 'https://www.googleapis.com/oauth2/v4/token',
    'exp': now + 3600,
    'iat': now,
    'iss': service_account_key.client_email,
    'scope': ' '.join(scopes),
  }))

  # Sign <header>.<claimset> with account's private key.
  signature_b64 = b64_encode(_rsa_sign(
      '%s.%s' % (header_b64, claimset_b64), service_account_key.private_key))

  # URL encoded body of a token request.
  request_body = urllib.urlencode({
    'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    'assertion': '%s.%s.%s' % (header_b64, claimset_b64, signature_b64),
  })

  # Exchange signed claimset for an access token.
  token = yield _call_async(
      url='https://www.googleapis.com/oauth2/v4/token',
      payload=request_body,
      method='POST',
      headers={
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
      })
  raise ndb.Return({
    'access_token': str(token['access_token']),
    'exp_ts': int(utils.time_time() + token['expires_in']),
  })


@ndb.tasklet
def _call_async(url, payload, method, headers):
  """Makes URL fetch call aggressively retrying on errors a bunch of times.

  On success returns deserialized JSON response body.
  On failure raises AccessTokenError.
  """
  attempt = 0
  while attempt < 4:
    if attempt:
      logging.info('Retrying...')
    attempt += 1
    logging.info('%s %s', method, url)
    try:
      response = yield _urlfetch(
          url=url,
          payload=payload,
          method=method,
          headers=headers,
          follow_redirects=False,
          deadline=5,  # all RPCs we do should be fast
          validate_certificate=True)
    except (apiproxy_errors.DeadlineExceededError, urlfetch.Error) as e:
      # Transient network error or URL fetch service RPC deadline.
      logging.warning('%s %s failed: %s', method, url, e)
      continue

    # Transient error on the other side.
    if response.status_code >= 500:
      logging.warning(
          '%s %s failed with HTTP %d: %r',
          method, url, response.status_code, response.content)
      continue

    # Non-transient error.
    if 300 <= response.status_code < 500:
      logging.warning(
          '%s %s failed with HTTP %d: %r',
          method, url, response.status_code, response.content)
      raise AccessTokenError(
          'Failed to call %s: HTTP %d' % (url, response.status_code))

    # Success.
    try:
      body = json.loads(response.content)
    except ValueError:
      logging.error('Non-JSON response from %s: %r', url, response.content)
      raise AccessTokenError('Non-JSON response from %s' % url)
    raise ndb.Return(body)

  raise AccessTokenError('Failed to call %s after multiple attempts' % url)


def _urlfetch(**kwargs):
  """To be mocked in tests."""
  return ndb.get_context().urlfetch(**kwargs)


def _memcache_get(*args, **kwargs):
  """To be mocked in tests."""
  return ndb.get_context().memcache_get(*args, **kwargs)


def _memcache_set(*args, **kwargs):
  """To be mocked in tests."""
  return ndb.get_context().memcache_set(*args, **kwargs)


def _rsa_sign(blob, private_key_pem):
  """Byte blob + PEM key => RSA-SHA256 signature byte blob."""
  # Lazy import crypto. It is not available in unit tests outside of sandbox.
  from Crypto.Hash import SHA256
  from Crypto.PublicKey import RSA
  from Crypto.Signature import PKCS1_v1_5
  pkey = RSA.importKey(private_key_pem)
  return PKCS1_v1_5.new(pkey).sign(SHA256.new(blob))
