# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generation of OAuth2 token for a service account.

A fancier version of app_identity.get_access_token(...) that can be used if
app_identity is not appropriate for some reasons. For example, there's no way
to invite @appspot.gserviceaccount.com account to some Cloud API projects,
while it is possible to create @developer.gserviceaccount.com belonging to such
project.

Supports three ways to generate OAuth2 tokens:
  * app_identity.get_access_token(...) to use native GAE service account.
  * OAuth flow with JWT token, for @developer.gserviceaccount.com service
    accounts (the one with a private key).
"""

import base64
import collections
import json
import logging
import random
import urllib

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch

from components import utils


# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'get_access_token',
  'AccessTokenError',
  'ServiceAccountKey',
]

# Information about @developer.gserviceaccount.com. Field values can be
# extracted from corresponding fields in JSON file produced by
# "Generate new JSON key" button in "Credentials" section of any Cloud Console
# project.
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


def get_access_token(scopes, service_account_key=None):
  """Returns an OAuth2 access token for a service account.

  If 'service_account_key' is specified, will use it to generate access token
  for corresponding @developer.gserviceaccount.com account. Otherwise will
  invoke app_identity.get_access_token(...) to use app's
  @appspot.gserviceaccount.com account.

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
  if service_account_key:
    # Empty private_key_id probably means that the app is not configured yet.
    if not service_account_key.private_key_id:
      raise AccessTokenError('Service account secret key is not initialized')
    return _get_jwt_based_token(scopes, service_account_key)
  return app_identity.get_access_token(scopes)


## Private stuff.


def _get_jwt_based_token(scopes, service_account_key):
  """Returns token for @developer.gserviceaccount.com service account."""
  # Derive memcache key from scopes and private_key_id.
  if isinstance(scopes, basestring):
    scopes = [scopes]
  assert all('@' not in scope for scope in scopes), scopes
  assert '@' not in service_account_key.private_key_id, service_account_key
  cache_key = 'access_token@%s@%s' % (
      ' '.join(scopes), service_account_key.private_key_id)

  # Randomize refresh time to avoid thundering herd effect when token expires.
  token_info = memcache.get(cache_key)
  should_refresh = (
      token_info is None or
      token_info['exp_ts'] - utils.time_time() < random.randint(300, 600))
  if should_refresh:
    token_info = _mint_jwt_based_token(scopes, service_account_key)
    memcache.set(cache_key, token_info, token_info['exp_ts'])
  return token_info['access_token'], token_info['exp_ts']


def _mint_jwt_based_token(scopes, service_account_key):
  """Creates new access token given service account private key."""
  # For more info see:
  # * https://developers.google.com/accounts/docs/OAuth2ServiceAccount.

  # JWT header.
  header_b64 = _b64_encode(utils.encode_to_json({
    'alg': 'RS256',
    'kid': service_account_key.private_key_id,
    'typ': 'JWT',
  }))

  # JWT claimset.
  now = int(utils.time_time())
  claimset_b64 = _b64_encode(utils.encode_to_json({
    'aud': 'https://www.googleapis.com/oauth2/v3/token',
    'exp': now + 3600,
    'iat': now,
    'iss': service_account_key.client_email,
    'scope': ' '.join(scopes),
  }))

  # Sign <header>.<claimset> with account's private key.
  signature_b64 = _b64_encode(_rsa_sign(
      '%s.%s' % (header_b64, claimset_b64), service_account_key.private_key))

  # URL encoded body of a token request.
  request_body = urllib.urlencode({
    'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    'assertion': '%s.%s.%s' % (header_b64, claimset_b64, signature_b64),
  })

  # Grab the token (with retries).
  for _ in xrange(0, 5):
    response = urlfetch.fetch(
        url='https://www.googleapis.com/oauth2/v3/token',
        payload=request_body,
        method='POST',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        follow_redirects=False,
        deadline=10,
        validate_certificate=True)
    if response.status_code == 200:
      token = json.loads(response.content)
      return {
        'access_token': str(token['access_token']),
        'exp_ts': utils.time_time() + token['expires_in'],
      }
    logging.error(
        'Failed to fetch access token (HTTP %d)\n%s',
        response.status_code, response.content)

  # All retried has failed, give up.
  raise AccessTokenError('Failed to fetch access token from /oauth2/v3/token')


def _b64_encode(data):
  return base64.urlsafe_b64encode(data).rstrip('=')


def _rsa_sign(blob, private_key_pem):
  """Byte blob + PEM key => RSA-SHA256 signature byte blob."""
  # Lazy import crypto. It is not available in unit tests outside of sandbox.
  from Crypto.Hash import SHA256
  from Crypto.PublicKey import RSA
  from Crypto.Signature import PKCS1_v1_5
  pkey = RSA.importKey(private_key_pem)
  return PKCS1_v1_5.new(pkey).sign(SHA256.new(blob))
