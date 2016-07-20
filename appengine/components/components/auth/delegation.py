# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Delegation token implementation.

See delegation.proto for general idea behind it.
"""

import collections
import datetime
import functools
import hashlib
import json
import logging
import threading
import urllib

from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors
from google.protobuf import message

from components import utils

from . import api
from . import model
from . import service_account
from . import signature
from . import tokens
from .proto import delegation_pb2


__all__ = [
  'delegate',
  'delegate_async',
  'DelegationToken',
  'DelegationTokenCreationError',
]


# TODO(vadimsh): Add a simple encryption layer, so that token's guts are not
# visible in plain form to anyone who sees the token.


# Tokens that are larger than this (before base64 encoding) are rejected.
MAX_TOKEN_SIZE = 8 * 1024

# Maximum allowed length of token chain.
MAX_SUBTOKEN_LIST_LEN = 8

# How much clock drift between machines we can tolerate, in seconds.
ALLOWED_CLOCK_DRIFT_SEC = 30

# Name of the HTTP header to look for delegation token.
HTTP_HEADER = 'X-Delegation-Token-V1'


class BadTokenError(Exception):
  """Raised on fatal errors (like bad signature). Results in 403 HTTP code."""


class TransientError(Exception):
  """Raised on errors that can go away with retry. Results in 500 HTTP code."""


class DelegationTokenCreationError(Exception):
  """Raised on delegation token creation errors."""


class DelegationAuthorizationError(DelegationTokenCreationError):
  """Raised on authorization error during delegation token creation."""


# A minted delegation token returned by delegate_async and delegate.
DelegationToken = collections.namedtuple('DelegationToken', [
  'token',  # urlsafe base64 encoded blob with delegation token.
  'expiry',  # datetime.datetime of expiration.
])


class SignatureChecker(object):
  """Defines a set of trusted token signers, used by unseal_token.

  Lives in local instance memory cache (with small expiration time to account
  for key rotation and possible AuthDB changes). Once constructed (i.e. after
  last register_trusted_signer call) may be shared by multiple threads.

  Lazily loads requested certs and holds them in memory.
  """
  class _TrustedSigner(object):
    """Synchronizes fetch of some signer's certs, caches them in memory."""
    def __init__(self, callback):
      self.lock = threading.Lock()
      self.callback = callback
      self.certs = None

  def __init__(self):
    self._signers = {} # {signer_id => _TrustedSigner}

  def register_trusted_signer(self, signer_id, fetch_callback):
    """Adds a new trusted signer to this config object.

    Args:
      signer_id: string with identity name of the signer.
      fetch_callback: function to grab bundle with certificates, has same return
          value format as signature.get_service_public_certificates().
    """
    assert signer_id not in self._signers
    self._signers[signer_id] = self._TrustedSigner(fetch_callback)

  def is_trusted_signer(self, signer_id):
    """True if given identity's signature is meaningful for us.

    Args:
      signer_id: string with identity name of a signer to check.
    """
    return signer_id in self._signers

  def get_x509_certificate_pem(self, signer_id, signing_key_id):
    """Returns PEM encoded x509 certificate of a given trusted signer.

    Args:
      signer_id: string with identity name of a signer.
      signing_key_id: name of a signing key.

    Returns:
      PEM encoded x509 certificate.

    Raises:
      signature.CertificateError if no such cert or if it can't be fetched.
    """
    # Use a lock so that all pending requests wait for a single fetch, instead
    # of initiating a bunch of redundant fetches.
    s = self._signers[signer_id]
    with s.lock:
      if s.certs is None:
        s.certs = s.callback()
    assert s.certs is not None
    return signature.get_x509_certificate_by_name(s.certs, signing_key_id)


@utils.cache_with_expiration(expiration_sec=300)
def get_signature_checker():
  """Returns configured SignatureChecker that knows whom to trust."""
  checker = SignatureChecker()
  # A service always trusts itself.
  checker.register_trusted_signer(
      signer_id=model.get_service_self_identity().to_bytes(),
      fetch_callback=signature.get_own_public_certificates)
  # Services linked to some primary auth_service trust that primary.
  if model.is_replica():
    state = model.get_replication_state()
    fetch_callback = functools.partial(
        signature.get_service_public_certificates,
        state.primary_url)
    checker.register_trusted_signer(
        signer_id=model.Identity('service', state.primary_id).to_bytes(),
        fetch_callback=fetch_callback)
  return checker


## Low level API for components.auth and services that know what they are doing.


def serialize_token(tok):
  """Converts delegation_pb2.DelegationToken to urlsafe base64 text.

  Raises:
    BadTokenError if token is too large.
  """
  assert isinstance(tok, delegation_pb2.DelegationToken)
  as_bytes = tok.SerializeToString()
  if len(as_bytes) > MAX_TOKEN_SIZE:
    raise BadTokenError('Unexpectedly huge token (%d bytes)' % len(as_bytes))
  return tokens.base64_encode(as_bytes)


def deserialize_token(blob):
  """Coverts urlsafe base64 text to delegation_pb2.DelegationToken.

  Raises:
    BadTokenError if blob doesn't look like a valid DelegationToken.
  """
  if isinstance(blob, unicode):
    blob = blob.encode('ascii', 'ignore')
  try:
    as_bytes = tokens.base64_decode(blob)
  except ValueError as exc:
    raise BadTokenError('Not base64: %s' % exc)
  if len(as_bytes) > MAX_TOKEN_SIZE:
    raise BadTokenError('Unexpectedly huge token (%d bytes)' % len(as_bytes))
  try:
    return delegation_pb2.DelegationToken.FromString(as_bytes)
  except message.DecodeError as exc:
    raise BadTokenError('Bad proto: %s' % exc)


def seal_token(subtokens):
  """Serializes SubtokenList and signs it using current service's key.

  The list must have at least one entry (it is asserted).

  Args:
    subtokens: delegation_pb2.SubtokenList message.

  Raises:
    BadTokenError if token list is too long or empty.

  Returns:
    delegation_pb2.DelegationToken message ready for serialization.
  """
  assert isinstance(subtokens, delegation_pb2.SubtokenList)
  toks = subtokens.subtokens
  if not toks:
    raise BadTokenError('Subtoken list is empty')
  if len(toks) > MAX_SUBTOKEN_LIST_LEN:
    raise BadTokenError(
        'Subtoken list is too long (%d tokens, max is %d)' %
        (len(toks), MAX_SUBTOKEN_LIST_LEN))
  serialized = subtokens.SerializeToString()
  signing_key_id, pkcs1_sha256_sig = signature.sign_blob(serialized, 0.5)
  return delegation_pb2.DelegationToken(
      serialized_subtoken_list=serialized,
      signer_id=model.get_service_self_identity().to_bytes(),
      signing_key_id=signing_key_id,
      pkcs1_sha256_sig=pkcs1_sha256_sig)


def unseal_token(tok):
  """Checks the signature of DelegationToken and deserializes subtoken list.

  Does not check subtokens themselves.

  Args:
    tok: delegation_pb2.DelegationToken message.

  Returns:
    delegation_pb2.SubtokenList message (with at least one entry).

  Raises:
    BadTokenError:
        On non-transient fatal errors: if token is not structurally valid,
        signature is bad or signer is untrusted.
    TransientError:
        On transient errors, that may go away on the next call: for example if
        the signer public key can't be fetched.
  """
  # Check all required fields are set.
  assert isinstance(tok, delegation_pb2.DelegationToken)
  if not tok.serialized_subtoken_list:
    raise BadTokenError('serialized_subtoken_list is missing')
  if not tok.signer_id:
    raise BadTokenError('signer_id is missing')
  if not tok.signing_key_id:
    raise BadTokenError('signing_key_id is missing')
  if not tok.pkcs1_sha256_sig:
    raise BadTokenError('pkcs1_sha256_sig is missing')

  # Make sure signer_id looks like model.Identity.
  try:
    model.Identity.from_bytes(tok.signer_id)
  except ValueError as exc:
    raise BadTokenError('signer_id is not a valid identity: %s' % exc)

  # Validate the signature.
  checker = get_signature_checker()
  if not checker.is_trusted_signer(tok.signer_id):
    raise BadTokenError('Unknown signer: "%s"' % tok.signer_id)
  try:
    cert = checker.get_x509_certificate_pem(tok.signer_id, tok.signing_key_id)
    is_valid_sig = signature.check_signature(
        blob=tok.serialized_subtoken_list,
        x509_certificate_pem=cert,
        signature=tok.pkcs1_sha256_sig)
  except signature.CertificateError as exc:
    if exc.transient:
      raise TransientError(str(exc))
    raise BadTokenError(
        'Bad certificate (signer_id == %s, signing_key_id == %s): %s' % (
        tok.signer_id, tok.signing_key_id, exc))
  if not is_valid_sig:
    raise BadTokenError(
        'Invalid signature (signer_id == %s, signing_key_id == %s)' % (
        tok.signer_id, tok.signing_key_id))

  # The signature is correct, deserialize subtoken list.
  try:
    toks = delegation_pb2.SubtokenList.FromString(tok.serialized_subtoken_list)
  except message.DecodeError as exc:
    raise BadTokenError('Bad serialized_subtoken_list: %s' % exc)
  if not toks.subtokens:
    raise BadTokenError('Bad serialized_subtoken_list: empty')
  if len(toks.subtokens) > MAX_SUBTOKEN_LIST_LEN:
    raise BadTokenError('Bad serialized_subtoken_list: too many subtokens')
  return toks


## Token creation.


def _urlfetch_async(**kwargs):
  """To be mocked in tests."""
  return ndb.get_context().urlfetch(**kwargs)


@ndb.tasklet
def _authenticated_request_async(url, method='GET', payload=None, params=None):
  """Sends an authenticated JSON API request, returns deserialized response.

  Raises:
    DelegationTokenCreationError if request failed or response is malformed.
    DelegationAuthorizationError on HTTP 401 or 403 response from auth service.
  """
  scope = 'https://www.googleapis.com/auth/userinfo.email'
  access_token = service_account.get_access_token(scope)[0]
  headers = {'Authorization': 'Bearer %s' % access_token}

  if payload is not None:
    assert method in ('CREATE', 'POST', 'PUT'), method
    headers['Content-Type'] = 'application/json; charset=utf-8'
    payload = utils.encode_to_json(payload)

  if utils.is_local_dev_server():
    protocols = ('http://', 'https://')
  else:
    protocols = ('https://',)
  assert url.startswith(protocols) and '?' not in url, url
  if params:
    url += '?' + urllib.urlencode(params)

  try:
    res = yield _urlfetch_async(
        url=url,
        payload=payload,
        method=method,
        headers=headers,
        follow_redirects=False,
        deadline=10,
        validate_certificate=True)
  except (apiproxy_errors.DeadlineExceededError, urlfetch.Error) as e:
    raise DelegationTokenCreationError(str(e))

  if res.status_code in (401, 403):
    raise DelegationAuthorizationError(
      'HTTP response status code: %s' % res.status_code)

  if res.status_code >= 300:
    raise DelegationTokenCreationError(
      'HTTP response status code: %s' % res.status_code)

  try:
    json_res = json.loads(res.content)
  except ValueError as e:
    raise DelegationTokenCreationError('Bad JSON response: %s' % e)
  raise ndb.Return(json_res)


@ndb.tasklet
def delegate_async(
    audience=None,
    services=None,
    min_validity_duration_sec=5*60,
    max_validity_duration_sec=60*60*10,
    impersonate=None,
    auth_service_url=None):
  """Creates a delegation token.

  An RPC. Memcaches the token.

  Does not support delegation of a delegation token.

  Args:
    audience (list of (str or Identity)): to WHOM caller's identity is
      delegated; a list of identities or groups.
      If None (default), the token can be used by anyone.
      Example: ['user:def@example.com', 'group:abcdef']
    services (list of (str or Identity)): WHERE token is accepted.
      If None (default), the token is accepted everywhere.
      Each list element must be an identity of 'service' kind.
      Example: ['service:gae-app1', 'service:gae-app2']
    min_validity_duration_sec (int): minimally acceptable lifetime of the token.
      If there's existing token cached locally that have TTL
      min_validity_duration_sec or more, it will be returned right away.
      Default is 5 min.
    max_validity_duration_sec (int): defines lifetime of a new token.
      It will bet set as tokens' TTL if there's no existing cached tokens with
      sufficiently long lifetime. Default is 10 hours.
    impersonate (str or Identity): a caller can mint a delegation token on some
      else's behalf (effectively impersonating them).
      Only a privileged set of callers can do that.
      If impersonation is allowed, token's issuer_id field will contain whatever
      is in 'impersonate' field.
      Example: 'user:abc@example.com'
    auth_service_url (str): the URL for the authentication service that will
      mint the token. Defaults to the URL of the primary if this is a replica.

  Returns:
    DelegationToken as ndb.Future.

  Raises:
    ValueError if args are invalid.
    DelegationTokenCreationError if could not create a token.
    DelegationAuthorizationError on HTTP 403 response from auth service.
  """
  # Validate audience.
  audience = audience or []
  for a in audience:
    assert isinstance(a, (basestring, model.Identity)), a
    if isinstance(a, basestring) and not a.startswith('group:'):
      model.Identity.from_bytes(a)  # May raise ValueError.
  id_to_str = lambda i: i.to_bytes() if isinstance(i, model.Identity) else i
  audience = map(id_to_str, audience)

  # Validate services.
  services = services or []
  for s in services:
    if isinstance(s, basestring):
      s = model.Identity.from_bytes(s)
    assert isinstance(s, model.Identity), s
    assert s.kind == model.IDENTITY_SERVICE, s
  services = map(id_to_str, services)

  # Validate validity durations.
  assert isinstance(min_validity_duration_sec, int), min_validity_duration_sec
  assert isinstance(max_validity_duration_sec, int), max_validity_duration_sec
  assert min_validity_duration_sec >= 5
  assert max_validity_duration_sec >= 5
  assert min_validity_duration_sec <= max_validity_duration_sec

  # Validate impersonate.
  if impersonate is not None:
    assert isinstance(impersonate, (basestring, model.Identity)), impersonate
    impersonate = id_to_str(impersonate)

  # Validate auth_service_url.
  if auth_service_url is None:
    auth_service_url = api.get_request_auth_db().primary_url
    if not auth_service_url:
      raise ValueError(
          'auth_service_url is unspecified and this is not a replica')

  # End of validation.

  req = {
    'audience': audience,
    'services': services,
    'validity_duration': max_validity_duration_sec,
    'impersonate': impersonate,
  }

  # Get from cache.
  cache_key_hash = hashlib.sha1(json.dumps(req, sort_keys=True)).hexdigest()
  cache_key = 'delegation_token/v1/%s' % cache_key_hash
  ctx = ndb.get_context()
  token = yield ctx.memcache_get(cache_key)
  min_validity_duration = datetime.timedelta(seconds=min_validity_duration_sec)
  now = utils.utcnow()
  if token and token.expiry - min_validity_duration > now:
    raise ndb.Return(token)

  # Request a new one.
  logging.info(
      'Minting a delegation token for %r',
      {k: v for k, v in req.iteritems() if v},
  )

  res = yield _authenticated_request_async(
      '%s/auth_service/api/v1/delegation/token/create' % auth_service_url,
      method='POST',
      payload=req)
  actual_validity_duration_sec = res.get('validity_duration')
  if not isinstance(actual_validity_duration_sec, int):
    raise DelegationTokenCreationError(
        'Unexpected response, validity_duration is absent '
        'or not a number: %s' % res)

  token = DelegationToken(
      token=res.get('delegation_token'),
      expiry=now + datetime.timedelta(seconds=actual_validity_duration_sec),
  )
  if not token.token:
    raise DelegationTokenCreationError(
      'Unexpected response, no delegation_token: %s' % res)

  # Put to cache. Refresh the token 10 sec in advance.
  if actual_validity_duration_sec > 10:
    yield ctx.memcache_add(
        cache_key, token, time=actual_validity_duration_sec - 10)

  raise ndb.Return(token)


def delegate(**kwargs):
  """Blocking version of delegate_async."""
  return delegate_async(**kwargs).get_result()


## Token validation.


def check_subtoken_list(subtokens, peer_identity):
  """Validates the chain of delegation subtokens, extracts original issuer_id.

  Args:
    subtokens: instance of delegation_pb2.SubtokenList with at least one token.
    peer_identity: identity of whoever tries to use this token chain.

  Returns:
    Delegated Identity extracted from the token chain (if it is valid).

  Raises:
    BadTokenError if token chain is invalid or not usable by peer_identity.
  """
  assert isinstance(subtokens, delegation_pb2.SubtokenList)
  toks = subtokens.subtokens
  if not toks:
    raise BadTokenError('Subtoken list is empty')
  if len(toks) > MAX_SUBTOKEN_LIST_LEN:
    raise BadTokenError(
        'Subtoken list is too long (%d tokens, max is %d)' %
        (len(toks), MAX_SUBTOKEN_LIST_LEN))

  # Do fast failing checks before heavy ones.
  now = int(utils.time_time())
  service_id = model.get_service_self_identity().to_bytes()
  for tok in toks:
    check_subtoken_expiration(tok, now)
    check_subtoken_services(tok, service_id)

  # Figure out delegated identity by following delegation chain.
  current_identity = peer_identity
  for tok in reversed(toks):
    check_subtoken_audience(tok, current_identity)
    try:
      current_identity = model.Identity.from_bytes(tok.issuer_id)
    except ValueError as exc:
      raise BadTokenError('Invalid issuer_id: %s' % exc)
  return current_identity


def check_subtoken_expiration(subtoken, now):
  """Checks 'creation_time' and 'validity_duration' fields.

  Args:
    subtoken: instance of delegation_pb2.Subtoken.
    now: current time (number of seconds since epoch).

  Raises:
    BadTokenError if token has expired or not valid yet.
  """
  if not subtoken.creation_time:
    raise BadTokenError('Missing "creation_time" field')
  if subtoken.validity_duration <= 0:
    raise BadTokenError(
        'Invalid validity_duration: %d' % subtoken.validity_duration)
  if subtoken.creation_time >= now + ALLOWED_CLOCK_DRIFT_SEC:
    raise BadTokenError(
        'Token is not active yet (%d < %d)' %
        (subtoken.creation_time, now + ALLOWED_CLOCK_DRIFT_SEC))
  if subtoken.creation_time + subtoken.validity_duration < now:
    exp = now - (subtoken.creation_time + subtoken.validity_duration)
    raise BadTokenError('Token has expired %d sec ago' % exp)


def check_subtoken_services(subtoken, service_id):
  """Checks 'services' field of the subtoken.

  Args:
    subtoken: instance of delegation_pb2.Subtoken.
    service_id: 'service:<id>' string to look for in 'services' field.

  Raises:
    BadTokenError if token is not intended for the current service.
  """
  # Empty services field -> allow all.
  if subtoken.services and service_id not in subtoken.services:
    raise BadTokenError('The token is not intended for %s' % service_id)


def check_subtoken_audience(subtoken, current_identity):
  """Checks 'audience' field of the subtoken.

  Args:
    subtoken: instance of delegation_pb2.Subtoken.
    current_identity: Identity to look for in 'audience' field.

  Raises:
    BadTokenError if token is not allowed to be used by current_identity.
  """
  # Empty audience field -> allow all.
  if not subtoken.audience:
    return
  # Try to find a direct hit first, to avoid calling expensive is_group_member.
  ident_as_bytes = current_identity.to_bytes()
  if ident_as_bytes in subtoken.audience:
    return
  # Search through groups now.
  for aud in subtoken.audience:
    if not aud.startswith('group:'):
      continue
    group = aud[len('group:'):]
    if api.is_group_member(group, current_identity):
      return
  raise BadTokenError('%s is not allowed to use the token' % ident_as_bytes)


## High level API to parse, validate and traverse delegation token.


def check_delegation_token(token, peer_identity):
  """Decodes the token, checks its validity, extracts delegated Identity.

  Args:
    token: blob with base64 encoded delegation token.
    peer_identity: Identity of whoever tries to wield the token.

  Returns:
    Delegated Identity.

  Raises:
    BadTokenError if token is invalid.
    TransientError if token can't be verified due to transient errors.
  """
  subtokens = unseal_token(deserialize_token(token))
  return check_subtoken_list(subtokens, peer_identity)
