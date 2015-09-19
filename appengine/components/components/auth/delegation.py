# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Delegation token implementation.

See delegation.proto for general idea behind it.
"""

import functools
import threading

from google.protobuf import message

from components import utils

from . import api
from . import model
from . import signature
from . import tokens
from .proto import delegation_pb2


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
