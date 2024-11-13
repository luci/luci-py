# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Utilities for working with the bot session."""

import base64

from google.protobuf.message import DecodeError

from components import utils
from proto.internals import session_pb2
from server import hmac_secret


class BadSessionToken(Exception):
  """Raised if the session token is malformed."""


def marshal(session):
  """Given a session_pb2.Session, returns base64-encoded session token."""
  assert isinstance(session, session_pb2.Session)
  session_bytes = session.SerializeToString()
  tok = session_pb2.SessionToken(hmac_tagged={
      'session': session_bytes,
      'hmac_sha256': _hmac(session_bytes),
  })
  return base64.b64encode(tok.SerializeToString())


def unmarshal(session_token):
  """Given a base64-encoded session token, returns session_pb2.Session inside.

  Doesn't check expiration time or validity of any Session fields. Just
  checks the HMAC and deserializes the proto.

  Raises:
    BadSessionToken if the token is malformed and can't be deserialized.
  """
  try:
    blob = base64.b64decode(session_token)
  except TypeError:
    raise BadSessionToken('Can\'t base64-decode the session token')

  tok = session_pb2.SessionToken()
  try:
    tok.ParseFromString(blob)
  except DecodeError:
    raise BadSessionToken('Can\'t parse SessionToken proto')
  if not tok.HasField('hmac_tagged'):
    raise BadSessionToken('Unsupported session token format')
  expected = _hmac(tok.hmac_tagged.session)
  if not utils.constant_time_equals(expected, tok.hmac_tagged.hmac_sha256):
    raise BadSessionToken('Bad session token HMAC')

  session = session_pb2.Session()
  try:
    session.ParseFromString(tok.hmac_tagged.session)
  except:
    raise BadSessionToken('Can\'t parse Session proto')
  return session


### Private stuff.


def _hmac(session_bytes):
  mac = hmac_secret.new_mac()
  mac.update('swarming.Session')
  mac.update(session_bytes)
  return mac.digest()
