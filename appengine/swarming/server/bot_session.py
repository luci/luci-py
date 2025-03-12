# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Utilities for working with the bot session."""

import base64
import datetime
import hashlib
import logging
import os

from google.protobuf.message import DecodeError

from components import utils
from proto.config import bots_pb2
from proto.internals import session_pb2
from server import bot_groups_config
from server import hmac_secret

# Expiration time of a session token.
#
# Should be larger than a delay between any two Swarming bot API calls (since
# the token is refreshed when the bot calls the backend).
#
# If a bot ends up using an expired session token (e.g. if it was stuck for
# a long time), it will be asked to restart to get a new session.
SESSION_TOKEN_EXPIRY = datetime.timedelta(hours=1)


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


def debug_log(session):
  """Logs details of the token."""
  logging.debug('Session token:\n%s', session)


def is_expired_session(session):
  """Returns True if this session has expired already."""
  return utils.utcnow() > session.expiry.ToDatetime()


def is_stale_handshake_config(session, bot_group_cfg):
  """Returns True if the bot needs to restart to pick up new handshake config.

  Handshake configs are config values that affect the bot's behavior in some
  global way (like custom bot hooks). The bot picks them up once, when it
  starts.

  Args:
    session: session_pb2.Session.
    bot_group_cfg: BotGroupConfig tuple with the bot config.
  """
  return session.handshake_config_hash != _handshake_config_hash(bot_group_cfg)


def create(bot_id, session_id, bot_group_cfg, rbe_instance):
  """Creates a new session_pb2.Session for an authorized connecting bot.

  Assumes all parameters have been validated already.

  Args:
    bot_id: the bot ID as reported by the bot.
    session_id: the session ID as reported by the bot.
    bot_group_cfg: BotGroupConfig tuple with the bot config.
    rbe_instance: the RBE instance the bot should use for bots in RBE mode.

  Returns:
    session_pb2.Session proto.
  """
  now = utils.utcnow()

  # Few fields are left intentionally unset, since they are set later by the
  # Go side: rbe_bot_session_id, last_seen_config.
  session = session_pb2.Session(
      bot_id=bot_id,
      session_id=session_id,
      debug_info=_debug_info(now),
      # Use default SESSION_TOKEN_EXPIRY expiry here as well. It will be updated
      # to a larger value before we launch a task to make sure the captured
      # config can survive as long as the task (but not much longer). This will
      # be needed to allow the task to complete even if the bot is removed from
      # the config.
      bot_config=_bot_config(bot_group_cfg, rbe_instance, now,
                             SESSION_TOKEN_EXPIRY),
      # This is used to know when to ask the bot to restart to pick up new
      # config values that affect the bot's behavior in some global way. Most
      # frequently this will be a change to the bot hooks script.
      handshake_config_hash=_handshake_config_hash(bot_group_cfg),
  )
  session.expiry.FromDatetime(now + SESSION_TOKEN_EXPIRY)
  return session


def update(session,
           bot_group_cfg=None,
           rbe_instance=None,
           rbe_effective_bot_id=None,
           rbe_effective_bot_id_dimension=None):
  """Bumps the token expiration time, optionally also updating the config in it.

  Args:
    session: session_pb2.Session proto to update in-place.
    bot_group_cfg: BotGroupConfig tuple with the bot config.
    rbe_instance: the RBE instance the bot should use for bots in RBE mode.

  Returns:
    The same session_pb2.Session proto.
  """
  now = utils.utcnow()
  if bot_group_cfg:
    # TODO: Use a larger config expiry when picking up a task. This will be
    # implemented in Go.
    session.bot_config.CopyFrom(
        _bot_config(bot_group_cfg, rbe_instance, now, SESSION_TOKEN_EXPIRY,
                    rbe_effective_bot_id, rbe_effective_bot_id_dimension))
  session.debug_info.CopyFrom(_debug_info(now))
  session.expiry.FromDatetime(now + SESSION_TOKEN_EXPIRY)
  return session


### Private stuff.


def _hmac(session_bytes):
  """Calculates HMAC using the server key."""
  mac = hmac_secret.new_mac()
  mac.update('swarming.Session')
  mac.update(session_bytes)
  return mac.digest()


def _debug_info(now):
  """Returns session_pb2.DebugInfo populated based on the server environment."""
  debug_info = session_pb2.DebugInfo()
  debug_info.created.FromDatetime(now)
  debug_info.swarming_version = 'py/' + utils.get_app_version()
  debug_info.request_id = os.environ.get('REQUEST_LOG_ID')
  return debug_info


def _bot_config(bot_group_cfg,
                rbe_instance,
                now,
                expiry,
                rbe_effective_bot_id=None,
                rbe_effective_bot_id_dimension=None):
  """Constructs session_pb2.BotConfig from bot_groups_config.BotGroupConfig."""
  assert isinstance(bot_group_cfg, bot_groups_config.BotGroupConfig)
  cfg = session_pb2.BotConfig(
      debug_info=_debug_info(now),
      bot_auth=[_bot_auth(x) for x in bot_group_cfg.auth],
      system_service_account=bot_group_cfg.system_service_account,
      logs_cloud_project=bot_group_cfg.logs_cloud_project,
      rbe_instance=rbe_instance,
      rbe_effective_bot_id=rbe_effective_bot_id,
      rbe_effective_bot_id_dimension=rbe_effective_bot_id_dimension,
  )
  cfg.expiry.FromDatetime(now + expiry)
  return cfg


def _bot_auth(cfg):
  """Converts bot_groups_config.BotAuth back to its proto form."""
  assert isinstance(cfg, bot_groups_config.BotAuth)
  bot_auth = bots_pb2.BotAuth(
      log_if_failed=cfg.log_if_failed,
      require_luci_machine_token=cfg.require_luci_machine_token,
      require_service_account=cfg.require_service_account,
      ip_whitelist=cfg.ip_whitelist,
  )
  if cfg.require_gce_vm_token:
    bot_auth.require_gce_vm_token.project = cfg.require_gce_vm_token.project
  return bot_auth


def _handshake_config_hash(bot_group_cfg):
  """Returns a hash of bot config parameters that affect the bot session.

  The hash of these parameters is capture in /handshake handler and put into
  the session token. If a /poll handler notices the current hash doesn't match
  the hash in the session, it will ask the bot to restart to pick up new
  parameters.

  This function is tightly coupled to what /handshake returns to the bot and
  to how bot uses these values.
  """
  data = _handshake_config_extract(bot_group_cfg)
  return hashlib.sha256('\n'.join(data)).digest()


def _handshake_config_extract(bot_group_cfg):
  """Returns a list of strings hashed to get a handshake config.

  The serialization format for hashing should be simple enough to be
  implemented **exactly** the same way from the Go side. The simplest one is
  just a sorted list of prefixed strings.
  """
  data = []

  # Need to restart the bot whenever the injected hooks script changes.
  if bot_group_cfg.bot_config_script_sha256:
    data.append('config_script_sha256:%s' %
                bot_group_cfg.bot_config_script_sha256)

  # Hooks script name (e.g. `android.py`) is exposed as a `bot_config` dimension
  # that hooks (in particular the default bot_config.py) can theoretically react
  # to. Need to restart the bot if the hooks script name changes. This is rare.
  if bot_group_cfg.bot_config_script:
    data.append('config_script_name:%s' % bot_group_cfg.bot_config_script)

  # Need to restart the bot whenever its server-assigned dimensions change,
  # since bot hooks can examine them (in particular in startup hooks).
  for key, vals in bot_group_cfg.dimensions.items():
    assert isinstance(vals, (tuple, list))
    for val in vals:
      data.append('dimension:%s:%s' % (key, val))

  data.sort()
  return data
