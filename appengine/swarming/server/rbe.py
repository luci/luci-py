# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functionality related to RBE Scheduler."""

import base64
import datetime
import hashlib
import hmac
import logging
import os
import uuid

from google.appengine.api import app_identity
from google.protobuf import timestamp_pb2

from components import auth
from components import gsm
from components import utils

from proto.internals import rbe_pb2
from server import pools_config


# How long a poll token should be considered valid. Should be larger than the
# interval between /bot/poll calls done by the Swarming RBE bot.
POLL_TOKEN_EXPIRY = datetime.timedelta(hours=1)


def warmup():
  """Warms up local in-memory caches, best effort."""
  try:
    _get_poll_token_hmac_secret().access()
  except Exception:
    logging.exception('Failed to warmup up RBE poll token HMAC key')


def get_rbe_instance(bot_id, pools, bot_group_cfg):
  """Returns an RBE instance to use for the given bot.

  If the bot should not be using RBE, returns None.

  Args:
    bot_id: ID of the bot as a string.
    pools: pool IDs the bot belongs to.
    bot_group_cfg: a BotGroupConfig tuple with bot's bot group config.

  Returns:
    A string with RBE instance name to use by the bot or None.
  """
  rbe_migration = bot_group_cfg.rbe_migration
  if not rbe_migration:
    return None

  if bot_id in rbe_migration.enable_rbe_on:
    use_rbe = True
  elif bot_id in rbe_migration.disable_rbe_on:
    use_rbe = False
  else:
    use_rbe = _quasi_random_100(bot_id) <= float(rbe_migration.rbe_mode_percent)

  if not use_rbe:
    logging.info('RBE: bot %s is not using RBE', bot_id)
    return None

  # Check all pools the bot belongs to (most commonly only one) have RBE
  # enabled and they all use the same RBE instance.
  assert isinstance(pools, list), pools
  rbe_instances = set()
  for pool in pools:
    cfg = pools_config.get_pool_config(pool)
    if cfg and cfg.rbe_migration and cfg.rbe_migration.rbe_instance:
      rbe_instances.add(cfg.rbe_migration.rbe_instance)
    else:
      rbe_instances.add(None)

  rbe_instances = sorted(rbe_instances)
  if len(rbe_instances) != 1:
    logging.warning(
        'RBE: disabling RBE for bot %s: bot pools disagree on RBE instance: %r',
        bot_id, rbe_instances)
    return None

  rbe_instance = rbe_instances[0]
  if not rbe_instance:
    logging.warning(
        'RBE: disabling RBE for bot %s: pools are not configure to use RBE',
        bot_id)
    return None

  logging.info('RBE: bot %s is using RBE instance %s', bot_id, rbe_instance)
  return rbe_instance


def generate_poll_token(bot_id, rbe_instance, enforced_dimensions,
                        bot_auth_cfg):
  """Generates a serialized HMAC-tagged rbe_pb2.PollState.

  It is sent to the bot, which then uses it to communicate with the Go portion
  of the Swarming service. It encodes various state the Python portion of the
  service wants to reliably send to the Go portion. The bot can't tamper with
  it, since it would break the HMAC tag.

  Must be called within a context of a bot request handler.

  Args:
    bot_id: the ID reported by the bot and authenticated by the service.
    rbe_instance: a full RBE instance name to use for the bot.
    enforced_dimensions: a dict with server-enforced dimensions (and only them).
    bot_auth_cfg: a BotAuth tuple with auth method used to authenticate the bot.

  Returns:
    A base64-encoded token.
  """
  # This normally will be only `pool` (and `id`), since we normally enforce only
  # `pool` dimension in configs.
  enforced_dimensions = (enforced_dimensions or {}).copy()
  enforced_dimensions['id'] = [bot_id]

  state = rbe_pb2.PollState(
      id=str(uuid.uuid4()),
      rbe_instance=rbe_instance,
      enforced_dimensions=[
          rbe_pb2.PollState.Dimension(key=key, values=values)
          for key, values in sorted(enforced_dimensions.items())
      ],
      debug_info=rbe_pb2.PollState.DebugInfo(
          swarming_version=utils.get_app_version(),
          request_id=os.environ.get('REQUEST_LOG_ID'),
      ),
      ip_allowlist=bot_auth_cfg.ip_whitelist,
  )

  # Populating Timestamp-valued fields. This doesn't work in the constructor.
  now = utils.utcnow()
  state.debug_info.created.FromDatetime(now)
  state.expiry.FromDatetime(now + POLL_TOKEN_EXPIRY)

  # Populate auth related fields based on how the bot was authenticated and
  # what credentials it is presenting. See _check_bot_auth in bot_auth.py for
  # a similar switch statement. Asserts below double check invariants set in
  # _check_bot_auth.
  if bot_auth_cfg.require_luci_machine_token:
    peer_ident = auth.get_peer_identity()
    assert peer_ident.kind == auth.IDENTITY_BOT, peer_ident
    state.luci_machine_token_auth.machine_fqdn = peer_ident.name
  elif bot_auth_cfg.require_service_account:
    peer_ident = auth.get_peer_identity()
    assert peer_ident.kind == auth.IDENTITY_USER, peer_ident
    assert peer_ident.name in bot_auth_cfg.require_service_account, peer_ident
    state.service_account_auth.service_account = peer_ident.name
  elif bot_auth_cfg.require_gce_vm_token:
    vm = auth.get_auth_details()
    assert vm.gce_project == bot_auth_cfg.require_gce_vm_token.project, vm
    state.gce_auth.gce_project = vm.gce_project
    state.gce_auth.gce_instance = vm.gce_instance
  elif bot_auth_cfg.ip_whitelist:
    # Note: already set state.ip_allowlist above.
    state.ip_allowlist_auth.SetInParent()
  else:
    raise AssertionError('Impossible BotAuth method %s' % (bot_auth_cfg, ))

  # Temporary log during the rollout.
  logging.info('PollState token:\n%s', state)
  state_blob = state.SerializeToString()

  # Calculate the HMAC of serialized PollState. See rbe_pb2.TaggedMessage.
  key = _get_poll_token_hmac_secret().access()
  mac = hmac.new(key, digestmod=hashlib.sha256)
  mac.update("%d\n" % rbe_pb2.TaggedMessage.POLL_STATE)
  mac.update(state_blob)
  hmac_digest = mac.digest()

  # Produce the final token with the state and the HMAC.
  envelope = rbe_pb2.TaggedMessage(
      payload_type=rbe_pb2.TaggedMessage.POLL_STATE,
      payload=state_blob,
      hmac_sha256=hmac_digest)
  return base64.b64encode(envelope.SerializeToString())


### Private stuff.


def _quasi_random_100(s):
  """Given a string, returns a quasi-random float in range [0; 100]."""
  digest = hashlib.sha256(s).digest()
  num = float(ord(digest[0]) + ord(digest[1]) * 256)
  return num * 100.0 / (256.0 + 256.0 * 256.0)


@utils.cache
def _get_poll_token_hmac_secret():
  """A gsm.Secret with a key used to HMAC-tag poll tokens."""
  return gsm.Secret(
      project=app_identity.get_application_id(),
      secret='poll-token-hmac',
      version='current',
  )
