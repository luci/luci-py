# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""A registry of known bots and server-side assigned (trusted) dimensions.

It is fetched from the config service. Functions here are used by bot API
handlers in handlers_bot.py.
"""

import logging

from components import auth
from components.auth import ipaddr

from server import bot_groups_config


def is_authenticated_bot(bot_id):
  """Returns True if bot with given ID is using correct credentials.

  Expected to be called in a context of a handler of a request coming from the
  bot with given ID.
  """
  try:
    validate_bot_id_and_fetch_config(bot_id)
    return True
  except auth.AuthorizationError:
    return False


def validate_bot_id_and_fetch_config(bot_id):
  """Verifies ID reported by a bot matches the credentials being used.

  Expected to be called in a context of some bot API request handler. Uses
  bots.cfg config to look up what credentials are expected to be used by the bot
  with given ID.

  Raises auth.AuthorizationError if bot_id is unknown or bot is using invalid
  credentials.

  On success returns the configuration for this bot (BotGroupConfig tuple), as
  defined in bots.cfg
  """
  cfg = bot_groups_config.get_bot_group_config(bot_id)
  if not cfg:
    logging.error(
        'bot_auth: unknown bot_id, not in the config\nbot_id: "%s"', bot_id)
    raise auth.AuthorizationError('Unknown bot ID, not in config')

  peer_ident = auth.get_peer_identity()
  if cfg.require_luci_machine_token:
    if not _is_valid_ident_for_bot(peer_ident, bot_id):
      logging.error(
          'bot_auth: bot ID doesn\'t match the machine token used\n'
          'bot_id: "%s", peer_ident: "%s"',
          bot_id, peer_ident.to_bytes())
      raise auth.AuthorizationError('Bot ID doesn\'t match the token used')
  elif cfg.require_service_account:
    expected_id = auth.Identity(auth.IDENTITY_USER, cfg.require_service_account)
    if peer_ident != expected_id:
      logging.error(
          'bot_auth: bot is not using expected service account\n'
          'bot_id: "%s", expected_id: "%s", peer_ident: "%s"',
          bot_id, expected_id.to_bytes(), peer_ident.to_bytes())
      raise auth.AuthorizationError('bot is not using expected service account')
  elif not cfg.ip_whitelist:
    # This branch should not be hit for validated configs.
    logging.error(
        'bot_auth: invalid bot group config, no auth method defined\n'
        'bot_id: "%s"', bot_id)
    raise auth.AuthorizationError('Invalid bot group config')

  # Check that IP whitelist applies (in addition to credentials).
  if cfg.ip_whitelist:
    ip = auth.get_peer_ip()
    if not auth.is_in_ip_whitelist(cfg.ip_whitelist, ip):
      logging.error(
          'bot_auth: bot IP is not whitelisted\n'
          'bot_id: "%s", peer_ip: "%s", ip_whitelist: "%s"',
          bot_id, ipaddr.ip_to_string(ip), cfg.ip_whitelist)
      raise auth.AuthorizationError('Not IP whitelisted')

  return cfg


def _is_valid_ident_for_bot(ident, bot_id):
  """True if bot_id matches the identity derived from a machine token.

  bot_id is usually hostname, and the identity derived from a machine token is
  'bot:<fqdn>', so we validate that <fqdn> starts with '<bot_id>.'.

  We also explicitly skip magical 'bot:ip-whitelisted' identity assigned to
  bots that use 'bots' IP whitelist for auth (not tokens).
  """
  # TODO(vadimsh): Should bots.cfg also contain a list of allowed domain names,
  # so this check is stricter?
  return (
      ident.kind == auth.IDENTITY_BOT and
      ident != auth.IP_WHITELISTED_BOT_ID and
      ident.name.startswith(bot_id + '.'))
