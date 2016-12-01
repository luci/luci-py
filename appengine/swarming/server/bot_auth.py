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
  original_bot_id = bot_id
  bot_id = _extract_primary_hostname(bot_id)
  cfg = bot_groups_config.get_bot_group_config(bot_id)
  if not cfg:
    logging.error(
        'bot_auth: unknown bot_id, not in the config\nbot_id: "%s", '
        'original bot_id: "%s"', bot_id, original_bot_id)
    raise auth.AuthorizationError('Unknown bot ID, not in config')

  peer_ident = auth.get_peer_identity()
  if cfg.require_luci_machine_token:
    if not _is_valid_ident_for_bot(peer_ident, bot_id):
      logging.error(
          'bot_auth: bot ID doesn\'t match the machine token used\n'
          'bot_id: "%s", peer_ident: "%s", original bot_id: "%s"',
          bot_id, peer_ident.to_bytes(), original_bot_id)
      raise auth.AuthorizationError('Bot ID doesn\'t match the token used')
  elif cfg.require_service_account:
    expected_id = auth.Identity(auth.IDENTITY_USER, cfg.require_service_account)
    if peer_ident != expected_id:
      logging.error(
          'bot_auth: bot is not using expected service account\n'
          'bot_id: "%s", expected_id: "%s", peer_ident: "%s", '
          'original bot_id: "%s"', bot_id, expected_id.to_bytes(),
          peer_ident.to_bytes(), original_bot_id)
      if peer_ident.to_bytes() == 'anonymous:anonymous':
        logging.error('bot is identifying as anonymous. Is the "userinfo" '
                      'scope enabled for this instance?')
      raise auth.AuthorizationError('bot is not using expected service account')
  elif not cfg.ip_whitelist:
    # This branch should not be hit for validated configs.
    logging.error(
        'bot_auth: invalid bot group config, no auth method defined\n'
        'bot_id: "%s", original bot_id: "%s"', bot_id, original_bot_id)
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


def _extract_primary_hostname(bot_id):
   """If the bot_id is a composed name, return just the primary hostname.

   Multiple bots running on the same host may use the host's token to
   authenticate. When this is the case, the hostname is needed to
   validate the token. It can be extracted from their bot_ids, which will take
   the form $(hostname)--$(random_identifier).
   """
   # TODO(bpastene): Change the '--' seperator to something more unique if/when
   # this is used in production.
   if not bot_id:
     return bot_id
   parts = bot_id.split('--')
   if len(parts) == 2:
     return parts[0]
   elif len(parts) > 2:
     logging.error('Unable to parse composed bot_id: %s', bot_id)
   return bot_id
