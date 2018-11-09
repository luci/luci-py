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


def is_authenticated_bot(bot_id, machine_type):
  """Returns True if bot with given ID is using correct credentials.

  Expected to be called in a context of a handler of a request coming from the
  bot with given ID.
  """
  try:
    validate_bot_id_and_fetch_config(bot_id, machine_type)
    return True
  except auth.AuthorizationError:
    return False


# pylint: disable=unused-argument
def validate_bot_id_and_fetch_config(bot_id, machine_type):
  """Verifies ID reported by a bot matches the credentials being used.

  Expected to be called in a context of some bot API request handler. Uses
  bots.cfg config to look up what credentials are expected to be used by the bot
  with given ID.

  Raises auth.AuthorizationError if bot_id is unknown or bot is using invalid
  credentials.

  On success returns the configuration for this bot (BotGroupConfig tuple), as
  defined in bots.cfg
  """
  bot_id = _extract_primary_hostname(bot_id)
  cfg = bot_groups_config.get_bot_group_config(bot_id, machine_type)
  if not cfg:
    logging.error(
        'bot_auth: unknown bot_id, not in the config\nbot_id: "%s"', bot_id)
    raise auth.AuthorizationError('Unknown bot ID, not in config')

  # This should not really happen for validated configs.
  if not cfg.auth:
    logging.error('bot_auth: no auth configured in bots.cfg')
    raise auth.AuthorizationError('No auth configured in bots.cfg')

  ip = auth.get_peer_ip()
  peer_ident = auth.get_peer_identity()

  # Sequentially try all auth methods. If at least one applies, the bot is
  # successfully authenticated (and logs from unsuccessful methods are emitted
  # at 'debug' level). If none applies, dump all errors messages to the log and
  # raise AuthorizationError with a list of rejection reasons from all auth
  # methods.
  public_errs = []
  internal_details = []
  for i, bot_auth in enumerate(cfg.auth):
    err, details = _check_bot_auth(bot_auth, bot_id, peer_ident, ip)
    if not err:
      if internal_details:
        logging.debug('Auth method #%d succeeded! Logs from other methods:', i)
        for msg in internal_details:
          logging.debug('%s', msg)
      return cfg
    public_errs.append(err)
    internal_details.extend(details)

  logging.error('All %d auth methods failed:', len(cfg.auth))
  for msg in internal_details:
    logging.error('%s', msg)

  # In most cases there's only one auth method used, so we can simplify the
  # error message to be less confusing.
  if len(public_errs) == 1:
    raise auth.AuthorizationError(public_errs[0])
  raise auth.AuthorizationError(
      'All auth methods failed: %s' % '; '.join(public_errs))


def _check_bot_auth(bot_auth, bot_id, peer_ident, ip):
  """Checks whether a bot matches some authorization method.

  Args:
    bot_auth: an instance of bot_groups_config.BotAuth with auth config.
    bot_id: ID of the bot host, as sent inside the RPC body.
    peer_ident: Identity the bot had authenticated with.
    ip: IP address of the bot.

  Returns:
    (None, []) on success.
    (Public error message, list of internal error messages) on failure.
  """
  errors = []
  def error(msg, *args):
    errors.append(msg % args)

  if bot_auth.require_luci_machine_token:
    if not _is_valid_ident_for_bot(peer_ident, bot_id):
      error(
          'bot_auth: bot ID doesn\'t match the machine token used\n'
          'bot_id: "%s", peer_ident: "%s"',
          bot_id, peer_ident.to_bytes())
      return 'Bot ID doesn\'t match the token used', errors
  elif bot_auth.require_service_account:
    expected_ids = [
      auth.Identity(auth.IDENTITY_USER, email)
      for email in bot_auth.require_service_account
    ]
    if peer_ident not in expected_ids:
      error(
          'bot_auth: bot is not using expected service account\n'
          'bot_id: "%s", expected_id: %s, peer_ident: "%s"',
          bot_id, [i.to_bytes() for i in expected_ids], peer_ident.to_bytes())
      if peer_ident.is_anonymous:
        error(
            'Bot is identifying as anonymous. Is the "userinfo" scope enabled '
            'for this instance?')
      return 'Bot is not using expected service account', errors
  elif not bot_auth.ip_whitelist:
    # This branch should not be hit for validated configs.
    error(
        'bot_auth: invalid bot group config, no auth method defined\n'
        'bot_id: "%s"', bot_id)
    return 'Invalid bot group config', errors

  # Check that IP whitelist applies (in addition to credentials).
  if bot_auth.ip_whitelist:
    if not auth.is_in_ip_whitelist(bot_auth.ip_whitelist, ip):
      error(
          'bot_auth: bot IP is not whitelisted\n'
          'bot_id: "%s", peer_ip: "%s", ip_whitelist: "%s"',
          bot_id, ipaddr.ip_to_string(ip), bot_auth.ip_whitelist)
      return 'Not IP whitelisted', errors

  # Success!
  return None, []


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
