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


# TODO(vadimsh): Get rid of this in favor of swarming config stored
# in luci-config.
BOTS_GROUP = 'swarming-bots'


def is_ip_whitelisted_machine():
  """Returns True if the call is made from IP whitelisted machine."""
  # TODO(vadimsh): Get rid of this. It's blocked on fixing /bot_code calls in
  # bootstrap code everywhere to use service accounts and switching all Swarming
  # Tasks API calls made from bots to use proper authentication.
  return auth.is_group_member(BOTS_GROUP)


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

  On success returns the configuration for this bot, as defined in bots.cfg
  """
  # TODO(vadimsh): Check swarming server configuration to decide whether the
  # supplied bot_id is allowed to be used with the supplied credentials.
  # For example, if the bot is using machine tokens, we check that
  # bot_id == hostname specified in the token. If the bot is using IP
  # whitelist, we check that its bot_id is allowed to use IP whitelist, etc.
  if not auth.is_group_member(BOTS_GROUP):
    logging.error(
        'Unauthorized bot request\n'
        'bot_id: "%s", peer_ident: "%s", peer_ip: "%s"',
        bot_id, auth.get_peer_identity().to_bytes(),
        ipaddr.ip_to_string(auth.get_peer_ip()))
    raise auth.AuthorizationError('Not allowed')
  return None
