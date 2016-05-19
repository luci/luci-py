# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A registry of known bots and server-side assigned (trusted) dimensions.

It is fetched from the config service. Functions here are used by bot API
handlers in handlers_bot.py.
"""

from components import auth


# TODO(vadimsh): Get rid of this in favor of swarming config stored
# in luci-config.
BOTS_GROUP = 'swarming-bots'


def is_known_bot():
  """Returns True if the current caller is mentioned in the swarming config.

  Expected to be called in a context of some bot API request handler.

  Other bots are considered unauthorized to access the Swarming server.
  """
  # TODO(vadimsh): Check that bot credentials are whitelisted in the server-side
  # bot config (same one that specifies trusted dimensions).
  return auth.is_group_member(BOTS_GROUP)



def validate_bot_id(bot_id):  # pylint: disable=unused-argument
  """Verifies ID the bot reports matches the credentials being used.

  Expected to be called in a context of some bot API request handler.

  Raises auth.AuthenticationError if bot_id is not correct.
  """
  # TODO(vadimsh): Check swarming server configuration to decide whether the
  # supplied bot_id is allowed to be used with the supplied credentials.
  # For example, if the bot is using machine tokens, we check that
  # bot_id == hostname specified in the token. If the bot is using IP
  # whitelist, we check that its bot_id is allowed to use IP whitelist, etc.


def fetch_trusted_dimensions(bot_id):  # pylint: disable=unused-argument
  """Returns a dict with bot dimensions, fetching them from the config.

  Expected to be called in a context of some bot API request handler.

  Dimensions returned here override corresponding dimensions reported by the
  bot itself (if any), that's why they are trusted: bot can't "spoof" them.
  """
  # TODO(vadimsh): Implement.
  return {}
