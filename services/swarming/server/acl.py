# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines access groups."""

from components import auth
from components import utils
from server import user_manager


# Names of groups.
# See https://code.google.com/p/swarming/wiki/SwarmingAccessGroups for each
# level.
ADMINS_GROUP = 'swarming-admins'
BOTS_GROUP = 'swarming-bots'
PRIVILEGED_USERS_GROUP = 'swarming-privileged-users'
USERS_GROUP = 'swarming-users'


def is_admin():
  return auth.is_group_member(ADMINS_GROUP) or auth.is_admin()


def is_bot():
  return auth.is_group_member(BOTS_GROUP) or is_admin()


def is_privileged_user():
  return auth.is_group_member(PRIVILEGED_USERS_GROUP) or is_admin()


def is_user():
  return auth.is_group_member(USERS_GROUP) or is_privileged_user()


def is_bot_or_user():
  return is_bot() or is_user()


def is_bot_or_privileged_user():
  return is_bot() or is_privileged_user()


def is_bot_or_admin():
  """Returns True if current user can execute user-side and bot-side calls."""
  return is_bot() or is_admin()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if is_admin():
    return 'admin'
  if is_privileged_user():
    return 'privileged user'
  if is_user():
    return 'user'
  if is_bot():
    return 'bot'
  return 'unknown user'


def bootstrap_dev_server_acls():
  """Adds localhost to IP whitelist and Swarming groups."""
  assert utils.is_local_dev_server()

  # Add a bot.
  user_manager.AddWhitelist('127.0.0.1')
  bot = auth.Identity(auth.IDENTITY_BOT, '127.0.0.1')
  auth.bootstrap_group(BOTS_GROUP, bot, 'Swarming bots')
  auth.bootstrap_group(USERS_GROUP, bot, 'Swarming users')

  # Add a swarming admin. smoke-test@example.com is used in server_smoke_test.py
  admin = auth.Identity(auth.IDENTITY_USER, 'smoke-test@example.com')
  auth.bootstrap_group(ADMINS_GROUP, admin, 'Swarming administrators')

  # Add an instance admin (for easier manual testing when running dev server).
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      auth.Identity(auth.IDENTITY_USER, 'test@example.com'),
      'Users that can manage groups')
