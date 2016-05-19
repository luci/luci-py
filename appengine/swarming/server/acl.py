# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines access groups."""

from components import auth
from components import utils

from server import bot_auth


# Names of groups.
# See https://code.google.com/p/swarming/wiki/SwarmingAccessGroups for each
# level.
#
# TODO(vadimsh): Move them to the config.
ADMINS_GROUP = 'swarming-admins'
BOTS_GROUP = bot_auth.BOTS_GROUP
PRIVILEGED_USERS_GROUP = 'swarming-privileged-users'
USERS_GROUP = 'swarming-users'


def is_admin():
  return auth.is_group_member(ADMINS_GROUP) or auth.is_admin()


def is_bot():
  return bot_auth.is_known_bot() or is_admin()


def is_privileged_user():
  return auth.is_group_member(PRIVILEGED_USERS_GROUP) or is_admin()


def is_user():
  return auth.is_group_member(USERS_GROUP) or is_privileged_user()


def is_bot_or_user():
  # TODO(vadimsh): Get rid of this. Swarming jobs will use service accounts
  # associated with the job when calling Swarming, not the machine ID itself.
  return is_bot() or is_user()


def is_bot_or_privileged_user():
  # TODO(vadimsh): Get rid of this. Swarming jobs will use service accounts
  # associated with the job when calling Swarming, not the machine ID itself.
  return is_bot() or is_privileged_user()


def is_bot_or_admin():
  """Returns True if current user can execute user-side and bot-side calls."""
  # TODO(vadimsh): Get rid of this. Swarming jobs will use service accounts
  # associated with the job when calling Swarming, not the machine ID itself.
  return is_bot() or is_admin()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if is_admin():
    return 'admin'
  if is_privileged_user():
    return 'privileged user'
  if is_user():
    return 'user'
  return 'unknown user'


def bootstrap_dev_server_acls():
  """Adds localhost to IP whitelist and Swarming groups."""
  assert utils.is_local_dev_server()
  if auth.is_replica():
    return

  bots = auth.bootstrap_loopback_ips()
  auth.bootstrap_group(BOTS_GROUP, bots, 'Swarming bots')
  auth.bootstrap_group(USERS_GROUP, bots, 'Swarming users')

  # Add a swarming admin. smoke-test@example.com is used in
  # server_smoke_test.py
  admin = auth.Identity(auth.IDENTITY_USER, 'smoke-test@example.com')
  auth.bootstrap_group(ADMINS_GROUP, [admin], 'Swarming administrators')

  # Add an instance admin (for easier manual testing when running dev server).
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')
