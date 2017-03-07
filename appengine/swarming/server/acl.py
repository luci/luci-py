# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Defines access groups."""

from components import auth
from components import utils
from server import config


def is_admin():
  admins = config.settings().auth.admins_group
  return auth.is_group_member(admins) or auth.is_admin()


def is_privileged_user():
  priv_users = config.settings().auth.privileged_users_group
  return auth.is_group_member(priv_users) or is_admin()


def is_user():
  users = config.settings().auth.users_group
  return auth.is_group_member(users) or is_privileged_user()


def is_bootstrapper():
  """Returns True if current user have access to bot code (for bootstrap)."""
  bot_group = config.settings().auth.bot_bootstrap_group
  return is_admin() or auth.is_group_member(bot_group)


def is_ip_whitelisted_machine():
  """Returns True if the call is made from IP whitelisted machine."""
  # TODO(vadimsh): Get rid of this. It's blocked on fixing /bot_code calls in
  # bootstrap code everywhere to use service accounts and switching all Swarming
  # Tasks API calls made from bots to use proper authentication.
  return auth.is_in_ip_whitelist(
      auth.bots_ip_whitelist(), auth.get_peer_ip(), False)


def is_bot():
  # TODO(vadimsh): Get rid of this. Swarming jobs will use service accounts
  # associated with the job when calling Swarming, not the machine IP.
  return is_ip_whitelisted_machine() or is_admin()


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


def can_schedule_high_priority_tasks():
  """Returns True if the current user can schedule high priority tasks."""
  return is_bot() or is_privileged_user()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if is_admin():
    return 'admin'
  if is_privileged_user():
    return 'privileged user'
  if is_user():
    return 'user'


def bootstrap_dev_server_acls():
  """Adds localhost to IP whitelist and Swarming groups."""
  assert utils.is_local_dev_server()
  if auth.is_replica():
    return

  bots = auth.bootstrap_loopback_ips()

  auth_settings = config.settings().auth
  admins_group = auth_settings.admins_group
  users_group = auth_settings.users_group
  bot_bootstrap_group = auth_settings.bot_bootstrap_group

  auth.bootstrap_group(users_group, bots, 'Swarming users')
  auth.bootstrap_group(bot_bootstrap_group, bots, 'Bot bootstrap')

  # Add a swarming admin. smoke-test@example.com is used in
  # server_smoke_test.py
  admin = auth.Identity(auth.IDENTITY_USER, 'smoke-test@example.com')
  auth.bootstrap_group(admins_group, [admin], 'Swarming administrators')

  # Add an instance admin (for easier manual testing when running dev server).
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')
