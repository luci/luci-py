# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines access groups."""

from components import auth
from components import ereporter2
from components import utils
from server import user_manager


# Names of groups.
# See https://code.google.com/p/swarming/wiki/SwarmingAccessGroups for each
# level.
ADMINS_GROUP = 'swarming-admins'
BOTS_GROUP = 'swarming-bots'
PRIVILEGED_USERS_GROUP = 'swarming-privileged-users'
USERS_GROUP = 'swarming-users'


### Private stuff.


def _ipv4_to_int(ip):
  values = [int(i) for i in ip.split('.')]
  factor = 256
  value = 0L
  for i in values:
    value = value * factor + i
  return value


def _int_to_ipv4(integer):
  values = []
  factor = 256
  for _ in range(4):
    values.append(integer % factor)
    integer = integer / factor
  return '.'.join(str(i) for i in reversed(values))


### Public API.


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


def expand_subnet(ip, mask):
  """Returns all the IP addressed comprised in a range."""
  if mask == 32:
    return [ip]
  bit = 1 << (32 - mask)
  return [_int_to_ipv4(_ipv4_to_int(ip) + r) for r in range(bit)]


def ip_whitelist_authentication(request):
  """Check to see if the request is from a whitelisted machine.

  Will use the remote machine's IP.

  Args:
    request: WebAPP request sent by remote machine.

  Returns:
    auth.Identity of a machine if IP is whitelisted, None otherwise.
  """
  if user_manager.IsWhitelistedMachine(request.remote_addr):
    # IP v6 addresses contain ':' that is not allowed in identity name.
    return auth.Identity(
        auth.IDENTITY_BOT, request.remote_addr.replace(':', '-'))

  ereporter2.log(
      source='server',
      category='auth',
      message='Authentication failure; params: %s' % request.params,
      endpoint=request.url,
      source_ip=request.remote_addr)
  return None


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
