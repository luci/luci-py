# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from components import auth
from components import utils


# Group with read and write access.
FULL_ACCESS_GROUP = 'isolate-access'
# Group with read only access (in addition to isolate-access).
READONLY_ACCESS_GROUP = 'isolate-readonly-access'


def isolate_writable():
  """Returns True if current user can write to isolate."""
  return auth.is_group_member(FULL_ACCESS_GROUP) or auth.is_admin()


def isolate_readable():
  """Returns True if current user can read from isolate."""
  return auth.is_group_member(READONLY_ACCESS_GROUP) or isolate_writable()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if auth.is_admin():
    return 'admin'
  if isolate_readable():
    return 'user'
  return 'unknown user'


def bootstrap():
  """Adds 127.0.0.1 as a whitelisted IP when testing."""
  if not utils.is_local_dev_server() or auth.is_replica():
    return

  # Allow local bots full access.
  bots = auth.bootstrap_loopback_ips()
  auth.bootstrap_group(
      FULL_ACCESS_GROUP, bots, 'Can read and write from/to Isolate')

  # Add a fake admin for local dev server.
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')
