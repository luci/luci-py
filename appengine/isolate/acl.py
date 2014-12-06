# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from components import auth
from components import utils


# Names of groups.
ADMINS_GROUP = 'isolate-admin-access'
READERS_GROUP = 'isolate-read-access'
WRITERS_GROUP = 'isolate-write-access'


def isolate_admin():
  """Returns True if current user can administer isolate server."""
  return auth.is_group_member(ADMINS_GROUP) or auth.is_admin()


def isolate_writable():
  """Returns True if current user can write to isolate."""
  # Admins have access by default.
  return auth.is_group_member(WRITERS_GROUP) or isolate_admin()


def isolate_readable():
  """Returns True if current user can read from isolate."""
  # Anyone that can write can also read.
  return auth.is_group_member(READERS_GROUP) or isolate_writable()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if isolate_admin():
    return 'admin'
  if isolate_writable():
    return 'user'
  return 'unknown user'


def bootstrap():
  """Adds 127.0.0.1 as a whitelisted IP when testing."""
  if not utils.is_local_dev_server() or auth.is_replica():
    return

  bots = auth.bootstrap_loopback_ips()
  auth.bootstrap_group(READERS_GROUP, bots, 'Can read from Isolate')
  auth.bootstrap_group(WRITERS_GROUP, bots, 'Can write to Isolate')

  # Add a fake admin for local dev server.
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')
