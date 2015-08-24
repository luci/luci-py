# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top level access control for Auth API itself."""

from .. import api


ACCESS_GROUP_NAME = 'auth-service-access'


def has_access(identity=None):
  """Returns True if current caller can access groups and other auth data.

  Used in @require(...) decorators of API handlers.

  It is a top level check that acts as an access guard for both reads and
  writes. Individual entities are protected by additional checks.

  By default, passing 'has_access' check grants read-only access to everything
  (via UI or API). Write access is controlled by more fine-grain ACLs.
  """
  # TODO(vadimsh): Remove 'groups-readonly-access' once everything is migrated
  # to 'auth-service-access'.
  identity = identity or api.get_current_identity()
  return (
      api.is_admin(identity) or
      api.is_group_member(ACCESS_GROUP_NAME, identity) or
      api.is_group_member('groups-readonly-access', identity))
