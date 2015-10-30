# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""ACL checks for endpoints exposed by auth_service."""

from components import auth
from components.auth.ui import acl

import replication


def has_access(identity=None):
  """Returns True if current caller can access groups and other auth data."""
  return acl.has_access(identity)


def is_trusted_service(identity=None):
  """Returns True if caller is in 'auth-trusted-services' group."""
  return auth.is_group_member('auth-trusted-services', identity)


def is_replica_or_trusted_service():
  """Returns True if caller is a replica or in 'auth-trusted-services' group."""
  return (
      is_trusted_service() or
      replication.is_replica(auth.get_current_identity()))
