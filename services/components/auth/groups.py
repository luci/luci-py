# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Group management utility functions used internally from REST API handlers."""

from . import model


def find_references(group):
  """Finds services and groups that reference the specified group.

  Used to verify that |group| is safe to delete, i.e. nothing depends on it.
  A service can reference group in its ACL rules. A group can reference
  another group in its list of nested groups.

  This function is slow and should not be used on performance critical paths.

  Returns:
    Pair (set(names of referencing groups), set(names of referencing services)).
  """
  # Try to find this group as a nested one in some other group.
  referencing_groups = model.AuthGroup.query(
      model.AuthGroup.nested == group,
      ancestor=model.ROOT_KEY).fetch_async(keys_only=True)

  # While the query is running, search for services that mention
  # the group in ACL rules.
  referencing_services = set()
  for service_config in model.AuthServiceConfig.query(ancestor=model.ROOT_KEY):
    if any(rule.group == group for rule in service_config.rules):
      referencing_services.add(service_config.key.id())

  # Wait for AuthGroup query to finish, return the result.
  referencing_groups = set(key.id() for key in referencing_groups.get_result())
  return referencing_groups, referencing_services
