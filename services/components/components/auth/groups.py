# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Group management utility functions used internally from REST API handlers."""

from google.appengine.ext import ndb

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


def get_missing_groups(groups):
  """Given a list of group names, returns a list of groups that do not exist."""
  # We need to iterate over |groups| twice. It won't work if |groups|
  # is a generator. So convert to list first.
  groups = list(groups)
  entities = ndb.get_multi(
      ndb.Key(model.AuthGroup, name, parent=model.ROOT_KEY)
      for name in groups)
  return [name for name, ent in zip(groups, entities) if not ent]


def find_dependency_cycle(group):
  """Searches for dependency cycle between nested groups.

  Traverses the dependency graph starting from |group|, fetching all necessary
  groups from datastore along the way.

  Args:
    group: instance of AuthGroup to start traversing from. It doesn't have to be
        committed to Datastore itself (but all its nested groups should be
        there already).

  Returns:
    List of names of groups that form a cycle or empty list if no cycles.
  """
  # It is a depth-first search on a directed graph with back edge detection.
  # See http://www.cs.nyu.edu/courses/summer04/G22.1170-001/6a-Graphs-More.pdf

  # Cache of already fetched groups.
  groups = {group.key.id(): group}

  # List of groups that are completely explored (all subtree is traversed).
  visited = []
  # Stack of groups that are being explored now. In case cycle is detected
  # it would contain that cycle.
  visiting = []

  def visit(group):
    """Recursively explores |group| subtree, returns True if finds a cycle."""
    assert group not in visiting
    assert group not in visited

    # Load bodies of nested groups not seen so far into |groups|.
    entities = ndb.get_multi(
        ndb.Key(model.AuthGroup, name, parent=model.ROOT_KEY)
        for name in group.nested if name not in groups)
    groups.update({entity.key.id(): entity for entity in entities if entity})

    visiting.append(group)
    for nested in group.nested:
      obj = groups.get(nested)
      # Do not crash if non-existent group is referenced somehow.
      if not obj:
        continue
      # Cross edge. Can happen in diamond-like graph, not a cycle.
      if obj in visited:
        continue
      # Back edge: |group| references its own ancestor -> cycle.
      if obj in visiting:
        return True
      # Explore subtree.
      if visit(obj):
        return True
    visiting.pop()

    visited.append(group)
    return False

  visit(group)
  return [group.key.id() for group in visiting]
