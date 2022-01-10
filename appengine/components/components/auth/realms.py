# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities to work with realms_pb2 messages."""

from .proto import realms_pb2


# Currently acceptable version of Realms API. See api_version in realms.proto.
API_VERSION = 1


def merge(permissions, realms, out=None):
  """Merges multiple realms_pb2.Realms into one, fills in `api_version`.

  The given list of permissions will become authoritative: if some realm uses
  a permission not in the list, it will be silently dropped from the bindings.
  This can potentially happen due to the asynchronous nature of realms config
  updates (e.g. a role change that deletes some permissions can be committed
  into the AuthDB before realms_pb2.Realms are reevaluated). Eventually the
  state should converge to be 100% consistent.

  Args:
    permissions: a sorted list of realms_pb2.Permission with all permissions.
    realms: a dict {project ID -> realms_pb2.Realms with its realms} to merge.
    out: a realms_pb2.Realms to write the result into (will not be cleared!).

  Returns:
    `out` or a new realms_pb2.Realms if out was None.
  """
  out = out or realms_pb2.Realms()
  out.api_version = API_VERSION
  out.permissions.extend(permissions)

  # Sorted list of pairs (project_id, realms_pb2.Realms), we'll visit it twice.
  sorted_realms = sorted(realms.items())

  # Merge the set of all conditions across all projects.
  conds = ConditionsSet()
  for _, proj_realms in sorted_realms:
    for cond in proj_realms.conditions:
      conds.add(cond)
  out.conditions.extend(conds.flat)

  # Permission name => its index in the merged realms_pb2.Realms.
  perm_index = {p.name: idx for idx, p in enumerate(permissions)}

  # Visit in order of project IDs.
  for proj_id, proj_realms in sorted_realms:
    # Calculate a mapping from the permission index in `proj_realms` to
    # the index in the final merged proto (or None if undefined).
    old_to_new = [perm_index.get(p.name) for p in proj_realms.permissions]

    # Visit all bindings in all realms.
    for old_realm in proj_realms.realms:
      # Relabel permission and condition indexes, drop empty bindings that may
      # appear due to unknown permissions.
      bindings = []
      for b in old_realm.bindings:
        perms = sorted(
            old_to_new[idx]
            for idx in b.permissions
            if old_to_new[idx] is not None
        )
        if perms:
          bindings.append((
              perms,
              conds.relabel(proj_realms.conditions, b.conditions),
              b.principals,
          ))

      # Add the relabeled realm to the output.
      assert old_realm.name.startswith(proj_id+':'), old_realm.name
      new_realm = out.realms.add()
      new_realm.name = old_realm.name
      new_realm.bindings.extend(
          realms_pb2.Binding(
              permissions=perms,
              principals=principals,
              conditions=conds,
          )
          for perms, conds, principals in sorted(bindings)
      )
      if old_realm.HasField('data'):
        new_realm.data.CopyFrom(old_realm.data)

  return out


class ConditionsSet(object):
  """Dedups identical conditions, maps them to integer indexes.

  Most often identical conditions appear from implicit root bindings that are
  similar across all projects.

  Assumes all incoming realms_pb2.Condition are immutable and already
  normalized. Retains the order in which they were added.
  """

  def __init__(self):
    self.flat = []  # the final list of dedupped realms_pb2.Condition
    self._mapping = {}  # serialized realms_pb2.Condition => its index
    self._id_mapping = {}  # id(realms_pb2.Condition) => its index
    self._retain = []  # list of all conditions ever passed to `add`

  def add(self, cond):
    idx = self._mapping.setdefault(cond.SerializeToString(), len(self.flat))
    if idx == len(self.flat):
      self.flat.append(cond)
    self._id_mapping[id(cond)] = idx
    self._retain.append(cond)  # keep the pointer alive to pin id(cond)

  def relabel(self, conds, indexes):
    return sorted(self._id_mapping[id(conds[idx])] for idx in indexes)
