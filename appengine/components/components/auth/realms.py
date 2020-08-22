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

  # Permission name => its index in the merged realms_pb2.Realms.
  perm_index = {p.name: idx for idx, p in enumerate(permissions)}

  # Visit in order of project IDs.
  for proj_id, proj_realms in sorted(realms.items()):
    # Calculate a mapping from the permission index in `proj_realms` to
    # the index in the final merged proto (or None if undefined).
    old_to_new = [perm_index.get(p.name) for p in proj_realms.permissions]

    # Visit all bindings in all realms.
    for old_realm in proj_realms.realms:
      # Relabel permission indexes, drop empty bindings that may appear.
      bindings = []
      for b in old_realm.bindings:
        perms = sorted(
            old_to_new[idx]
            for idx in b.permissions
            if old_to_new[idx] is not None
        )
        if perms:
          bindings.append((perms, b.principals))

      # Add the relabeled realm to the output.
      assert old_realm.name.startswith(proj_id+':'), old_realm.name
      new_realm = out.realms.add()
      new_realm.name = old_realm.name
      new_realm.bindings.extend(
          realms_pb2.Binding(permissions=perms, principals=principals)
          for perms, principals in sorted(bindings, key=lambda x: x[0])
      )
      if old_realm.HasField('data'):
        new_realm.data.CopyFrom(old_realm.data)

  return out
