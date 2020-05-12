# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Database of defined permissions and roles."""

import collections

from components import utils
from components.auth.proto import realms_pb2

# Prefix for role names defined in the Auth service code.
BUILTIN_ROLE_PREFIX = 'role/'
# Prefix for role names that can be defined in user-supplied realms.cfg.
CUSTOM_ROLE_PREFIX = 'customRole/'


# Representation of all defined roles and permissions.
#
# Must be treated as immutable once constructed. Do not mess with it.
#
# 'revision' property is expected to comply with the follow rule: if two DB
# instances have the exact same revision, then they must be identical. But
# different revisions do not imply DB are necessarily different too (they still
# may be identical).
DB = collections.namedtuple(
    'DB',
    [
        'revision',     # an arbitrary string identifying a particular version
        'permissions',  # a dict {permission str => realms_pb2.Permission}
        'roles',        # a dict {full role name str => Role}
    ])

# Represents a single role.
Role = collections.namedtuple(
    'Role',
    [
        'name',         # full name of the role
        'permissions',  # a tuple with permission strings sorted alphabetically
    ])


@utils.cache
def db():
  """Returns the current set of all permissions and roles as DB object.

  Right now returns the exact same object all the time, but don't rely on this
  property, it may change in the future.
  """
  # Mini DSL for defining permissions and roles. See Builder comments.
  builder = Builder(revision='auth_service_ver:' + utils.get_app_version())
  permission = builder.permission
  include = builder.include
  role = builder.role

  # Used for adhoc testing only. Will likely be deleted once we have some real
  # permissions and roles.
  role('role/dev.testing1', [
      permission('luci.dev.testing1'),
      permission('luci.dev.testing2'),
  ])
  role('role/dev.testing2', [
      permission('luci.dev.testing2'),
      permission('luci.dev.testing3'),
  ])
  role('role/dev.testing3', [
      include('role/dev.testing1'),
      include('role/dev.testing2'),
  ])

  # LUCI Config permissions and roles (crbug.com/1068817).
  role('role/configs.reader', [
      permission('configs.configSets.read'),
  ])
  role('role/configs.developer', [
      include('role/configs.reader'),
      permission('configs.configSets.validate'),
      permission('configs.configSets.reimport'),
  ])

  # LUCI Scheduler permissions and roles (crbug.com/1070761).
  role('role/scheduler.reader', [
      permission('scheduler.jobs.get'),
  ])
  role('role/scheduler.triggerer', [
      include('role/scheduler.reader'),
      permission('scheduler.jobs.trigger'),
  ])
  role('role/scheduler.owner', [
      include('role/scheduler.reader'),
      include('role/scheduler.triggerer'),
      permission('scheduler.jobs.pause'),
      permission('scheduler.jobs.resume'),
      permission('scheduler.jobs.abort'),
  ])

  # Swarming permissions and roles (crbug.com/1066839).
  # See swarming/proto/config/realms.proto for more details.
  role('role/swarming.poolUser', [
      permission('swarming.pools.createTask'),
  ])

  return builder.finish()


class Builder(object):
  """Builder is used internally by db() to assemble the permissions DB."""

  PermRef = collections.namedtuple('PermRef', ['name'])
  RoleRef = collections.namedtuple('RoleRef', ['name'])

  def __init__(self, revision):
    self.revision = revision
    self.permissions = set()  # set of str with all permissions
    self.roles = {}  # role name => set of str with permissions

  def permission(self, name):
    """Defines a permission if it hasn't been defined before.

    Idempotent. Returns a reference to permission that can be passed to `role`
    as an element of `includes`.
    """
    if name.count('.') != 2:
      raise ValueError(
          'Permissions must have form <service>.<subject>.<verb> for now, '
          'got %s' % (name,))
    self.permissions.add(name)
    return self.PermRef(name)

  def include(self, role):
    """Returns a reference to some role, so it can be included in another role.

    This reference can be passed to `role` as an element of `includes`. The
    referenced role should be defined already.
    """
    if role not in self.roles:
      raise ValueError('Role %s hasn\'t been defined yet' % (role,))
    return self.RoleRef(role)

  def role(self, name, includes):
    """Defines a role that includes given permissions and other roles.

    The role should not have been defined before. To include this role into
    another role, create a reference to it via `include` and pass it as an
    element of `includes`.

    Note that `includes` should be a a list containing either PermRef (returned
    by `permissions`) or RoleRef (returned by `include`). Raw strings are not
    allowed.
    """
    if not name.startswith(BUILTIN_ROLE_PREFIX):
      raise ValueError(
          'Built-in roles must start with %s, got %s' %
          (BUILTIN_ROLE_PREFIX, name))
    if name in self.roles:
      raise ValueError('Role %s has already been defined' % (name,))
    perms = set()
    for inc in includes:
      if isinstance(inc, self.PermRef):
        perms.add(inc.name)
      elif isinstance(inc, self.RoleRef):
        perms.update(self.roles[inc.name])
      else:
        raise ValueError('Unknown include %s' % (inc,))
    self.roles[name] = perms

  def finish(self):
    return DB(
        revision=self.revision,
        permissions={
            name: realms_pb2.Permission(name=name) for name in self.permissions
        },
        roles={
            name: Role(name=name, permissions=tuple(sorted(perms)))
            for name, perms in self.roles.items()
        })
