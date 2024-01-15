# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Database of defined permissions and roles."""

import collections

from components.auth.proto import realms_pb2


# Prefix for role names defined in the Auth service code.
BUILTIN_ROLE_PREFIX = 'role/'
# Prefix for role names that can be defined in user-supplied realms.cfg.
CUSTOM_ROLE_PREFIX = 'customRole/'
# Prefix for internally used roles that are forbidden in realms.cfg.
INTERNAL_ROLE_PREFIX = 'role/luci.internal.'


# Representation of all defined roles, permissions and implicit bindings.
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
        'revision',       # an arbitrary string identifying a particular version
        'permissions',    # a dict {permission str => realms_pb2.Permission}
        'roles',          # a dict {full role name str => Role}
        'attributes',     # a set with attribute names allowed in conditions
        'implicit_root_bindings',  # f(proj_id) -> [realms_config_pb2.Binding]
    ])

# Represents a single role.
Role = collections.namedtuple(
    'Role',
    [
        'name',         # full name of the role
        'permissions',  # a tuple with permission strings sorted alphabetically
    ])


class Builder(object):
  """Builder is used internally by db() to assemble the permissions DB."""
  PermRef = collections.namedtuple('PermRef', ['name'])
  RoleRef = collections.namedtuple('RoleRef', ['name'])

  def __init__(self, revision):
    self.revision = revision
    self.permissions = {}  # permission name -> realms_pb2.Permission
    self.roles = {}  # role name => set of str with permissions
    self.attributes = set()  # a set of str
    self.implicit_root_bindings = lambda _: []  # see DB.implicit_root_bindings

  def permission(self, name, internal=False):
    """Defines a permission if it hasn't been defined before.

    Idempotent. Raises ValueError when attempting to redeclare the permission
    with the same name but different attributes.

    Returns a reference to the permission that can be passed to `role` as
    an element of `includes`.
    """
    if name.count('.') != 2:
      raise ValueError(
          'Permissions must have form <service>.<subject>.<verb> for now, '
          'got %s' % (name,))

    perm = realms_pb2.Permission(name=name, internal=internal)

    existing = self.permissions.get(name)
    if existing:
      if existing != perm:
        raise ValueError('Redeclaring permission %s' % name)
    else:
      self.permissions[name] = perm

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

  def attribute(self, name):
    """Defines an attribute that can be referenced in conditions."""
    if not isinstance(name, basestring):
      raise TypeError('Attribute names must be strings')
    self.attributes.add(name)

  def finish(self):
    return DB(
        revision=self.revision,
        permissions=self.permissions,
        roles={
            name: Role(name=name, permissions=tuple(sorted(perms)))
            for name, perms in self.roles.items()
        },
        attributes=self.attributes,
        implicit_root_bindings=self.implicit_root_bindings)
