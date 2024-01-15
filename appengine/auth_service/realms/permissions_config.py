# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Contains model and adapter functions so Auth Service v1 uses
permissions.cfg from LUCI Config service.
"""

from google.appengine.ext import ndb

from components import datastore_utils

from proto import config_pb2, realms_config_pb2

import realms.permissions

# The filename to use when querying LUCI Config service for the
# permissions configuration.
FILENAME = 'permissions.cfg'


class FetchError(Exception):
  """Raised if permissions.cfg is missing from LUCI Config, or the
  fetched config is invalid.
  """


class MissingError(Exception):
  """Raised if there is no PermissionsConfig entity in datastore, or the
  stored config is invalid.
  """


class PermissionsConfig(ndb.Model):
  """Singleton entity for permissions.cfg imported from luci-config."""
  # Last imported SHA1 revision of the config.
  revision = ndb.StringProperty(indexed=False)
  # The config itself.
  config = datastore_utils.ProtobufProperty(config_pb2.PermissionsConfig)


def config_key():
  """Key of PermissionsConfig singleton entity."""
  return ndb.Key(PermissionsConfig, 'config')


def get_stored():
  """Returns the PermissionsConfig entity in datastore as a
  permissions.DB.

  Raises: PermissionsConfigMissingError if
    * there is no PermissionsConfig entity in datastore; or
    * the config value is not a config_pb2.PermissionsConfig.
  """
  e = config_key().get()
  if not e or not isinstance(e.config, config_pb2.PermissionsConfig):
    raise MissingError('No valid PermissionsConfig entity in datastore')
  return e


def get_stored_as_db():
  """Returns the PermissionsConfig entity in datastore as a
  permissions.DB.

  Raises: PermissionsConfigMissingError if
    * there is no PermissionsConfig entity in datastore; or
    * the config value is not a config_pb2.PermissionsConfig.
  """
  stored = get_stored()
  return to_db(revision=stored.revision, config=stored.config)


def to_db(revision, config):
  """Helper to construct a permissions.DB from the given revision string
  and config_pb2.PermissionsConfig.
  """
  all_permissions = {}
  all_roles = {}
  for role in config.role:
    role_perms = set()

    # Process all direct permissions for this role.
    for p in role.permissions:
      all_permissions[p.name] = p
      role_perms.add(p.name)

    # Process all indirect permissions inherited from included roles.
    for included_role_name in role.includes:
      included_role_perms = all_roles.get(included_role_name, tuple())
      role_perms.update(included_role_perms)

    # Record permissions (direct & indirect) for this role, sorted.
    all_roles[role.name] = tuple(sorted(role_perms))

  # Bindings implicitly added into the root realm of every project.
  #
  # NOTE: This was copied from realms.permissions.py - implicit root bindings
  # are not part of PermissionsConfig proto.
  implicit_root_bindings = lambda project_id: [
      realms_config_pb2.Binding(
          role='role/luci.internal.system',
          principals=['project:' + project_id],
      ),
      realms_config_pb2.Binding(
          role='role/luci.internal.buildbucket.reader',
          principals=['group:buildbucket-internal-readers'],
      ),
      realms_config_pb2.Binding(
          role='role/luci.internal.resultdb.reader',
          principals=['group:resultdb-internal-readers'],
      ),
      realms_config_pb2.Binding(
          role='role/luci.internal.resultdb.invocationSubmittedSetter',
          principals=['group:resultdb-internal-invocation-submitters'],
      ),
  ]

  return realms.permissions.DB(
      revision="{}:{}".format(FILENAME, revision),
      permissions=all_permissions,
      roles={
          name: realms.permissions.Role(name=name, permissions=perms)
          for name, perms in all_roles.items()
      },
      attributes=set(attr for attr in config.attribute),
      implicit_root_bindings=implicit_root_bindings,
  )
