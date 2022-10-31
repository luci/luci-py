# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Database of defined permissions and roles."""

import collections

from components import utils
from components.auth.proto import realms_pb2

from proto import realms_config_pb2

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
  attribute = builder.attribute

  # LUCI Token Server permissions and roles (crbug.com/1082960).
  role('role/luci.realmServiceAccount', [
      permission('luci.serviceAccounts.existInRealm'),
  ])
  role('role/luci.serviceAccountTokenCreator', [
      permission('luci.serviceAccounts.mintToken'),
  ])

  # LUCI Config permissions and roles (crbug.com/1068817).
  role('role/configs.reader', [
      permission('configs.configSets.read'),
  ])
  role('role/configs.validator', [
      include('role/configs.reader'),
      permission('configs.configSets.validate'),
  ])
  role('role/configs.developer', [
      include('role/configs.validator'),
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

  # LUCI Scheduler attributes.
  attribute('scheduler.job.name')

  # Swarming permissions and roles (crbug.com/1066839).
  # See swarming/proto/config/realms.proto for more details.
  role('role/swarming.taskServiceAccount', [
      include('role/luci.realmServiceAccount'),
      permission('swarming.tasks.actAs'),
  ])
  role('role/swarming.taskViewer', [
      permission('swarming.tasks.get'),
  ])
  role('role/swarming.taskTriggerer', [
      include('role/swarming.taskViewer'),
      permission('swarming.tasks.createInRealm'),
      permission('swarming.tasks.cancel'),
  ])
  role('role/swarming.poolViewer', [
      permission('swarming.pools.listBots'),
      permission('swarming.pools.listTasks'),
  ])
  role('role/swarming.poolUser', [
      permission('swarming.pools.createTask'),
  ])
  role('role/swarming.poolOwner', [
      include('role/swarming.poolViewer'),
      permission('swarming.pools.cancelTask'),
      permission('swarming.pools.createBot'),
      permission('swarming.pools.deleteBot'),
      permission('swarming.pools.terminateBot'),
  ])

  # LogDog permissions and roles.
  role('role/logdog.reader', [
      permission('logdog.logs.get'),
      permission('logdog.logs.list'),
  ])
  role('role/logdog.writer', [
      permission('logdog.logs.create'),
  ])

  # ResultDB permissions and roles (crbug.com/1013316).
  role('role/resultdb.invocationCreator', [
      permission('resultdb.invocations.create'),
      permission('resultdb.invocations.update'),
  ])
  role('role/resultdb.reader', [
      permission('resultdb.invocations.list'),
      permission('resultdb.invocations.get'),
      permission('resultdb.invocations.include'),
      permission('resultdb.testResults.list'),
      permission('resultdb.testResults.get'),
      permission('resultdb.artifacts.list'),
      permission('resultdb.artifacts.get'),
      permission('resultdb.testExonerations.list'),
      permission('resultdb.testExonerations.get'),
  ])

  # Weetbix permissions and roles (b/239768873).
  # Weetbix checks all permissions against the <project>:@root
  # realm.
  # TODO(b/243488110): Delete roles when Weetbix renaming to LUCI
  # analysis complete.
  role(
      'role/weetbix.limitedReader',
      [
          # Allows viewing project configuration, and listing all rules
          # and clusters in the project (with associated bugs and
          # aggregated impact for each), except:
          # - For rule-based clusters, access to the rule definition
          #   ('reason where '%some failure%') is not granted.
          #   The user can however see the failures they already have
          #   access to (in ResultDB) which match the definition.
          # - For suggested clusters, cluster definition is not visible
          #   unless they user has access at least one test failure in
          #   the cluster.
          permission('weetbix.config.get'),
          permission('weetbix.rules.get'),
          permission('weetbix.rules.list'),
          permission('weetbix.clusters.get'),
          permission('weetbix.clusters.list'),
          permission('weetbix.clusters.getByFailure'),
      ])
  role(
      'role/weetbix.reader',
      [
          # Access to the definition of a failure association rule
          # e.g. ('reason where '%some failure%'). This could allow
          # the user to see arbitrary failure reasons or test IDs from
          # the project.
          include('role/weetbix.limitedReader'),
          permission('weetbix.rules.getDefinition'),
      ])
  role(
      'role/weetbix.editor',
      [
          # Ability to update failure association rules.
          include('role/weetbix.reader'),
          permission('weetbix.rules.create'),
          permission('weetbix.rules.update'),
      ])
  role(
      'role/weetbix.queryUser',
      [
          # Grants the ability to run queries that hit BigQuery.
          # Often this role is granted in conjunction with
          # another role (e.g. reader or editor).
          include('role/weetbix.limitedReader'),
          permission('weetbix.clusters.expensiveQueries'),
      ])

  # LUCI Analysis permissions and roles (b/239768873).
  # LUCI Analysis checks all permissions against the <project>:@root
  # realm.
  role(
      'role/analysis.limitedReader',
      [
          # Allows viewing project configuration, and listing all rules
          # and clusters in the project (with associated bugs and
          # aggregated impact for each), except:
          # - For rule-based clusters, access to the rule definition
          #   ('reason where '%some failure%') is not granted.
          #   The user can however see the failures they already have
          #   access to (in ResultDB) which match the definition.
          # - For suggested clusters, cluster definition is not visible
          #   unless they user has access at least one test failure in
          #   the cluster.
          permission('analysis.config.get'),
          permission('analysis.rules.get'),
          permission('analysis.rules.list'),
          permission('analysis.clusters.get'),
          permission('analysis.clusters.list'),
          permission('analysis.clusters.getByFailure'),
      ])
  role(
      'role/analysis.reader',
      [
          # Access to the definition of a failure association rule
          # e.g. ('reason where '%some failure%'). This could allow
          # the user to see arbitrary failure reasons or test IDs from
          # the project.
          include('role/analysis.limitedReader'),
          permission('analysis.rules.getDefinition'),
      ])
  role(
      'role/analysis.editor',
      [
          # Ability to update failure association rules.
          include('role/analysis.reader'),
          permission('analysis.rules.create'),
          permission('analysis.rules.update'),
      ])
  role(
      'role/analysis.queryUser',
      [
          # Grants the ability to run queries that hit BigQuery.
          # Often this role is granted in conjunction with
          # another role (e.g. reader or editor).
          include('role/analysis.limitedReader'),
          permission('analysis.clusters.expensiveQueries'),
      ])

  # Buildbucket permissions and roles (crbug.com/1091604).
  role(
      'role/buildbucket.limitedReader',
      [
          # Grants limited read access to builds.
          # Designed to allow listing builds and viewing a restricted set of
          # build and test metadata (e.g. name, status).
          # Explicitly does *not* grant access to any kind of logs.
          # May grant access to failure summaries that have been sanitized to
          # remove sensitive data.
          # This role is designed to allow users to see build and test failures
          # in Gerrit even if they don't have full read permission on the builds
          # themselves.
          permission('buildbucket.builds.list'),
          permission('buildbucket.builds.getLimited'),
          permission('buildbucket.builders.get'),
          permission('buildbucket.builders.list'),
      ])
  role('role/buildbucket.reader', [
      include('role/resultdb.reader'),  # to read results of the build
      include('role/logdog.reader'),    # to read build logs
      permission('buildbucket.builds.get'),
      permission('buildbucket.builds.list'),
      permission('buildbucket.builders.get'),
      permission('buildbucket.builders.list'),
      permission('buildbucket.buckets.get'),  # used by v1 API only
  ])
  role(
      'role/buildbucket.creator',
      [
          include('role/buildbucket.reader'),
          # Permission to call buildbucket's CreateBuild RPC with a provided
          # Build proto.
          permission('buildbucket.builds.create'),
          permission('buildbucket.builds.cancel'),
      ])
  role(
      'role/buildbucket.triggerer',
      [
          include('role/buildbucket.reader'),
          # Permission to call buildbucket's ScheduleBuild RPC.
          permission('buildbucket.builds.add'),
          permission('buildbucket.builds.cancel'),
      ])
  role('role/buildbucket.owner', [
      include('role/buildbucket.reader'),
      include('role/buildbucket.triggerer'),
      permission('buildbucket.builds.lease'),  # used by v1 API only
      permission('buildbucket.builds.reset'),  # used by v1 API only
      permission('buildbucket.builders.setBuildNumber'),
      permission('buildbucket.buckets.deleteBuilds'),
      permission('buildbucket.buckets.pause'),  # used by v1 API only
  ])
  role('role/buildbucket.builderServiceAccount', [
      include('role/logdog.writer'),                # to create build logs
      include('role/resultdb.reader'),              # to include invocations
      include('role/swarming.taskServiceAccount'),  # to run on Swarming itself
      include('role/swarming.taskTriggerer'),       # to trigger isolated tests
      permission('buildbucket.builds.update'),      # to update build steps
  ])

  # CQ permissions and roles. Placeholders for now.
  role('role/cq.committer', [])
  role('role/cq.dryRunner', [])

  # Milo permissions and roles.
  role('role/milo.reader', [
      # Allows user to view the console. Milo will still verify access to the
      # individual builders referenced by the console with
      # 'buildbucket.builders.get'.
      permission('milo.consoles.get'),
  ])

  # This role is implicitly granted to identity "project:X" in all realms of
  # the project X (and only it!). See below. Identity "project:X" is used by
  # RPCs when one LUCI micro-service calls another in a context of some project.
  # Thus this role authorizes various internal RPCs between LUCI micro-services
  # when they are scoped to a single project.
  role('role/luci.internal.system', [
      # Allow Swarming to use realm accounts.
      include('role/luci.serviceAccountTokenCreator'),
      # Allow Buildbucket to trigger Swarming tasks and use project's pools.
      include('role/swarming.taskTriggerer'),
      include('role/swarming.poolUser'),
      # Allow Scheduler and CQ to trigger Buildbucket builds.
      include('role/buildbucket.triggerer'),
      # Allow Buildbucket and Swarming to create new invocations.
      include('role/resultdb.invocationCreator'),
      # Allow trusted services to create invocations with custom IDs, e.g.
      # `build:8878494550606210560`.
      permission('resultdb.invocations.createWithReservedID', internal=True),
      # Allow trusted services to populate reserved fields in new
      # invocations.
      permission('resultdb.invocations.setProducerResource', internal=True),
      permission('resultdb.invocations.exportToBigQuery', internal=True),
  ])

  # Allows to see the list of builders and read builds in a LUCI project.
  # Granted to Milo's own account in all LUCI projects to allow it to fetch
  # (and cache) global builder list with a single RPC (instead of doing N
  # per-project RPCs) and to grab any build by its numeric ID without knowing
  # in advance what project it belongs to.
  role('role/luci.internal.buildbucket.reader', [
      permission('buildbucket.builders.get'),
      permission('buildbucket.builders.list'),
      permission('buildbucket.builds.get'),
  ])

  # UFS permissions and roles.
  # Read permission for Registration resources in UFS
  role('role/ufs.registration.reader', [
      permission('ufs.registrations.get'),
      permission('ufs.registrations.list'),
  ])
  # Write permission for Registration resources in UFS
  role('role/ufs.registration.writer', [
      permission('ufs.registrations.create'),
      permission('ufs.registrations.update'),
      permission('ufs.registrations.delete'),
  ])
  # Read permission for Inventory resources in UFS
  role('role/ufs.inventory.reader', [
      permission('ufs.inventories.get'),
      permission('ufs.inventories.list'),
  ])
  # Write permission for Inventory resources in UFS
  role('role/ufs.inventory.writer', [
      permission('ufs.inventories.create'),
      permission('ufs.inventories.update'),
      permission('ufs.inventories.delete'),
  ])
  # Reserve permission for Inventory resources in UFS
  role('role/ufs.inventory.reserver', [
      permission('ufs.inventories.reserve'),
  ])
  # Read permission for Configuration resources in UFS
  role('role/ufs.configuration.reader', [
      permission('ufs.configurations.get'),
      permission('ufs.configurations.list'),
  ])
  # Write permission for Configuration resources in UFS
  role('role/ufs.configuration.writer', [
      permission('ufs.configurations.create'),
      permission('ufs.configurations.update'),
      permission('ufs.configurations.delete'),
  ])
  # Read permission for Network resources in UFS
  role('role/ufs.network.reader', [
      permission('ufs.networks.get'),
      permission('ufs.networks.list'),
  ])
  # Write permission for Network resources in UFS
  role('role/ufs.network.writer', [
      permission('ufs.networks.create'),
      permission('ufs.networks.update'),
      permission('ufs.networks.delete'),
  ])
  # Read permission for State resources in UFS
  role('role/ufs.state.reader', [
      permission('ufs.states.get'),
      permission('ufs.states.list'),
  ])
  # Write permission for State resources in UFS
  role('role/ufs.state.writer', [
      permission('ufs.states.create'),
      permission('ufs.states.update'),
      permission('ufs.states.delete'),
  ])
  # Admin permission for Registration resources in UFS
  role(
      'role/ufs.registration.admin',
      [
          # Allow users to read Registration resources
          include('role/ufs.registration.reader'),
          # Allow users to write Registration resources
          include('role/ufs.registration.writer'),
      ])
  # Admin permission for Inventory resources in UFS
  role(
      'role/ufs.inventory.admin',
      [
          # Allow users to read Inventory resources
          include('role/ufs.inventory.reader'),
          # Allow users to write Inventory resources
          include('role/ufs.inventory.writer'),
          # Allow users to reserve Inventory resources
          include('role/ufs.inventory.reserver'),
      ])
  # Admin permission for Configuration resources in UFS
  role(
      'role/ufs.configuration.admin',
      [
          # Allow users to read Configuration resources
          include('role/ufs.configuration.reader'),
          # Allow users to write Configuration resources
          include('role/ufs.configuration.writer'),
      ])
  # Admin permission for Network resources in UFS
  role(
      'role/ufs.network.admin',
      [
          # Allow users to read Network resources
          include('role/ufs.network.reader'),
          # Allow users to write Network resources
          include('role/ufs.network.writer'),
      ])
  # Admin permission for State records in UFS
  role(
      'role/ufs.state.admin',
      [
          # Allow users to read State resources
          include('role/ufs.state.reader'),
          # Allow users to write State resources
          include('role/ufs.state.writer'),
      ])
  # Admin permission for UFS
  role(
      'role/ufs.admin',
      [
          # Allow users to read/write Registration resources
          include('role/ufs.registration.admin'),
          # Allow users to read/write Inventory resources
          include('role/ufs.inventory.admin'),
          # Allow users to read/write Configuration resources
          include('role/ufs.configuration.admin'),
          # Allow users to read/write Network resources
          include('role/ufs.network.admin'),
          # Allow users to read/write State resources
          include('role/ufs.state.admin'),
          # Allow users to import resources
          permission('ufs.resources.import'),
      ])

  # Bindings implicitly added into the root realm of every project.
  builder.implicit_root_bindings = lambda project_id: [
      realms_config_pb2.Binding(
          role='role/luci.internal.system',
          principals=['project:'+project_id],
      ),
      realms_config_pb2.Binding(
          role='role/luci.internal.buildbucket.reader',
          principals=['group:buildbucket-internal-readers'],
      ),
  ]

  return builder.finish()


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
