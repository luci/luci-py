# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Functions to fetch and parse pools.cfg file with list of pools.

See comments in proto/pools.proto for more context. Structures defined here are
used primarily by task_scheduler.check_schedule_request_acl_caller.
"""

import collections
import logging
import random
import re

from components import auth
from components import config
from components import cipd
from components import utils
from components.config import validation

from proto.config import pools_pb2
from server import config as local_config
from server import directory_occlusion


POOLS_CFG_FILENAME = 'pools.cfg'

NAMESPACE_RE = re.compile(r'^[a-z0-9A-Z\-._]+$')


# Validated read-only representation of one pool.
PoolConfig = collections.namedtuple(
    'PoolConfig',
    [
        # Name of the pool.
        'name',
        # Revision of pools.cfg file this config came from.
        'rev',
        # Set of auth.Identity that can schedule jobs in the pool.
        'scheduling_users',
        # Set of group names with users that can schedule jobs in the pool.
        'scheduling_groups',
        # Map {auth.Identity of a delegatee => TrustedDelegatee tuple}.
        'trusted_delegatees',
        # Pool realm.
        'realm',
        # Set of enforced realm permission enums.
        'enforced_realm_permissions',
        # Realm to assign to tasks if they don't have any.
        'default_task_realm',
        # resolved TaskTemplateDeployment (optional).
        'task_template_deployment',
        # resolved BotMonitoring.
        'bot_monitoring',
        # Tuple of ExternalSchedulerConfigs for this pool, if defined (or None).
        'external_schedulers',
        # resolved default CipdServer
        'default_cipd',
        # Controls RBE migration parameters, pools_pb2.Pool.RBEMigration.
        'rbe_migration',
    ])


def init_pool_config(**kwargs):
  """Initializees PoolConfig with given arguments."""
  args = {
      'name': None,
      'rev': None,
      'scheduling_users': frozenset(),
      'scheduling_groups': frozenset(),
      'trusted_delegatees': {},
      'realm': None,
      'enforced_realm_permissions': frozenset(),
      'default_task_realm': None,
      'task_template_deployment': None,
      'bot_monitoring': None,
      'external_schedulers': None,
      'default_cipd': None,
      'rbe_migration': None,
  }
  args.update(kwargs)
  return PoolConfig(**args)


CipdServer = collections.namedtuple('CipdServer', [
    'server',
    'package_name',
    'client_version',
])

# Validated read-only fields of one trusted delegation scenario.
TrustedDelegatee = collections.namedtuple(
    'TrustedDelegatee',
    [
        # auth.Identity of the delegatee
        # (the one who's minting the delegation token).
        'peer_id',
        # A set of tags to look for in the delegation token to allow
        # the delegation.
        'required_delegation_tags',
    ])

# Read-only hashable representation of a single ExternalSchedulerConfig
ExternalSchedulerConfig = collections.namedtuple(
    'ExternalScheduler',
    [
        # Service address.
        'address',
        # Scheduler ID (opaque to swarming).
        'id',
        # Dimension set in ['key1:value1', 'key2:value2'] format.
        #
        # To-be-deprecated.
        #
        # Only 1 of this and 'all_dimensions' should be specified.
        'dimensions',
        # Bot or task should have all of these dimensions in order to be
        # eligible for scheduler.
        #
        # Dimensions should be in 'key1:value1' format.
        #
        # Only 1 of this and 'dimensions' should be specified.
        'all_dimensions',
        # If non-empty, bot or task should have any of these dimensions
        # in order to be eligible for scheduler.
        'any_dimensions',
        # Whether this config is enabled.
        'enabled',
        # Whether to allow fall back to other es-owned tasks if external
        # scheduler has no tasks for a bot.
        'allow_es_fallback',
    ])

# Describes how task templates apply to a pool.
_TaskTemplateDeployment = collections.namedtuple(
    '_TaskTemplateDeployment',
    [
        # The TaskTemplate for prod builds (optional).
        'prod',
        # The TaskTemplate for canary builds (optional).
        'canary',
        # The chance (int [0, 9999]) of the time that the canary template should
        # be selected. Must be 0 if no canary is specified.
        #
        # NOTE: some tests set this to >9999 in order to force canary selection
        # without mocking randomint; in the live server
        # TaskTemplateDeployment.from_pb prevents this from occurring.
        'canary_chance',
    ])


class TaskTemplateDeployment(_TaskTemplateDeployment):

  @classmethod
  def from_pb(cls, ctx, d, template_map):
    """This returns a TaskTemplateDeployment from `d` and `template_map`.

    Args:
      ctx (validation.Context) - The validation context.
      d (pools_pb2.TaskTemplateDeployment) - The proto TaskTemplateDeployment
        message to convert.
      template_map (dict[str,TaskTemplate]) - This map should map from any
        potential include statements to a resolved TaskTemplate instance.

    Returns a TaskTemplateDeployment object (this class), None if there were
    errors.
    """
    assert isinstance(ctx, validation.Context)
    assert isinstance(d, pools_pb2.TaskTemplateDeployment)
    assert isinstance(template_map, dict)

    prod = None
    if d.HasField('prod'):
      prod = TaskTemplate.from_pb(ctx, d.prod, template_map)

    canary = None
    if d.HasField('canary'):
      canary = TaskTemplate.from_pb(ctx, d.canary, template_map)

    if not (0 <= d.canary_chance <= 9999):
      ctx.error('canary_chance out of range `[0,9999]`: %d -> %%%.2f',
                d.canary_chance, d.canary_chance / 100.)
    elif d.canary_chance and not canary:
      ctx.error('canary_chance specified without a canary')

    return None if ctx.result().has_errors else cls(
        prod=prod,
        canary=canary,
        canary_chance=d.canary_chance,
    )


# A set of default task parameters to apply to tasks issued within a pool.
_TaskTemplate = collections.namedtuple(
    '_TaskTemplate',
    [
        # sequence of CacheEntry.
        'cache',
        # sequence of CipdPackage.
        'cipd_package',
        # sequence of Env.
        'env',

        # An internal frozenset<str> of the transitive inclusions that went into
        # the creation of this _TaskTemplate. Users outside of this file should
        # ignore this field.
        'inclusions',
    ])


def _singleton(name):
  """Returns a singleton object which represents itself as `name` when printed,
  but is only comparable (via identity) to itself."""
  return type(name, (), {'__repr__': lambda self: name})()


class TaskTemplate(_TaskTemplate):
  CYCLE = _singleton('CYCLE')

  class _Intermediate(object):
    """_Intermediate represents an in-flux TaskTemplate instance.

    This is used internally by the .from_pb method to build up a finalized
    TaskTemplate instance.
    """
    def __init__(self, ctx, t):
      assert isinstance(t, pools_pb2.TaskTemplate)

      self.cache = {}
      for i, ce in enumerate(t.cache):
        name, path = ce.name, ce.path
        with ctx.prefix('cache[%r]: ', name if name else i):
          if not name:
            ctx.error('empty name')
          if not path:
            ctx.error('empty path')
        self.cache[name] = path

      self.cipd_package = {}
      for i, cp in enumerate(t.cipd_package):
        path, pkg, version = cp.path, cp.pkg, cp.version
        # We do this little dance to avoid having u'str' show up in the error
        # messages.
        fmt, args = 'cipd_package[%r]: ', (i,)
        if path and pkg:
          fmt, args = 'cipd_package[(%r, %r)]: ', (path, pkg)
        with ctx.prefix(fmt, *args):
          if not pkg:
            ctx.error('empty pkg')
          if not version:
            ctx.error('empty version')
        self.cipd_package[(path, pkg)] = version

      self.env = {}
      for i, env in enumerate(t.env):
        var, value, prefix, soft = env.var, env.value, env.prefix, env.soft
        with ctx.prefix('env[%r]: ', var if var else i):
          if not var:
            ctx.error('empty var')
          if not value and not prefix:
            ctx.error('empty value AND prefix')
        self.env[var] = (value, tuple(prefix), soft)

      # We don't need to initialize this here, update() will adjust this as it
      # processes includes.
      self.inclusions = set()

    def update(self, ctx, other, include_name):
      """Updates this _Intermediate with the other (TaskTemplate)

      If this update is due to an inclusion in other, set include_name (str) to
      that inclusion name; the name will be added to the `inclusions` field, and
      an error check will prevent the same item from being included more than
      once. Otherwise set this to None.
      """
      assert isinstance(other, TaskTemplate)

      if include_name:
        if include_name in self.inclusions:
          ctx.error(
              'template %r included multiple times',
              include_name)
        self.inclusions.add(include_name)

      for transitive_include in other.inclusions:
        if transitive_include in self.inclusions:
          ctx.error('template %r included (transitively) multiple times',
                    transitive_include)
        self.inclusions.add(transitive_include)

      for entry in other.cache:
        self.cache[entry.name] = entry.path

      for entry in other.cipd_package:
        self.cipd_package[(entry.path, entry.pkg)] = entry.version

      for entry in other.env:
        val, pfx = '', ()
        if entry.var in self.env:
          val, pfx, _ = self.env[entry.var]
        self.env[entry.var] = (entry.value or val, (pfx + entry.prefix),
                               entry.soft)

    def finalize(self, ctx):
      doc = directory_occlusion.Checker()
      for (path, pkg), version in self.cipd_package.items():
        # all cipd packages are considered compatible in terms of paths: it's
        # totally legit to install many packages in the same directory. Thus we
        # set the owner for all cipd packages to 'cipd'.
        doc.add(path, 'cipd', '%s:%s' % (pkg, version))

      for name, path in self.cache.items():
        # caches are all unique; they can't overlap. Thus, we give each of them
        # a unique (via the cache name) owner.
        doc.add(path, 'cache %r' % name, '')

      if doc.conflicts(ctx):
        return

      return TaskTemplate(
          cache=tuple(
              CacheEntry(name, path)
              for name, path in sorted(self.cache.items())),
          cipd_package=tuple(
              CipdPackage(path, pkg, version)
              for (path, pkg), version in sorted(self.cipd_package.items())),
          env=tuple(
              Env(var, value, prefix, soft)
              for var, (value, prefix, soft) in sorted(self.env.items())),
          inclusions=frozenset(self.inclusions),
      )

  @classmethod
  def from_pb(cls, ctx, t, resolve_func=lambda _: None):
    """This returns a TaskTemplate from `t` and `resolve_func`.

    Args:
      ctx (validation.Context) - The validation context.
      t (pools_pb2.TaskTemplate) - The proto TaskTemplate message to convert.
      resolve_func (func(include_name) -> TaskTemplate) - A function which,
        given `include_name` returns the resolved TaskTemplate object.
          If the resolved TaskTemplate has errors, it should return None.
          If an include cycle was detected, it should return TaskTemplate.CYCLE.
          If the include_name is not resolvable, it should raise KeyError.
        As a convenience, resolve_func may also be a dict, and an appropriate
        resolution function will be generated for it (dict.__getitem__).

    Returns a TaskTemplate object (this class), None if there were errors. If
    any of the included object results in a CYCLE, this returns CYCLE.
    """
    assert isinstance(t, pools_pb2.TaskTemplate)
    if isinstance(resolve_func, dict):
      resolve_func = resolve_func.__getitem__

    tmp = cls._Intermediate(ctx, pools_pb2.TaskTemplate())

    found_cycle = False
    for include in t.include:
      try:
        resolved = resolve_func(include)
        if isinstance(resolved, TaskTemplate):
          tmp.update(ctx, resolved, include)
        elif resolved is None:
          ctx.error('depends on %r, which has errors', include)
        elif resolved is cls.CYCLE:
          found_cycle = True
          ctx.error('depends on %r, which causes an import cycle', include)
        else:
          assert False, (
              'resolve_func returned a bad type: %s: %r' % (
                  type(resolved), resolved))
      except KeyError:
        ctx.error('unknown include: %r', include)

    tmp.update(ctx, cls._Intermediate(ctx, t).finalize(ctx), None)

    # Evaluate this here so that ctx can contain all errors before returning.
    ret = tmp.finalize(ctx)

    if found_cycle:
      return cls.CYCLE
    return None if ctx.result().has_errors else ret


CacheEntry = collections.namedtuple('CacheEntry', ['name', 'path'])
CipdPackage = collections.namedtuple('CipdPackage', ['path', 'pkg', 'version'])
Env = collections.namedtuple('Env', ['var', 'value', 'prefix', 'soft'])


def get_pool_config(pool_name):
  """Returns PoolConfig for the given pool or None if not defined."""
  if pool_name is None:
    raise ValueError('get_pool_config called with None')
  return _fetch_pools_config().pools.get(pool_name)


def known():
  """Returns the list of all pool names."""
  return sorted(_fetch_pools_config().pools)


### Private stuff.


# Used only on dev server as an ultimate fallback to enable local_smoke_test to
# work.
_LOCAL_FAKE_CONFIG = None

# Parsed representation of pools.cfg ready for queries.
_PoolsCfg = collections.namedtuple('_PoolsCfg', [
  'pools',  # dict {pool name => PoolConfig tuple}

  'default_external_services', # (CipdServer)
])


def _resolve_task_template_inclusions(ctx, task_templates):
  """Resolves all task template inclusions in the provided
  pools_pb2.TaskTemplate list.

  Returns a new dictionary with {name -> TaskTemplate} namedtuples.
  """
  template_map = {t.name: t for t in task_templates}
  if '' in template_map:
    ctx.error('one or more templates has a blank name')
    return

  if len(template_map) != len(task_templates):
    ctx.error('one or more templates has a duplicate name')
    return

  resolved = {}  # name -> TaskTemplate | None | TaskTemplate.CYCLE
  def resolve(name):
    # Let this raise KeyError if not found
    template = template_map[name]

    if name not in resolved:
      # Set this entry to CYCLE in case we're asked to resolve ourselves.
      resolved[name] = TaskTemplate.CYCLE
      with ctx.prefix('template[%r]: ', name):
        resolved[name] = TaskTemplate.from_pb(ctx, template, resolve)
    return resolved[name]

  map(resolve, sorted(template_map))

  return resolved


def _resolve_task_template_deployments(
    ctx, template_map, task_template_deployments):
  ret = {}

  for i, deployment in enumerate(task_template_deployments):
    if not deployment.name:
      ctx.error('deployment[%d]: has no name', i)
      return
    with ctx.prefix('deployment[%r]: ', deployment.name):
      ret[deployment.name] = TaskTemplateDeployment.from_pb(
          ctx, deployment, template_map)

  return ret


def _resolve_deployment(
    ctx, pool_msg, template_map, deployment_map):
  deployment_scheme = pool_msg.WhichOneof('task_deployment_scheme')
  if deployment_scheme == 'task_template_deployment':
    if pool_msg.task_template_deployment not in deployment_map:
      ctx.error('unknown deployment: %r', pool_msg.task_template_deployment)
      return
    return deployment_map[pool_msg.task_template_deployment]

  if deployment_scheme == 'task_template_deployment_inline':
    return TaskTemplateDeployment.from_pb(
        ctx, pool_msg.task_template_deployment_inline, template_map)


def _resolve_bot_monitoring(ctx, bot_monitorings):
  """Validates and simplifies bot_monitoring entries in pools.cfg."""
  out = {}
  for m in bot_monitorings:
    with ctx.prefix('bot_monitoring %r: ', m.name):
      # Use the same rules for the name as for the dimensions for simplicity
      # here.
      if not local_config.validate_dimension_key(m.name):
        ctx.error('invalid name')
      if m.name in out:
        ctx.error('duplicate name')
      keys = set(m.dimension_key)
      if len(keys) != len(m.dimension_key):
        ctx.error('duplicate dimension_key')
      for k in keys:
        if not local_config.validate_dimension_key(k):
          ctx.error('invalid dimension_key %r', k)
      # pool is always implicit.
      keys.add('pool')
      out[m.name] = sorted(keys)
  return out


def _resolve_external_schedulers(external_schedulers):
  """Turns external_schedulers into a hashable representation."""
  return tuple(
      ExternalSchedulerConfig(
          e.address, e.id, frozenset(e.dimensions), frozenset(e.all_dimensions),
          frozenset(e.any_dimensions), e.enabled, e.allow_es_fallback)
      for e in external_schedulers)


def _to_ident(s):
  if ':' not in s:
    s = 'user:' + s
  return auth.Identity.from_bytes(s)


def _validate_ident(ctx, title, s):
  try:
    return _to_ident(s)
  except ValueError as exc:
    ctx.error('bad %s value "%s" - %s', title, s, exc)


def _validate_realm(ctx, title, s):
  try:
    auth.validate_realm_name(str(s))
  except ValueError as exc:
    ctx.error('bad %s value: %s', title, exc)


def _validate_url(ctx, value):
  if not value:
    ctx.error('is not set')
  elif not validation.is_valid_secure_url(value):
    ctx.error('must start with "https://" or "http://localhost"')


def _validate_external_services_cipd(ctx, cfg):
  """Validates ExternalServices.CIPD message."""
  with ctx.prefix('cipd '):
    with ctx.prefix('server '):
      _validate_url(ctx, cfg.server)

    with ctx.prefix('client_package '):
      if not cipd.is_valid_package_name_template(
          cfg.client_package.package_name):
        ctx.error('is invalid "%s"', cfg.client_package.package_name)

    with ctx.prefix('client_version '):
      if not cipd.is_valid_version(cfg.client_package.version):
        ctx.error('is invalid "%s"', cfg.client_package.version)


def _validate_external_services(ctx, cfg):
  """Validates an ExternalServices message"""
  _validate_external_services_cipd(ctx, cfg.cipd)


@utils.cache_with_expiration(60)
def _fetch_pools_config():
  """Loads pools.cfg and parses it into a _PoolsCfg instance."""
  # store_last_good=True tells config components to update the config file
  # in a cron job. Here we just read from the datastore. In case it's the first
  # call ever, or config doesn't exist, it returns (None, None).
  rev, cfg = config.get_self_config(
      POOLS_CFG_FILENAME, pools_pb2.PoolsCfg, store_last_good=True)
  if not cfg:
    if _LOCAL_FAKE_CONFIG:
      assert utils.is_local_dev_server()
      return _LOCAL_FAKE_CONFIG
    logging.error('There is no pools.cfg, no task is accepted')
    return _PoolsCfg({}, (None, None))

  # The config is already validated at this point.

  ctx = validation.Context.logging()
  template_map = _resolve_task_template_inclusions(ctx, cfg.task_template)
  deployment_map = _resolve_task_template_deployments(
      ctx, template_map, cfg.task_template_deployment)
  bot_monitorings = _resolve_bot_monitoring(ctx, cfg.bot_monitoring)

  default_cipd = None
  if cfg.HasField('default_external_services'):
    ext = cfg.default_external_services
    default_cipd = CipdServer(
        ext.cipd.server,
        ext.cipd.client_package.package_name,
        ext.cipd.client_package.version)

  pools = {}
  for msg in cfg.pool:
    for name in msg.name:
      pools[name] = init_pool_config(
          name=name,
          rev=rev,
          scheduling_users=frozenset(_to_ident(u) for u in msg.schedulers.user),
          scheduling_groups=frozenset(msg.schedulers.group),
          trusted_delegatees={
              _to_ident(d.peer_id): TrustedDelegatee(
                  peer_id=_to_ident(d.peer_id),
                  required_delegation_tags=frozenset(d.require_any_of.tag))
              for d in msg.schedulers.trusted_delegation
          },
          realm=msg.realm if msg.realm else None,
          default_task_realm=(msg.default_task_realm
                              if msg.default_task_realm else None),
          enforced_realm_permissions=frozenset(msg.enforced_realm_permissions),
          task_template_deployment=_resolve_deployment(ctx, msg, template_map,
                                                       deployment_map),
          bot_monitoring=bot_monitorings.get(name),
          external_schedulers=_resolve_external_schedulers(
              msg.external_schedulers),
          default_cipd=default_cipd,
          rbe_migration=(
              msg.rbe_migration if msg.HasField('rbe_migration') else None),
          )
  return _PoolsCfg(pools, (default_cipd))


@validation.self_rule(POOLS_CFG_FILENAME, pools_pb2.PoolsCfg)
def _validate_pools_cfg(cfg, ctx):
  """Validates pools.cfg file."""

  template_map = _resolve_task_template_inclusions(
      ctx, cfg.task_template)
  deployment_map = _resolve_task_template_deployments(
      ctx, template_map, cfg.task_template_deployment)
  bot_monitorings = _resolve_bot_monitoring(ctx, cfg.bot_monitoring)
  bot_monitoring_unreferred = set(bot_monitorings)

  # Currently optional
  if cfg.HasField("default_external_services"):
    _validate_external_services(ctx, cfg.default_external_services)

  pools = set()
  for i, msg in enumerate(cfg.pool):
    with ctx.prefix('pool #%d (%s): ', i, '|'.join(msg.name) or 'unnamed'):
      # Validate names.
      if not msg.name:
        ctx.error('at least one pool name must be given')
      for name in msg.name:
        if not local_config.validate_dimension_value(name):
          ctx.error('bad pool name "%s", not a valid dimension value', name)
        elif name in pools:
          ctx.error('pool "%s" was already declared', name)
        else:
          pools.add(name)

      # Validate realm names. They are optional for now.
      if msg.realm:
        _validate_realm(ctx, 'realm', msg.realm)
      if msg.default_task_realm:
        _validate_realm(ctx, 'default_task_realm', msg.default_task_realm)

      # TODO(crbug.com/1066839): Validate enforced_realm_permissions.
      # The following permissions must be enforced at the same time.
      # REALM_PERMISSION_TASKS_CREATE_IN_REALM
      # REALM_PERMISSION_POOLS_CREATE_TASK
      # REALM_PERMISSION_TASKS_ACT_AS

      # Validate schedulers.user.
      for u in msg.schedulers.user:
        _validate_ident(ctx, 'user', u)

      # Validate schedulers.group.
      for g in msg.schedulers.group:
        if not auth.is_valid_group_name(g):
          ctx.error('bad group name "%s"', g)

      # Validate schedulers.trusted_delegation.
      seen_peers = set()
      for d in msg.schedulers.trusted_delegation:
        with ctx.prefix('trusted_delegation #%d (%s): ', i, d.peer_id):
          if not d.peer_id:
            ctx.error('"peer_id" is required')
          else:
            peer_id = _validate_ident(ctx, 'peer_id', d.peer_id)
            if peer_id in seen_peers:
              ctx.error('peer "%s" was specified twice', d.peer_id)
            elif peer_id:
              seen_peers.add(peer_id)
          for j, tag in enumerate(d.require_any_of.tag):
            if ':' not in tag:
              ctx.error('bad tag #%d "%s" - must be <key>:<value>', j, tag)

      # Validate external schedulers.
      for j, es in enumerate(msg.external_schedulers):
        if not es.address:
          ctx.error('%sth external scheduler config had no address', j)

      _resolve_deployment(ctx, msg, template_map, deployment_map)

      if msg.bot_monitoring:
        if msg.bot_monitoring not in bot_monitorings:
          ctx.error('refer to missing bot_monitoring %r', msg.bot_monitoring)
        else:
          bot_monitoring_unreferred.discard(msg.bot_monitoring)
  if bot_monitoring_unreferred:
    ctx.error(
        'bot_monitoring not referred to: %s',
        ', '.join(sorted(bot_monitoring_unreferred)))


def bootstrap_dev_server_acls():
  """Adds default pools.cfg."""
  assert utils.is_local_dev_server()
  global _LOCAL_FAKE_CONFIG
  _LOCAL_FAKE_CONFIG = _PoolsCfg(
      {
          'default':
              init_pool_config(
                  name='default',
                  rev='pools_cfg_rev',
                  scheduling_users=frozenset([
                      auth.Identity(auth.IDENTITY_USER,
                                    'smoke-test@example.com'),
                      auth.Identity(auth.IDENTITY_BOT, 'whitelisted-ip'),
                  ]),
              ),
      },
      (None, None),
  )
