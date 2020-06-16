# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging

from components import auth

from proto.config import realms_pb2
from server import config
from server import task_scheduler

_TRACKING_BUG = 'crbug.com/1066839'


def get_permission(enum_permission):
  """ Generates Realm permission instance from enum value.

  e.g. realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK
       -> 'swarming.pools.createTask'

  Args:
    enum_permission: realms_pb2.RealmPermission enum value.

  Returns:
    realm_permission: an instance of auth.Permission.
  """
  enum_name = realms_pb2.RealmPermission.Name(enum_permission)
  words = enum_name.replace('REALM_PERMISSION_', '').split('_')
  # convert first word to subject e.g. pools, tasks
  subject = words[0].lower()
  # convert following words to verb e.g. createTask, listBots
  verb = words[1].lower() + ''.join(map(lambda x: x.capitalize(), words[2:]))
  return auth.Permission('swarming.%s.%s' % (subject, verb))


def is_enforced_permission(perm, pool_cfg=None):
  """ Checks if the Realm permission is enforced.

  Checks if the permission is specified in `enforced_realm_permissions`
  in settings.cfg or pools.cfg for the pool.

  Args:
    perm: realms_pb2.RealmPermission enum value.
    pool_cfg: PoolConfig of the pool

  Returns:
    bool: True if it's enforced, False if it's legacy-compatible.
  """
  if pool_cfg and perm in pool_cfg.enforced_realm_permissions:
    return True
  return perm in config.settings().auth.enforced_realm_permissions


def check_pools_create_task(pool, pool_cfg):
  """Checks if the caller can create the task in the pool.

  Realm permission `swarming.pools.createTask` will be checked,
  using auth.has_permission() or auth.has_permission_dryrun().

  If the realm permission check is enforced,
    It just calls auth.has_permission()

  If it's legacy-compatible,
    It calls the legacy task_scheduler.check_schedule_request_acl_caller() and
    compare the legacy result with the realm permission check using the dryrun.

  Args:
    pool: Pool in which the caller is scheduling a new task.
    pool_cfg: PoolCfg of the pool.

  Returns:
    None

  Raises:
    auth.AuthorizationError: if the caller is not allowed to schedule the task
                             in the pool.
  """
  # 'swarming.pools.createTask'
  perm = get_permission(realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK)

  if is_enforced_permission(realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK,
                            pool_cfg):
    _check_permission(perm, [pool_cfg.realm])
    return

  # legacy-compatible path

  # pool.realm is optional.
  if not pool_cfg.realm:
    logging.warning('%s: realm is missing in Pool "%s"', _TRACKING_BUG, pool)

  legacy_allowed = True
  try:
    task_scheduler.check_schedule_request_acl_caller(pool, pool_cfg)
  except auth.AuthorizationError:
    legacy_allowed = False
    raise  # re-raise the exception
  finally:
    # compare the legacy check result with realm check result if the pool realm
    # is specified.
    if pool_cfg.realm:
      auth.has_permission_dryrun(
          perm, [pool_cfg.realm], legacy_allowed, tracking_bug=_TRACKING_BUG)


def check_tasks_create_in_realm(realm, pool_cfg):
  """Checks if the caller is allowed to create a task in the realm.

  Args:
    realm: Realm that a task will be created in.
    pool_cfg: PoolConfig of the pool where the task will run.

  Returns:
    None

  Raises:
    auth.AuthorizationError: if the caller is not allowed.
  """
  # 'swarming.tasks.createInRealm'
  perm_enum = realms_pb2.REALM_PERMISSION_TASKS_CREATE_IN_REALM
  perm = get_permission(perm_enum)

  if realm or is_enforced_permission(perm_enum, pool_cfg):
    _check_permission(perm, [realm])
    return

  if pool_cfg.dry_run_task_realm:
    # There is no existing permission that corresponds to the realm
    # permission. So always pass expected_result=True to the dryrun.
    auth.has_permission_dryrun(
        perm, [pool_cfg.dry_run_task_realm],
        expected_result=True,
        tracking_bug=_TRACKING_BUG)


def check_tasks_run_as(task_request, pool_cfg):
  """Checks if the task service account is allowed to run in the task realm.

  Realm permission `swarming.tasks.runAs` will be checked,
  using auth.has_permission() or auth.has_permission_dryrun().

  If the realm permission check is enforced,
    It just calls auth.has_permission()

  If it's legacy-compatible,
    It calls task_scheduler.check_schedule_request_acl_service_account()
    and compare the legacy result with the realm permission check using
    the dryrun.

  Args:
    task_request: TaskRequest entity to be scheduled.
    pool_cfg: PoolConfig of the pool where the task will run.

  Returns:
    None

  Raises:
    auth.AuthorizationError: if the service account is not allowed to run
                             in the task realm.
  """
  perm_enum = realms_pb2.REALM_PERMISSION_TASKS_RUN_AS
  perm = get_permission(perm_enum)
  identity = auth.Identity(auth.IDENTITY_USER, task_request.service_account)

  # Enforce if the requested task has realm or it's configured in pools.cfg or
  # in settings.cfg globally.
  if task_request.realm or is_enforced_permission(perm_enum, pool_cfg):
    _check_permission(perm, [task_request.realm], identity)
    return

  # legacy-compatible path

  legacy_allowed = True

  try:
    # ACL check
    task_scheduler.check_schedule_request_acl_service_account(
        task_request, pool_cfg)
  except auth.AuthorizationError:
    legacy_allowed = False
    raise  # re-raise the exception
  finally:
    if pool_cfg.dry_run_task_realm:
      auth.has_permission_dryrun(
          perm, [pool_cfg.dry_run_task_realm],
          legacy_allowed,
          identity=identity,
          tracking_bug='crbug.com/1066839')


# Private section


def _check_permission(perm, realms, identity=None):
  """Checks if the caller has the realm permission.

  Args:
    perm: An instance of auth.Permission.
    realms: List of realms.
    identity: An instance of auth.Identity to check permission.
              default is auth.get_current_identity().

  Returns:
    None

  Raises:
    auth.AuthorizationError: if the caller is not allowed or realm is missing.
  """

  # Remove None from list
  realms = [r for r in realms if r]

  if not identity:
    identity = auth.get_current_identity()

  if not realms:
    raise auth.AuthorizationError('Realm is missing')

  if not auth.has_permission(perm, realms, identity=identity):
    logging.warning(
        '[realms] %s "%s" does not have permission "%s" in any realms %s',
        identity.kind, identity.name, perm.name, realms)
    raise auth.AuthorizationError('%s "%s" does not have permission "%s"' %
                                  (identity.kind, identity.name, perm.name))
  logging.info('[realms] %s "%s" has permission "%s" in any realms %s',
               identity.kind, identity.name, perm.name, realms)
