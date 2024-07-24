# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functions that do common acl checking/processing on internal ndb objects."""

import bisect
import datetime
import itertools
import logging
import re

from google.appengine.api import datastore_errors
from google.appengine.api import memcache

from components import auth

import handlers_exceptions
from server import acl
from server import config
from server import pools_config
from server import rbe
from server import realms
from server import service_accounts
from server import service_accounts_utils
from server import task_request


def process_task_request(tr, template_apply):
  # type: (task_request.TaskRequest, task_request.TemplateApplyEnum)
  #     -> None
  """Initializes the given TaskRequest and does acl checking.

  Raises:
    handlers_exceptions.BadRequestException: if the request is missing
        fields or cannot be accepted.
    handlers_exceptions.PermissionException: if permissions are insufficient
        for fetching oauth tokens.
    handlers_exceptions.InternalException: if something fails unexpectedly.
    auth.AuthorizationError: if a realms action is not allowed.
  """

  # Refuse tags reserved for internal use. They can only be set by Swarming
  # itself. These are tags that have any inherent magical meaning that some
  # other parts of Swarming rely on.
  for tag in itertools.chain(tr.manual_tags, tr.tags):
    if task_request.is_reserved_tag(tag):
      raise handlers_exceptions.BadRequestException(
          'Tag %s is reserved for internal use and can\'t be assigned manually'
          % tag)

  try:
    task_request.init_new_request(tr, acl.can_schedule_high_priority_tasks(),
                                  template_apply)

  except (datastore_errors.BadValueError, TypeError, ValueError) as e:
    logging.warning('Incorrect new task request', exc_info=True)
    raise handlers_exceptions.BadRequestException(e.message)

  # Retrieve pool_cfg, and check the existence.
  pool_cfg = pools_config.get_pool_config(tr.pool)
  if not pool_cfg:
    logging.warning('Pool "%s" is not in pools.cfg', tr.pool)
    raise handlers_exceptions.PermissionException(
        'No such pool or no permission to use it: %s' % tr.pool)

  # Use the scheduling algorithm configured for the pool.
  assert pool_cfg.scheduling_algorithm is not None
  tr.scheduling_algorithm = pool_cfg.scheduling_algorithm

  # Decide if the task should use the RBE Scheduler.
  tr.rbe_instance = rbe.get_rbe_instance_for_task(tr.tags, pool_cfg)
  if tr.rbe_instance:
    logging.info('RBE: scheduling through %s', tr.rbe_instance)

  # For tasks in pools that have the RBE config, add an extra tag useful to see
  # what tasks are *actually* scheduled through RBE. Note that init_new_request
  # sorted tags already.
  if pool_cfg.rbe_migration and pool_cfg.rbe_migration.rbe_instance:
    bisect.insort(tr.tags, u'rbe:%s' % (tr.rbe_instance or 'none'))

  # TODO(crbug.com/1109378): Check ACLs before calling init_new_request to
  # avoid leaking information about pool templates to unauthorized callers.

  # If the task request supplied a realm it means the task is in a realm-aware
  # mode and it wants *all* realm ACLs to be enforced. Otherwise assume
  # the task runs in pool_cfg.default_task_realm and enforce only permissions
  # specified in enforced_realm_permissions pool config (using legacy ACLs
  # for the rest). This should simplify the transition to realm ACLs.
  # Enforce realm acls.
  enforce_realms_acl = False
  if tr.realm:
    logging.info('Using task realm %r', tr.realm)
    enforce_realms_acl = True
  elif pool_cfg.default_task_realm:
    logging.info('Using default_task_realm %r', pool_cfg.default_task_realm)
    tr.realm = pool_cfg.default_task_realm
  else:
    logging.info('Not using realms')

  # Warn if the pool has realms configured, but the task is using old ACLs.
  if pool_cfg.realm and not tr.realm:
    logging.warning('crbug.com/1066839: %s: %r is not using realms', tr.pool,
                    tr.name)

  # Realm permission 'swarming.pools.createInRealm' checks if the
  # caller is allowed to create a task in the task realm.
  tr.realms_enabled = realms.check_tasks_create_in_realm(
      tr.realm, pool_cfg, enforce_realms_acl)
  if tr.resultdb and tr.resultdb.enable and not tr.realms_enabled:
    raise handlers_exceptions.BadRequestException(
        'ResultDB is enabled, but realm is not')

  # Realm permission 'swarming.pools.create' checks if the caller is allowed
  # to create a task in the pool.
  realms.check_pools_create_task(pool_cfg, enforce_realms_acl)

  # If the request has a service account email, check if the service account
  # is allowed to run.
  if service_accounts_utils.is_service_account(tr.service_account):
    if not service_accounts.has_token_server():
      raise handlers_exceptions.BadRequestException(
          'This Swarming server doesn\'t support task service accounts '
          'because Token Server URL is not configured')

    # Service accounts are supported only in realms mode.
    if not tr.realms_enabled:
      raise handlers_exceptions.BadRequestException(
          'Task service accounts are supported only if the task is associated '
          'with a realm')

    # Realm permission 'swarming.tasks.actAs' checks if the service account is
    # allowed to run in the task realm.
    realms.check_tasks_act_as(tr, pool_cfg, enforce_realms_acl)


def cache_request(namespace, request_uuid, func):
  # type: (str, str, Callable[[], Any]) -> Tuple[Any, bool]
  """Checks and returns the cached result of the identical request.

  If the cache doesn't exist, the result of the function will be cached
  and the boolean will be False.
  """
  request_idempotency_key = None
  if request_uuid:
    if not re.match(
        r'^[\da-fA-F]{8}-[\da-fA-F]{4}-[\da-fA-F]{4}-[\da-fA-F]{4}-'
        r'[\da-fA-F]{12}$', request_uuid):
      raise handlers_exceptions.BadRequestException(
          'invalid uuid is given as request_uuid')

    request_idempotency_key = 'request_id/%s/%s' % (
        request_uuid, auth.get_current_identity().to_bytes())

    result_cache = memcache.get(request_idempotency_key, namespace=namespace)
    if result_cache is not None:
      return result_cache, True

  result = func()
  if request_idempotency_key:
    memcache.add(
        request_idempotency_key, result, namespace=namespace, time=10 * 60)
  return result, False


def validate_backend_configs(configs, full_validation=False):
  # type: (Sequence[swarming_pb2.SwarmingTaskBackendConfig], Bool) ->
  #     Sequence[Tuple[int, str]]
  """Checks the validity of each config.

  This function will be called to validate the configs sent to ValidateConfig
  and the config sent by RunTask. The two require different validation.

  For the config provided in RunTask, the following fields are required:
    - agent_binary_cipd_filename
    - agent_binary_cipd_pkg
    - agent_binary_cipd_vers
    - bot_ping_tolerance
    - priority
    - tags

  When validating configs sent to ValidateConfig, some fields (like
  agent_binary_cipd fields) are not available since they are added to the config
  when the buildbucket build is created. Thus we only need to validate fields
  that are provided in the config, meaning that an empty config will return
  without errors.

    Args
      configs: The configs that need to be validated.
      full_validation: (optional) If set True, the above mentioned fields
        will be required to be validated.
    Return
      A tuple of (i, error_message) where i is the index of the
      config that has the error.
  """
  errors = []  # type: Sequence[Tuple[int, str]]
  for i, cfg in enumerate(configs):
    try:
      if cfg.priority or full_validation:
        task_request.validate_priority(cfg.priority)
    except (datastore_errors.BadValueError, TypeError) as e:
      errors.append((i, e.message))

    try:
      if cfg.bot_ping_tolerance or full_validation:
        task_request.validate_ping_tolerance('bot_ping_tolerance',
                                             cfg.bot_ping_tolerance)
    except datastore_errors.BadValueError as e:
      errors.append((i, e.message))

    if cfg.parent_run_id:  # Optional value.
      try:
        task_request.validate_task_run_id('parent_run_id', cfg.parent_run_id)
      except (ValueError, TypeError) as e:
        errors.append((i, e.message))

    if cfg.service_account:
      try:
        task_request.validate_service_account(
            'service_account', cfg.service_account)
      except datastore_errors.BadValueError as e:
        errors.append((i, e.message))

    try:
      if cfg.agent_binary_cipd_pkg or full_validation:
        task_request.validate_package_name_template('agent_binary_cipd_pkg',
                                                    cfg.agent_binary_cipd_pkg)
    except datastore_errors.BadValueError as e:
      errors.append((i, e.message))

    try:
      if cfg.agent_binary_cipd_vers or full_validation:
        task_request.validate_package_version('agent_binary_cipd_vers',
                                              cfg.agent_binary_cipd_vers)
    except datastore_errors.BadValueError as e:
      errors.append((i, e.message))

    if full_validation and not cfg.agent_binary_cipd_filename:
      errors.append((i, 'missing `agent_binary_cipd_filename`'))

    for tag in cfg.tags:
      if ':' not in tag:
        errors.append((i, 'tag must be in key:value form, not {}'.format(tag)))

  return errors
