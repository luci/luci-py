# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging

from components import auth
from components import config
from components import utils
from components.config.proto import service_config_pb2

import common
import projects
import services
import storage


_PERMISSION_READ = auth.Permission('configs.configSets.read')
_PERMISSION_VALIDATE = auth.Permission('configs.configSets.validate')
_PERMISSION_REIMPORT = auth.Permission('configs.configSets.reimport')


def can_reimport(config_set):
  project_id = _extract_project_id(config_set)
  if project_id:
    return _can_reimport_project_cs(project_id)
  service_id = _extract_service_id(config_set)
  if service_id:
    return _can_reimport_service_cs(service_id)
  raise ValueError('invalid config_set %r' % config_set)


def _can_reimport_project_cs(project_id):
  return _check_project_acl(
      project_id=project_id,
      perm=_PERMISSION_REIMPORT,
      global_acl_group='project_reimport_group',
      legacy_acl_group='reimport_group')


def _can_reimport_service_cs(service_id):
  return _check_service_acl(
      service_id=service_id,
      global_acl_group='service_reimport_group',
      legacy_acl_group='reimport_group')


def can_validate(config_set):
  project_id = _extract_project_id(config_set)
  if project_id:
    return _can_validate_project_cs(project_id)
  service_id = _extract_service_id(config_set)
  if service_id:
    return _can_validate_service_cs(service_id)
  raise ValueError('invalid config_set %r' % config_set)


def _can_validate_project_cs(project_id):
  return _check_project_acl(
      project_id=project_id,
      perm=_PERMISSION_VALIDATE,
      global_acl_group='project_validation_group',
      legacy_acl_group='validation_group')


def _can_validate_service_cs(service_id):
  return _check_service_acl(
      service_id=service_id,
      global_acl_group='service_validation_group',
      legacy_acl_group='validation_group')


def can_read_config_sets(config_sets):
  """Returns a mapping {config_set: has_access}.

  has_access is True if current requester has access to the config set.

  Raise:
    ValueError if any config_set is malformed.
  """
  assert isinstance(config_sets, list)

  PROJECT = 1
  SERVICE = 2

  project_ids = set()
  service_ids = set()

  check_via = {}  # config set -> (PROJECT|SERVICE, id)

  for cs in config_sets:
    project_id = _extract_project_id(cs)
    if project_id:
      check_via[cs] = (PROJECT, project_id)
      project_ids.add(project_id)
      continue
    service_id = _extract_service_id(cs)
    if service_id:
      check_via[cs] = (SERVICE, service_id)
      service_ids.add(service_id)
      continue
    raise ValueError('invalid config_set %r' % cs)

  access_map = {}
  for project_id, access in has_projects_access(list(project_ids)).items():
    access_map[(PROJECT, project_id)] = access
  for service_id, access in has_services_access(list(service_ids)).items():
    access_map[(SERVICE, service_id)] = access

  return {cs: access_map[check_via[cs]] for cs in config_sets}


def is_admin():
  if auth.is_superuser():
    return True
  acl_cfg = _get_acl_cfg()
  return auth.is_group_member(
      acl_cfg and acl_cfg.admin_group or auth.ADMIN_GROUP)


def has_services_access(service_ids):
  """Returns a mapping {service_id: has_access}.

  has_access is True if the current requester can read service configs.
  """
  assert isinstance(service_ids, list)
  if not service_ids:
    return {}

  # If allowed through a global group, no need to check per-service permissions.
  if _check_acl_cfg('service_access_group'):
    return {sid: True for sid in service_ids}

  cfgs = {cfg.id: cfg for cfg in services.get_services_async().get_result()}
  return _has_access({sid: cfgs.get(sid) for sid in service_ids})


def has_service_access(service_id):
  return has_services_access([service_id])[service_id]


def has_projects_access(project_ids):
  """Returns a mapping {project_id: has_access}.

  has_access is True if the current requester can read project configs.
  """
  assert isinstance(project_ids, list)
  if not project_ids:
    return {}

  # If allowed through a global group, no need to check per-project permissions.
  if _check_acl_cfg('project_access_group'):
    return {pid: True for pid in project_ids}

  # TODO(crbug.com/1068817): During the migration we'll use the union of legacy
  # and realm ACLs, but log when falling back to legacy ACLs.

  realms = {
      pid: auth.has_permission(_PERMISSION_READ, realms=[auth.root_realm(pid)])
      for pid in project_ids
  }

  legacy = _has_projects_access_legacy(project_ids)

  for pid in project_ids:
    if not realms[pid] and legacy[pid]:
      _log_acl_fallback(_PERMISSION_READ, pid)

  return {pid: realms[pid] or legacy[pid] for pid in project_ids}


def has_project_access(project_id):
  return has_projects_access([project_id])[project_id]


def _has_projects_access_legacy(project_ids):
  assert isinstance(project_ids, list)
  if not project_ids:
    return {}

  if _check_acl_cfg('legacy_project_access_group'):
    return {pid: True for pid in project_ids}

  metadata = projects.get_metadata_async(project_ids).get_result()
  return _has_access({pid: metadata.get(pid) for pid in project_ids})


def _check_service_acl(service_id, global_acl_group, legacy_acl_group):
  """Checks global ACLs and per-service permissions for an action.

  Args:
    service_id: a service whose config set is being acted on.
    global_acl_group: a field name in AclCfg with the global group to check.
    legacy_acl_group: a field name in AclCfg with the legacy group to check.

  Returns:
    True to allow the action, False to deny.
  """
  # If the service is not visible, no actions are allowed.
  if not has_service_access(service_id):
    return False

  # Unlike projects, service config sets use only global ACLs for actions.
  if _check_acl_cfg(global_acl_group):
    return True

  # Log if we had to fallback to the legacy group.
  if _check_acl_cfg(legacy_acl_group):
    logging.warning('crbug.com/1068817: using legacy %s', legacy_acl_group)
    return True

  return False


def _check_project_acl(project_id, perm, global_acl_group, legacy_acl_group):
  """Checks service ACLs and per-project permissions for an action.

  Args:
    project_id: a project whose config set is being acted on.
    perm: a permission to check.
    global_acl_group: a field name in AclCfg with the global group to check.
    legacy_acl_group: a field name in AclCfg with the legacy group to check.

  Returns:
    True to allow the action, False to deny.
  """
  # If the project is not visible, no actions are allowed.
  if not has_project_access(project_id):
    return False

  # If allowed through a global group, no need to check per-project permissions.
  if _check_acl_cfg(global_acl_group):
    return True

  # TODO(crbug.com/1068817): During the migration we'll use the union of legacy
  # and realm ACLs, but log when falling back to legacy ACLs.

  realms = auth.has_permission(perm, realms=[auth.root_realm(project_id)])
  legacy = _check_acl_cfg(legacy_acl_group)

  if not realms and legacy:
    _log_acl_fallback(perm, project_id)

  return realms or legacy


def _log_acl_fallback(perm, project_id):
  logging.warning(
      'crbug.com/1068817: fallback %r %r %r',
      perm, project_id, auth.get_current_identity().to_bytes())


# Cache acl.cfg for 10min. It never changes.
@utils.cache_with_expiration(10 * 60)
def _get_acl_cfg():
  acl_cfg = storage.get_self_config_async(
      common.ACL_FILENAME, service_config_pb2.AclCfg).get_result()
  if not acl_cfg.project_validation_group:
    acl_cfg.project_validation_group = acl_cfg.validation_group
  if not acl_cfg.project_reimport_group:
    acl_cfg.project_reimport_group = acl_cfg.reimport_group
  if not acl_cfg.service_validation_group:
    acl_cfg.service_validation_group = acl_cfg.validation_group
  if not acl_cfg.service_reimport_group:
    acl_cfg.service_reimport_group = acl_cfg.reimport_group
  if not acl_cfg.legacy_project_access_group:
    acl_cfg.legacy_project_access_group = acl_cfg.project_access_group
  return acl_cfg


def _check_acl_cfg(group_id):
  """Checks the caller is an admin or a member of a group from acl.cfg."""
  assert group_id in (
      'project_access_group',
      'project_reimport_group',
      'project_validation_group',
      'service_access_group',
      'service_reimport_group',
      'service_validation_group',
      'reimport_group',
      'validation_group',
      'legacy_project_access_group',
  )
  if is_admin():
    return True
  acl_cfg = _get_acl_cfg()
  return (
      acl_cfg and
      getattr(acl_cfg, group_id) and
      auth.is_group_member(getattr(acl_cfg, group_id)))


def _has_access(resources):
  """Checks `access` fields in a bunch of configs at once.

  Args:
    resources: {id => service_config_pb2.Service|project_config_pb2.ProjectCfg}.

  Returns:
    {id => True|False}.
  """
  # Collect a set of strings like "group:X" to check them only once.
  access_values = set()
  for cfg in resources.values():
    if cfg:
      access_values.update(cfg.access)

  # Do all the checks and get a dict e.g. {"group:X" => True|False}.
  identity = auth.get_current_identity()
  has_access = {a: _check_access_entry(a, identity) for a in access_values}

  # Use results of the checks to construct the final output.
  return {
      rid: bool(cfg) and any(has_access[a] for a in cfg.access)
      for rid, cfg in resources.items()
  }


def _check_access_entry(access, identity):
  """Checks a single access entry like "group:X" or "user:Y"."""
  if access.startswith('group:'):
    group = access.split(':', 2)[1]
    return auth.is_group_member(group, identity)

  ac_identity_str = access
  if ':' not in ac_identity_str:
    ac_identity_str = 'user:%s' % ac_identity_str
  return identity.to_bytes() == ac_identity_str


def _extract_project_id(config_set):
  """Returns a project ID for projects/... config sets or None for the rest."""
  ref_match = config.REF_CONFIG_SET_RGX.match(config_set)
  if ref_match:
    return ref_match.group(1)
  project_match = config.PROJECT_CONFIG_SET_RGX.match(config_set)
  if project_match:
    return project_match.group(1)
  return None


def _extract_service_id(config_set):
  """Returns a service ID for services/... config sets or None for the rest."""
  service_match = config.SERVICE_CONFIG_SET_RGX.match(config_set)
  if service_match:
    return service_match.group(1)
  return None
