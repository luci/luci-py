# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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
      permission=_PERMISSION_REIMPORT,
      legacy_acl_group='reimport_group')


def _can_reimport_service_cs(service_id):
  return has_service_access(service_id) and _check_acl_cfg('reimport_group')


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
      permission=_PERMISSION_VALIDATE,
      legacy_acl_group='validation_group')


def _can_validate_service_cs(service_id):
  return has_service_access(service_id) and _check_acl_cfg('validation_group')


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

  has_access is True if current requester can read service configs.
  """
  assert isinstance(service_ids, list)
  if not service_ids:
    return {}

  if _check_acl_cfg('service_access_group'):
    return {sid: True for sid in service_ids}

  service_id_set = set(service_ids)
  cfgs = {
    s.id: s
    for s in services.get_services_async().get_result()
    if s.id in service_id_set
  }
  has_access = _has_access([cfgs.get(sid) for sid in service_ids])
  return dict(zip(service_ids, has_access))


def has_service_access(service_id):
  return has_services_access([service_id])[service_id]


def has_projects_access(project_ids):
  # TODO(crbug.com/1068817): During the migration we'll use the legacy response
  # as the final result, but will compare it to realms checks and log
  # discrepancies (this is what has_permission_dryrun does).
  legacy = _has_projects_access_legacy(project_ids)
  for pid in project_ids:
    auth.has_permission_dryrun(
        permission=_PERMISSION_READ,
        realms=[auth.root_realm(pid)],
        tracking_bug='crbug.com/1068817',
        expected_result=legacy[pid])
  return legacy


def has_project_access(project_id):
  return has_projects_access([project_id])[project_id]


def _has_projects_access_legacy(project_ids):
  assert isinstance(project_ids, list)
  if not project_ids:
    return {}

  if _check_acl_cfg('project_access_group'):
    return {pid: True for pid in project_ids}

  metadata = projects.get_metadata_async(project_ids).get_result()
  has_access = _has_access([metadata.get(pid) for pid in project_ids])
  return dict(zip(project_ids, has_access))


def _check_project_acl(project_id, permission, legacy_acl_group):
  """Checks legacy and realms ACLs, comparing them.

  Returns the result of the legacy ACL check for now.
  """
  # TODO(crbug.com/1068817): Switch to using Realms for real.
  legacy_result = (
      _has_projects_access_legacy([project_id])[project_id] and
      _check_acl_cfg(legacy_acl_group))
  auth.has_permission_dryrun(
      permission=permission,
      realms=[auth.root_realm(project_id)],
      tracking_bug='crbug.com/1068817',
      expected_result=legacy_result)
  return legacy_result


# Cache acl.cfg for 10min. It never changes.
@utils.cache_with_expiration(10 * 60)
def _get_acl_cfg():
  return storage.get_self_config_async(
      common.ACL_FILENAME, service_config_pb2.AclCfg).get_result()


def _check_acl_cfg(group_id):
  """Checks the caller is an admin or a member of a group from acl.cfg."""
  assert group_id in (
      'reimport_group',
      'validation_group',
      'service_access_group',
      'project_access_group',
  )
  if is_admin():
    return True
  acl_cfg = _get_acl_cfg()
  return (
      acl_cfg and
      getattr(acl_cfg, group_id) and
      auth.is_group_member(getattr(acl_cfg, group_id)))


def _has_access(resources):
  access_values = set()
  for r in resources:
    if r:
      access_values.update(r.access)

  identity = auth.get_current_identity()
  has_access = {a: _check_access_entry(a, identity) for a in access_values}
  return [
    bool(r) and any(has_access[a] for a in r.access)
    for r in resources
  ]


def _check_access_entry(access, identity):
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
