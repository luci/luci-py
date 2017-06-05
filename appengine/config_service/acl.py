# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import re

from components import auth
from components import config
from components import utils
from components.config.proto import service_config_pb2

import common
import projects
import services
import storage


def read_acl_cfg():
  return storage.get_self_config_async(
      common.ACL_FILENAME, service_config_pb2.AclCfg).get_result()


def can_read_config_sets(config_sets):
  """Returns a mapping {config_set: has_access}.

  has_access is True if current requester has access to the config set.

  Raise:
    ValueError if any config_set is malformed.
  """
  assert isinstance(config_sets, list)
  check_via = {}
  for cs in config_sets:
    ref_match = config.REF_CONFIG_SET_RGX.match(cs)
    if ref_match:
      project_id = ref_match.group(1)
      check_via[cs] = 'projects/' + project_id
    else:
      check_via[cs] = cs

  project_ids = []
  service_ids = []
  for cs in set(check_via.itervalues()):
    service_match = config.SERVICE_CONFIG_SET_RGX.match(cs)
    if service_match:
      service_ids.append(service_match.group(1))
    else:
      project_match = config.PROJECT_CONFIG_SET_RGX.match(cs)
      if project_match:
        project_ids.append(project_match.group(1))
      else:
        raise ValueError('invalid config_set %r' % cs)

  access_map = {}
  for pid, access in has_projects_access(project_ids).iteritems():
    access_map['projects/' + pid] = access
  for sid, access in has_services_access(service_ids).iteritems():
    access_map['services/' + sid] = access

  return {
    cs: access_map[check_via[cs]]
    for cs in config_sets
  }


def has_services_access(service_ids):
  """Returns a mapping {service_id: has_access}.

  has_access is True if current requester can read service configs.
  """
  if not service_ids:
    return {}
  for sid in service_ids:
    assert isinstance(sid, basestring)
    assert sid

  if auth.is_admin():
    return {sid: True for sid in service_ids}

  cfgs = {
    sid: s
    for s in services.get_services_async().get_result()
  }
  return {
    sid: cfgs.get(sid) and config.api._has_access(cfgs.get(sid).access)
    for sid in service_ids
  }


def has_projects_access(project_ids):
  if not project_ids:
    return {}
  super_group = read_acl_cfg().project_access_group
  if auth.is_admin() or super_group and auth.is_group_member(super_group):
    return {pid: True for pid in project_ids}
  return {
    pid: meta and config.api._has_access(meta.access)
    for pid, meta in projects.get_metadata(project_ids).iteritems()
  }
