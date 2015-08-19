# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

from components import auth
from components import config
from components import utils
from components.config.proto import service_config_pb2

import common
import projects
import services
import storage


@utils.memcache('acl.cfg', time=60)
def read_acl_cfg():
  return storage.get_self_config_async(
      common.ACL_FILENAME, service_config_pb2.AclCfg).get_result()


def can_read_config_set(config_set):
  """Returns True if current requester has access to the |config_set|.

  Raise:
    ValueError if config_set is malformed.
  """
  try:
    service_match = config.SERVICE_CONFIG_SET_RGX.match(config_set)
    if service_match:
      service_name = service_match.group(1)
      return has_service_access(service_name)

    project_match = config.PROJECT_CONFIG_SET_RGX.match(config_set)
    if project_match:
      project_id = project_match.group(1)
      return has_project_access(project_id)

    ref_match = config.REF_CONFIG_SET_RGX.match(config_set)
    if ref_match:
      project_id = ref_match.group(1)
      return has_project_access(project_id)

  except ValueError:  # pragma: no cover
    # Make sure we don't let ValueError raise for a reason different than
    # malformed config_set.
    logging.exception('Unexpected ValueError in can_read_config_set()')
    return False
  raise ValueError()


def has_service_access(service_id):
  """Returns True if current requester can read service configs.

  An app <app-id> has access to configs of service with id <app-id>.
  """
  assert isinstance(service_id, basestring)
  assert service_id

  if auth.is_admin():
    return True

  service_cfg = services.get_service_async(service_id).get_result()
  return service_cfg and config.api._has_access(service_cfg.access)


def has_project_access(project_id):
  metadata = projects.get_metadata(project_id)
  super_group = read_acl_cfg().project_access_group
  return (
      auth.is_admin() or
      super_group and auth.is_group_member(super_group) or
      metadata and config.api._has_access(metadata.access)
  )
