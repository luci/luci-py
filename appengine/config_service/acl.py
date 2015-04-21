# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

from components import auth
from components import utils

from proto import service_config_pb2
import common
import storage


RGX_CONFIG_SET_SERVICE = re.compile('services/([a-z\-]+)')


@utils.memcache('acl.cfg', time=60)
def read_acl_cfg():
  return storage.get_self_config(common.ACL_FILENAME, service_config_pb2.AclCfg)


def can_read_config_set(config_set, headers=None):
  """Returns True if current requester has access to the |config_set|.

  Raise:
    ValueError if config_set is malformed.
  """
  try:
    service_match = common.SERVICE_CONFIG_SET_RGX.match(config_set)
    if service_match:
      service_name = service_match.group(1)
      return can_read_service_config(service_name, headers=headers)

    project_match = common.PROJECT_CONFIG_SET_RGX.match(config_set)
    if project_match:
      project_id = project_match.group(1)
      return can_read_project_config(project_id)

    ref_match = common.REF_CONFIG_SET_RGX.match(config_set)
    if ref_match:
      project_id = ref_match.group(1)
      return can_read_project_config(project_id)

  except ValueError:  # pragma: no cover
    # Make sure we don't let ValueError raise for a reason different than
    # malformed config_set.
    logging.exception('Unexpected ValueError in can_read_config_set()')
    return False
  raise ValueError()


def can_read_service_config(service_id, headers=None):
  """Returns True if current requester can read service configs.

  If X-Appengine-Inbound-Appid header matches service_id, the permission is
  granted.
  """
  assert isinstance(service_id, basestring)
  assert service_id

  group = read_acl_cfg().service_access_group
  return (
      auth.is_admin() or
      group and auth.is_group_member(group) or
      (headers or {}).get('X-Appengine-Inbound-Appid') == service_id
  )


# pylint: disable=W0613
def can_read_project_config(project_id):  # pragma: no cover
  return has_project_access()


def can_read_project_list():  # pragma: no cover
  return has_project_access()


def has_project_access():
  group = read_acl_cfg().project_access_group
  return auth.is_admin() or (group and auth.is_group_member(group))
