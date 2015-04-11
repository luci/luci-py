#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import wsgiref.headers

import test_env
test_env.setup_test_env()

from test_support import test_case
import mock

from components import auth

from proto import service_config_pb2
import acl
import storage


class AclTestCase(test_case.TestCase):
  def setUp(self):
    super(AclTestCase, self).setUp()
    self.mock(auth, 'is_admin', lambda *_: False)
    self.mock(auth, 'is_group_member', mock.Mock(return_value=False))

    acl_cfg = service_config_pb2.AclCfg(
        service_access_group='service-admins',
        project_access_group='project-admins',
    )
    self.mock(storage, 'get_self_config', lambda *_: acl_cfg)

  def test_admin_can_read_all(self):
    self.mock(auth, 'is_admin', mock.Mock(return_value=True))
    self.assertTrue(acl.can_read_config_set('services/swarming'))
    self.assertTrue(acl.can_read_config_set('projects/chromium'))
    self.assertTrue(acl.can_read_project_list())

  def test_can_read_service_config(self):
    auth.is_group_member.return_value = True
    self.assertTrue(acl.can_read_config_set('services/swarming'))
    auth.is_group_member.access_called_once_with('service-admins')

  def test_can_read_service_config_header(self):
    headers = wsgiref.headers.Headers([
      ('X-Appengine-Inbound-Appid', 'swarming'),
    ])
    self.assertTrue(
        acl.can_read_config_set('services/swarming', headers=headers))

  def test_can_read_service_config_no_access(self):
    self.assertFalse(acl.can_read_config_set('services/swarming'))

  def test_can_read_project_config(self):
    auth.is_group_member.return_value = True
    self.assertTrue(acl.can_read_config_set('projects/swarming'))
    auth.is_group_member.access_called_once_with('project-admins')

  def test_can_read_project_config_no_access(self):
    self.assertFalse(acl.can_read_config_set('projects/swarming'))
    self.assertFalse(acl.can_read_config_set('projects/swarming/branches/x'))

  def test_malformed_config_set(self):
    with self.assertRaises(ValueError):
      acl.can_read_config_set('invalid config set')


if __name__ == '__main__':
  test_env.main()
