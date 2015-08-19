#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from test_env import future
import test_env
test_env.setup_test_env()

from test_support import test_case
import mock

from components import auth
from components.config.proto import project_config_pb2
from components.config.proto import service_config_pb2

import acl
import projects
import services
import storage


class AclTestCase(test_case.TestCase):
  def setUp(self):
    super(AclTestCase, self).setUp()
    self.mock(auth, 'get_current_identity', mock.Mock())
    auth.get_current_identity.return_value = auth.Anonymous
    self.mock(auth, 'is_admin', lambda *_: False)
    self.mock(auth, 'is_group_member', mock.Mock(return_value=False))
    self.mock(services, 'get_service_async', mock.Mock())
    services.get_service_async.side_effect = lambda sid: future(None)

    acl_cfg = service_config_pb2.AclCfg(
        project_access_group='project-admins',
    )
    self.mock(storage, 'get_self_config_async', lambda *_: future(acl_cfg))

  def test_admin_can_read_all(self):
    self.mock(auth, 'is_admin', mock.Mock(return_value=True))
    self.assertTrue(acl.can_read_config_set('services/swarming'))
    self.assertTrue(acl.can_read_config_set('projects/chromium'))
    self.assertTrue(acl.has_project_access('chromium'))

  def test_has_service_access(self):
    self.assertFalse(acl.can_read_config_set('services/swarming'))

    service_cfg = service_config_pb2.Service(
        id='swarming', access=['group:swarming-app'])
    services.get_service_async.side_effect = lambda sid: future(service_cfg)
    auth.is_group_member.side_effect = lambda g: g == 'swarming-app'

    self.assertTrue(acl.can_read_config_set('services/swarming'))

  def test_has_service_access_no_access(self):
    self.assertFalse(acl.can_read_config_set('services/swarming'))

  def test_has_project_access_group(self):
    self.mock(projects, 'get_metadata', mock.Mock())
    projects.get_metadata.return_value = project_config_pb2.ProjectCfg(
        access=['group:googlers', 'a@a.com']
    )

    self.assertFalse(acl.can_read_config_set('projects/secret'))

    auth.is_group_member.side_effect = lambda name: name == 'googlers'
    self.assertTrue(acl.can_read_config_set('projects/secret'))

    auth.is_group_member.side_effect = lambda name: name == 'project-admins'
    self.assertTrue(acl.can_read_config_set('projects/secret'))

  def test_has_project_access_identity(self):
    self.mock(projects, 'get_metadata', mock.Mock())
    projects.get_metadata.return_value = project_config_pb2.ProjectCfg(
        access=['group:googlers', 'a@a.com']
    )

    self.assertFalse(acl.can_read_config_set('projects/secret'))

    auth.get_current_identity.return_value = auth.Identity('user', 'a@a.com')
    self.assertTrue(acl.can_read_config_set('projects/secret'))

  def test_can_read_project_config_no_access(self):
    self.assertFalse(acl.has_project_access('projects/swarming'))
    self.assertFalse(acl.can_read_config_set('projects/swarming/refs/heads/x'))

  def test_malformed_config_set(self):
    with self.assertRaises(ValueError):
      acl.can_read_config_set('invalid config set')


if __name__ == '__main__':
  test_env.main()
