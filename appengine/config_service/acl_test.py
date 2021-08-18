#!/usr/bin/env vpython
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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


TEST_USER = auth.Identity.from_bytes('user:someone@example.com')
TEST_PROJECT = 'some-project'
TEST_SERVICE = 'some-service'
PROJECT_SET = 'projects/' + TEST_PROJECT
SERVICE_SET = 'services/' + TEST_SERVICE


def can_read(config_set):
  return acl.can_read_config_sets([config_set])[config_set]

can_validate = acl.can_validate
can_reimport = acl.can_reimport


class AclTestCase(test_case.TestCase):
  def setUp(self):
    super(AclTestCase, self).setUp()

    self.mock(auth, 'is_group_member', mock.Mock(return_value=False))
    self.mock(auth, 'get_current_identity', mock.Mock())
    auth.get_current_identity.return_value = TEST_USER

    self.mock(services, 'get_services_async', mock.Mock(return_value=future([
        service_config_pb2.Service(
            id=TEST_SERVICE,
            access=['group:some-service-access'],
        ),
        service_config_pb2.Service(
            id='hidden',
            access=['group:hidden'],
        ),
    ])))

    # TODO(vadimsh): Replace with has_permission mocking.
    self.mock(projects, 'get_metadata_async', mock.Mock(return_value=future({
        TEST_PROJECT: project_config_pb2.ProjectCfg(
            access=['group:some-project-access'],
        ),
        'hidden': project_config_pb2.ProjectCfg(
            access=['group:hidden'],
        ),
    })))

    self.mock(storage, 'get_self_config_async', lambda *_: future(
        service_config_pb2.AclCfg(
            project_access_group='project-access',
            project_reimport_group='project-reimport',
            project_validation_group='project-validation',
            service_access_group='service-access',
            service_reimport_group='service-reimport',
            service_validation_group='service-validation',
            reimport_group='legacy-reimport',
            validation_group='legacy-validation',
    )))

    self.mock(projects, '_filter_existing', lambda pids: pids)

  def mock_membership(self, *groups):
    auth.is_group_member.side_effect = lambda name, *_: name in groups

  def test_malformed_config_set(self):
    with self.assertRaises(ValueError):
      can_read('something/odd')

  def test_access_project_forbidden(self):
    self.assertFalse(can_read(PROJECT_SET))

  def test_access_project_via_project_acl(self):
    self.mock_membership('some-project-access')
    self.assertTrue(can_read(PROJECT_SET))

  def test_access_project_via_global_acl(self):
    self.mock_membership('project-access')
    self.assertTrue(can_read(PROJECT_SET))

  def test_access_project_unknown(self):
    self.mock_membership('project-access')
    self.assertTrue(can_read('projects/unknown'))

  def test_validate_project_invisible(self):
    self.assertFalse(can_validate(PROJECT_SET))

  def test_validate_project_forbidden(self):
    self.mock_membership('project-access')
    self.assertFalse(can_validate(PROJECT_SET))

  def test_validate_project_via_project_acl(self):
    # TODO(vadimsh): Implement when realms are enforced. There's no per-project
    # ACL with legacy ACLs.
    pass

  def test_validate_project_via_global_acl(self):
    self.mock_membership('project-access', 'project-validation')
    self.assertTrue(can_validate(PROJECT_SET))

  def test_validate_project_via_legacy_acl(self):
    self.mock_membership('project-access', 'legacy-validation')
    self.assertTrue(can_validate(PROJECT_SET))

  def test_reimport_project_invisible(self):
    self.assertFalse(can_reimport(PROJECT_SET))

  def test_reimport_project_forbidden(self):
    self.mock_membership('project-access')
    self.assertFalse(can_reimport(PROJECT_SET))

  def test_reimport_project_via_project_acl(self):
    # TODO(vadimsh): Implement when realms are enforced. There's no per-project
    # ACL with legacy ACLs.
    pass

  def test_reimport_project_via_global_acl(self):
    self.mock_membership('project-access', 'project-reimport')
    self.assertTrue(can_reimport(PROJECT_SET))

  def test_reimport_project_via_legacy_acl(self):
    self.mock_membership('project-access', 'legacy-reimport')
    self.assertTrue(can_reimport(PROJECT_SET))

  def test_access_service_frobidden(self):
    self.assertFalse(can_read(SERVICE_SET))

  def test_access_service_via_service_acl(self):
    self.mock_membership('some-service-access')
    self.assertTrue(can_read(SERVICE_SET))

  def test_access_service_via_global_acl(self):
    self.mock_membership('service-access')
    self.assertTrue(can_read(SERVICE_SET))

  def test_access_service_unknown(self):
    self.mock_membership('service-access')
    self.assertTrue(can_read('services/unknown'))

  def test_validate_service_invisible(self):
    self.assertFalse(can_validate(SERVICE_SET))

  def test_validate_service_forbidden(self):
    self.mock_membership('service-access')
    self.assertFalse(can_validate(SERVICE_SET))

  def test_validate_service_via_global_acl(self):
    self.mock_membership('service-access', 'service-validation')
    self.assertTrue(can_validate(SERVICE_SET))

  def test_validate_service_via_legacy_acl(self):
    self.mock_membership('service-access', 'legacy-validation')
    self.assertTrue(can_validate(SERVICE_SET))

  def test_reimport_service_invisible(self):
    self.assertFalse(can_reimport(SERVICE_SET))

  def test_reimport_service_forbidden(self):
    self.mock_membership('service-access')
    self.assertFalse(can_reimport(SERVICE_SET))

  def test_reimport_service_via_global_acl(self):
    self.mock_membership('service-access', 'service-reimport')
    self.assertTrue(can_reimport(SERVICE_SET))

  def test_reimport_service_via_legacy_acl(self):
    self.mock_membership('service-access', 'legacy-reimport')
    self.assertTrue(can_reimport(SERVICE_SET))

  ### Tests for can_read_config_sets batch mode.

  def test_can_read_config_sets_many(self):
    self.mock_membership('some-service-access', 'some-project-access')
    res = acl.can_read_config_sets([
        PROJECT_SET, 'projects/hidden', 'projects/unknown',
        SERVICE_SET, 'services/hidden', 'services/unknown',
        PROJECT_SET, SERVICE_SET,  # intentional dups
    ])
    self.assertEqual(res, {
        PROJECT_SET: True,
        SERVICE_SET: True,
        'projects/hidden': False,
        'projects/unknown': False,
        'services/hidden': False,
        'services/unknown': False,
    })

  ### Tests for _has_access helper.

  def test_has_access_as_identity(self):
    res = acl._has_access({
        'missing': None,
        'empty': service_config_pb2.Service(),
        'yes': service_config_pb2.Service(
            access=['nope@example.com', 'group:nope', 'someone@example.com']),
        'no': service_config_pb2.Service(
            access=['nope@example.com', 'group:nope']),
        'also-yes': service_config_pb2.Service(
            access=['user:someone@example.com']),
    })
    self.assertEqual(res, {
        'missing': False,
        'empty': False,
        'yes': True,
        'no': False,
        'also-yes': True,
    })

  def test_has_access_via_group(self):
    self.mock_membership('some-group')
    res = acl._has_access({
        'missing': None,
        'empty': service_config_pb2.Service(),
        'yes': service_config_pb2.Service(
            access=['nope@example.com', 'group:nope', 'group:some-group']),
        'no': service_config_pb2.Service(
            access=['nope@example.com', 'group:nope']),
        'also-yes': service_config_pb2.Service(
            access=['group:some-group']),
    })
    self.assertEqual(res, {
        'missing': False,
        'empty': False,
        'yes': True,
        'no': False,
        'also-yes': True,
    })


if __name__ == '__main__':
  test_env.main()
