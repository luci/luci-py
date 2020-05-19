#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import mock
from parameterized import parameterized

import test_env
test_env.setup_test_env()

from components import auth
from components import utils
from test_support import test_case

from proto.config import config_pb2
from proto.config import pools_pb2
from proto.config import realms_pb2
from server import config
from server import pools_config
from server import realms
from server import task_scheduler


class RealmsTest(test_case.TestCase):

  def setUp(self):
    super(RealmsTest, self).setUp()
    self._has_permission_mock = mock.Mock()
    self._has_permission_dryrun_mock = mock.Mock()
    self.mock(auth, 'has_permission', self._has_permission_mock)
    self.mock(auth, 'has_permission_dryrun', self._has_permission_dryrun_mock)

  def tearDown(self):
    super(RealmsTest, self).tearDown()
    utils.clear_cache(config.settings)

  def test_get_permission_names(self):
    self.assertEqual(
        'swarming.pools.createTask',
        realms.get_permission_name(
            realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK))

  @parameterized.expand([
      # should return False if the permissiion is not configured in settings.cfg
      # and in pools.cfg.
      (
          False,
          config_pb2.SettingsCfg(),
          pools_pb2.Pool(),
      ),
      # should return True if the permission is enforced in the pool.
      (
          True,
          config_pb2.SettingsCfg(),
          pools_pb2.Pool(enforced_realm_permissions=[
              realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK
          ])
      ),
      # return True if the permission is enforced globally.
      (
          True,
          config_pb2.SettingsCfg(
              auth=config_pb2.AuthSettings(enforced_realm_permissions=[
                  realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK
          ])),
          pools_pb2.Pool()
      ),
  ])
  def test_is_enforced_permission(self, expected, settings_cfg, pool_cfg):
    self.mock(config, '_get_settings', lambda: (None, settings_cfg))
    self.assertEqual(expected, realms.is_enforced_permission(
        realms_pb2.REALM_PERMISSION_POOLS_CREATE_TASK, pool_cfg))

  def _mock_for_check_pools_create_task_legacy(self,
                                               is_allowed_legacy,
                                               pool_realm='test:pool'):
    self.mock(realms, 'is_enforced_permission', lambda *_: False)
    self.mock(pools_config,
              'get_pool_config', lambda _: pools_pb2.Pool(realm=pool_realm))
    self.mock(task_scheduler,
              '_is_allowed_to_schedule', lambda _: is_allowed_legacy)

  def _mock_for_check_pools_create_task(self, pool_realm='test:pool'):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self.mock(pools_config,
              'get_pool_config', lambda _: pools_pb2.Pool(realm=pool_realm))

  def test_check_pools_create_task_legacy_allowed(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=True)
    self.assertIsNone(realms.check_pools_create_task('test_pool'))
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        True,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_allowed_no_pool_realm(self):
    self._mock_for_check_pools_create_task_legacy(
        is_allowed_legacy=True, pool_realm=None)
    self.assertIsNone(realms.check_pools_create_task('test_pool'))
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_pools_create_task_legacy_not_allowed(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=False)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool')
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        False,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_not_allowed_no_pool_realm(self):
    self._mock_for_check_pools_create_task_legacy(
        is_allowed_legacy=False, pool_realm=None)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool')
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_pools_create_task_enforced_allowed(self):
    self._mock_for_check_pools_create_task()
    self._has_permission_mock.return_value = True
    self.assertIsNone(realms.check_pools_create_task('test_pool'))
    self._has_permission_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'])

  def test_check_pools_create_task_enforced_not_allowed(self):
    self._mock_for_check_pools_create_task()
    self._has_permission_mock.return_value = False
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool')
    self._has_permission_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'])

  def test_check_tasks_create_in_realm_legacy(self):
    realms.check_tasks_create_in_realm('test:realm')
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.tasks.createInRealm', [u'test:realm'],
        expected_result=True, tracking_bug=realms._TRACKING_BUG)

  def test_check_tasks_create_in_realm_legacy_no_realm(self):
    realms.check_tasks_create_in_realm(None)
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_tasks_create_in_realm_enforced_allowed(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self._has_permission_mock.return_value = True
    realms.check_tasks_create_in_realm('test:realm')
    self._has_permission_mock.assert_called_once_with(
        'swarming.tasks.createInRealm', [u'test:realm'])

  def test_check_tasks_create_in_realm_enforced_not_allowed(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self._has_permission_mock.return_value = False
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_create_in_realm('test:realm')
    self._has_permission_mock.assert_called_once_with(
        'swarming.tasks.createInRealm', [u'test:realm'])

  def test_check_tasks_create_in_realm_enforced_no_realm(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_create_in_realm(None)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
