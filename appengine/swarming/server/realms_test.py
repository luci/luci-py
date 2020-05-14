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
    utils.clear_cache(config.settings)

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

  def _mock_for_check_pools_create_task(self,
                                        pool_realm='test:pool',
                                        is_enforced=None,
                                        is_allowed_legacy=None,
                                        is_allowed_realms=None):
    task_request_mock = mock.Mock(pool='test_pool')
    self.mock(pools_config,
              'get_pool_config', lambda _: pools_pb2.Pool(realm=pool_realm))
    self.mock(realms, 'is_enforced_permission', lambda *_: is_enforced)
    self.mock(task_scheduler,
              '_is_allowed_to_schedule', lambda _: is_allowed_legacy)
    if is_enforced:
      has_permission_mock = mock.Mock(return_value=is_allowed_realms)
      self.mock(auth, 'has_permission', has_permission_mock)
    else:
      has_permission_mock = mock.Mock()
      self.mock(auth, 'has_permission_dryrun', has_permission_mock)
    return task_request_mock, has_permission_mock

  def test_check_pools_create_task_legacy_allowed(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        is_enforced=False, is_allowed_legacy=True)

    self.assertIsNone(realms.check_pools_create_task(request_mock))

    has_permission_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        True,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_allowed_no_pool_realm(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        pool_realm=None, is_enforced=False, is_allowed_legacy=True)

    self.assertIsNone(realms.check_pools_create_task(request_mock))

    has_permission_mock.assert_not_called()

  def test_check_pools_create_task_legacy_not_allowed(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        is_enforced=False, is_allowed_legacy=False)

    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task(request_mock)

    has_permission_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        False,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_not_allowed_no_pool_realm(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        pool_realm=None, is_enforced=False, is_allowed_legacy=False)

    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task(request_mock)

    has_permission_mock.assert_not_called()

  def test_check_pools_create_task_enforced_allowed(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        is_enforced=True, is_allowed_realms=True)

    self.assertIsNone(realms.check_pools_create_task(request_mock))

    has_permission_mock.assert_called_once_with('swarming.pools.createTask',
                                                [u'test:pool'])

  def test_check_pools_create_task_enforced_not_allowed(self):
    request_mock, has_permission_mock = self._mock_for_check_pools_create_task(
        is_enforced=True, is_allowed_realms=False)

    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task(request_mock)
    has_permission_mock.assert_called_once_with('swarming.pools.createTask',
                                                [u'test:pool'])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
