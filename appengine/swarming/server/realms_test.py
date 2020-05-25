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

import endpoints

from components import auth
from components import utils
from test_support import test_case

from proto.config import config_pb2
from proto.config import pools_pb2
from proto.config import realms_pb2
from server import config
from server import pools_config
from server import realms
from server import service_accounts
from server import task_request
from server import task_scheduler


def _gen_task_request_mock(realm='test:realm'):
  mocked = mock.create_autospec(task_request.TaskRequest, spec_set=True)()
  mocked.max_lifetime_secs = 1
  mocked.service_account = 'test@test-service-accounts.iam.gserviceaccount.com'
  mocked.realm = realm
  return mocked


class RealmsTest(test_case.TestCase):

  def setUp(self):
    super(RealmsTest, self).setUp()
    self._has_permission_mock = mock.Mock()
    self._has_permission_dryrun_mock = mock.Mock()
    self.mock(auth, 'has_permission', self._has_permission_mock)
    self.mock(auth, 'has_permission_dryrun', self._has_permission_dryrun_mock)
    delegatee = auth.Identity.from_bytes('user:delegatee@example.com')
    self.mock(auth, 'get_peer_identity', lambda: delegatee)
    self.mock(service_accounts, 'has_token_server', lambda: True)

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

  def _mock_for_check_pools_create_task_legacy(self, is_allowed_legacy):
    self.mock(realms, 'is_enforced_permission', lambda *_: False)
    self.mock(task_scheduler,
              '_is_allowed_to_schedule', lambda _: is_allowed_legacy)

  def _mock_for_check_pools_create_task(self, pool_realm='test:pool'):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self.mock(pools_config,
              'get_pool_config', lambda _: pools_pb2.Pool(realm=pool_realm))

  def test_check_pools_create_task_legacy_allowed(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=True)
    realms.check_pools_create_task('test_pool',
                                   pools_pb2.Pool(realm='test:pool'))
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        True,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_allowed_no_pool_realm(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=True)
    realms.check_pools_create_task('test_pool', pools_pb2.Pool())
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_pools_create_task_legacy_not_allowed(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=False)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool',
                                     pools_pb2.Pool(realm='test:pool'))
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'],
        False,
        tracking_bug='crbug.com/1066839')

  def test_check_pools_create_task_legacy_not_allowed_no_pool_realm(self):
    self._mock_for_check_pools_create_task_legacy(is_allowed_legacy=False)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool', pools_pb2.Pool())
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_pools_create_task_enforced_allowed(self):
    self._mock_for_check_pools_create_task()
    self._has_permission_mock.return_value = True
    realms.check_pools_create_task('test_pool',
                                   pools_pb2.Pool(realm='test:pool'))
    self._has_permission_mock.assert_called_once_with(
        'swarming.pools.createTask', [u'test:pool'])

  def test_check_pools_create_task_enforced_not_allowed(self):
    self._mock_for_check_pools_create_task()
    self._has_permission_mock.return_value = False
    with self.assertRaises(auth.AuthorizationError):
      realms.check_pools_create_task('test_pool',
                                     pools_pb2.Pool(realm='test:pool'))
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

  def _mock_for_check_tasks_run_as_legacy(self,
                                          is_allowed_legacy,
                                          token_server_err=None):
    self.mock(realms, 'is_enforced_permission', lambda *_: False)
    get_oauth_token_grant_mock = mock.Mock(return_value='service_account_token')
    get_oauth_token_grant_mock.side_effect = token_server_err
    self.mock(service_accounts, 'get_oauth_token_grant',
              get_oauth_token_grant_mock)
    self.mock(task_scheduler,
              '_is_allowed_service_account', lambda *_: is_allowed_legacy)

  def test_check_tasks_run_as_legacy_allowed(self):
    self._mock_for_check_tasks_run_as_legacy(is_allowed_legacy=True)
    task_request_mock = _gen_task_request_mock()
    realms.check_tasks_run_as(task_request_mock)
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.tasks.runAs', [u'test:realm'],
        True,
        identity=auth.Identity.from_bytes('user:delegatee@example.com'),
        tracking_bug=realms._TRACKING_BUG)
    self.assertEqual(task_request_mock.service_account_token,
                     'service_account_token')

  def test_check_tasks_run_as_legacy_allowed_no_realm(self):
    self._mock_for_check_tasks_run_as_legacy(is_allowed_legacy=True)
    task_request_mock = _gen_task_request_mock(realm=None)
    realms.check_tasks_run_as(task_request_mock)
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_tasks_run_as_legacy_not_allowed(self):
    self._mock_for_check_tasks_run_as_legacy(is_allowed_legacy=False)
    task_request_mock = _gen_task_request_mock()
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_run_as(task_request_mock)
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.tasks.runAs', [u'test:realm'],
        False,
        identity=auth.Identity.from_bytes('user:delegatee@example.com'),
        tracking_bug=realms._TRACKING_BUG)

  def test_check_tasks_run_as_legacy_token_server_permission_error(self):
    self._mock_for_check_tasks_run_as_legacy(
        is_allowed_legacy=True,
        token_server_err=service_accounts.PermissionError('not permitted'))
    task_request_mock = _gen_task_request_mock()
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_run_as(task_request_mock)
    self._has_permission_dryrun_mock.assert_called_once_with(
        'swarming.tasks.runAs', [u'test:realm'],
        False,
        identity=auth.Identity.from_bytes('user:delegatee@example.com'),
        tracking_bug=realms._TRACKING_BUG)

  @parameterized.expand([
      (service_accounts.MisconfigurationError('config is wrong'),),
      (service_accounts.InternalError('something is wrong'),),
  ])
  def test_check_tasks_run_as_legacy_token_server_config_error(self, err):
    self._mock_for_check_tasks_run_as_legacy(
        is_allowed_legacy=True, token_server_err=err)
    task_request_mock = _gen_task_request_mock()
    with self.assertRaises(endpoints.ServiceException):
      realms.check_tasks_run_as(task_request_mock)
    self._has_permission_dryrun_mock.assert_not_called()

  def test_check_tasks_run_as_enforced_allowed(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self._has_permission_mock.return_value = True
    task_request_mock = _gen_task_request_mock()
    realms.check_tasks_run_as(task_request_mock)
    self._has_permission_mock.assert_called_once_with(
        'swarming.tasks.runAs', [u'test:realm'],
        identity=auth.Identity.from_bytes('user:delegatee@example.com'))

  def test_check_tasks_run_as_enforced_no_realm(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self._has_permission_mock.return_value = False
    task_request_mock = _gen_task_request_mock(realm=None)
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_run_as(task_request_mock)
    self._has_permission_mock.assert_not_called()

  def test_check_tasks_run_as_enforced_not_allowed(self):
    self.mock(realms, 'is_enforced_permission', lambda *_: True)
    self._has_permission_mock.return_value = False
    task_request_mock = _gen_task_request_mock()
    with self.assertRaises(auth.AuthorizationError):
      realms.check_tasks_run_as(task_request_mock)
    self._has_permission_mock.assert_called_once_with(
        'swarming.tasks.runAs', [u'test:realm'],
        identity=auth.Identity.from_bytes('user:delegatee@example.com'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
