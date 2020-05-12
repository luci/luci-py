#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

from parameterized import parameterized

import test_env
test_env.setup_test_env()

from components import utils
from test_support import test_case

from proto.config import config_pb2
from proto.config import pools_pb2
from proto.config import realms_pb2
from server import config
from server import realms


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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
