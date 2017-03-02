#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

# from components.auth import api
from components import auth
from components import auth_testing
from components import utils
from server import config
from test_support import test_case

from proto import config_pb2

import acl


# Default names of authorization groups.
ADMINS_GROUP = 'administrators'
PRIVILEGED_USERS_GROUP = ADMINS_GROUP
USERS_GROUP = ADMINS_GROUP
BOT_BOOTSTRAP_GROUP = ADMINS_GROUP


class AclTest(test_case.TestCase):
  def setUp(self):
    super(AclTest, self).setUp()

    auth_testing.reset_local_state()
    utils.clear_cache(config.settings)

  @staticmethod
  def add_to_group(group):
    auth.bootstrap_group(group, [auth.get_current_identity()])

  def add_to_admin(self):
    auth_testing.mock_is_admin(self, True)

  def mock_auth_config(self, **kwargs):
    cfg = config_pb2.SettingsCfg(auth=config_pb2.AuthSettings(**kwargs))
    self.mock(config, '_get_settings', lambda: ('test_rev', cfg))

  def test_is_admin_app_admin(self):
    self.add_to_admin()
    self.assertTrue(acl.is_admin())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_admin_not_app_admin(self):
    self.assertFalse(acl.is_admin())
    self.assertIsNone(acl.get_user_type())

  def test_is_admin_default_group(self):
    self.add_to_group(ADMINS_GROUP)
    self.assertTrue(acl.is_admin())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_admin_custom_group(self):
    self.mock_auth_config(admins_group='test_group')
    self.add_to_group('test_group')
    self.assertTrue(acl.is_admin())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_privileged_user_admin(self):
    self.add_to_admin()
    self.assertTrue(acl.is_privileged_user())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_privileged_user_default_group(self):
    self.add_to_group(PRIVILEGED_USERS_GROUP)
    self.assertTrue(acl.is_privileged_user())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_privileged_user_custom_group(self):
    self.mock_auth_config(privileged_users_group='test_group')
    self.add_to_group('test_group')
    self.assertTrue(acl.is_privileged_user())
    self.assertEqual(acl.get_user_type(), 'privileged user')

  def test_is_privileged_user_wrong_group(self):
    self.mock_auth_config(privileged_users_group='test_group')
    self.add_to_group('wrong_test_group')
    self.assertFalse(acl.is_privileged_user())
    self.assertIsNone(acl.get_user_type())

  def test_is_user_privileged(self):
    self.mock_auth_config(privileged_users_group='test_group')
    self.add_to_group('test_group')
    self.assertTrue(acl.is_user())
    self.assertEqual(acl.get_user_type(), 'privileged user')

  def test_is_user_default_group(self):
    self.add_to_group(USERS_GROUP)
    self.assertTrue(acl.is_user())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_user_custom_group(self):
    self.mock_auth_config(users_group='test_group')
    self.add_to_group('test_group')
    self.assertTrue(acl.is_user())
    self.assertEqual(acl.get_user_type(), 'user')

  def test_is_user_wrong_group(self):
    self.mock_auth_config(users_group='test_group')
    self.add_to_group('wrong_test_group')
    self.assertFalse(acl.is_user())
    self.assertIsNone(acl.get_user_type())

  def test_is_bootstrapper_admin(self):
    self.add_to_admin()
    self.assertTrue(acl.is_bootstrapper())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_bootstrapper_default_group(self):
    self.add_to_group(BOT_BOOTSTRAP_GROUP)
    self.assertTrue(acl.is_bootstrapper())
    self.assertEqual(acl.get_user_type(), 'admin')

  def test_is_bootstrapper_custom_group(self):
    self.mock_auth_config(bot_bootstrap_group='test_group')
    self.add_to_group('test_group')
    self.assertTrue(acl.is_bootstrapper())
    self.assertIsNone(acl.get_user_type())

  def test_is_bootstrapper_wrong_group(self):
    self.mock_auth_config(privileged_users_group='test_wrong_group',
                          bot_bootstrap_group='test_correct_group')
    self.add_to_group('test_wrong_group')
    self.assertFalse(acl.is_bootstrapper())
    self.assertEqual(acl.get_user_type(), 'privileged user')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()

