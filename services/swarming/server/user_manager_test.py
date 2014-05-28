#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from server import user_manager
from support import test_case
from third_party.mox import mox

IP = ['192.168.0.1', '192.168.0.2']


class UserManagerTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(UserManagerTest, self).setUp()
    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()
    super(UserManagerTest, self).tearDown()

  def testModifyUserProfileWhitelist(self):
    # Make multiple add requests, then a single remove request,
    # then another remove.
    for _ in range(3):
      user_manager.AddWhitelist(IP[0])
    self.assertEqual(1, user_manager.MachineWhitelist.query().count())
    self.assertTrue(user_manager.IsWhitelistedMachine(IP[0]))

    user_manager.DeleteWhitelist(IP[0])
    self.assertEqual(0, user_manager.MachineWhitelist.query().count())
    self.assertFalse(user_manager.IsWhitelistedMachine(IP[0]))

    user_manager.DeleteWhitelist(IP[0])
    self.assertEqual(0, user_manager.MachineWhitelist.query().count())

    user_manager.AddWhitelist(IP[0])
    user_manager.AddWhitelist(IP[0])
    self.assertEqual(1, user_manager.MachineWhitelist.query().count())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.INFO)
  unittest.main()
