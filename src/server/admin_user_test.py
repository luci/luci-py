#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tests for MachineStats class."""


import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import testbed

from server import admin_user


class AdminUserTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

  def tearDown(self):
    self.testbed.deactivate()

  def testEmailAdmins(self):
    # No admins are set, so no email should be sent.
    self.assertFalse(admin_user.EmailAdmins('', ''))

    # Set an admin and ensure emails can get sent to them.
    user = admin_user.AdminUser(email='fake@email.com')
    user.put()
    self.assertTrue(admin_user.EmailAdmins('', ''))


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
