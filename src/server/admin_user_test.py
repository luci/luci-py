#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for MachineStats class."""



import logging
import unittest

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
