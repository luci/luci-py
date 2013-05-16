#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests for User Manager."""




import logging
import unittest


from google.appengine.ext import testbed
from .server import user_manager
from third_party.mox import mox

IP = ['192.168.0.1', '192.168.0.2']


class UserManagerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testModifyUserProfileWhitelist(self):
    password = 'wasspord'

    # Make multiple add requests, then a single remove request,
    # then another remove.
    for _ in range(3):
      user_manager.AddWhitelist(IP[0])
    self.assertEqual(1, user_manager.MachineWhitelist.all().count())
    self.assertTrue(user_manager.IsWhitelistedMachine(IP[0], None))

    user_manager.DeleteWhitelist(IP[0])
    self.assertEqual(0, user_manager.MachineWhitelist.all().count())
    self.assertFalse(user_manager.IsWhitelistedMachine(IP[0], None))

    user_manager.DeleteWhitelist(IP[0])
    self.assertEqual(0, user_manager.MachineWhitelist.all().count())

    # Make one request with password, one without one. Second one should
    # be ignored.
    user_manager.AddWhitelist(IP[0], password=password)
    user_manager.AddWhitelist(IP[0])
    self.assertEqual(1, user_manager.MachineWhitelist.all().count())

    # Should fail whitelist check because no password is given.
    self.assertFalse(user_manager.IsWhitelistedMachine(IP[0], None))

    # Make sure removes are done based on ip, ignoring password.
    user_manager.DeleteWhitelist(IP[0])
    self.assertEqual(0, user_manager.MachineWhitelist.all().count())

  def testModifyUserProfileWhitelistArguments(self):
    # We accept None for ip.
    user_manager.AddWhitelist(ip=None, password=IP[0])
    self.assertEqual(1, user_manager.MachineWhitelist.all().count())
    self.assertEqual(None, user_manager.MachineWhitelist.all().get().ip)
    self.assertEqual(IP[0], user_manager.MachineWhitelist.all().get().password)
    self.assertEqual(
        1, user_manager.MachineWhitelist.gql('WHERE ip = :1', None).count())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.INFO)
  unittest.main()
