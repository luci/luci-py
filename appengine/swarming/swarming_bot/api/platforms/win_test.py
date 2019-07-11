#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import collections
import logging
import platform
import re
import subprocess
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub

# Disable caching before importing win.
from utils import tools
tools.cached = lambda func: func

import win


MockWinVer = collections.namedtuple('MockWinVer', ['product_type'])


class TestWin(auto_stub.TestCase):
  def test_from_cygwin_path(self):
    data = [
      ('foo', None),
      ('x:\\foo$', None),
      ('X:\\foo$', None),
      ('/cygdrive/x/foo$', 'x:\\foo$'),
    ]
    for i, (inputs, expected) in enumerate(data):
      actual = win.from_cygwin_path(inputs)
      self.assertEqual(expected, actual, (inputs, expected, actual, i))

  def test_to_cygwin_path(self):
    data = [
      ('foo', None),
      ('x:\\foo$', '/cygdrive/x/foo$'),
      ('X:\\foo$', '/cygdrive/x/foo$'),
      ('/cygdrive/x/foo$', None),
    ]
    for i, (inputs, expected) in enumerate(data):
      actual = win.to_cygwin_path(inputs)
      self.assertEqual(expected, actual, (inputs, expected, actual, i))

  def test_get_os_version_names_native(self):
    if sys.platform == 'win32':
      names = win.get_os_version_names()
      # All versions have at least two names.
      self.assertTrue(len(names) >= 2)
      self.assertTrue(isinstance(name, unicode) for name in names)

  def assert_get_os_dims_mock(self, product_type_int, cmd_ver_out,
                              win32_ver_out, expected_version_names):
    if sys.platform != 'win32':
      # This method only exists on Windows, so we have to trick the mock into
      # allowing us to mock a nonexistent method.
      sys.getwindowsversion = None
    self.mock(sys, 'getwindowsversion', lambda: MockWinVer(product_type_int))
    # Assume this is "cmd.exe /c ver".
    self.mock(subprocess, 'check_output', lambda _: cmd_ver_out)
    self.mock(platform, 'win32_ver', lambda: win32_ver_out)
    names = win.get_os_version_names()
    self.assertEqual(expected_version_names, names)
    self.assertTrue(isinstance(name, unicode) for name in names)

  def test_get_os_dims_mock_win10(self):
    self.assert_get_os_dims_mock(
        1,
        u'\nMicrosoft Windows [Version 10.0.17763.503]',
        ('10', '10.0.17763', '', u'Multiprocessor Free'),
        [u'10', u'10-17763', u'10-17763.503'])

  def test_get_os_dims_mock_win2016(self):
    self.assert_get_os_dims_mock(
        3,
        '\nMicrosoft Windows [Version 10.0.14393]\n',
        ('10', '10.0.14393', '', u'Multiprocessor Free'),
        [u'2016Server', u'2016Server-14393', u'Server', u'Server-14393'])

  def test_get_os_dims_mock_win2019(self):
    self.assert_get_os_dims_mock(
        3,
        '\nMicrosoft Windows [Version 10.0.17763.557]\n',
        ('10', '10.0.17763', '', u'Multiprocessor Free'),
        [u'2016Server', u'2016Server-17763.557', u'Server', u'Server-17763',
         u'Server-17763.557'])

  def test_get_os_dims_mock_win7sp1(self):
    self.assert_get_os_dims_mock(
        1,
        '\nMicrosoft Windows [Version 6.1.7601]\n',
        ('7', '6.1.7601', 'SP1', u'Multiprocessor Free'),
        [u'7', u'7-SP1'])

  def test_get_os_dims_mock_win8_1(self):
    self.assert_get_os_dims_mock(
        1,
        '\nMicrosoft Windows [Version 6.3.9600]\n',
        ('8.1', '6.3.9600', '', u'Multiprocessor Free'),
        [u'8.1', u'8.1-SP0'])


  def test_list_top_windows(self):
    if sys.platform == 'win32':
      win.list_top_windows()

  def test_version(self):
    m = re.search(
        win._CMD_RE, 'Microsoft Windows [version 10.0.15063]', re.IGNORECASE)
    self.assertEqual(('10.0', '15063'), m.groups())
    m = re.search(
        win._CMD_RE, 'Microsoft Windows [version 10.0.16299.19]', re.IGNORECASE)
    self.assertEqual(('10.0', '16299.19'), m.groups())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
