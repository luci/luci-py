#!/usr/bin/env vpython3
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

import mock
# TODO(github.com/wolever/parameterized/issues/91)
# use parameterized after the bug is resolved.
from nose2.tools import params
import six

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub

# Disable caching before importing win.
from utils import tools
tools.cached = lambda func: func

from api.platforms import win


MockWinVer = collections.namedtuple('MockWinVer', ['product_type'])


class TestWin(auto_stub.TestCase):

  def setUp(self):
    super(TestWin, self).setUp()
    tools.clear_cache_all()

  def tearDown(self):
    super(TestWin, self).tearDown()
    tools.clear_cache_all()

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

  def test_get_client_versions(self):
    if sys.platform != 'win32':
      return
    marketing_name_client_ver_map = {
        u'Server': u'10',
        u'2012ServerR2': u'8.1',
        u'2012Server': u'8',
        u'2008ServerR2': u'7',
        u'2008Server': u'Vista',
        u'2003Server': u'XP',
        u'10': u'10',
        u'8.1': u'8.1',
        u'8': u'8',
        u'7': u'7',
        u'Vista': u'Vista',
        u'XP': u'XP',
        u'2000': u'2000',
    }
    marketing_name = win.get_os_version_names()[0]
    client_ver = win.get_client_versions()
    self.assertEqual([marketing_name_client_ver_map[marketing_name]],
                     client_ver)

  def test_get_os_dims_mock_win10(self):
    self.assert_get_os_dims_mock(
        1, b'\nMicrosoft Windows [Version 10.0.17763.503]',
        ('10', '10.0.17763', '', u'Multiprocessor Free'),
        [u'10', u'10-17763', u'10-17763.503'])

  def test_get_os_dims_mock_win2016(self):
    self.assert_get_os_dims_mock(
        3, b'\nMicrosoft Windows [Version 10.0.14393]\n',
        ('10', '10.0.14393', '', u'Multiprocessor Free'),
        [u'Server', u'Server-14393'])

  def test_get_os_dims_mock_win2019(self):
    self.assert_get_os_dims_mock(
        3, b'\nMicrosoft Windows [Version 10.0.17763.557]\n',
        ('10', '10.0.17763', '', u'Multiprocessor Free'),
        [u'Server', u'Server-17763', u'Server-17763.557'])

  def test_get_os_dims_mock_win7sp1(self):
    self.assert_get_os_dims_mock(
        1, b'\nMicrosoft Windows [Version 6.1.7601]\n',
        ('7', '6.1.7601', 'SP1', u'Multiprocessor Free'), [u'7', u'7-SP1'])

  def test_get_os_dims_mock_win8_1(self):
    self.assert_get_os_dims_mock(
        1, b'\nMicrosoft Windows [Version 6.3.9600]\n',
        ('8.1', '6.3.9600', '', u'Multiprocessor Free'), [u'8.1', u'8.1-SP0'])

  @params(
      (0, 3, 32, 'i386'),
      (9, None, 64, 'amd64'),
      (9, 6, 32, 'i686'),
      (10, 6, 32, 'i686'),
      (999, None, None, None),
  )
  def test_get_cpu_type_with_wmi(self, arch, level, addr_width, expected):
    SWbemObjectSet = mock.Mock(
        Architecture=arch, Level=level, AddressWidth=addr_width)
    SWbemServices = mock.Mock()
    SWbemServices.ExecQuery.return_value = [SWbemObjectSet]
    with mock.patch(
        'api.platforms.win._get_wmi_wbem', return_value=SWbemServices):
      self.assertEqual(win.get_cpu_type_with_wmi(), expected)

  def test_get_gpu(self):
    SWbemObjectSet = mock.Mock(
      PNPDeviceID=
      u'PCI\\VEN_1AE0&DEV_A002&SUBSYS_00011AE0&REV_01\\3&13C0B0C5&0&28',
      VideoProcessor=u'GGA',
      DriverVersion=u'1.1.1.18')
    SWbemServices = mock.Mock()
    SWbemServices.ExecQuery.return_value = [SWbemObjectSet]
    with mock.patch(
      'api.platforms.win._get_wmi_wbem', return_value=SWbemServices):
      actual = win.get_gpu()
      SWbemServices.ExecQuery.assert_called_once_with(
        'SELECT * FROM Win32_VideoController')
      expected = (
        ['1ae0', '1ae0:a002', '1ae0:a002-1.1.1.18'], ['Unknown GGA 1.1.1.18'])
      self.assertEqual(expected, actual)

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
