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

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub

# Disable caching before importing win.
from utils import tools
tools.cached = lambda func: func

from api.platforms import win


MockWinVer = collections.namedtuple('MockWinVer', ['product_type'])


@unittest.skipUnless(sys.platform == 'win32',
                     'Tests only run under Windows platform')
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

  def test_get_os_version_names(self):
    names = win.get_os_version_names()
    # All versions have at least two names.
    self.assertTrue(len(names) >= 2)
    self.assertTrue(isinstance(name, unicode) for name in names)

  def assert_get_os_dims_mock(self, product_type_int, cmd_ver_out,
                              win32_ver_out, expected_version_names):
    self.mock(sys, 'getwindowsversion', lambda: MockWinVer(product_type_int))
    # Assume this is "cmd.exe /c ver".
    self.mock(subprocess, 'check_output', lambda _: cmd_ver_out)
    self.mock(platform, 'win32_ver', lambda: win32_ver_out)
    names = win.get_os_version_names()
    self.assertEqual(expected_version_names, names)
    self.assertTrue(isinstance(name, unicode) for name in names)

  def test_get_client_versions(self):
    marketing_name_client_ver_map = {
        'Server': '10',
        '2012ServerR2': '8.1',
        '2012Server': '8',
        '2008ServerR2': '7',
        '2008Server': 'Vista',
        '2003Server': 'XP',
        '10': '10',
        '8.1': '8.1',
        '8': '8',
        '7': '7',
        'Vista': 'Vista',
        'XP': 'XP',
        '2000': '2000',
    }
    marketing_name = win.get_os_version_names()[0]
    client_ver = win.get_client_versions()
    self.assertEqual([marketing_name_client_ver_map[marketing_name]],
                     client_ver)

  def test_get_os_dims_mock_win10(self):
    self.assert_get_os_dims_mock(
        1, b'\nMicrosoft Windows [Version 10.0.17763.503]',
        ('10', '10.0.17763', '', 'Multiprocessor Free'),
        ['10', '10-17763', '10-17763.503'])

  def test_get_os_dims_mock_win2016(self):
    self.assert_get_os_dims_mock(
        3, b'\nMicrosoft Windows [Version 10.0.14393]\n',
        ('10', '10.0.14393', '', 'Multiprocessor Free'),
        ['Server', 'Server-14393'])

  def test_get_os_dims_mock_win2019(self):
    self.assert_get_os_dims_mock(
        3, b'\nMicrosoft Windows [Version 10.0.17763.557]\n',
        ('10', '10.0.17763', '', 'Multiprocessor Free'),
        ['Server', 'Server-17763', 'Server-17763.557'])

  def test_get_os_dims_mock_win7sp1(self):
    self.assert_get_os_dims_mock(
        1, b'\nMicrosoft Windows [Version 6.1.7601]\n',
        ('7', '6.1.7601', 'SP1', 'Multiprocessor Free'), ['7', '7-SP1'])

  def test_get_os_dims_mock_win8_1(self):
    self.assert_get_os_dims_mock(1, b'\nMicrosoft Windows [Version 6.3.9600]\n',
                                 ('8.1', '6.3.9600', '', 'Multiprocessor Free'),
                                 ['8.1', '8.1-SP0'])

  @params(
      (0, 32, 'i686'),
      (9, 64, 'amd64'),
      (9, 32, 'i686'),
      (10, 32, 'i686'),
      (12, 64, 'arm64'),
      (999, None, None),
  )
  def test_get_cpu_type_with_wmi(self, arch, addr_width, expected):
    SWbemObjectSet = mock.Mock(Architecture=arch, AddressWidth=addr_width)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObjectSet]
    with mock.patch(
        'api.platforms.win._get_wmi_wbem', return_value=SWbemServices):
      self.assertEqual(win.get_cpu_type_with_wmi(), expected)

  def test_get_gpu(self):
    SWbemObjectSet = mock.Mock(
        PNPDeviceID=
        'PCI\\VEN_1AE0&DEV_A002&SUBSYS_00011AE0&REV_01\\3&13C0B0C5&0&28',
        VideoProcessor='GGA',
        DriverVersion='1.1.1.18')
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObjectSet]
    with mock.patch(
      'api.platforms.win._get_wmi_wbem', return_value=SWbemServices):
      actual = win.get_gpu()
      SWbemServices.query.assert_called_once_with(
        'SELECT * FROM Win32_VideoController')
      expected = (
        ['1ae0', '1ae0:a002', '1ae0:a002-1.1.1.18'], ['Unknown GGA 1.1.1.18'])
      self.assertEqual(expected, actual)

  def test_get_gpu_qualcomm(self):
    SWbemObjectSet = mock.Mock(
        PNPDeviceID='ACPI\\VEN_QCOM&DEV_043A&SUBSYS_CLS08180&REV_0913\\0',
        # "680" instead of "690" is intentional since the incorrect naming shows
        # up on at least one real world device.
        VideoProcessor='Adreno 680',
        DriverVersion='27.20.1870.0')
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObjectSet]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      actual = win.get_gpu()
      SWbemServices.query.assert_called_once_with(
          'SELECT * FROM Win32_VideoController')
      expected = (['qcom', 'qcom:043a', 'qcom:043a-27.20.1870.0'],
                  ['Qualcomm Adreno 690 27.20.1870.0'])
      self.assertEqual(expected, actual)

  def test_list_top_windows(self):
    win.list_top_windows()

  def test_version(self):
    m = re.search(
        win._CMD_RE, 'Microsoft Windows [version 10.0.15063]', re.IGNORECASE)
    self.assertEqual(('10.0', '15063'), m.groups())
    m = re.search(
        win._CMD_RE, 'Microsoft Windows [version 10.0.16299.19]', re.IGNORECASE)
    self.assertEqual(('10.0', '16299.19'), m.groups())

  def test_get_screen_scaling_percent_success(self):
    cases = [
        (100, 100, '100'),
        (100, 50, '200'),
        (100, 80, '125'),
        (100, 0, None),
    ]
    for physical, logical, result in cases:
      with mock.patch('api.platforms.win.is_display_attached',
                      return_value=True):

        def mock_impl(*_, **__):
          mock_impl.call_count += 1
          if mock_impl.call_count == 1:
            return logical
          if mock_impl.call_count == 2:
            return physical
          raise RuntimeError('Called more than 2 times')

        mock_impl.call_count = 0

        with mock.patch('win32ui.GetDeviceCaps', side_effect=mock_impl):
          with mock.patch('win32gui.GetDC'), mock.patch('win32gui.ReleaseDC'):
            self.assertEqual(win.get_screen_scaling_percent(), result)


class TestWinPlatformIndendent(auto_stub.TestCase):
  """Like TestWin, but not limited to running on Windows."""
  def setUp(self):
    super().setUp()
    tools.clear_cache_all()

  def tearDown(self):
    super().tearDown()
    tools.clear_cache_all()

  def test_is_display_attached_valid_display(self):
    SWbemObject = mock.Mock(CurrentHorizontalResolution=1280,
                            CurrentVerticalResolution=720)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertTrue(win.is_display_attached())

  def test_is_display_attached_missing_display(self):
    # Both fields missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=None,
                            CurrentVerticalResolution=None)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertFalse(win.is_display_attached())

    # First field missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=None,
                            CurrentVerticalResolution=720)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertFalse(win.is_display_attached())

    # Second field missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=1280,
                            CurrentVerticalResolution=None)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertFalse(win.is_display_attached())

  def test_is_display_attached_no_wbem(self):
    with mock.patch('api.platforms.win._get_wmi_wbem', return_value=None):
      self.assertIsNone(win.is_display_attached())

  def test_is_display_attached_scripting_error_handled(self):
    SWbemServices = mock.Mock()
    SWbemServices.query.side_effect = win._WbemScriptingError()
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.is_display_attached())

  def test_get_screen_scaling_percent_no_display(self):
    with mock.patch('api.platforms.win.is_display_attached',
                    return_value=False):
      self.assertIsNone(win.get_screen_scaling_percent())

  # Use non-Windows platforms as a proxy for Windows deployments where the
  # required pywin32 modules aren't present.
  @unittest.skipIf(sys.platform == 'win32', 'pywin32 might exist')
  def test_get_screen_scaling_percent_import_error(self):
    with mock.patch('api.platforms.win.is_display_attached', return_value=True):
      self.assertIsNone(win.get_screen_scaling_percent())

  def test_get_display_resolution_success(self):
    SWbemObject = mock.Mock(CurrentHorizontalResolution=1280,
                            CurrentVerticalResolution=720)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertEqual(win.get_display_resolution(), (1280, 720))

  def test_get_display_resolution_missing_display(self):
    # Both fields missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=None,
                            CurrentVerticalResolution=None)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.get_display_resolution())

    # First field missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=None,
                            CurrentVerticalResolution=720)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.get_display_resolution())

    # Second field missing.
    SWbemObject = mock.Mock(CurrentHorizontalResolution=1280,
                            CurrentVerticalResolution=None)
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = [SWbemObject]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.get_display_resolution())

  def test_get_display_resolution_no_wbem(self):
    with mock.patch('api.platforms.win._get_wmi_wbem', return_value=None):
      self.assertIsNone(win.get_display_resolution())

  def test_get_display_resolution_scripting_error_handled(self):
    SWbemServices = mock.Mock()
    SWbemServices.query.side_effect = win._WbemScriptingError()
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.get_display_resolution())

  def test_get_active_displays_success(self):
    display_a = mock.Mock(PNPDeviceID='DISPLAY\\AAAA\\1234')
    display_b = mock.Mock(PNPDeviceID='DISPLAY\\BBBB\\1234')
    SWbemServices = mock.Mock()
    # Deliberately swap the order to test that the return value is sorted.
    SWbemServices.query.return_value = [display_b, display_a]
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertEqual(win.get_active_displays(),
                       ['DISPLAY\\AAAA\\1234', 'DISPLAY\\BBBB\\1234'])

  def test_get_active_displays_success_no_results(self):
    SWbemServices = mock.Mock()
    SWbemServices.query.return_value = []
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertEqual(win.get_active_displays(), [])

  def test_get_active_displays_no_wbem(self):
    with mock.patch('api.platforms.win._get_wmi_wbem', return_value=None):
      self.assertIsNone(win.get_active_displays())

  def test_get_active_displays_scripting_error_handled(self):
    SWbemServices = mock.Mock()
    SWbemServices.query.side_effect = win._WbemScriptingError()
    with mock.patch('api.platforms.win._get_wmi_wbem',
                    return_value=SWbemServices):
      self.assertIsNone(win.get_active_displays())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
