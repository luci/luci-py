#!/usr/bin/env python
# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests ADB protocol implementation against a real device.

The device must be running an user-debug build or commonly said as "rooted".
"""

import argparse
import cStringIO
import logging
import os
import pipes
import random
import re
import sys
import tempfile
import time
import unittest


import usb1


from adb import adb_commands_safe
from adb import adb_protocol
from adb import common
from adb import high
from adb import usb_exceptions


class Filter(object):
  def filter(self, record):  # pragma: no cover
    record.severity = record.levelname[0]
    return True


def GetOnlyDevice(on_error, timeout_ms=1000):
  """Returns a device if only one is connected."""
  handles = list(
      common.UsbHandle.FindDevicesSafe(
        adb_commands_safe.DeviceIsAvailable, timeout_ms=timeout_ms))
  if not handles:
    on_error('Please connect an Android device first!')  # pragma: no cover
  if len(handles) > 1:  # pragma: no cover
    print('Available devices:')
    for handle in handles:
      path = '/'.join(map(str, handle.port_path))
      print('%s - %s' % (handle.serial_number, path))
    on_error('Use exactly one of --serial or --usb-path')

  # Choose the single device connected directly.
  serial = handles[0].serial_number
  path = '/'.join(map(str, handles[0].port_path))
  logging.info('Automatically selected %s : %s', path, serial)
  return path


def FindDevice(on_error, serial, timeout_ms=1000):  # pragma: no cover
  """Finds a device with serial if relevant, otherwise take the device connect
  if there is exactly one.
  """
  # Find the usb path for this device, so the next operations are done with
  # the usb path.
  handle = common.UsbHandle.Find(
      adb_commands_safe.DeviceIsAvailable, serial=serial, timeout_ms=timeout_ms)
  if handle:
    path = '/'.join(map(str, handle.port_path))
    logging.info('Automatically selected %s : %s', path, serial)
    return path
  on_error('Didn\'t find device with serial %s' % serial)


class Test(unittest.TestCase):
  PORT_PATH = None
  KEYS = None

  def setUp(self):
    super(Test, self).setUp()
    self.cmd = None

  def tearDown(self):
    try:
      if self.cmd:
        self.cmd.Close()
    finally:
      super(Test, self).tearDown()

  def safe(self):
    """Open the device and ensure it's running as root."""
    self.assertEqual(None, self.cmd)
    port_path = map(int, self.PORT_PATH.split('/'))
    self.cmd = adb_commands_safe.AdbCommandsSafe.ConnectDevice(
        port_path=port_path, banner='python-adb', rsa_keys=self.KEYS,
        on_error=None, default_timeout_ms=10000, auth_timeout_ms=10000,
        lost_timeout_ms=30000)
    self.assertEqual(True, self.cmd.is_valid)
    self.assertTrue(self.cmd.GetUptime())

  def high(self):
    """Open the device and ensure it's running as root."""
    self.assertEqual(None, self.cmd)
    port_path = map(int, self.PORT_PATH.split('/'))
    self.cmd = high.HighDevice.ConnectDevice(
        port_path=port_path, banner='python-adb', on_error=None,
        default_timeout_ms=10000, auth_timeout_ms=10000, lost_timeout_ms=10000)
    self.assertEqual(True, self.cmd.is_valid)
    self.assertEqual(True, self.cmd.WaitForDevice())
    if self.cmd.cache.has_su and not self.cmd.IsRoot():
      try:
        self.cmd.Root()
      finally:
        # Restarting the Android device changed the port path. Find it back by
        # the serial number.
        logging.info('Updating port path to %s', self.cmd.port_path)
        self.__class__.PORT_PATH = self.cmd.port_path

  def test_GetDevices(self):
    devices = high.GetDevices(
        banner='python-adb', default_timeout_ms=1000, auth_timeout_ms=1000)
    try:
      self.assertEqual(
          1, len([d for d in devices if d.port_path == self.PORT_PATH]))
    finally:
      high.CloseDevices(devices)

  def test_Initialize(self):
    with self.assertRaises(AssertionError):
      high.Initialize(None, None)

  def test_Push_Pull(self):
    """Pushes and fetch a file."""
    self.high()
    self.assertTrue(repr(self.cmd))
    path = '/storage/emulated/legacy/yo'
    try:
      # Small file.
      self.assertEqual(True, self.cmd.PushContent('Yo', path))
      self.assertEqual('Yo', self.cmd.PullContent(path))

      # Large file of 1Mb.
      large = '0' * 1024 * 1024
      self.assertEqual(True, self.cmd.PushContent(large, path))
      self.assertEqual(large, self.cmd.PullContent(path))

      h, name = tempfile.mkstemp(prefix='python-adb')
      os.close(h)
      try:
        with open(name, 'wb') as f:
          f.write('Too')
        self.assertEqual(True, self.cmd.Push(name, path))
        os.remove(name)
        self.assertEqual(True, self.cmd.Pull(path, name))
        with open(name, 'rb') as f:
          self.assertEqual('Too', f.read())
      finally:
        try:
          os.remove(name)
        except OSError:  # pragma: no cover
          pass
    finally:
      # Cleanup
      out, exit_code = self.cmd.Shell('rm %s' % pipes.quote(path))
      self.assertEqual('', out)
      self.assertEqual(0, exit_code)

  def test_Root_Unroot(self):
    """Switches from root to user then to root again."""
    # This test is a bit intensive, adbd can take several seconds to restart
    # from user to root.
    self.safe()
    self.assertTrue(repr(self.cmd))

    try:
      if not self.cmd.IsRoot():
        self.assertEqual(True, self.cmd.Root())  # pragma: no cover

      self.assertEqual(True, self.cmd.IsRoot())
      self.assertEqual(True, self.cmd.Root())
      self.assertEqual(True, self.cmd.IsRoot())
      self.assertEqual(True, self.cmd.Unroot())
      self.assertEqual(False, self.cmd.IsRoot())
      self.assertEqual(True, self.cmd.Unroot())
      self.assertEqual(False, self.cmd.IsRoot())
      self.assertEqual(True, self.cmd.Root())
      self.assertEqual(True, self.cmd.IsRoot())
      self.assertEqual(True, self.cmd.Root())
      self.assertEqual(True, self.cmd.IsRoot())
    finally:
      # Restarting adbd changed the port path. Find it back by the serial
      # number.
      logging.info('Updating port path to %s', self.cmd.port_path)
      self.__class__.PORT_PATH = self.cmd.port_path

  def test_Shell_low(self):
    """Tests limits of Shell."""
    self.safe()
    cmd = 'echo i'
    while self.cmd.IsShellOk(cmd):
      cmd += 'i'
    cmd = cmd[:-1]
    actual, exit_code = self.cmd.Shell(cmd)
    self.assertEqual(cmd[5:] + '\n', actual)
    self.assertEqual(0, exit_code)

    with self.assertRaises(AssertionError):
      self.cmd.Shell(cmd + 'i')

    # Test a command that prints out large output (64kb).
    # - Bash syntax "{1..100}" is not supported.
    # - Was tested with an additional loop to generate 1Mb but it takes >4s to
    #   run.
    cmd = (
        'for x in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do '
        'for y in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do '
        'for y in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do '
        'echo iiiiiiiiiiiiiii; done; done; done')
    actual, exit_code = self.cmd.Shell(cmd)
    self.assertEqual( 'iiiiiiiiiiiiiii\n' * 4096, actual)
    self.assertEqual(0, exit_code)

  def test_Shell_high(self):
    """Tests limits of Shell."""
    self.high()
    cmd = 'echo i'
    while self.cmd._device.IsShellOk(cmd):
      cmd += 'i'
    # TODO(maruel): Update this invariant when found. It will likely be 262123.
    self.assertEqual(4075, len(cmd))

    # This transparently call WrappedShell().
    actual, exit_code = self.cmd.Shell(cmd)
    self.assertEqual(cmd[5:] + '\n', actual)
    self.assertEqual(0, exit_code)

    # This won't.
    cmd = cmd[:-1]
    actual, exit_code = self.cmd.Shell(cmd)
    self.assertEqual(cmd[5:] + '\n', actual)
    self.assertEqual(0, exit_code)

  def test_WrappedShell(self):
    """Works around shell length limitation due to packet size."""
    self.high()
    actual, exit_code = self.cmd.WrappedShell(['echo hi', 'echo bye'])
    self.assertEqual('hi\nbye\n', actual)
    self.assertEqual(0, exit_code)

  def test_Reboot(self):
    # Warning: this test is observed to take over 35 seconds.
    self.high()
    # Take uptime, reboot, ensure uptime is lower.
    uptime = self.cmd.GetUptime()
    logging.info('GetUptime() = %.2f seconds', uptime)
    self.assertGreater(uptime, 20.)
    start = time.time()
    try:
      actual = self.cmd.Reboot()
      logging.info('Reboot() took %.2f seconds', time.time() - start)
      self.assertEqual(True, actual)
      self.assertGreater(uptime, self.cmd.GetUptime())
    finally:
      # Restarting the Android device changed the port path. Find it back by the
      # serial number.
      logging.info('Updating port path to %s', self.cmd.port_path)
      self.__class__.PORT_PATH = self.cmd.port_path

  def test_cpu(self):
    """Adjust the CPU speed to power save then max speed then back to normal."""
    self.high()

    # Only one of these 2 scaling governor is supported, not both for one
    # kernel.
    unknown = {
        'conservative', 'interactive'} - set(self.cmd.cache.available_governors)
    self.assertEqual(1, len(unknown), unknown)
    unknown = unknown.pop()

    expected = {u'cur', u'governor'}
    previous = self.cmd.GetCPUScale()
    self.assertEqual(expected, set(previous))
    try:
      self.assertEqual(True, self.cmd.SetCPUScalingGovernor('powersave'))
      self.assertIn(
          self.cmd.GetCPUScale()['governor'], ('powersave', 'userspace'))

      self.assertEqual(
          True, self.cmd.SetCPUSpeed(self.cmd.cache.available_frequencies[0]))
      self.assertEqual('userspace', self.cmd.GetCPUScale()['governor'])
      with self.assertRaises(AssertionError):
        self.cmd.SetCPUSpeed(self.cmd.cache.available_frequencies[0]-1)

      self.assertEqual(False, self.cmd.SetCPUScalingGovernor(unknown))
      self.assertEqual(True, self.cmd.SetCPUScalingGovernor('ondemand'))
    finally:
      self.assertEqual(
          True, self.cmd.SetCPUScalingGovernor(previous['governor']))

  def test_stuff(self):
    # Tests a lot of small functions.
    self.high()
    self.assertTrue(self.cmd.serial)
    self.assertTrue(self.cmd.cache.external_storage_path.startswith('/'))
    self.assertIn(self.cmd.IsRoot(), (True, False))
    temps = self.cmd.GetTemperatures()
    self.assertEqual(2, len(temps))
    self.assertTrue(all(isinstance(t, int) for t in temps), temps)
    self.assertTrue(all(15 < t < 60 for t in temps))
    battery = self.cmd.GetBattery()
    self.assertIn(u'USB', battery['power'])
    self.assertGreater(self.cmd.GetUptime(), 10.)
    disks = self.cmd.GetDisk()
    self.assertGreater(disks['cache'], 10.)
    self.assertGreater(disks['data'], 10.)
    self.assertGreater(disks['system'], 10.)
    imei = self.cmd.GetIMEI()
    self.assertTrue(imei is None or len(imei) == 15, imei)
    self.assertTrue(re.match(r'^\d+\.\d+\.\d+\.\d+$', self.cmd.GetIP()))
    last_uid = self.cmd.GetLastUID()
    self.assertTrue(2000 < last_uid < 200000)
    apps = self.cmd.GetPackages()
    self.assertIn('com.google.android.email', apps)
    self.assertEqual(
        u'/system/app/PrebuiltEmailGoogle/PrebuiltEmailGoogle.apk',
        self.cmd.GetApplicationPath('com.google.android.email'))


def main():
  # First, find a device to test against if none is provided.
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument('--serial', help='Serial number of the device to use')
  parser.add_argument(
      '--path', help='USB path to the device to use, e.g. \'2/7\'')
  parser.add_argument('test_case', nargs='?')
  parser.add_argument('-v', '--verbose', action='count', default=0)
  args = parser.parse_args()

  level = [logging.ERROR, logging.INFO, logging.DEBUG][min(args.verbose, 2)]
  logging.basicConfig(
      level=level,
      format='%(asctime)s %(severity)s %(name)-7s: %(message)s')
  logging.getLogger().handlers[0].addFilter(Filter())
  adb_protocol._LOG.setLevel(level)
  adb_commands_safe._LOG.setLevel(level)
  common._LOG.setLevel(level)
  high._LOG.setLevel(level)

  Test.KEYS = high.Initialize(None, None)
  if not Test.KEYS:
    parser.error('Failed initializing adb keys')  # pragma: no cover

  if args.serial and args.path:  # pragma: no cover
    parser.error('Use exactly one of --serial or --usb-path')
  if not args.path:
    if not args.serial:
      args.path = GetOnlyDevice(parser.error)
    else:  # pragma: no cover
      args.path = FindDevice(parser.error, args.serial)

  Test.PORT_PATH = args.path
  argv = sys.argv[:1]
  if args.verbose:
    argv.append('-v')  # pragma: no cover
  if args.test_case:
    argv.append(args.test_case)  # pragma: no cover
  unittest.main(argv=argv)


if __name__ == '__main__':
  sys.exit(main())
