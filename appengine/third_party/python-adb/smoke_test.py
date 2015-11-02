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
import sys
import time
import unittest


import usb1


from adb import adb_commands
from adb import adb_protocol
from adb import common
from adb import usb_exceptions


class Filter(object):
  def filter(self, record):
    record.severity = record.levelname[0]
    return True



def get_usb_devices():
  handles = []
  # Warning: do not use timeout too low, it could make the USB enumeration fail.
  generator = common.UsbHandle.FindDevices(
      adb_commands.DeviceIsAvailable, timeout_ms=10000)
  while True:
    try:
      # Use manual iterator handling instead of "for handle in generator" to
      # catch USB exception explicitly.
      handle = generator.next()
    except usb1.USBErrorOther as e:
      logging.error(
          'Failed to open USB device, is user in group plugdev? %s', e)
      continue
    except StopIteration:
      break
    handles.append(handle)
  return handles


def open_device(port_path):
  # Intentionally use an overly long timeout to detect issues.
  start = time.time()
  handle = common.UsbHandle.Find(
      adb_commands.DeviceIsAvailable, port_path=port_path,
      timeout_ms=10000)
  logging.info('open_device(%s) took %.2fs', port_path, time.time() - start)
  return handle


def find_device(on_error, serial):
  if not serial:
    handles = get_usb_devices()
    if not handles:
      on_error('Please connect an Android device first!')
    if len(handles) > 1:
      print('Available devices:')
      for handle in handles:
        args.path = '/'.join(str(i) for i in handle.port_path)
        print('%s - %s' % (handle.serial_number, args.path ))
        handle.Close()
      on_error('Use exactly one of --serial or --usb-path')
    # Choose the single device connected directly.
    serial = handles[0].serial_number
    path = '/'.join(str(i) for i in handles[0].port_path)
    handles[0].Close()
    logging.info('Automatically selected %s : %s', path, serial)
    return path

  # Find the usb path for this device, so the next operations are done with
  # the usb path.
  for handle in get_usb_devices():
    if handle.serial_number == serial:
      path = '/'.join(str(i) for i in handle.port_path)
    handle.Close()
  if not path:
    parser.error('Didn\'t find device with serial %s' % args.serial)
  return path


def load_adb_keys(on_error, paths):
  try:
    import sign_m2crypto
    signer = sign_m2crypto.M2CryptoSigner
  except ImportError:
    try:
      import sign_pythonrsa
      class signer(sign_pythonrsa.PythonRSASigner):
        def __init__(self, path):
          with open(path + '.pub', 'rb') as f:
            public_key = f.read()
          with open(path, 'rb') as f:
            private_key = f.read()
          super(signer, self).__init__(public_key, private_key)
    except ImportError:
      on_error('Please install M2Crypto or python-rsa')

  try:
    return [signer(os.path.expanduser(path)) for path in paths]
  except IOError as e:
    # TODO(maruel): If ~/.android/adbkey doesn't exist, create a new pair.
    on_error('Failed to load keys: %s' % e)


class Test(unittest.TestCase):
  PATH = None
  KEYS = []
  SLEEP = 0.
  TRIES = 3

  def setUp(self):
    super(Test, self).setUp()
    self.cmd = None
    self._open()

  def tearDown(self):
    try:
      if self.cmd:
        self.cmd.Close()
    finally:
      super(Test, self).tearDown()

  def _open(self):
    """Opens the device and ensures it's running as root."""
    userid = self._inner_open()
    # Start each test case with adbd in root mode.
    if userid == 'uid=2000(shell)':
      self.switch_root()

  def _inner_open(self):
    """Opens the device and queries the current userid."""
    if self.cmd:
      self.cmd.Close()
      self.cmd = None
    for i in xrange(self.TRIES):
      try:
        handle = open_device(self.PATH)
      except usb_exceptions.DeviceNotFoundError as e:
        if i == self.TRIES - 1:
          raise
        logging.info('Retrying open_device() due to %s', e)
        self._sleep()
        continue

      # Assert that the serial_number is readable, that is, the device can be
      # accessed.
      try:
        handle.serial_number
      except usb1.USBErrorNoDevice as e:
        if i == self.TRIES - 1:
          raise
        logging.info('Retrying handle.serial_number due to %s', e)
        self._sleep()
        continue

      try:
        start = time.time()
        handle.Open()
        logging.info('handle.Open() took %.1fs', time.time() - start)
      except usb1.USBErrorBusy as e:
        if i == self.TRIES - 1:
          raise
        logging.info('Retrying Open() due to %s', e)
        self._sleep()
        continue

      try:
        start = time.time()
        self.cmd = adb_commands.AdbCommands.Connect(
            handle, rsa_keys=self.KEYS, auth_timeout_ms=60000)
        logging.info('Connect() took %.1fs', time.time() - start)
      except IOError as e:
        self.cmd = None
        handle.Close()
        if i == self.TRIES - 1:
          raise
        logging.info('Retrying Connect() due to %s', e)
        self._sleep()
        continue

      userid = self.cmd.Shell('id').split(' ', 1)[0]
      self.assertIn(userid, ('uid=2000(shell)', 'uid=0(root)'))
      return userid

  def switch_root(self):
    """Switches adbd to run as root.

    Asserts that adbd was not running as root.
    """
    logging.debug('switch_root()')
    for i in xrange(min(3, self.TRIES)):
      try:
        #self.assertIn(self.cmd.Root(), ('restarting adbd as root\n', ''))
        self.assertIn(
            self.cmd.Root(), ('restarting adbd as root\n', '', 'adbd is already running as root\n'))
      except usb_exceptions.LibusbWrappingError as e:
        if isinstance(e.usb_error, (usb1.USBErrorIO, usb1.USBErrorNoDevice)):
          # TODO(maruel): This is great engineering. What happens is that if we
          # reopen the device too fast, we could get a hold of the old adbd
          # process while it is shutting down. This is bad because it may accept
          # the USB connection but will likely ignore part of our messages,
          # causing a read timeout.
          self._sleep()
          # Reopen the device.
          self._inner_open()
        else:
          raise

      for i in xrange(self.TRIES):
        try:
          self.assertEqual('uid=0(root)', self.cmd.Shell('id').split(' ', 1)[0])
          return
        except usb_exceptions.LibusbWrappingError as e:
          self._sleep()

  def switch_user(self):
    """Downgrades adbd from root context to user.

    Asserts that adbd was running as root.
    """
    logging.debug('switch_user()')
    # It's defined in the adb code but doesn't work on 4.4.
    #self.assertEqual('', self.cmd.conn.Command(service='unroot'))
    # It is expected that that reply may not be read since adbd is experiencing
    # a race condition.
    self.assertEqual(
        '',
        self.cmd.Shell('setprop service.adb.root 0; setprop ctl.restart adbd'))
    while True:
      try:
        if 'uid=2000(shell)' == self.cmd.Shell('id').split(' ', 1)[0]:
          break
      except usb_exceptions.WriteFailedError as e:
        if isinstance(e.usb_error, usb1.USBErrorNoDevice):
          # TODO(maruel): This is great engineering. What happens is that if we
          # reopen the device too fast, we could get a hold of the old adbd
          # process while it is shutting down. This is bad because it may accept
          # the USB connection but will likely ignore part of our messages,
          # causing a read timeout.
          self._sleep()
          # Reopen the device.
          self._inner_open()
        else:
          raise
    self.assertEqual('uid=2000(shell)', self.cmd.Shell('id').split(' ', 1)[0])

  def _sleep(self):
    if self.SLEEP:
      logging.debug('sleep(%s)', self.SLEEP)
      time.sleep(self.SLEEP)

  def test_restart(self):
    """Switches from root to user then to root again."""
    # This test is a bit intensive, adbd can take several seconds to restart
    # from user to root.
    self.assertEqual('adbd is already running as root\n', self.cmd.Root())

    self.switch_user()
    self.switch_root()
    self.assertIn(self.cmd.Root(), ('adbd is already running as root\n', ''))
    self.assertEqual('uid=0(root)', self.cmd.Shell('id').split(' ', 1)[0])

  def test_io(self):
    """Pushes and fetch a file."""
    path = '/storage/emulated/legacy/yo'
    self.cmd.Push(cStringIO.StringIO('Yo'), path)
    self.assertEqual('Yo', self.cmd.Pull(path))
    # Large file of 1Mb.
    large = '0' * 1024 * 1024
    self.cmd.Push(cStringIO.StringIO(large), path)
    self.assertEqual(large, self.cmd.Pull(path))
    self.cmd.Shell('rm %s' % pipes.quote(path))


def main():
  # First, find a device to test against if none is provided.
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument('--serial', help='Serial number of the device to use')
  parser.add_argument(
      '--path', help='USB path to the device to use, e.g. \'2/7\'')
  parser.add_argument(
      '--key', action='append', default=['~/.android/adbkey'])
  parser.add_argument(
      '--sleep', type=float, default=0.2,
      help='Seconds to sleep between retries. No sleeping or long sleeping '
           'will trigger different edge casees')
  parser.add_argument(
      '--tries', type=int, default=20,
      help='Number of tries when opening a device')
  parser.add_argument('test_case', nargs='?')
  parser.add_argument('-v', '--verbose', action='count', default=0)
  args = parser.parse_args()
  level = [logging.ERROR, logging.INFO, logging.DEBUG][min(args.verbose, 2)]
  logging.basicConfig(
      level=level,
      format='%(asctime)s %(severity)s %(name)-7s: %(message)s')
  logging.getLogger().handlers[0].addFilter(Filter())
  adb_protocol._LOG.setLevel(level)
  keys = load_adb_keys(parser.error, args.key)

  if args.tries < 1:
    parser.error('--tries must be >= 1')
  if args.sleep < 0.:
    parser.error('--sleep must be >= 0.0')
  if args.serial and args.path:
    parser.error('Use exactly one of --serial or --usb-path')
  if not args.path:
    args.path = find_device(parser.error, args.serial)

  # It's a bit cheezy but #goodenough.
  Test.PATH = args.path
  Test.KEYS = keys
  Test.SLEEP = args.sleep
  Test.TRIES = args.tries
  argv = sys.argv[:1]
  if args.verbose:
    argv.append('-v')
  if args.test_case:
    argv.append(args.test_case)
  unittest.main(argv=argv)


if __name__ == '__main__':
  sys.exit(main())
