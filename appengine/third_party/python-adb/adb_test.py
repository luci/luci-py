#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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
"""Tests for adb."""

import cStringIO
import logging
import struct
import unittest
import sys


import common_mock


from adb import adb_commands
from adb import adb_protocol


BANNER = 'blazetest'
LOCAL_ID = 16
REMOTE_ID = 2


def _ConvertCommand(command):
  return sum(ord(c) << (i * 8) for i, c in enumerate(command))


def _MakeHeader(command, arg0, arg1, data):
  command = _ConvertCommand(command)
  magic = command ^ 0xFFFFFFFF
  checksum = adb_protocol._CalculateChecksum(data)
  return struct.pack('<6I', command, arg0, arg1, len(data), checksum, magic)


def _MakeSyncHeader(command, *int_parts):
  command = _ConvertCommand(command)
  return struct.pack('<%dI' % (len(int_parts) + 1), command, *int_parts)


def _MakeWriteSyncPacket(command, data='', size=None):
  return _MakeSyncHeader(command, size or len(data)) + data


class BaseAdbTest(unittest.TestCase):

  def setUp(self):
    super(BaseAdbTest, self).setUp()
    self.usb = common_mock.MockUsb()

  def tearDown(self):
    try:
      self.usb.Close()
    finally:
      super(BaseAdbTest, self).tearDown()

  def _ExpectWrite(self, command, arg0, arg1, data):
    self.usb.ExpectWrite(_MakeHeader(command, arg0, arg1, data))
    self.usb.ExpectWrite(data)
    if command == 'WRTE':
      self._ExpectRead('OKAY', REMOTE_ID, LOCAL_ID)

  def _ExpectRead(self, command, arg0, arg1, data=''):
    self.usb.ExpectRead(_MakeHeader(command, arg0, arg1, data))
    if data:
      self.usb.ExpectRead(data)
    if command == 'WRTE':
      self._ExpectWrite('OKAY', LOCAL_ID, REMOTE_ID, '')

  def _ExpectConnection(self):
    self._ExpectWrite('CNXN', 0x01000000, 256*1024, 'host::%s\0' % BANNER)
    self._ExpectRead('CNXN', 0x01000000, 4096, 'device::\0')

  def _ExpectOpen(self, service):
    self._ExpectWrite('OPEN', LOCAL_ID, 0, service)
    self._ExpectRead('OKAY', REMOTE_ID, LOCAL_ID)

  def _ExpectClose(self):
    self._ExpectRead('CLSE', REMOTE_ID, LOCAL_ID)
    # TODO(maruel): The new adb_protocol doesn't bother sending a CLSE back.
    # self._ExpectWrite('CLSE', LOCAL_ID, REMOTE_ID, '')

  def _Connect(self):
    return adb_commands.AdbCommands.Connect(
        self.usb, BANNER, rsa_keys=[], auth_timeout_ms=0)


class AdbTest(BaseAdbTest):

  def _ExpectCommand(self, service, command, *responses):
    self._ExpectConnection()
    self._ExpectOpen('%s:%s\0' % (service, command))
    for response in responses:
      self._ExpectRead('WRTE', REMOTE_ID, LOCAL_ID, response)
    self._ExpectClose()

  def testSmallResponseShell(self):
    command = 'keepin it real'
    response = 'word.'
    self._ExpectCommand('shell', command, response)

    cmd = self._Connect()
    self.assertEqual(response, cmd.Shell(command))
    cmd.Close()

  def testBigResponseShell(self):
    command = 'keepin it real big'
    responses = ['other stuff, ', 'and some words.'] * 50
    self._ExpectCommand('shell', command, *responses)

    cmd = self._Connect()
    self.assertEqual(''.join(responses), cmd.Shell(command))
    cmd.Close()

  def testStreamingResponseShell(self):
    command = 'keepin it real big'
    # expect multiple lines
    responses = ['other stuff, ', 'and some words.']
    self._ExpectCommand('shell', command, *responses)

    cmd = self._Connect()
    actual = ''.join(cmd.StreamingShell(command))
    self.assertEqual(''.join(responses), actual)
    cmd.Close()

  def testReboot(self):
    self._ExpectCommand('reboot', '', '')
    cmd = self._Connect()
    cmd.Reboot()
    cmd.Close()

  def testRebootBootloader(self):
    self._ExpectCommand('reboot', 'bootloader', '')
    cmd = self._Connect()
    cmd.RebootBootloader()
    cmd.Close()

  def testRemount(self):
    self._ExpectCommand('remount', '', '')
    cmd = self._Connect()
    cmd.Remount()
    cmd.Close()

  def testRoot(self):
    self._ExpectCommand('root', '', '')
    cmd = self._Connect()
    cmd.Root()
    cmd.Close()


class FilesyncAdbTest(BaseAdbTest):

  def _ExpectClose(self):
    # TODO(maruel): There's an inconsistency between sync protocol and raw adb
    # protocol.
    self._ExpectWrite('CLSE', LOCAL_ID, REMOTE_ID, '')
    self._ExpectRead('CLSE', REMOTE_ID, LOCAL_ID)

  def _ExpectSyncCommand(self, write_commands, read_commands):
    self._ExpectConnection()
    self._ExpectOpen('sync:\0')
    while write_commands or read_commands:
      if write_commands:
        command = write_commands.pop(0)
        self._ExpectWrite('WRTE', LOCAL_ID, REMOTE_ID, command)
      if read_commands:
        command = read_commands.pop(0)
        self._ExpectRead('WRTE', REMOTE_ID, LOCAL_ID, command)

    self._ExpectClose()

  def testPush(self):
    filedata = 'alo there, govnah'
    mtime = 100
    send = [
        _MakeWriteSyncPacket('SEND', '/data,33272'),
        _MakeWriteSyncPacket('DATA', filedata),
        _MakeWriteSyncPacket('DONE', size=mtime),
    ]
    data = 'OKAY\0\0\0\0'

    self._ExpectSyncCommand([''.join(send)], [data])
    self._Connect().Push(cStringIO.StringIO(filedata), '/data', mtime=mtime)

  def testPull(self):
    filedata = "g'ddayta, govnah"
    recv = _MakeWriteSyncPacket('RECV', '/data')
    data = [
        _MakeWriteSyncPacket('DATA', filedata),
        _MakeWriteSyncPacket('DONE'),
    ]

    self._ExpectSyncCommand([recv], [''.join(data)])
    self.assertEqual(filedata, self._Connect().Pull('/data'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    logging.basicConfig(level=logging.DEBUG)  # pragma: no cover
    adb_protocol._LOG.setLevel(logging.DEBUG)  # pragma: no cover
  else:
    logging.basicConfig(level=logging.ERROR)
  unittest.main()
