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

import logging
import sys
import unittest


from adb import high


class MockDevice(object):
  def __init__(self, cmds):
    super(MockDevice, self).__init__()
    self._cmds = cmds[:]
    self.port_path = (0, 0)

  def IsShellOk(self, cmd):  # pylint: disable=unused-argument
    return True

  def Shell(self, cmd):
    data = self._cmds.pop(0)
    assert data[0] == cmd, (data, cmd)
    return data[1], 0


RAW_IMEI = """Result: Parcel(
  0x00000000: 00000000 0000000f 00350033 00320035 '........3.5.5.2.'
  0x00000010: 00360033 00350030 00360038 00350038 '3.6.0.5.8.6.8.5.'
  0x00000020: 00390038 00000034                   '8.9.4...        ')
"""

class TestAndroid(unittest.TestCase):
  def test_GetIMEI(self):
    device = MockDevice(
        [
          ('dumpsys iphonesubinfo', ''),
          ('service call iphonesubinfo 1', RAW_IMEI),
        ])
    cache = high.DeviceCache(None, None, None, None, None, None)
    self.assertEqual(
        u'355236058685894', high.HighDevice(device, cache).GetIMEI())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None  # pragma: no cover
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
