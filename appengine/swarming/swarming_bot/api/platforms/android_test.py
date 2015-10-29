#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

import android


class MockDevice(object):
  def __init__(self, cmds):
    super(MockDevice, self).__init__()
    self._cmds = cmds[:]

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
    cache = android.DeviceCache(None, None, None, None, None, None)
    self.assertEqual(
        u'355236058685894',
        android.HighDevice(device, cache, []).GetIMEI())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
