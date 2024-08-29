#!/usr/bin/env vpython3
# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env_platforms

test_env_platforms.setup_test_env()

from depot_tools import auto_stub
from utils import tools

import aix


class TestCPUInfo(auto_stub.TestCase):

  def setUp(self):
    super(TestCPUInfo, self).setUp()
    tools.clear_cache_all()

  def tearDown(self):
    super(TestCPUInfo, self).tearDown()
    tools.clear_cache_all()

  def test_get_cpuinfo_power8(self):
    self.mock(aix, '_getsystemcfg', lambda x: 0x10000)
    self.assertEqual({'name': 'POWER8'}, aix.get_cpuinfo())

  def test_get_cpuinfo_power9(self):
    self.mock(aix, '_getsystemcfg', lambda x: 0x20000)
    self.assertEqual({'name': 'POWER9'}, aix.get_cpuinfo())

  def test_get_cpuinfo_power10(self):
    self.mock(aix, '_getsystemcfg', lambda x: 0x40000)
    self.assertEqual({'name': 'POWER10'}, aix.get_cpuinfo())

  def test_get_cpuinfo_unknown(self):
    self.mock(aix, '_getsystemcfg', lambda x: 0x00000)
    self.assertEqual({}, aix.get_cpuinfo())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
