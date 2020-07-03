#!/usr/bin/env vpython3
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

from api import platforms


@unittest.skipIf(sys.platform == 'win32', 'Tests only run under posix platform')
class TestPosix(unittest.TestCase):

  def test_get_disk_info(self):
    disks = platforms.posix.get_disks_info()
    self.assertGreater(len(disks), 0)
    for _, info in disks.items():
      self.assertGreater(info['free_mb'], 0)
      self.assertGreater(info['size_mb'], 0)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
