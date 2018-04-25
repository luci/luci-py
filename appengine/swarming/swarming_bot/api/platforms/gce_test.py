#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub

# Disable caching before importing gce.
from utils import tools
tools.cached = lambda func: func

import gce


class TestGCE(auto_stub.TestCase):
  def test_get_zones(self):
    self.mock(gce, 'get_zone', lambda: 'us-central2-a')
    self.assertEqual(
        ['us', 'us-central', 'us-central2', 'us-central2-a'], gce.get_zones())
    self.mock(gce, 'get_zone', lambda: 'europe-west1-b')
    self.assertEqual(
        ['europe', 'europe-west', 'europe-west1', 'europe-west1-b'],
        gce.get_zones())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
