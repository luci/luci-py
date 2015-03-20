# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest


# config/
APP_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
# config/components/third_party
THIRD_PARTY = os.path.join(APP_DIR, 'components', 'third_party')


def setup_test_env():
  """Sets up App Engine test environment."""
  sys.path.insert(0, APP_DIR)

  from test_support import test_env
  test_env.setup_test_env()

  sys.path.insert(0, THIRD_PARTY)

  from components import utils
  utils.fix_protobuf_package()


def main():
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
