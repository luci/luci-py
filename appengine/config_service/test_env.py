# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import sys
import unittest


# config/
APP_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up App Engine test environment."""
  from test_support import test_env
  test_env.setup_test_env(APP_DIR)


def main():
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()


def future(result):
  # Assume setup_test_env was called.
  from google.appengine.ext import ndb
  f = ndb.Future()
  f.set_result(result)
  return f
