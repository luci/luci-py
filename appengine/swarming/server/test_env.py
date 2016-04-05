# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up App Engine test environment."""
  # For application modules.
  sys.path.insert(0, APP_DIR)

  from test_support import test_env
  test_env.setup_test_env()
