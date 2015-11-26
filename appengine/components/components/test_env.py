# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

COMPONENTS_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up App Engine test environment."""
  sys.path.insert(0, os.path.join(COMPONENTS_DIR, 'third_party'))
  if COMPONENTS_DIR not in sys.path:
    sys.path.insert(0, COMPONENTS_DIR)

  from test_support import test_env
  test_env.setup_test_env()
