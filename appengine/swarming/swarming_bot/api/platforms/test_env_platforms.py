# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys


# swarming_bot/api/
API_DIR = os.path.dirname(
    os.path.dirname(os.path.realpath(os.path.abspath(__file__))))


def setup_test_env():
  """Sets up the environment for bot tests."""
  sys.path.insert(0, API_DIR)
  import test_env_api
  test_env_api.setup_test_env()
