# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os

# auth_service/
APP_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up App Engine test environment."""
  from test_support import test_env
  test_env.setup_test_env(APP_DIR)
