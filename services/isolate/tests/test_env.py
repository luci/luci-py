# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
# isolate/
APP_DIR = os.path.dirname(TESTS_DIR)
ROOT_DIR = os.path.dirname(APP_DIR)

sys.path.insert(0, APP_DIR)
# For webtest, depot_tools/auto_stub.py and friends.
sys.path.insert(0, os.path.join(ROOT_DIR, 'tools', 'third_party'))
# For find_gae_sdk.py and test_case.py
sys.path.insert(0, os.path.join(ROOT_DIR, 'tools'))
# For unit tests not importing main.py, which should be ALL unit tests.
sys.path.insert(0, os.path.join(APP_DIR, 'third_party'))

import find_gae_sdk


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  gae_dir = find_gae_sdk.find_gae_sdk()
  find_gae_sdk.setup_gae_sdk(gae_dir)
  find_gae_sdk.setup_env(APP_DIR, None, None, None)
