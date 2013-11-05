# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(TESTS_DIR)

sys.path.insert(0, APP_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, 'third_party'))
sys.path.insert(0, os.path.join(APP_DIR, 'third_party'))
sys.path.insert(0, os.path.join(APP_DIR, 'tools'))

import find_gae_sdk  # pylint: disable=F0401


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  gae_dir = find_gae_sdk.find_gae_sdk()
  find_gae_sdk.setup_gae_sdk(gae_dir)
  find_gae_sdk.setup_env(APP_DIR, None, None, None)
