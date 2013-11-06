# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
COMPONENTS_DIR = os.path.dirname(TESTS_DIR)
ROOT_DIR = os.path.dirname(COMPONENTS_DIR)

sys.path.insert(0, ROOT_DIR)
# For auto_stub.py
sys.path.insert(0, os.path.join(COMPONENTS_DIR, 'third_party'))
# For find_gae_sdk.py
sys.path.insert(0, os.path.join(ROOT_DIR, 'tools'))

import find_gae_sdk


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  gae_dir = find_gae_sdk.find_gae_sdk()
  find_gae_sdk.setup_gae_sdk(gae_dir)
  find_gae_sdk.setup_env(None, 'sample-app', 'v1a', None)
