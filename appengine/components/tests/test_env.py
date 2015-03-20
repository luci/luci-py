# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# For 'from components import ...' and 'from tool_support import ...'.
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'components', 'third_party'))
# For depot_tools/auto_stub.py.
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

from tool_support import gae_sdk_utils


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  gae_dir = gae_sdk_utils.find_gae_sdk()
  gae_sdk_utils.setup_gae_sdk(gae_dir)
  gae_sdk_utils.setup_env(None, 'sample-app', 'v1a', None)
