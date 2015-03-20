# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

# /appengine/
ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.realpath(os.path.abspath(__file__))))

_INITIALIZED = False


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  global _INITIALIZED
  if _INITIALIZED:
    raise Exception('Do not call test_env.setup_test_env() twice.')
  _INITIALIZED = True

  # For 'from components import ...' and 'from test_support import ...'.
  sys.path.insert(0, ROOT_DIR)
  sys.path.insert(0, os.path.join(ROOT_DIR, 'components', 'third_party'))
  # For depot_tools/auto_stub.py.
  sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

  from tool_support import gae_sdk_utils
  gae_sdk_utils.setup_gae_env()
  gae_sdk_utils.setup_env(None, 'sample-app', 'v1a', None)
