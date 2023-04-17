# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys

# /appengine/components
ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.realpath(os.path.abspath(__file__))))

_INITIALIZED = False


def setup_test_env(app_dir=None, app_id='sample-app'):
  """Sets up App Engine test environment."""
  global _INITIALIZED
  if _INITIALIZED:
    return
  _INITIALIZED = True

  # For depot_tools.
  sys.path.insert(
      0, os.path.join(ROOT_DIR, '..', '..', 'client', 'third_party'))

  # When testing a GAE app, the root app directory must have symlinks to
  # `test_support`, `tool_support` and `components` already. It is sufficient
  # just to add the root directory to sys.path.
  #
  # When running tests for components, use `appengine/components` (aka ROOT_DIR)
  # as the root. It has the same directories.
  sys.path.insert(0, app_dir or ROOT_DIR)

  # Import the rest of GAE packages bundled with dev appserver.
  from tool_support import gae_sdk_utils
  gae_sdk_utils.setup_gae_env()
  gae_sdk_utils.setup_env(None, app_id, 'v1a', None)

  from components import utils
  utils.import_third_party()
  utils.fix_protobuf_package()
