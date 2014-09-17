# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

# swarming/
APP_DIR = os.path.dirname(os.path.abspath(__file__))
# components/ (NOT swarming/components/).
COMPONENTS_DIR = os.path.join(os.path.dirname(APP_DIR), 'components')

_INITIALIZED = False


def setup_test_env_base():
  """Sets up a base environment which doesn't require AppEngine."""
  global _INITIALIZED
  if _INITIALIZED:
    raise Exception('Do not call test_env.setup_test_env() twice.')
  _INITIALIZED = True

  # For 'from support import ...'
  sys.path.insert(0, COMPONENTS_DIR)
  # For dependencies of support code.
  sys.path.insert(0, os.path.join(COMPONENTS_DIR, 'third_party'))


def setup_test_env():
  """Sets up App Engine test environment."""
  setup_test_env_base()

  # For unit tests not importing main.py, which should be ALL unit tests.
  sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))
  # For application modules.
  sys.path.insert(0, APP_DIR)

  from support import gae_sdk_utils
  gae_sdk_dir = gae_sdk_utils.find_gae_sdk()
  if not gae_sdk_dir:
    raise Exception('Unable to find gae sdk path, aborting test.')

  gae_sdk_utils.setup_gae_sdk(gae_sdk_dir)
  gae_sdk_utils.setup_env(APP_DIR, None, None, None)
