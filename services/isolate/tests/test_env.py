# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
# isolate/
APP_DIR = os.path.dirname(TESTS_DIR)
# components/
COMPONENTS_DIR = os.path.join(os.path.dirname(APP_DIR), 'components')

_INITIALIZED = False


def setup_test_env():
  """Sets up App Engine test environment."""
  global _INITIALIZED
  if _INITIALIZED:
    raise Exception('Do not call test_env.setup_test_env() twice.')
  _INITIALIZED = True

  # For find_gae_sdk.py and test_case.py.
  sys.path.insert(0, os.path.join(COMPONENTS_DIR, 'tools'))

  import find_gae_sdk  # pylint: disable=F0401
  gae_sdk_dir = find_gae_sdk.find_gae_sdk()
  if not gae_sdk_dir:
    raise Exception('Unable to find gae sdk path, aborting test.')

  find_gae_sdk.setup_gae_sdk(gae_sdk_dir)
  find_gae_sdk.setup_env(APP_DIR, None, None, None)

  # For webtest, depot_tools/auto_stub.py and friends.
  sys.path.insert(0, os.path.join(COMPONENTS_DIR, 'third_party'))
  # For unit tests not importing main.py, which should be ALL unit tests.
  sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))
  # For cloudstorage and mapreduce.
  sys.path.insert(0, os.path.join(APP_DIR, 'third_party'))
  # For application modules.
  sys.path.insert(0, APP_DIR)
