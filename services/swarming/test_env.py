# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)

sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))


def setup_test_env():
  """Sets up system path to allow importing the AE files."""
  # For tools/find_gae_sdk.py and test_case.py.
  sys.path.insert(0, os.path.join(ROOT_DIR, 'tools'))

  import find_gae_sdk  # pylint: disable=F0401
  gae_sdk = find_gae_sdk.find_gae_sdk()
  if not gae_sdk:
    raise Exception('Unable to find gae sdk path, aborting test.')

  find_gae_sdk.setup_gae_sdk(gae_sdk)
  find_gae_sdk.setup_env(APP_DIR, None, None, None)

  # For depot_tools/auto_stub.py, bs4 and webtest.
  sys.path.insert(0, os.path.join(ROOT_DIR, 'tools', 'third_party'))


setup_test_env()
