# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def setup_test_env():
  """Sets up system path to allow importing the AE files."""
  # Add the tools directory to allow importing the app engine detector.
  TOOLS_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'tools')
  sys.path.insert(0, TOOLS_DIR)

  import find_gae_sdk  # pylint: disable=F0401
  gae_sdk = find_gae_sdk.find_gae_sdk()
  if not gae_sdk:
    raise Exception('Unable to find gae sdk path, aborting test.')

  find_gae_sdk.setup_gae_sdk(gae_sdk)
  find_gae_sdk.setup_env(ROOT_DIR, None, None, None)

  # Add the path for the third party code.
  sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))


setup_test_env()
