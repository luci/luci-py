# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys


# swarming_bot/
BOT_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up the environment for bot tests."""
  # Small hack to make it work on Windows even without symlink support. This is
  # the only code here that needs to run on Windows.
  for i in ('third_party', 'utils'):
    path = os.path.join(BOT_DIR, i)
    if os.path.isfile(path):
      with open(path) as f:
        link = f.read().strip()
      sys.path.insert(0, os.path.join(BOT_DIR, link))

  client_tests = os.path.join(BOT_DIR, '..', '..', '..', 'client', 'tests')
  sys.path.insert(0, client_tests)
  sys.path.insert(0, os.path.join(BOT_DIR, 'third_party'))
