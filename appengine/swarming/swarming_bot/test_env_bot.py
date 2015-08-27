# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys


# swarming_bot/
BOT_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def init_symlinks(root):
  """Adds support for symlink-as-file on Windows.

  Manually resolves symlinks in path for directory and add them to sys.path.
  """
  if sys.platform != 'win32':
    return
  for i in os.listdir(root):
    if '.' in i:
      continue
    path = os.path.join(root, i)
    if os.path.isfile(path):
      # Found a file instead of a symlink to a directory. Adjust sys.path
      # accordingly to where the symlink points.
      with open(path) as f:
        link = f.read()
      if '\n' in link:
        continue
      dest = os.path.normpath(os.path.join(root, link))
      # This is not exactly right but close enough.
      sys.path.insert(0, os.path.dirname(dest))


def setup_test_env():
  """Sets up the environment for bot tests."""
  init_symlinks(BOT_DIR)
  client_tests = os.path.normpath(
      os.path.join(BOT_DIR, '..', '..', '..', 'client', 'tests'))
  sys.path.insert(0, client_tests)
  sys.path.insert(0, os.path.join(BOT_DIR, 'third_party'))
