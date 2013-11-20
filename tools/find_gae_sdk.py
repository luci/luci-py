# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Finds AppEngine SDK.

This was copied from the isolate-server checkout.
"""

import os
import subprocess
import sys

# Directory with this file.
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))


def find_gae_sdk(search_dir=TOOLS_DIR):
  """Returns the path to GAE SDK if found, else None."""
  # First search up the directories up to root.
  while True:
    attempt = os.path.join(search_dir, 'google_appengine')
    if os.path.isfile(os.path.join(attempt, 'dev_appserver.py')):
      return attempt
    prev_dir = search_dir
    search_dir = os.path.dirname(search_dir)
    if search_dir == prev_dir:
      break

  # Next search PATH.
  for item in os.environ['PATH'].split(os.pathsep):
    if not item:
      continue
    item = os.path.normpath(os.path.abspath(item))
    if os.path.isfile(os.path.join(item, 'dev_appserver.py')):
      return item


def find_gae_dev_server(search_dir=TOOLS_DIR):
  """Returns the path to the GAE dev server if found, else None."""
  gae_sdk = find_gae_sdk(search_dir)
  if gae_sdk:
    return os.path.join(gae_sdk, 'dev_appserver.py')



def setup_gae_sdk(sdk_path):
  """Sets up App Engine environment.

  Then any AppEngine included module can be imported. The change is global and
  permanent.
  """
  sys.path.insert(0, sdk_path)

  import dev_appserver
  dev_appserver.fix_sys_path()
