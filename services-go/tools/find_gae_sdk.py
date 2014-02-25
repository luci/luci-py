# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Finds Golang AppEngine SDK."""

import os
import subprocess
import sys
import time

# Directory with this file.
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))


def find_go_gae_sdk(search_dir=TOOLS_DIR):
  """Returns the path to Golang GAE SDK if found, else None."""
  # First search up the directories up to root.
  while True:
    attempt = os.path.join(search_dir, 'go_appengine')
    if os.path.isfile(os.path.join(attempt, 'goroot', 'bin', 'go-app-builder')):
      return attempt
    prev_dir = search_dir
    search_dir = os.path.dirname(search_dir)
    if search_dir == prev_dir:
      break

  # Next search PATH.
  for item in os.environ['PATH'].split(os.pathsep):
    if not item:
      continue
    attempt = os.path.normpath(os.path.abspath(item))
    if os.path.isfile(os.path.join(attempt, 'goroot', 'bin', 'go-app-builder')):
      return item


def run(cmd):
  gae_sdk = find_go_gae_sdk()
  if not gae_sdk:
    print >> sys.stderr, 'Failed to find Golang GAE SDK'
    return 1
  return subprocess.call([os.path.join(gae_sdk, cmd[0])] + cmd[1:])
