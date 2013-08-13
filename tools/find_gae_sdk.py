# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Finds AppEngine SDK."""

import os
import sys

# pylint doesn't know where the AppEngine SDK is, so silence these errors.
# F0401: Unable to import 'XXX'
# E0611: No name 'XXX' in module 'YYY'
# pylint: disable=E0611,F0401


def find_gae_sdk(search_dir):
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


def setup_gae_sdk(sdk_path):
  """Sets up App Engine environment.

  Then any AppEngine included module can be imported. The change is global and
  permanent.
  """
  sys.path.insert(0, sdk_path)

  import dev_appserver
  dev_appserver.fix_sys_path()


def default_app_id(app_dir):
  """Returns the application name."""
  import yaml

  return yaml.load(open(os.path.join(app_dir, 'app.yaml')))['application']
