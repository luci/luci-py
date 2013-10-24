# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Finds AppEngine SDK."""

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

  with open(os.path.join(app_dir, 'app.yaml')) as f:
    return yaml.load(f)['application']


def get_app_modules(app_dir, module_files):
  """Returns a list of app module names (fetched from |module_files| yamls)."""
  import yaml

  modules = []
  for name in module_files:
    with open(os.path.join(app_dir, name)) as f:
      modules.append(yaml.load(f)['module'])
  return modules


def appcfg(app_dir, args, sdk_path, app_id=None, version=None, verbose=False):
  """Runs appcfg.py subcommand in |app_dir| and returns its exit code."""
  cmd = [
      sys.executable,
      os.path.join(sdk_path, 'appcfg.py'),
      '--oauth2',
      '--noauth_local_webserver',
  ]
  if version:
    cmd.extend(('--version', version))
  if app_id:
    cmd.extend(('--application', app_id))
  if verbose:
    cmd.append('--verbose')
  cmd.extend(args)
  return subprocess.call(cmd, cwd=app_dir)
