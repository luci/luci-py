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


def setup_env(app_dir, app_id, version, module_id):
  """Setups os.environ so GAE code works."""
  os.environ['SERVER_SOFTWARE'] = 'Development yo dawg/1.0'
  if app_dir:
    app_id = app_id or default_app_id(app_dir)
    version = version or default_version(app_dir, module_id)
  if app_id:
    os.environ['APPLICATION_ID'] = app_id
  if version:
    os.environ['CURRENT_VERSION_ID'] = str(version)
  if module_id:
    os.environ['CURRENT_MODULE_ID'] = module_id


def load_module_yaml(app_dir, module_id):
  import yaml
  using_module_id = module_id and module_id != 'default'
  if using_module_id:
    p = os.path.join(app_dir, 'module-%s.yaml' % module_id)
  else:
    p = os.path.join(app_dir, 'app.yaml')
  with open(p) as f:
    data = yaml.load(f)
    if using_module_id:
      assert data['module'] == module_id
    return data


def default_app_id(app_dir):
  """Returns the application name."""
  return load_module_yaml(app_dir, None)['application']


def default_version(app_dir, module_id=None):
  """Returns the application default version."""
  return load_module_yaml(app_dir, module_id)['version']


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
