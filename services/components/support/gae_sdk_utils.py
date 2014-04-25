# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Finds AppEngine SDK."""

import glob
import os
import subprocess
import sys
import time

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


def find_module_yamls(app_dir):
  """Searches |app_dir| for YAML files with module definitions.

  Returns a list of absolute paths to the yamls (app.yaml in front).
  """
  if not is_application_directory(app_dir):
    raise ValueError('Not a GAE application directory: %s' % app_dir)
  yamls = [os.path.join(app_dir, 'app.yaml')]
  yamls.extend(glob.glob(os.path.join(app_dir, 'module-*.yaml')))
  return yamls


def is_application_directory(app_dir):
  """Returns True if |app_dir| is a directory with app.yaml inside."""
  return os.path.isfile(os.path.join(app_dir, 'app.yaml'))


def setup_gae_sdk(sdk_path):
  """Sets up App Engine environment.

  Then any AppEngine included module can be imported. The change is global and
  permanent.
  """
  sys.path.insert(0, sdk_path)
  # Sadly, coverage may inject google.protobuf in the path. Forcibly expulse it.
  if 'google' in sys.modules:
    del sys.modules['google']

  import dev_appserver
  dev_appserver.fix_sys_path()


def setup_env(app_dir, app_id, version, module_id, remote_api=False):
  """Setups os.environ so GAE code works."""
  # GCS library behaves differently when running under remote_api. It uses
  # SERVER_SOFTWARE to figure this out. See cloudstorage/common.py, local_run().
  if remote_api:
    os.environ['SERVER_SOFTWARE'] = 'remote_api'
  else:
    os.environ['SERVER_SOFTWARE'] = 'Development yo dawg/1.0'
  if app_dir:
    app_id = app_id or default_app_id(app_dir)
    version = version or default_version(app_dir, module_id)
  if app_id:
    os.environ['APPLICATION_ID'] = app_id
  if version:
    os.environ['CURRENT_VERSION_ID'] = '%s.%d' % (
        version, int(time.time()) << 28)
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


def get_app_modules(app_dir):
  """Returns a list of app module names."""
  import yaml

  modules = []
  for yaml_path in find_module_yamls(app_dir):
    with open(yaml_path) as f:
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
