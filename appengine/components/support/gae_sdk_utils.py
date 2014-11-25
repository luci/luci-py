# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Set of functions to work with GAE SDK tools."""

import collections
import glob
import logging
import os
import subprocess
import sys
import time

# 'setup_gae_sdk' loads 'yaml' module and modifies this variable.
yaml = None

# Directory with this file.
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to a current SDK, set in setup_gae_sdk, accessible by gae_sdk_path.
_GAE_SDK_PATH = None


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
  """Modifies sys.path and other global process state to be able to use GAE SDK.

  Once this is called, other functions from this module know where to find GAE
  SDK and any AppEngine included module can be imported. The change is global
  and permanent.
  """
  global _GAE_SDK_PATH
  if _GAE_SDK_PATH:
    raise ValueError('setup_gae_sdk was already called.')
  _GAE_SDK_PATH = sdk_path

  sys.path.insert(0, sdk_path)
  # Sadly, coverage may inject google.protobuf in the path. Forcibly expulse it.
  if 'google' in sys.modules:
    del sys.modules['google']

  import dev_appserver
  dev_appserver.fix_sys_path()

  # Make 'yaml' variable (defined on top of this module) point to loaded module.
  global yaml
  import yaml as yaml_module
  yaml = yaml_module


def gae_sdk_path():
  """Checks that 'setup_gae_sdk' was called and returns a path to GAE SDK."""
  if not _GAE_SDK_PATH:
    raise ValueError('setup_gae_sdk wasn\'t called')
  return _GAE_SDK_PATH


class Application(object):
  """Configurable GAE application.

  Can be used to query and change GAE application configuration (default
  serving version, uploaded versions, etc.). Built on top of appcfg.py calls.
  """

  def __init__(self, app_dir, app_id=None, verbose=False):
    """Args:
      app_dir: application directory (should contain app.yaml).
      app_id: application ID to use, or None to use one from app.yaml.
      verbose: if True will run all appcfg.py operations in verbose mode.
    """
    if not _GAE_SDK_PATH:
      raise ValueError('Call setup_gae_sdk first')
    if not os.path.isfile(os.path.join(app_dir, 'app.yaml')):
      raise ValueError('Not a GAE application directory: %s' % app_dir)

    self._app_dir = os.path.abspath(app_dir)
    self._app_id = app_id
    self._verbose = verbose

    # Module ID -> (path to YAML, deserialized content of module YAML).
    self._modules = {}

    ModuleFile = collections.namedtuple('ModuleFile', ['path', 'data'])

    yamls = [os.path.join(self._app_dir, 'app.yaml')]
    yamls.extend(glob.glob(os.path.join(self._app_dir, 'module-*.yaml')))

    for yaml_path in yamls:
      with open(yaml_path) as f:
        data = yaml.load(f)
        module_id = data.get('module', 'default')
        if module_id in self._modules:
          raise ValueError(
              'Multiple *.yaml files define same module %s' % module_id)
        self._modules[module_id] = ModuleFile(yaml_path, data)

    if 'default' not in self._modules:
      raise ValueError('Default module is missing')

  @property
  def app_dir(self):
    """Absolute path to application directory."""
    return self._app_dir

  @property
  def app_id(self):
    """Application ID as passed to constructor, or as read from app.yaml."""
    return self._app_id or self._modules['default'].data['application']

  @property
  def modules(self):
    """List of module IDs that this application contain."""
    return self._modules.keys()

  @property
  def module_yamls(self):
    """List of paths to all module YAMLs (include app.yaml as a first item)."""
    # app.yaml first (correspond to 'default' module), then everything else.
    yamls = self._modules.copy()
    return [yamls.pop('default').path] + [m.path for m in yamls.itervalues()]

  def run_appcfg(self, args):
    """Runs appcfg.py <args>, deserializes its output and returns it."""
    cmd = [
      sys.executable,
      os.path.join(gae_sdk_path(), 'appcfg.py'),
      '--application', self.app_id,
      '--oauth2',
      '--noauth_local_webserver',
    ]
    if self._verbose:
      cmd.append('--verbose')
    cmd.extend(args)

    logging.debug('Running %s', cmd)
    proc = subprocess.Popen(
        cmd, cwd=self._app_dir, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output, _ = proc.communicate()

    if proc.returncode:
      raise RuntimeError(
          '\'appcfg.py %s\' failed with exit code %d' % (
          args[0], proc.returncode))

    return yaml.safe_load(output)

  def list_versions(self):
    """List all uploaded versions.

    Returns:
      Dict {module name -> [list of uploaded versions]}.
    """
    return self.run_appcfg(['list_versions'])

  def set_default_version(self, version, modules=None):
    """Switches default version of given |modules| to |version|."""
    self.run_appcfg([
      'set_default_version',
      '--module', ','.join(sorted(modules or self.modules)),
      '--version', version,
    ])

  def delete_version(self, version, modules=None):
    """Deletes the specified version of the given module names."""
    # For some reason 'delete_version' call processes only one module at a time,
    # unlike all other related appcfg.py calls.
    for module in sorted(modules or self.modules):
      self.run_appcfg([
        'delete_version',
        '--module', module,
        '--version', version,
      ])

  def update_modules(self, version, modules=None):
    """Deploys new version of the given module names."""
    modules = modules or self.modules
    try:
      yamls = sorted(self._modules[m].path for m in modules)
    except KeyError as e:
      raise ValueError('Unknown module: %s' % e)
    return self.run_appcfg(
        ['update'] + yamls + ['--version', version])

  def update_indexes(self):
    """Deploys new index.yaml."""
    self.run_appcfg(['update_indexes', '.'])

  def update_queues(self):
    """Deploys new queues.yaml."""
    self.run_appcfg(['update_queues', '.'])

  def update_cron(self):
    """Deploys new cron.yaml."""
    self.run_appcfg(['update_cron', '.'])

  def spawn_dev_appserver(self, args, open_ports=False, **kwargs):
    """Launches subprocess with dev_appserver.py.

    Args:
      args: extra arguments to dev_appserver.py.
      open_ports: if True will bind TCP ports to 0.0.0.0 interface.
      kwargs: passed as is to subprocess.Popen.

    Returns:
      Instance of subprocess.Popen.
    """
    cmd = [
      sys.executable,
      os.path.join(gae_sdk_path(), 'dev_appserver.py'),
      '--skip_sdk_update_check=yes',
      '--require_indexes=yes',
    ] + self.module_yamls + args
    if open_ports:
      cmd.extend(('--host', '0.0.0.0', '--admin_host', '0.0.0.0'))
    if self._verbose:
      cmd.extend(('--log_level', 'debug'))
    return subprocess.Popen(cmd, cwd=self.app_dir, **kwargs)

  def run_dev_appserver(self, args, open_ports=False):
    """Runs the application locally via dev_appserver.py.

    Args:
      args: extra arguments to dev_appserver.py.
      open_ports: if True will bind TCP ports to 0.0.0.0 interface.

    Returns:
      dev_appserver.py exit code.
    """
    return self.spawn_dev_appserver(args, open_ports).wait()

  def get_uploaded_versions(self, modules=None):
    """Returns list of versions that are deployed to all given |modules|.

    If a version is deployed only to one module, it won't be listed. Versions
    are sorted by a version number, oldest first.
    """
    # Build a mapping: version -> list of modules that have it.
    versions = collections.defaultdict(list)
    for module, version_list in self.list_versions().iteritems():
      for version in version_list:
        versions[version].append(module)

    # Keep only versions that are deployed to all requested modules.
    modules = modules or self.modules
    actual_versions = [
      version for version, modules_with_it in versions.iteritems()
      if set(modules_with_it).issuperset(modules)
    ]

    # Sort by version number (best effort, nonconforming version names will
    # appear first in the list).
    def extract_version_num(version):
      try:
        return int(version.split('-', 1)[0])
      except ValueError:
        return -1
    return sorted(actual_versions, key=extract_version_num)


def setup_env(app_dir, app_id, version, module_id, remote_api=False):
  """Setups os.environ so GAE code works."""
  # GCS library behaves differently when running under remote_api. It uses
  # SERVER_SOFTWARE to figure this out. See cloudstorage/common.py, local_run().
  if remote_api:
    os.environ['SERVER_SOFTWARE'] = 'remote_api'
  else:
    os.environ['SERVER_SOFTWARE'] = 'Development yo dawg/1.0'
  if app_dir:
    app_id = app_id or Application(app_dir).app_id
    version = version or 'default-version'
  if app_id:
    os.environ['APPLICATION_ID'] = app_id
  if version:
    os.environ['CURRENT_VERSION_ID'] = '%s.%d' % (
        version, int(time.time()) << 28)
  if module_id:
    os.environ['CURRENT_MODULE_ID'] = module_id


def app_sdk_options(parser, app_dir=None):
  """Adds common command line options used by tools that wrap GAE SDK.

  Args:
    parser: OptionParser to add options to.
    app_dir: if give, --app-dir option won't be added, and passed directory will
        be used to locate app.yaml instead.
  """
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  if not app_dir:
    parser.add_option(
        '-p', '--app-dir', help='Path to application directory with app.yaml.')
  parser.add_option('-A', '--app-id', help='Defaults to name in app.yaml.')
  parser.add_option('-v', '--verbose', action='store_true')


def process_sdk_options(parser, options, app_dir):
  """Handles values of options added by 'add_sdk_options'.

  Modifies global process state by configuring logging and path to GAE SDK.

  Args:
    parser: OptionParser instance to use to report errors.
    options: parsed options, as returned by parser.parse_args.
    app_dir: path to application directory to use by default.

  Returns:
    New instance of Application configured based on passed options.
  """
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  sdk_path = options.sdk_path or find_gae_sdk()
  if not sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  setup_gae_sdk(sdk_path)

  if not app_dir and not options.app_dir:
    parser.error('--app-dir option is required')
  app_dir = os.path.abspath(app_dir or options.app_dir)

  try:
    return Application(app_dir, options.app_id, options.verbose)
  except ValueError as e:
    parser.error(str(e))


def confirm(text, app, version, modules=None):
  """Asks a user to confirm the action related to GAE app.

  Args:
    text: actual text of the prompt.
    app: instance of Application.
    version: version or a list of versions to operate upon.
    modules: list of modules to operate upon (or None for all).

  Returns:
    True on approval, False otherwise.
  """
  print(text)
  print('  Directory: %s' % os.path.basename(app.app_dir))
  print('  App ID:    %s' % app.app_id)
  print('  Version:   %s' % version)
  print('  Modules:   %s' % ', '.join(modules or app.modules))
  return raw_input('Continue? [y/N] ') in ('y', 'Y')
