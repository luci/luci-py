# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Set of functions to work with GAE SDK tools."""

import collections
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time


# 'setup_gae_sdk' loads 'yaml' module and modifies this variable.
yaml = None

# Directory with this file.
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))

# Name of a directory with Python GAE SDK.
PYTHON_GAE_SDK = 'google_appengine'

# Name of a directory with Go GAE SDK.
GO_GAE_SDK = 'go_appengine'

# Value of 'runtime: ...' in app.yaml -> SDK to use.
RUNTIME_TO_SDK = {
  'go': GO_GAE_SDK,
  'python27': PYTHON_GAE_SDK,
}

# Path to a current SDK, set in setup_gae_sdk, accessible by gae_sdk_path.
_GAE_SDK_PATH = None


class Error(Exception):
  """Base class for a fatal error."""


class BadEnvironmentError(Error):
  """Raised when required tools or environment are missing."""


class UnsupportedModuleError(Error):
  """Raised when trying to deploy MVM or Flex module."""


class LoginRequiredError(Error):
  """Raised by Application methods if use has to go through login flow."""


def find_gcloud():
  """Searches for 'gcloud' binary in PATH and returns absolute path to it.

  Raises BadEnvironmentError error if it's not there.
  """
  for path in os.environ['PATH'].split(os.pathsep):
    exe_file = os.path.join(path, 'gcloud')
    if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
      return exe_file
  raise BadEnvironmentError(
      'Can\'t find "gcloud" in PATH. Install the Google Cloud SDK from '
      'https://cloud.google.com/sdk/')


def find_gae_sdk(sdk_name=PYTHON_GAE_SDK, search_dir=TOOLS_DIR):
  """Returns the path to GAE SDK if found, else None."""
  # First search up the directories up to root.
  while True:
    attempt = os.path.join(search_dir, sdk_name)
    if os.path.isfile(os.path.join(attempt, 'dev_appserver.py')):
      return attempt
    prev_dir = search_dir
    search_dir = os.path.dirname(search_dir)
    if search_dir == prev_dir:
      break
  # Next search PATH.
  markers = ['dev_appserver.py']
  if sdk_name == GO_GAE_SDK:
    markers.append('goroot')
  for item in os.environ['PATH'].split(os.pathsep):
    if not item:
      continue
    item = os.path.normpath(os.path.abspath(item))
    if all(os.path.exists(os.path.join(item, m)) for m in markers):
      return item
  return None


def find_app_yamls(app_dir):
  """Searches for app.yaml and module-*.yaml in app_dir or its subdirs.

  Recognizes Python and Go GAE apps.

  Returns:
    List of abs path to module yamls.

  Raises:
    ValueError if not a valid GAE app.
  """
  # Look in the root first. It's how python apps and one-module Go apps look.
  yamls = []
  app_yaml = os.path.join(app_dir, 'app.yaml')
  if os.path.isfile(app_yaml):
    yamls.append(app_yaml)
  yamls.extend(glob.glob(os.path.join(app_dir, 'module-*.yaml')))
  if yamls:
    return yamls

  # Look in per-module subdirectories. Only Go apps are structured that way.
  # See https://cloud.google.com/appengine/docs/go/#Go_Organizing_Go_apps.
  for subdir in os.listdir(app_dir):
    subdir = os.path.join(app_dir, subdir)
    if not os.path.isdir(subdir):
      continue
    app_yaml = os.path.join(subdir, 'app.yaml')
    if os.path.isfile(app_yaml):
      yamls.append(app_yaml)
    yamls.extend(glob.glob(os.path.join(subdir, 'module-*.yaml')))
  if not yamls:
    raise ValueError(
        'Not a GAE application directory, no module *.yamls found: %s' %
        app_dir)

  # There should be one and only one app.yaml.
  app_yamls = [p for p in yamls if os.path.basename(p) == 'app.yaml']
  if not app_yamls:
    raise ValueError(
        'Not a GAE application directory, no app.yaml found: %s' % app_dir)
  if len(app_yamls) > 1:
    raise ValueError(
        'Not a GAE application directory, multiple app.yaml found (%s): %s' %
        (app_yamls, app_dir))
  return yamls


def is_app_dir(path):
  """Returns True if |path| is structure like GAE app directory."""
  try:
    find_app_yamls(path)
    return True
  except ValueError:
    return False


def get_module_runtime(yaml_path):
  """Finds 'runtime: ...' property in module YAML (or None if missing)."""
  # 'yaml' module is not available yet at this point (it is loaded from SDK).
  with open(yaml_path, 'rt') as f:
    m = re.search(r'^runtime\:\s+(.*)$', f.read(), re.MULTILINE)
  return m.group(1) if m else None


def get_app_runtime(yaml_paths):
  """Examines all app's yamls making sure they specify single runtime.

  Raises:
    ValueError if multiple (or unknown) runtimes are specified.
  """
  runtimes = sorted(set(get_module_runtime(p) for p in yaml_paths))
  if len(runtimes) != 1:
    raise ValueError('Expecting single runtime, got %s' % ', '.join(runtimes))
  if runtimes[0] not in RUNTIME_TO_SDK:
    raise ValueError('Unknown runtime \'%s\' in %s' % (runtimes[0], yaml_paths))
  return runtimes[0]


def setup_gae_sdk(sdk_path):
  """Modifies sys.path and to be able to use Python portion of GAE SDK.

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
  for i in sys.path[:]:
    if 'jinja2-2.6' in i:
      sys.path.remove(i)

  # Make 'yaml' variable (defined on top of this module) point to loaded module.
  global yaml
  import yaml as yaml_module
  yaml = yaml_module


def gae_sdk_path():
  """Checks that 'setup_gae_sdk' was called and returns a path to GAE SDK."""
  if not _GAE_SDK_PATH:
    raise ValueError('setup_gae_sdk wasn\'t called')
  return _GAE_SDK_PATH


ModuleFile = collections.namedtuple('ModuleFile', ['path', 'data'])


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

    self._app_dir = os.path.abspath(app_dir)
    self._app_id = app_id
    self._verbose = verbose

    # Module ID -> (path to YAML, deserialized content of module YAML).
    self._modules = {}
    for yaml_path in find_app_yamls(self._app_dir):
      with open(yaml_path) as f:
        data = yaml.load(f)
        module_id = data.get('service', data.get('module', 'default'))
        if module_id in self._modules:
          raise ValueError(
              'Multiple *.yaml files define same module %s: %s and %s' %
              (module_id, yaml_path, self._modules[module_id].path))
        self._modules[module_id] = ModuleFile(yaml_path, data)

    self.dispatch_yaml = os.path.join(app_dir, 'dispatch.yaml')
    if not os.path.isfile(self.dispatch_yaml):
      self.dispatch_yaml= None

    if 'default' not in self._modules:
      raise ValueError('Default module is missing')
    if not self.app_id:
      raise ValueError('application id is neither specified in default module, '
                       'nor provided explicitly')

  @property
  def app_dir(self):
    """Absolute path to application directory."""
    return self._app_dir

  @property
  def app_id(self):
    """Application ID as passed to constructor, or as read from app.yaml."""
    return self._app_id or self._modules['default'].data.get('application')

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

  @property
  def default_module_dir(self):
    """Absolute path to a directory with app.yaml of the default module.

    It's different from app_dir for Go apps. dev_appserver.py searches for
    cron.yaml, index.yaml etc in this directory.
    """
    return os.path.dirname(self._modules['default'].path)

  def run_cmd(self, cmd, cwd=None):
    """Runs subprocess, capturing the output.

    Doesn't close stdin, since gcloud may be asking for user input. If this is
    undesirable (e.g when gae.py is used from scripts), close 'stdin' of gae.py
    process itself.
    """
    logging.debug('Running %s', cmd)
    proc = subprocess.Popen(
        cmd,
        cwd=cwd or self._app_dir,
        stdout=subprocess.PIPE)
    output, _ = proc.communicate()
    if proc.returncode:
      sys.stderr.write('\n' + output + '\n')
      raise subprocess.CalledProcessError(proc.returncode, cmd, output)
    return output

  def run_appcfg(self, args):
    """Runs appcfg.py <args>, deserializes its output and returns it."""
    if not is_gcloud_oauth2_token_cached():
      raise LoginRequiredError('Login first using \'gcloud auth login\'.')
    cmd = [
      sys.executable,
      os.path.join(gae_sdk_path(), 'appcfg.py'),
      '--application', self.app_id,
    ]
    if self._verbose:
      cmd.append('--verbose')
    cmd.extend(args)
    return yaml.safe_load(self.run_cmd(cmd))

  def run_gcloud(self, args):
    """Runs gcloud <args>."""
    gcloud = find_gcloud()
    if not is_gcloud_oauth2_token_cached():
      raise LoginRequiredError('Login first using \'gcloud auth login\'')
    return self.run_cmd([gcloud] + args)

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
    """Deploys new version of the given module names.

    Supports deploying modules both with Managed VMs and AppEngine v1 runtime.
    """
    mods = []
    try:
      for m in sorted(modules or self.modules):
        mod = self._modules[m]
        if mod.data.get('vm'):
          raise UnsupportedModuleError('MVM is not supported: %s' % m)
        if mod.data.get('env') == 'flex':
          raise UnsupportedModuleError('Flex is not supported yet: %s' % m)
        if mod.data.get('runtime') == 'go' and not os.environ.get('GOROOT'):
          raise BadEnvironmentError('GOROOT must be set when deploying Go app')
        mods.append(mod)
    except KeyError as e:
      raise ValueError('Unknown module: %s' % e)
    # Always make 'default' the first module to be uploaded.
    mods.sort(key=lambda x: '' if x == 'default' else x)
    self.run_appcfg(
        ['update'] + [m.path for m in mods] + ['--version', version])

  def update_indexes(self):
    """Deploys new index.yaml."""
    if os.path.isfile(os.path.join(self.default_module_dir, 'index.yaml')):
      self.run_appcfg(['update_indexes', self.default_module_dir])

  def update_queues(self):
    """Deploys new queue.yaml."""
    if os.path.isfile(os.path.join(self.default_module_dir, 'queue.yaml')):
      self.run_appcfg(['update_queues', self.default_module_dir])

  def update_cron(self):
    """Deploys new cron.yaml."""
    if os.path.isfile(os.path.join(self.default_module_dir, 'cron.yaml')):
      self.run_appcfg(['update_cron', self.default_module_dir])

  def update_dispatch(self):
    """Deploys new dispatch.yaml."""
    if self.dispatch_yaml:
      self.run_appcfg(['update_dispatch', self.app_dir])

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
      '--application', self.app_id,
      '--skip_sdk_update_check=yes',
      '--require_indexes=yes',
    ] + self.module_yamls
    if self.dispatch_yaml:
      cmd += [self.dispatch_yaml]
    cmd += args
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

  def get_actives(self, modules=None):
    """Returns active version(s)."""
    args = [
      'app', 'versions', 'list',
      '--project', self.app_id,
      '--format', 'json',
      '--hide-no-traffic',
    ]
    raw = self.run_gcloud(args)
    try:
      data = json.loads(raw)
    except ValueError:
      sys.stderr.write('Failed to decode %r as JSON\n' % raw)
      raise
    # TODO(maruel): Handle when traffic_split != 1.0.
    # TODO(maruel): There's a lot more data, decide what is generally useful in
    # there.
    return [
      {
        'creationTime': service['version']['createTime'],
        'deployer': service['version']['createdBy'],
        'id': service['id'],
        'service': service['service'],
      } for service in data if not modules or service['service'] in modules
    ]


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


def add_sdk_options(parser, app_dir=None):
  """Adds common command line options used by tools that wrap GAE SDK.

  Args:
    parser: OptionParser to add options to.
    app_dir: default value for --app-dir option.
  """
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  if not app_dir:
    parser.add_option(
        '-p', '--app-dir',
        default=app_dir,
        help='Path to application directory with app.yaml.')
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

  if not app_dir and not options.app_dir:
    parser.error('--app-dir option is required')
  app_dir = os.path.abspath(app_dir or options.app_dir)

  try:
    runtime = get_app_runtime(find_app_yamls(app_dir))
  except (Error, ValueError) as exc:
    parser.error(str(exc))

  sdk_path = options.sdk_path or find_gae_sdk(RUNTIME_TO_SDK[runtime], app_dir)
  if not sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  setup_gae_sdk(sdk_path)

  try:
    return Application(app_dir, options.app_id, options.verbose)
  except (Error, ValueError) as e:
    parser.error(str(e))


def confirm(text, app, version, modules=None, default_yes=False):
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
  if default_yes:
    return raw_input('Continue? [Y/n] ') not in ('n', 'N')
  else:
    return raw_input('Continue? [y/N] ') in ('y', 'Y')


def is_gcloud_oauth2_token_cached():
  """Returns false if 'gcloud auth login' needs to be run."""
  p = os.path.join(os.path.expanduser('~'), '.config', 'gcloud', 'credentials')
  try:
    with open(p) as f:
      return len(json.load(f)['data']) != 0
  except (KeyError, IOError, OSError, ValueError):
    return False


def setup_gae_env():
  """Sets up App Engine Python test environment."""
  sdk_path = find_gae_sdk(PYTHON_GAE_SDK)
  if not sdk_path:
    raise BadEnvironmentError('Couldn\'t find GAE SDK.')
  setup_gae_sdk(sdk_path)
