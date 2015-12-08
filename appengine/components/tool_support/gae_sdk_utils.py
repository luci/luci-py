# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Set of functions to work with GAE SDK tools."""

import collections
import getpass
import glob
import hashlib
import logging
import os
import re
import socket
import subprocess
import sys
import time

try:
  # https://pypi.python.org/pypi/keyring
  import keyring
except ImportError:
  keyring = None

try:
  # Keyring doesn't has native "unlock keyring" support.
  import gnomekeyring
except ImportError:
  gnomekeyring = None

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

# Exe name => instructions how to install it.
KNOWN_TOOLS = {
  'gcloud':
      'Download and install the Google Cloud SDK from '
      'https://cloud.google.com/sdk/',
  'aedeploy':
      'Install with: go install '
      'google.golang.org/appengine/cmd/aedeploy',
}


# Path to a current SDK, set in setup_gae_sdk, accessible by gae_sdk_path.
_GAE_SDK_PATH = None


class BadEnvironmentConfig(Exception):
  """Raised when required tools or environment are missing."""


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


def find_app_runtime_and_yamls(app_dir):
  """Searches for app.yaml and module-*.yaml in app_dir or its subdirs.

  Recognizes Python and Go GAE apps.

  Returns:
    (app runtime e.g. 'python27', list of abs path to module yamls).

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
    return get_app_runtime(yamls), yamls

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
  if yamls:
    rt = get_app_runtime(yamls)
    if rt != 'go':
      raise ValueError(
          'Per-module directories imply "go" runtime, got "%s" instead' % rt)
    return rt, yamls

  raise ValueError(
      'Not a GAE application directory, no module *.yamls found: %s' % app_dir)


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


class LoginRequiredError(Exception):
  """Raised by Application methods if use has to go through login flow."""


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
    for yaml_path in find_app_runtime_and_yamls(self._app_dir)[1]:
      with open(yaml_path) as f:
        data = yaml.load(f)
        module_id = data.get('module', 'default')
        if module_id in self._modules:
          raise ValueError(
              'Multiple *.yaml files define same module %s: %s and %s' %
              (module_id, yaml_path, self._modules[module_id].path))
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

  @property
  def default_module_dir(self):
    """Absolute path to a directory with app.yaml of the default module.

    It's different from app_dir for Go apps. dev_appserver.py searches for
    cron.yaml, index.yaml etc in this directory.
    """
    return os.path.dirname(self._modules['default'].path)

  def login(self):
    """Runs OAuth2 login flow and returns true on success."""
    # HACK: Call a command with no side effect to launch the flow.
    cmd = [
      sys.executable,
      os.path.join(gae_sdk_path(), 'appcfg.py'),
      '--application', self.app_id,
      '--noauth_local_webserver',
      'list_versions',
    ]
    return subprocess.call(cmd, cwd=self._app_dir) == 0

  def run_cmd(self, cmd, cwd=None):
    """Runs subprocess, capturing the output."""
    logging.debug('Running %s', cmd)
    proc = subprocess.Popen(
        cmd,
        cwd=cwd or self._app_dir,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE)
    output, _ = proc.communicate(None)
    if proc.returncode:
      sys.stderr.write('\n' + output + '\n')
      raise RuntimeError('Call %s failed with code %d' % (cmd, proc.returncode))
    return output

  def run_appcfg(self, args):
    """Runs appcfg.py <args>, deserializes its output and returns it."""
    if not is_oauth_token_cached():
      raise LoginRequiredError()
    cmd = [
      sys.executable,
      os.path.join(gae_sdk_path(), 'appcfg.py'),
      '--application', self.app_id,
    ]
    if self._verbose:
      cmd.append('--verbose')
    cmd.extend(args)
    return yaml.safe_load(self.run_cmd(cmd))

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
    reg_modules = []
    mvm_modules = []
    try:
      for m in sorted(modules or self.modules):
        mod = self._modules[m]
        if mod.data.get('vm'):
          mvm_modules.append(mod)
        else:
          reg_modules.append(mod)
        if mod.data.get('runtime') == 'go' and not os.environ.get('GOROOT'):
          raise BadEnvironmentConfig('GOROOT must be set when deploying Go app')
    except KeyError as e:
      raise ValueError('Unknown module: %s' % e)
    if reg_modules:
      self.run_appcfg(
          ['update'] + [m.path for m in reg_modules] + ['--version', version])
    # Go modules have to be deployed one at a time, based on docs.
    for m in mvm_modules:
      self.deploy_mvm_module(m, version)

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

  def deploy_mvm_module(self, mod, version):
    """Uses gcloud to upload MVM module using remote docker build.

    Assumes 'gcloud' and 'aedeploy' are in PATH.
    """
    # 'aedeploy' requires cwd to be set to module path.
    check_tool_in_path('gcloud')
    cmd = [
      'gcloud', 'preview', 'app', 'deploy', os.path.basename(mod.path),
      '--project', self.app_id,
      '--version', version,
      '--docker-build', 'remote',
      '--no-promote', '--force',
    ]
    if self._verbose:
      cmd.extend(['--verbosity', 'debug'])
    if mod.data.get('runtime') == 'go':
      check_tool_in_path('aedeploy')
      cmd = ['aedeploy'] + cmd
    self.run_cmd(cmd, cwd=os.path.dirname(mod.path))


def check_tool_in_path(tool):
  """Raises BadEnvironmentConfig error if no such executable in PATH."""
  for path in os.environ['PATH'].split(os.pathsep):
    exe_file = os.path.join(path, tool)
    if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
      return
  msg = 'Can\'t find "%s" in PATH.' % tool
  if tool in KNOWN_TOOLS:
    msg += ' ' + KNOWN_TOOLS[tool]
  raise BadEnvironmentConfig(msg)


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

  if not app_dir and not options.app_dir:
    parser.error('--app-dir option is required')
  app_dir = os.path.abspath(app_dir or options.app_dir)

  try:
    runtime = find_app_runtime_and_yamls(app_dir)[0]
  except (BadEnvironmentConfig, ValueError) as exc:
    parser.error(str(exc))

  sdk_path = options.sdk_path or find_gae_sdk(RUNTIME_TO_SDK[runtime], app_dir)
  if not sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  setup_gae_sdk(sdk_path)

  try:
    return Application(app_dir, options.app_id, options.verbose)
  except (BadEnvironmentConfig, ValueError) as e:
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


def is_oauth_token_cached():
  """True if appcfg.py should be able to use OAuth2 without user interaction."""
  return os.path.exists(
      os.path.join(os.path.expanduser('~'), '.appcfg_oauth2_tokens'))


def get_authentication_function(bucket='gae_sdk_utils'):
  """Returns a function that ask user for email/password.

  If keyring module is present, keyring will be used to remember the password.

  Args:
    bucket: a string that defines where to store the password in keyring,
        ignored if keyring is not supported.

  Returns:
    Function that accepts no arguments and returns tuple (email, password).
  """
  if keyring:
    auth = KeyringAuth('%s:%s' % (bucket, socket.getfqdn()))
  else:
    auth = DefaultAuth()
  return auth.auth_func


class DefaultAuth(object):
  """Simple authentication support based on getpass.getpass().

  Used by 'get_authentication_function' internally.
  """

  def __init__(self):
    self._last_key = os.environ.get('EMAIL_ADDRESS')

  def _retrieve_password(self):  # pylint: disable=R0201
    return getpass.getpass('Please enter the password: ' )

  def auth_func(self):
    if self._last_key:
      result = raw_input('Username (default: %s): ' % self._last_key)
      if result:
        self._last_key = result
    else:
      self._last_key = raw_input('Username: ')
    return self._last_key, self._retrieve_password()


class KeyringAuth(DefaultAuth):
  """Enhanced password support based on keyring.

  Used by 'get_authentication_function' internally.
  """

  def __init__(self, bucket):
    super(KeyringAuth, self).__init__()
    self._bucket = bucket
    self._unlocked = False
    self._keys = set()

  def _retrieve_password(self):
    """Returns the password for the corresponding key.

    'key' is retrieved from self._last_key.

    The key is salted and hashed so the direct mapping between an email address
    and its password is not stored directly in the keyring.
    """
    assert keyring, 'Requires keyring module'

    # Create a salt out of the bucket name.
    salt = hashlib.sha512('1234' + self._bucket + '1234').digest()
    # Create a salted hash from the key.
    key = self._last_key
    actual_key = hashlib.sha512(salt + key).hexdigest()

    if key in self._keys:
      # It was already tried. Clear up keyring if any value was set.
      try:
        logging.info('Clearing password for key %s (%s)', key, actual_key)
        keyring.set_password(self._bucket, actual_key, '')
      except Exception as e:
        print >> sys.stderr, 'Failed to erase in keyring: %s' % e
    else:
      self._keys.add(key)
      try:
        logging.info('Getting password for key %s (%s)', key, actual_key)
        value = keyring.get_password(self._bucket, actual_key)
        if value:
          return value
      except Exception as e:
        print >> sys.stderr, 'Failed to get password from keyring: %s' % e
        if self._unlock_keyring():
          # Unlocking worked, try getting the password again.
          try:
            value = keyring.get_password(self._bucket, actual_key)
            if value:
              return value
          except Exception as e:
            print >> sys.stderr, 'Failed to get password from keyring: %s' % e

    # At this point, it failed to get the password from keyring. Ask the user.
    value = super(KeyringAuth, self)._retrieve_password()

    answer = raw_input('Store password in system keyring (y/N)? ').strip()
    if answer == 'y':
      try:
        logging.info('Saving password for key %s (%s)', key, actual_key)
        keyring.set_password(self._bucket, actual_key, value)
      except Exception as e:
        print >> sys.stderr, 'Failed to save in keyring: %s' % e

    return value

  @staticmethod
  def _unlock_keyring():
    """Tries to unlock the gnome keyring. Returns True on success."""
    if not os.environ.get('DISPLAY') or not gnomekeyring:
      return False
    try:
      gnomekeyring.unlock_sync(None, getpass.getpass('Keyring password: '))
      return True
    except Exception as e:
      print >> sys.stderr, 'Failed to unlock keyring: %s' % e
      return False


def setup_gae_env():
  """Sets up App Engine Python test environment."""
  sdk_path = find_gae_sdk(PYTHON_GAE_SDK)
  if not sdk_path:
    raise BadEnvironmentConfig('Couldn\'t find GAE SDK.')
  setup_gae_sdk(sdk_path)
