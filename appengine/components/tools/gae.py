#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Wrapper around GAE SDK tools to simplify working with multi module apps."""

__version__ = '1.0'

import atexit
import code
import optparse
import os
import signal
import sys
import tempfile
import urllib2

try:
  import readline
except ImportError:
  readline = None

# In case gae.py was run via symlink, find the original file since it's where
# third_party libs are. Handle a chain of symlinks too.
SCRIPT_PATH = os.path.abspath(__file__)
IS_SYMLINKED = False
while True:
  try:
    SCRIPT_PATH = os.path.abspath(
        os.path.join(os.path.dirname(SCRIPT_PATH), os.readlink(SCRIPT_PATH)))
    IS_SYMLINKED = True
  except OSError:
    break

ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, '..', 'third_party_local'))

import colorama
from depot_tools import subcommand

from tool_support import gae_sdk_utils
from tools import calculate_version


@subcommand.usage('[version_id version_id ...]')
def CMDcleanup(parser, args):
  """Removes old versions of GAE application modules.

  Removes the specified versions from all app modules. If no versions are
  provided via command line, will ask interactively.

  When asking interactively, uses EDITOR environment variable to edit the list
  of versions. Otherwise uses notepad.exe on Windows, or vi otherwise.
  """
  parser.add_force_option()
  parser.allow_positional_args = True
  app, options, versions_to_remove = parser.parse_args(args)

  if not versions_to_remove:
    # List all deployed versions, dump them to a temp file to be edited.
    versions = app.get_uploaded_versions()
    fd, path = tempfile.mkstemp()
    atexit.register(lambda: os.remove(path))
    with os.fdopen(fd, 'w') as f:
      header = (
        '# Remove lines that correspond to versions\n'
        '# you\'d like to delete from \'%s\'.\n')
      f.write(header % app.app_id + '\n'.join(versions) + '\n')

    # Let user remove versions that are no longer needed.
    editor = os.environ.get(
        'EDITOR', 'notepad.exe' if sys.platform == 'win32' else 'vi')
    exit_code = os.system('%s %s' % (editor, path))
    if exit_code:
      print('Aborted.')
      return exit_code

    # Read back the file that now contains only versions to keep.
    keep = []
    with open(path, 'r') as f:
      for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
          continue
        if line not in versions:
          print >> sys.stderr, 'Unknown version: %s' % line
          return 1
        if line not in keep:
          keep.append(line)

    # Calculate a list of versions to remove.
    versions_to_remove = [v for v in versions if v not in keep]
    if not versions_to_remove:
      print('Nothing to do.')
      return 0

  # Deleting a version is a destructive operation, confirm.
  if not options.force:
    ok = gae_sdk_utils.confirm(
        'Delete the following versions?', app, versions_to_remove)
    if not ok:
      print('Aborted.')
      return 1

  for version in versions_to_remove:
    print('Deleting %s...' % version)
    app.delete_version(version)

  return 0


@subcommand.usage('[extra arguments for dev_appserver.py]')
def CMDdevserver(parser, args):
  """Runs the app locally via dev_appserver.py."""
  parser.allow_positional_args = True
  parser.disable_interspersed_args()
  parser.add_option(
      '-o', '--open', action='store_true',
      help='Listen to all interfaces (less secure)')
  app, options, args = parser.parse_args(args)
  # Let dev_appserver.py handle Ctrl+C interrupts.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  return app.run_dev_appserver(args, options.open)


def CMDlogin(parser, args):
  """Initiates OAuth2 login flow if cached OAuth2 token is missing."""
  app, _, _ = parser.parse_args(args)
  return int(not gae_sdk_utils.is_oauth_token_cached() and not app.login())


@subcommand.usage('[module_id version_id]')
def CMDshell(parser, args):
  """Opens interactive remote shell with app's GAE environment.

  Connects to a specific version of a specific module (an active version of
  'default' module by default). The app must have 'remote_api: on' builtin
  enabled in app.yaml.

  Always uses password based authentication.
  """
  parser.allow_positional_args = True
  parser.add_option(
      '-H', '--host', help='Only necessary if not hosted on .appspot.com')
  parser.add_option(
      '--local', action='store_true',
      help='Operates locally on an empty dev instance')
  app, options, args = parser.parse_args(args)

  module = 'default'
  version = None
  if len(args) == 2:
    module, version = args
  elif len(args) == 1:
    module = args[0]
  elif args:
    parser.error('Unknown args: %s' % args)

  if module not in app.modules:
    parser.error('No such module: %s' % module)

  if not options.host and not options.local:
    prefixes = filter(None, (version, module, app.app_id))
    options.host = '%s.appspot.com' % '-dot-'.join(prefixes)

  # Ensure remote_api is initialized and GAE sys.path is set.
  gae_sdk_utils.setup_env(
      app.app_dir, app.app_id, version, module, remote_api=True)

  if options.host:
    # Open the connection.
    from google.appengine.ext.remote_api import remote_api_stub
    try:
      print('Connecting...')
      remote_api_stub.ConfigureRemoteApi(
          None,
          '/_ah/remote_api',
          gae_sdk_utils.get_authentication_function(),
          options.host,
          save_cookies=True,
          secure=True)
    except urllib2.URLError:
      print >> sys.stderr, 'Failed to access %s' % options.host
      return 1
    remote_api_stub.MaybeInvokeAuthentication()

  def register_sys_path(*path):
    abs_path = os.path.abspath(os.path.join(*path))
    if os.path.isdir(abs_path) and not abs_path in sys.path:
      sys.path.insert(0, abs_path)

  # Simplify imports of app modules (with dependencies). This code is optimized
  # for layout of apps that use 'components'.
  register_sys_path(app.app_dir)
  register_sys_path(app.app_dir, 'third_party')
  register_sys_path(app.app_dir, 'components', 'third_party')

  # Import some common modules into interactive console namespace.
  def setup_context():
    # pylint: disable=unused-variable
    from google.appengine.api import app_identity
    from google.appengine.api import memcache
    from google.appengine.api import urlfetch
    from google.appengine.ext import ndb
    return locals().copy()
  context = setup_context()

  # Fancy readline support.
  if readline is not None:
    readline.parse_and_bind('tab: complete')
    history_file = os.path.expanduser(
        '~/.config/gae_tool/remote_api_%s' % app.app_id)
    if not os.path.exists(os.path.dirname(history_file)):
      os.makedirs(os.path.dirname(history_file))
    atexit.register(lambda: readline.write_history_file(history_file))
    if os.path.exists(history_file):
      readline.read_history_file(history_file)

  prompt = [
    'App Engine interactive console for "%s".' % app.app_id,
    'Available symbols:',
  ]
  prompt.extend(sorted('  %s' % symbol for symbol in context))
  code.interact('\n'.join(prompt), None, context)
  return 0


@subcommand.usage('[version_id]')
def CMDswitch(parser, args):
  """Switches default version of all app modules.

  The version must be uploaded already. If no version is provided via command
  line, will ask interactively.
  """
  parser.add_force_option()
  parser.allow_positional_args = True
  app, options, version = parser.parse_args(args)
  if len(version) > 1:
    parser.error('Unknown args: %s' % version[1:])
  version = None if not version else version[0]

  # Interactively pick a version if not passed via command line.
  if not version:
    versions = app.get_uploaded_versions()
    if not versions:
      print('Upload a version first.')
      return 1

    print('Specify a version to switch to:')
    for version in versions:
      print('  %s' % version)

    version = (
        raw_input('Switch to version [%s]: ' % versions[-1]) or versions[-1])
    if version not in versions:
      print('No such version.')
      return 1

  # Switching a default version is disruptive operation. Require confirmation.
  if not options.force:
    ok = gae_sdk_utils.confirm('Switch default version?', app, version)
    if not ok:
      print('Aborted.')
      return 1

  app.set_default_version(version)
  return 0


@subcommand.usage('[module_id module_id ...]')
def CMDupload(parser, args):
  """Uploads a new version of specific (or all) modules of an app.

  Version name looks like <number>-<commit sha1>[-tainted-<who>], where:
    number      git commit number, monotonically increases with each commit
    commit sha1 upstream commit hash the branch is based of
    tainted     git repo has local modifications compared to upstream branch
    who         username who uploads the tainted version

  Doesn't make it a default unless --switch is specified. Use 'switch'
  subcommand to change default serving version.
  """
  parser.add_tag_option()
  parser.add_switch_option()
  parser.add_force_option()
  parser.allow_positional_args = True
  app, options, modules = parser.parse_args(args)

  for module in modules:
    if module not in app.modules:
      parser.error('No such module: %s' % module)

  version = calculate_version.calculate_version(app.app_dir, options.tag)

  # Updating indexes, queues, etc is a disruptive operation. Confirm.
  if not options.force:
    approved = gae_sdk_utils.confirm(
        'Upload new version, update indexes, queues and cron jobs?',
        app, version, modules)
    if not approved:
      print('Aborted.')
      return 1

  # 'appcfg.py update <list of modules>' does not update the rest of app engine
  # app like 'appcfg.py update <app dir>' does. It updates only modules. So do
  # index, queues, etc. updates manually afterwards.
  app.update_modules(version, modules)
  app.update_indexes()
  app.update_queues()
  app.update_cron()
  app.update_dispatch()

  print('-' * 80)
  print('New version:')
  print('  %s' % version)
  print('Uploaded as:')
  print('  https://%s-dot-%s.appspot.com' % (version, app.app_id))
  print('Manage at:')
  print('  https://appengine.google.com/deployment?app_id=s~' + app.app_id)
  print('-' * 80)

  if options.switch:
    if 'tainted-' in version:
      print('')
      print >> sys.stderr, 'Can\'t use --switch with a tainted version!'
      return 1
    print('Switching as default version')
    app.set_default_version(version)

  return 0


def CMDversion(parser, args):
  """Prints version name that correspond to current state of the checkout.

  'update' subcommand uses this version when uploading code to GAE.

  Version name looks like <number>-<commit sha1>[-tainted-<who>], where:
    number      git commit number, monotonically increases with each commit
    commit sha1 upstream commit hash the branch is based of
    tainted     git repo has local modifications compared to upstream branch
    who         username who uploads the tainted version
  """
  parser.add_tag_option()
  app, options, _ = parser.parse_args(args)
  print(calculate_version.calculate_version(app.app_dir, options.tag))
  return 0


class OptionParser(optparse.OptionParser):
  """OptionParser with some canned options."""

  def __init__(self, app_dir, **kwargs):
    optparse.OptionParser.__init__(
        self,
        version=__version__,
        description=sys.modules['__main__'].__doc__,
        **kwargs)
    self.app_dir = app_dir
    self.allow_positional_args = False

  def add_tag_option(self):
    self.add_option('-t', '--tag', help='Tag to attach to a tainted version')

  def add_switch_option(self):
    self.add_option(
        '-x', '--switch', action='store_true',
        help='Switch version after uploading new code')

  def add_force_option(self):
    self.add_option(
        '-f', '--force', action='store_true',
        help='Do not ask for confirmation')

  def parse_args(self, *args, **kwargs):
    gae_sdk_utils.add_sdk_options(self, self.app_dir)
    options, args = optparse.OptionParser.parse_args(self, *args, **kwargs)
    if not self.allow_positional_args and args:
      self.error('Unknown arguments: %s' % args)
    app = gae_sdk_utils.process_sdk_options(self, options, self.app_dir)
    return app, options, args


def find_app_yaml(search_dir):
  """Locates app.yaml file in |search_dir| or any of its parent directories."""
  while True:
    attempt = os.path.join(search_dir, 'app.yaml')
    if os.path.isfile(attempt):
      return attempt
    prev_dir = search_dir
    search_dir = os.path.dirname(search_dir)
    if search_dir == prev_dir:
      return None


def main(args):
  # gae.py may be symlinked into app's directory or its subdirectory (to avoid
  # typing --app-dir all the time). If linked into subdirectory, discover root
  # by locating app.yaml. It is used for Python GAE apps and one-module Go apps
  # that have all YAMLs in app root dir. For multi-module Go apps (that put
  # app.yaml into per-module dir) gae.py MUST be symlinked into app root dir.
  app_dir = None
  if IS_SYMLINKED:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app_yaml_path = find_app_yaml(script_dir)
    if app_yaml_path:
      app_dir = os.path.dirname(app_yaml_path)
    else:
      app_dir = script_dir

  colorama.init()
  dispatcher = subcommand.CommandDispatcher(__name__)
  try:
    return dispatcher.execute(OptionParser(app_dir), args)
  except gae_sdk_utils.BadEnvironmentConfig as e:
    print >> sys.stderr, str(e)
    return 1
  except gae_sdk_utils.LoginRequiredError:
    print >> sys.stderr, 'Login first using \'login\' subcommand.'
    return 1
  except KeyboardInterrupt:
    # Don't dump stack traces on Ctrl+C, it's expected flow in some commands.
    print >> sys.stderr, '\nInterrupted'
    return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
