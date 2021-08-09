#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Wrapper around GAE SDK tools to simplify working with multi-service apps."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__version__ = '2.1'

import atexit
import code
import optparse
import os
import signal
import sys
import tempfile

from six.moves import urllib

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

# It's appengine/components
COMPONENTS_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))
# For tools/
sys.path.insert(0, COMPONENTS_DIR)
# For depot_tools/
sys.path.insert(
    0, os.path.join(COMPONENTS_DIR, '..', '..', 'client', 'third_party'))

import colorama
from depot_tools import subcommand

from tool_support import gae_sdk_utils
from tools import calculate_version
from tools import log_since


def _print_version_log(app, to_version):
  """Queries the server active version and prints the log between the active
  version and the new version.
  """
  try:
    from_version = app.oldest_active_version()
  except ValueError:
    return

  if not from_version:
    print("Can't find any active versions.")
    return

  try:
    start = int(from_version.split('-', 1)[0])
    end = int(to_version.split('-', 1)[0])
  except ValueError:
    print("Can't get version number. from: %s, to: %s" %
          (from_version, to_version))
    return

  if start < end:
    pseudo_revision, mergebase = calculate_version.get_remote_pseudo_revision(
        app.app_dir, 'origin/main')
    logs, _ = log_since.get_logs(app.app_dir, pseudo_revision, mergebase, start,
                                 end)
    print('\nLogs between %s and %s:' % (from_version, to_version))
    print('%s\n' % logs)


##


def CMDactive(parser, args):
  """Prints the active versions on the server.

  This is an approximation of querying which version is the default.
  """
  parser.add_option(
      '-b', '--bare', action='store_true',
      help='Only print the version(s), nothing else')
  app, options, _services = parser.parse_args(args)
  data = app.get_actives()
  if options.bare:
    print('\n'.join(sorted(set(i['id'] for i in data))))
    return 0
  print('%s:' % app.app_id)

  for service in data:
    msg = (
        '  %s: %s by %s at %s' % (service['service'], service['id'],
                                  service['deployer'], service['creationTime']))
    if service['traffic_split'] < 1:
      msg += ' (traffic: %.1f%%)' % (service['traffic_split'] * 100)
    print(msg)
  return 0


def CMDapp_dir(parser, args):
  """Prints a root directory of the application."""
  # parser.app_dir is None if app root directory discovery fails. Fail the
  # command even before invoking CLI parser, or it will ask to pass --app_dir to
  # 'app-dir' subcommand, which is ridiculous.
  if not parser.app_dir:
    print('Can\'t discover an application root directory.', file=sys.stderr)
    return 1
  parser.add_tag_option()
  app, _, _ = parser.parse_args(args)
  print(app.app_dir)
  return 0


@subcommand.usage('[version_id version_id ...]')
def CMDcleanup(parser, args):
  """Removes old versions of GAE application services.

  Removes the specified versions from all app services. If no versions are
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
          print('Unknown version: %s' % line, file=sys.stderr)
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


@subcommand.usage('[service_id version_id]')
def CMDshell(parser, args):
  """Opens interactive remote shell with app's GAE environment.

  Connects to a specific version of a specific service (an active version of
  'default' service by default). The app must have 'remote_api: on' builtin
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

  service = 'default'
  version = None
  if len(args) == 2:
    service, version = args
  elif len(args) == 1:
    service = args[0]
  elif args:
    parser.error('Unknown args: %s' % args)

  if service not in app.services:
    parser.error('No such service: %s' % service)

  if not options.host and not options.local:
    prefixes = list(filter(None, (version, service, app.app_id)))
    options.host = '%s.appspot.com' % '-dot-'.join(prefixes)

  # Ensure remote_api is initialized and GAE sys.path is set.
  gae_sdk_utils.setup_env(
      app.app_dir, app.app_id, version, service, remote_api=True)

  if options.host:
    # Open the connection.
    from google.appengine.ext.remote_api import remote_api_stub
    try:
      print('If asked to login, run:\n')
      print(
          'gcloud auth application-default login '
          '--scopes=https://www.googleapis.com/auth/appengine.apis,'
          'https://www.googleapis.com/auth/userinfo.email\n')
      remote_api_stub.ConfigureRemoteApiForOAuth(
          options.host, '/_ah/remote_api')
    except urllib.error.URLError:
      print('Failed to access %s' % options.host, file=sys.stderr)
      return 1
    remote_api_stub.MaybeInvokeAuthentication()

  def register_sys_path(*path):
    abs_path = os.path.abspath(os.path.join(*path))
    if os.path.isdir(abs_path) and not abs_path in sys.path:
      sys.path.insert(0, abs_path)

  # Simplify imports of app services (with dependencies). This code is optimized
  # for layout of apps that use 'components'.
  register_sys_path(app.app_dir)
  register_sys_path(app.app_dir, 'third_party')
  register_sys_path(app.app_dir, 'components', 'third_party')

  # Import some common services into interactive console namespace.
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
  """Switches default version of all app services.

  The version must be uploaded already. If no version is provided via command
  line, will ask interactively.
  """
  parser.add_switch_option()
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

    prompt = 'Switch to version [%s]: ' % versions[-1]
    version = _raw_input(prompt) or versions[-1]
    if version not in versions:
      print('No such version.')
      return 1

  _print_version_log(app, version)
  # Switching a default version is disruptive operation. Require confirmation.
  if (not options.force and
      not gae_sdk_utils.confirm('Switch default version?', app, version)):
    print('Aborted.')
    return 1

  print()
  app.set_default_version(version, roll_duration=options.roll_duration)
  return 0


@subcommand.usage('[service_id service_id ...]')
def CMDupload(parser, args):
  """Uploads a new version of specific (or all) services of an app.

  Note that service yamls are expected to be named module-<service-name>.yaml

  Version name looks like <number>-<commit sha1>[-tainted-<who>], where:
    number      git commit number, monotonically increases with each commit
    commit sha1 upstream commit hash the branch is based of
    tainted     git repo has local modifications compared to upstream branch
    who         username who uploads the tainted version

  Doesn't make it a default unless --switch is specified. Use 'switch'
  subcommand to change default serving version.
  """
  parser.add_tag_option()
  parser.add_option(
      '-x', '--switch', action='store_true',
      help='Switch version after uploading new code')
  parser.add_switch_option()
  parser.add_force_option()
  parser.allow_positional_args = True
  app, options, services = parser.parse_args(args)

  for service in services:
    if service not in app.services:
      parser.error('No such service: %s' % service)

  # Additional chars is for the app_id as well as 5 chars for '-dot-'.
  version = calculate_version.calculate_version(
    app.app_dir, options.tag, len(app.app_id)+5)

  # Updating indexes, queues, etc is a disruptive operation. Confirm.
  if not options.force:
    approved = gae_sdk_utils.confirm(
        'Upload new version, update indexes, queues and cron jobs?',
        app, version, services, default_yes=True)
    if not approved:
      print('Aborted.')
      return 1

  app.update(version, services)

  print('-' * 80)
  print('New version:')
  print('  %s' % version)
  print('Uploaded as:')
  print('  https://%s-dot-%s.appspot.com' % (version, app.app_id))
  print('Manage at:')
  print('  https://console.cloud.google.com/appengine/versions?project=' +
        app.app_id)
  print('-' * 80)

  if not (options.switch or options.roll_duration):
    return 0
  if 'tainted-' in version:
    print('')
    print('Can\'t use --switch with a tainted version!', file=sys.stderr)
    return 1
  _print_version_log(app, version)
  print('Switching as default version')
  print()
  app.set_default_version(version, roll_duration=options.roll_duration)
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
  # Additional chars is for the app_id as well as 5 chars for '-dot-'.
  print(calculate_version.calculate_version(
    app.app_dir, options.tag, len(app.app_id)+5))
  return 0


class OptionParser(optparse.OptionParser):
  """OptionParser with some canned options."""

  def __init__(self, app_dir, **kwargs):
    optparse.OptionParser.__init__(
        self,
        version=__version__,
        description=sys.modules['__main__'].__doc__,
        **kwargs)
    self.default_app_dir = app_dir
    self.allow_positional_args = False

  def add_tag_option(self):
    self.add_option('-t', '--tag', help='Tag to attach to a tainted version')

  def add_switch_option(self):
    self.add_option(
        '-n', '--no-log', action='store_true',
        help='Do not print logs from the current server active version to the '
             'one being switched to')
    gae_sdk_utils.add_roll_duration_option(self)

  def add_force_option(self):
    self.add_option(
        '-f', '--force', action='store_true',
        help='Do not ask for confirmation')

  def parse_args(self, *args, **kwargs):
    gae_sdk_utils.add_sdk_options(self, self.default_app_dir)
    options, args = optparse.OptionParser.parse_args(self, *args, **kwargs)
    if not self.allow_positional_args and args:
      self.error('Unknown arguments: %s' % args)
    app = gae_sdk_utils.process_sdk_options(self, options)
    return app, options, args


def _find_app_dir(search_dir):
  """Locates GAE app root directory (or returns None if not found).

  Starts by examining search_dir, then its parent, and so on, until it discovers
  git repository root or filesystem root.

  A directory is a suspect for an app root if it looks like an app root (has
  app.yaml or some of its subdir have app.yaml), but its parent directory does
  NOT look like an app root.

  It allows to detect multi-service Go apps. Their default service directory
  usually contains app.yaml, and this directory by itself looks like a single
  service GAE app. By looking at the parent we can detect that it's indeed just
  one service of a multi-service app.

  This logic gives false positives if multiple different single-service GAE
  apps are located in sibling directories of some root directory (e.g.
  appengine/<app1>, appengine/<app2). To prevent this directory to be
  incorrectly used as an app root, we forbid root directories of this kind to
  directly contain apps.

  A root directory is denoted either by presence of '.git' subdir, or 'ROOT'
  file.
  """
  def is_root(p):
    return (
        os.path.isdir(os.path.join(p, '.git')) or
        os.path.isfile(os.path.join(p, 'ROOT')) or
        os.path.dirname(p) == p)

  cached_check = {}
  def is_app_dir(p):
    if p not in cached_check:
      cached_check[p] = not is_root(p) and gae_sdk_utils.is_app_dir(p)
    return cached_check[p]

  while not is_root(search_dir):
    parent = os.path.dirname(search_dir)
    if is_app_dir(search_dir) and not is_app_dir(parent):
      return search_dir
    search_dir = parent
  return None


def _raw_input(prompt):
  """Get raw input in a Python 2&3 compatible way."""
  # pylint: disable=input-builtin,raw_input-builtin
  if sys.version_info.major >= 3:
    return input(prompt)
  else:
    return raw_input(prompt)


def main(args):
  # gae.py may be symlinked into an app's directory or subdirectory (to avoid
  # typing --app-dir all the time). If linked into a subdirectory, discover root
  # by locating app.yaml. It is used for Python GAE apps and single-service Go
  # apps that have all YAMLs in app root dir.
  default_app_dir = None
  if IS_SYMLINKED:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_app_dir = _find_app_dir(script_dir)

  # If not symlinked into an app directory, try to discover app root starting
  # from cwd.
  default_app_dir = default_app_dir or _find_app_dir(os.getcwd())

  colorama.init()
  dispatcher = subcommand.CommandDispatcher(__name__)
  try:
    return dispatcher.execute(OptionParser(default_app_dir), args)
  except gae_sdk_utils.Error as e:
    print(str(e), file=sys.stderr)
    return 1
  except KeyboardInterrupt:
    # Don't dump stack traces on Ctrl+C, it's expected flow in some commands.
    print('\nInterrupted', file=sys.stderr)
    return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
