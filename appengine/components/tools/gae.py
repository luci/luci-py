#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Wrapper around GAE SDK tools to simplify working with multi module apps."""

__version__ = '1.0'

import atexit
import optparse
import os
import sys
import tempfile

# In case gae.py was run via symlink, find the original file since it's where
# third_party libs are.
SCRIPT_PATH = os.path.abspath(__file__)
try:
  SCRIPT_PATH = os.path.abspath(
      os.path.join(os.path.dirname(SCRIPT_PATH), os.readlink(SCRIPT_PATH)))
except OSError:
  pass

ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

import colorama
from depot_tools import subcommand

from support import gae_sdk_utils
from tools import calculate_version


@subcommand.usage('[version_id version_id ...]')
def CMDcleanup(parser, args):
  """Removes old versions of GAE application modules.

  Removes the specified versions from all app modules. If no versions are
  provided via command line, will ask interactively.
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

  # 'delete_version' is buggy, it may require cookie based authentication for
  # some (old) applications.
  for version in versions_to_remove:
    print('Deleting %s...' % version)
    app.delete_version(version)

  return 0


def CMDlogin(parser, args):
  """Initiates OAuth2 login flow if cached OAuth2 token is missing."""
  app, _, _ = parser.parse_args(args)
  return int(not gae_sdk_utils.is_oauth_token_cached() and not app.login())


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

  version = calculate_version.calculate_version(app.app_dir, options.tag)

  # Updating indexes, queues, etc is a disruptive operation. Confirm.
  if not options.force:
    approved = gae_sdk_utils.confirm(
        'Upload new version, update indexes, queues and cron jobs?',
        app, version)
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
    gae_sdk_utils.app_sdk_options(self, self.app_dir)
    self.add_option(
        '--no-oauth', action='store_true',
        help='Use password authentication instead of OAuth2')
    options, args = optparse.OptionParser.parse_args(self, *args, **kwargs)
    if not self.allow_positional_args and args:
      self.error('Unknown arguments: %s' % args)
    app = gae_sdk_utils.process_sdk_options(self, options, self.app_dir)
    if options.no_oauth:
      app.use_cookie_auth()
    return app, options, args


def main(args, app_dir=None):
  """Main entry points.

  Args:
    args: command line arguments excluding executable name.
    app_dir: directory with app.yaml, or None to autodiscover based on __file__.
  """
  # Search for app.yaml in parent directory. If found, it means gae.py was
  # symlinked to some GAE app directory. Such symlink allows to avoid typing
  # --app-dir parameter all the time.
  if app_dir is None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app_yaml_path = gae_sdk_utils.find_app_yaml(script_dir)
    if app_yaml_path:
      app_dir = os.path.dirname(app_yaml_path)

  colorama.init()
  dispatcher = subcommand.CommandDispatcher(__name__)
  try:
    return dispatcher.execute(OptionParser(app_dir), args)
  except gae_sdk_utils.LoginRequiredError:
    print >> sys.stderr, 'Login first using \'login\' subcommand.'
    return 1
  except KeyboardInterrupt:
    # Don't dump stack traces on Ctrl+C, it's expected flow in some commands.
    print >> sys.stderr, '\nInterrupted'
    return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
