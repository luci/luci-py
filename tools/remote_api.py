#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an isolateserver instance via remote_api."""

import code
import getpass
import logging
import optparse
import os
import sys
import urllib2

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, APP_DIR)

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


def default_auth_func():
  user = os.environ.get('EMAIL_ADDRESS')
  if user:
    result = raw_input('Username (default: %s): ' % user)
    if result:
      user = result
  else:
    user = raw_input('Username: ')
  return user, getpass.getpass('Password: ')


def load_context(sdk_path, app_dir, host, app_id, version):
  """Returns a closure where the GAE SDK is initialized."""
  setup_gae_sdk(sdk_path)

  # Import GAE's SDK modules as needed.
  from google.appengine.ext.remote_api import remote_api_stub
  import yaml


  def default_app_id():
    """Returns the application name."""
    return yaml.load(open(os.path.join(app_dir, 'app.yaml')))['application']


  def setup_env(host):
    """Setup remote access to a GAE instance."""
    # Unused variable 'XXX'; they are accessed via locals().
    # pylint: disable=W0612
    from google.appengine.api import memcache
    from google.appengine.api.users import User
    from google.appengine.ext import ndb

    try:
      remote_api_stub.ConfigureRemoteDatastore(
          None, '/_ah/remote_api', default_auth_func, host,
          save_cookies=True, secure=True)
    except urllib2.URLError:
      print >> sys.stderr, 'Failed to access %s' % host
      return None
    remote_api_stub.MaybeInvokeAuthentication()

    os.environ['SERVER_SOFTWARE'] = 'Development (remote_api_shell)/1.0'

    # Create shortcuts.
    import acl
    import config
    import gsfiles
    import main
    import stats
    from main import ContentNamespace, ContentEntry

    # Symbols presented to the user.
    predefined_vars = locals().copy()
    return predefined_vars

  app_id = app_id or default_app_id()
  if not host:
    if version:
      host = '%s-dot-%s.appspot.com' % (version, app_id)
    else:
      host = '%s.appspot.com' % (app_id)
  return setup_env(host), app_id


def Main():
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-A', '--app-id', help='Defaults to name in app.yaml')
  parser.add_option(
      '-H', '--host', help='Only necessary if not hosted on .appspot.com')
  parser.add_option(
      '-V', '--version',
      help='Defaults to the default active instance. Override to connect to a '
           'non-default instance.')
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  if args:
    parser.error('Unknown arguments, %s' % args)
  options.sdk_path = options.sdk_path or find_gae_sdk(APP_DIR)
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  predefined_vars, app_id = load_context(options.sdk_path, APP_DIR,
      options.host, options.app_id, options.version)
  if not predefined_vars:
    return 1
  prompt = (
      'App Engine interactive console for "%s".\n'
      'Available symbols:\n'
      '  %s\n') % (app_id, ', '.join(sorted(predefined_vars)))
  code.interact(prompt, None, predefined_vars)


if __name__ == '__main__':
  sys.exit(Main())
