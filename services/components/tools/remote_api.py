#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an app engine instance via remote_api."""

import atexit
import code
import optparse
import os
import sys
import urllib2

try:
  import readline
except ImportError:
  readline = None

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))


from support import gae_sdk_utils


def initialize_remote_api(app_dir, host, app_id, version, module_id):
  """Initializes remote_api_stub bridge."""
  gae_sdk_utils.setup_env(app_dir, app_id, version, module_id, remote_api=True)

  # Make application modules importable.
  sys.path.insert(0, app_dir)

  from google.appengine.ext.remote_api import remote_api_stub

  try:
    remote_api_stub.ConfigureRemoteApi(
        None,
        '/_ah/remote_api',
        gae_sdk_utils.get_authentication_function(),
        host,
        save_cookies=True,
        secure=True)
  except urllib2.URLError:
    print >> sys.stderr, 'Failed to access %s' % host
    return False

  remote_api_stub.MaybeInvokeAuthentication()
  return True


def setup_default_context():
  """Symbols available from interactive console by default."""
  # Unused variable 'XXX'; they are accessed via locals().
  # pylint: disable=W0612
  from google.appengine.api import memcache
  from google.appengine.ext import ndb

  # Symbols presented to the user.
  predefined_vars = locals().copy()
  return predefined_vars


def main(args, app_dir=None, setup_context_callback=None):
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      usage='%prog [options]')
  parser.add_option(
      '-H', '--host', help='Only necessary if not hosted on .appspot.com')
  parser.add_option(
      '-V', '--version',
      help='Defaults to the default active instance. Override to connect to a '
           'non-default instance.')
  parser.add_option(
      '-M', '--module', default='default',
      help='Module to connect to. default: %default')

  gae_sdk_utils.app_sdk_options(parser, app_dir)
  options, args = parser.parse_args(args)
  app = gae_sdk_utils.process_sdk_options(parser, options, app_dir)

  if not options.host:
    prefixes = filter(None, (options.version, options.module, app.app_id))
    options.host = '%s.appspot.com' % '-dot-'.join(prefixes)

  # Ensure remote_api is initialized and GAE sys.path is set.
  success = initialize_remote_api(
      app.app_dir, options.host, app.app_id, options.version, options.module)
  if not success:
    return 1

  # Import all modules the app wants to be present in interactive console.
  context = setup_default_context()
  if setup_context_callback:
    context.update(setup_context_callback())

  # Fancy readline support.
  if readline is not None:
    readline.parse_and_bind('tab: complete')
    history_file = os.path.expanduser('~/.remote_api_%s' % app.app_id)
    atexit.register(lambda: readline.write_history_file(history_file))
    if os.path.exists(history_file):
      readline.read_history_file(history_file)

  prompt = [
    'App Engine interactive console for "%s".' % app.app_id,
    'Available symbols:',
  ]
  prompt.extend(sorted('  %s' % symbol for symbol in context))
  code.interact('\n'.join(prompt), None, context)


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
