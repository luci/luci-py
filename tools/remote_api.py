#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an isolateserver instance via remote_api."""

import code
import getpass
import socket
import hashlib
import logging
import optparse
import os
import sys
import urllib2

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


APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, APP_DIR)

import find_gae_sdk


def unlock_keyring():
  """Tries to unlock the gnome keyring. Returns True on success."""
  if not os.environ.get('DISPLAY') or not gnomekeyring:
    return False
  try:
    gnomekeyring.unlock_sync(None, getpass.getpass('Keyring password: '))
    return True
  except Exception as e:
    print >> sys.stderr, 'Failed to unlock keyring: %s' % e
    return False


class DefaultAuth(object):
  """Simple authentication support based on getpass.getpass()."""
  def __init__(self):
    self._last_key = os.environ.get('EMAIL_ADDRESS')

  def _retrieve_password(self):  # pylint: disable=R0201
    return getpass.getpass('Please enter the password:' )

  def auth_func(self):
    if self._last_key:
      result = raw_input('Username (default: %s): ' % self._last_key)
      if result:
        self._last_key = result
    else:
      self._last_key = raw_input('Username: ')

    return self._last_key, self._retrieve_password()


class KeyringAuth(DefaultAuth):
  """Enhanced password support based on keyring."""
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
        if unlock_keyring():
          # Unlocking worked, try getting the password again.
          try:
            value = keyring.get_password(self._bucket, actual_key)
            if value:
              return value
          except Exception as e:
            print >> sys.stderr, 'Failed to get password from keyring: %s' % e

    # At this point, it failed to get the password from keyring. Ask the user.
    value = super(KeyringAuth, self)._retrieve_password()

    answer = raw_input('Store password in system keyring (y/N)?').strip()
    if answer == 'y':
      try:
        logging.info('Saving password for key %s (%s)', key, actual_key)
        keyring.set_password(self._bucket, actual_key, value)
      except Exception as e:
        print >> sys.stderr, 'Failed to save in keyring: %s' % e

    return value


def load_context(sdk_path, app_dir, host, app_id, version):
  """Returns a closure where the GAE SDK is initialized."""
  find_gae_sdk.setup_gae_sdk(sdk_path)
  # pylint doesn't know where the AppEngine SDK is, so silence these errors.
  # E0611: No name 'XXX' in module 'YYY'
  # F0401: Unable to import 'XXX'
  # pylint: disable=E0611,F0401

  # Import GAE's SDK modules as needed.
  from google.appengine.ext.remote_api import remote_api_stub

  def setup_env(host):
    """Setup remote access to a GAE instance."""
    # Unused variable 'XXX'; they are accessed via locals().
    # pylint: disable=W0612
    from google.appengine.api import memcache
    from google.appengine.api.users import User
    from google.appengine.ext import ndb

    if keyring:
      auth = KeyringAuth('remote_api_isolate_%s' % socket.getfqdn())
    else:
      auth = DefaultAuth()
    try:
      remote_api_stub.ConfigureRemoteDatastore(
          None, '/_ah/remote_api', auth.auth_func, host,
          save_cookies=True, secure=True)
    except urllib2.URLError:
      print >> sys.stderr, 'Failed to access %s' % host
      return None
    remote_api_stub.MaybeInvokeAuthentication()

    os.environ['SERVER_SOFTWARE'] = 'Development (remote_api_shell)/1.0'

    # Create shortcuts.
    import acl
    import config
    import gcs
    import main
    import stats
    from main import ContentNamespace, ContentEntry

    # Symbols presented to the user.
    predefined_vars = locals().copy()
    return predefined_vars

  app_id = app_id or find_gae_sdk.default_app_id(app_dir)
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
  options.sdk_path = options.sdk_path or find_gae_sdk.find_gae_sdk(APP_DIR)
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
