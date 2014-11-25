#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an app engine instance via remote_api."""

import atexit
import code
import getpass
import hashlib
import logging
import optparse
import os
import socket
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

try:
  import readline
except ImportError:
  readline = None

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))


from support import gae_sdk_utils


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
        get_authentication_function(),
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
