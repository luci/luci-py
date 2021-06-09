# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys


# swarming_bot/
BOT_DIR = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


def setup_test_env():
  """Sets up the environment for bot tests."""
  client = os.path.normpath(os.path.join(BOT_DIR, '..', '..', '..', 'client'))
  client_tests = os.path.join(client, 'tests')
  sys.path.insert(0, client_tests)

  tp = os.path.join(BOT_DIR, 'third_party')
  if os.path.isfile(tp):
    # check if third_party is symlink or regular existing file
    # because support for symlink in git for Windows may be disabled
    sys.stderr.write(
      'Please enable symlink. '
      'Run \'git config --global core.symlinks true\' '
      'and fetch the repository again')
    exit(1)
  sys.path.insert(0, tp)

  # libusb1 expects to be directly in sys.path.
  sys.path.insert(0, os.path.join(BOT_DIR, 'python_libusb1'))

  # For python-rsa.
  sys.path.insert(0, os.path.join(tp, 'rsa'))
  sys.path.insert(0, os.path.join(tp, 'pyasn1'))

  # Protobuf is now used in the bot itself.
  # See fix_protobuf_package() in appengine/components/components/utils.py
  # but until this code, the version under client is used.
  if 'google' in sys.modules:
    # It may be in lib/python2.7/site-packages/google, take not chance and flush
    # it out.
    del sys.modules['google']
  # This should import client/third_party/google
  import google
  google_pkg = os.path.join(client, 'third_party', 'google')
  if google_pkg not in google.__path__:
    google.__path__.insert(0, google_pkg)
  six_path = os.path.join(client, 'third_party', 'six')
  if six_path not in sys.path:
    sys.path.insert(0, six_path)
  sys.path.insert(
      0,
      os.path.join(client, 'third_party', 'httplib2',
                   'python%d' % sys.version_info.major))
