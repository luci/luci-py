#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs app engine app locally."""

import optparse
import os
import signal
import subprocess
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components'))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components', 'third_party'))

from support import gae_sdk_utils


def main():
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      usage='%prog [options] [extra arguments for dev_appserver.py]')
  parser.disable_interspersed_args()
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  parser.add_option(
      '-o', '--open', action='store_true',
      help='Listen to all interfaces (less secure)')
  options, args = parser.parse_args()

  options.sdk_path = options.sdk_path or gae_sdk_utils.find_gae_sdk()
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  cmd = [
    sys.executable,
    os.path.join(options.sdk_path, 'dev_appserver.py'),
    '--skip_sdk_update_check=yes',
  ] + gae_sdk_utils.find_module_yamls(APP_DIR) + args
  if options.open:
    cmd.extend(('--host', '0.0.0.0', '--admin_host', '0.0.0.0'))
  # Let dev_appserver.py handle interrupts.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  return subprocess.call(cmd, cwd=APP_DIR)


if __name__ == '__main__':
  sys.exit(main())
