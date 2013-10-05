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

import app_config
import find_gae_sdk


def main():
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      usage='%prog [options] [extra arguments for dev_appserver.py]')
  parser.disable_interspersed_args()
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args()

  options.sdk_path = options.sdk_path or find_gae_sdk.find_gae_sdk()
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  cmd = [
      sys.executable,
      os.path.join(options.sdk_path, 'dev_appserver.py'),
      '--skip_sdk_update_check=yes',
  ] + app_config.MODULES + args

  # Let dev_appserver.py handle interrupts.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  return subprocess.call(cmd, cwd=app_config.APP_DIR)


if __name__ == '__main__':
  sys.exit(main())
