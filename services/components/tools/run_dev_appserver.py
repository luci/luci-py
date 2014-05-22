#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs app engine app locally via dev_appserver.py.

This script is mostly useful for apps that are composed of multiple modules.
The script will discover and launch them together with the default module.
"""

import optparse
import os
import signal
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

from support import gae_sdk_utils


def main(args, app_dir=None):
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      usage='%%prog [options] [extra arguments for dev_appserver.py]')
  parser.disable_interspersed_args()
  parser.add_option(
      '-o', '--open', action='store_true',
      help='Listen to all interfaces (less secure)')

  gae_sdk_utils.app_sdk_options(parser, app_dir)
  options, args = parser.parse_args(args)
  app = gae_sdk_utils.process_sdk_options(parser, options, app_dir)

  # Let dev_appserver.py handle Ctrl+C interrupts.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  return app.run_dev_appserver(args, options.open)


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
