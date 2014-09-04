#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Switches default version of all app modules."""

import optparse
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

from support import gae_sdk_utils


def main(args, app_dir=None):
  parser = optparse.OptionParser(
      usage='usage: %prog [options] <version>',
      description=sys.modules[__name__].__doc__)

  gae_sdk_utils.app_sdk_options(parser, app_dir)
  options, args = parser.parse_args(args)
  app = gae_sdk_utils.process_sdk_options(parser, options, app_dir)

  # Interactively pick a version if not passed in command line.
  version = None
  if not args:
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
  elif len(args) == 1:
    version = args[0]
  else:
    parser.error('Too many arguments passed')

  # Switching a default version is disruptive operation. Require confirmation.
  if not gae_sdk_utils.confirm('Switch default version?', app, version):
    print('Aborted.')
    return 0

  app.set_default_version(version)
  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
