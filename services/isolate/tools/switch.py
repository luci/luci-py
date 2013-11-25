#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Switches default version of all app modules."""

import logging
import optparse
import os
import sys

import app_config

ROOT_DIR = os.path.dirname(app_config.APP_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'tools'))

import find_gae_sdk


def main():
  parser = optparse.OptionParser(
      usage='usage: %prog [options] <version>',
      description=sys.modules[__name__].__doc__)
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-A', '--app-id', help='Defaults to name in app.yaml')
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  options.sdk_path = (
      options.sdk_path or find_gae_sdk.find_gae_sdk())
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  find_gae_sdk.setup_gae_sdk(options.sdk_path)
  options.app_id = (
      options.app_id or find_gae_sdk.default_app_id(app_config.APP_DIR))

  if len(args) != 1:
    print('Specify a version to switch to, uploaded versions:\n')
    find_gae_sdk.appcfg(
        app_config.APP_DIR,
        ['list_versions', '.'],
        options.sdk_path,
        options.app_id,
        None,
        options.verbose)
    return 1

  version = args[0]
  modules = find_gae_sdk.get_app_modules(app_config.APP_DIR, app_config.MODULES)

  print('Switching default version:')
  print('  App: %s' % options.app_id)
  print('  Version: %s' % version)
  print('  Modules: %s' % ', '.join(modules))
  if raw_input('Continue? [y/N] ') not in ('y', 'Y'):
    print('Aborted.')
    return 0

  return find_gae_sdk.appcfg(
      app_config.APP_DIR,
      ['set_default_version', '--module', ','.join(modules)],
      options.sdk_path,
      options.app_id,
      version,
      options.verbose)


if __name__ == '__main__':
  sys.exit(main())
