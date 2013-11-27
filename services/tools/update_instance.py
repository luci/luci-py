# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates an AppEngine instance with the version derived from the current git
checkout state.
"""

import logging
import optparse
import os
import sys

import calculate_version
import find_gae_sdk


def main(args, app_dir, default_modules):
  parser = optparse.OptionParser(description=sys.modules['__main__'].__doc__)
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-A', '--app-id', help='Defaults to name in app.yaml')
  parser.add_option(
      '-m', '--module', action='append', help='Module yaml to update')
  parser.add_option(
      '-t', '--tag', help='Tag to attach to a tainted version')
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args(args)
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  if args:
    parser.error('Unknown arguments, %s' % args)
  options.sdk_path = options.sdk_path or find_gae_sdk.find_gae_sdk()
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  find_gae_sdk.setup_gae_sdk(options.sdk_path)
  options.app_id = options.app_id or find_gae_sdk.default_app_id(app_dir)

  version = calculate_version.calculate_version(app_dir, options.tag)
  modules = options.module or default_modules

  # 'appcfg.py update <list of modules>' does not update the rest of app engine
  # app like 'appcfg.py update <app dir>' does. It updates only modules. So do
  # index, queues, etc. updates manually afterwards.
  commands = (
      ['update'] + modules,
      ['update_indexes', '.'],
      ['update_queues', '.'],
      ['update_cron', '.'],
  )

  for cmd in commands:
    ret = find_gae_sdk.appcfg(
        app_dir, cmd, options.sdk_path, options.app_id, version,
        options.verbose)
    if ret:
      print('Command \'%s\' failed with code %d' % (' '.join(cmd), ret))
      return ret

  print(' https://%s-dot-%s.appspot.com' % (version, options.app_id))
  print(' https://appengine.google.com/deployment?app_id=s~' + options.app_id)
  return 0
