#!/usr/bin/env python
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

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

from support import gae_sdk_utils
from tools import calculate_version


def main(args, app_dir=None):
  parser = optparse.OptionParser(
      description=sys.modules['__main__'].__doc__,
      usage='%prog [options]' + (' <app dir>' if app_dir is None else ''))
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

  # If |app_dir| is not provided, it must be passed via command line. Happens
  # when update.py is directly executed as a CLI tool.
  if not app_dir:
    if len(args) != 1:
      parser.error('Expecting a path to a GAE application directory')
    app_dir = args[0]
  else:
    # If |app_dir| is provided , it must NOT be passed via command line. Happens
    # when 'main' is called from a wrapper script.
    if args:
      parser.error('Unknown arguments, %s' % args)

  # Ensure app_dir points to a directory with app.yaml.
  app_dir = os.path.abspath(app_dir)
  if not gae_sdk_utils.is_application_directory(app_dir):
    parser.error('Not a GAE application directory: %s' % app_dir)

  options.sdk_path = options.sdk_path or gae_sdk_utils.find_gae_sdk()
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  gae_sdk_utils.setup_gae_sdk(options.sdk_path)
  options.app_id = options.app_id or gae_sdk_utils.default_app_id(app_dir)

  version = calculate_version.calculate_version(app_dir, options.tag)
  modules = options.module or gae_sdk_utils.find_module_yamls(app_dir)

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
    ret = gae_sdk_utils.appcfg(
        app_dir, cmd, options.sdk_path, options.app_id, version,
        options.verbose)
    if ret:
      print('Command \'%s\' failed with code %d' % (' '.join(cmd), ret))
      return ret

  print(' https://%s-dot-%s.appspot.com' % (version, options.app_id))
  print(' https://appengine.google.com/deployment?app_id=s~' + options.app_id)
  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
