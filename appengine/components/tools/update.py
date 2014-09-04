#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates an AppEngine instance with the version derived from the current git
checkout state.
"""

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
      usage='%prog [options]')
  parser.add_option(
      '-m', '--module', action='append', help='Module id to update')
  parser.add_option(
      '-t', '--tag', help='Tag to attach to a tainted version')

  gae_sdk_utils.app_sdk_options(parser, app_dir)
  options, args = parser.parse_args(args)
  app = gae_sdk_utils.process_sdk_options(parser, options, app_dir)
  if args:
    parser.error('Unknown arguments: %s' % args)
  version = calculate_version.calculate_version(app.app_dir, options.tag)

  # Updating indexes, queues, etc is a disruptive operation. Confirm.
  approved = gae_sdk_utils.confirm(
      'Deploy new version, update indexes, queues and cron jobs?', app, version)
  if not approved:
    print('Aborted.')
    return 0

  # 'appcfg.py update <list of modules>' does not update the rest of app engine
  # app like 'appcfg.py update <app dir>' does. It updates only modules. So do
  # index, queues, etc. updates manually afterwards.
  app.update_modules(version, options.module)
  app.update_indexes()
  app.update_queues()
  app.update_cron()

  print('-' * 80)
  print('New version:')
  print('  %s' % version)
  print('Deployed as:')
  print('  https://%s-dot-%s.appspot.com' % (version, app.app_id))
  print('Manage at:')
  print('  https://appengine.google.com/deployment?app_id=s~' + app.app_id)
  print('-' * 80)
  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
