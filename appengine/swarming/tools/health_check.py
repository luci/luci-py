#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Check the health of a Swarming version."""

import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(__file__)
SWARMING_TOOL = os.path.join(HERE, '..', '..', '..', 'client', 'swarming.py')

# TODO(flowblok): remove this hard-coded pool
POOL = 'ChromeOS'


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('application')
  parser.add_argument('server_version')
  args = parser.parse_args()

  url = 'https://{server_version}-dot-{application}.appspot.com'.format(
      application=args.application,
      server_version=args.server_version)

  print 'Scheduling no-op task on', url

  rv = subprocess.call([
      SWARMING_TOOL, 'run',
      '-S', url,
      '--expiration', '120',
      '--hard-timeout', '120',
      '-d', 'pool', POOL,
      '-d', 'server_version', args.server_version,
      '--raw-cmd', '--', 'python', '-c', 'pass'])
  if rv != 0:
    print>>sys.stderr, 'Failed to run no-op task'
    return 2
  return 0


if __name__ == '__main__':
  sys.exit(main())
