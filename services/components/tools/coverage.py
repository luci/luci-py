#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs the unit tests under coverage."""

import optparse
import os
import subprocess
import sys

SERVICES_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import find_gae_sdk


def coverage_module(directory, omitted, env):
  offset = len(os.path.dirname(directory)) + 1
  for root, _, files in os.walk(directory):
    for test in files:
      if test.endswith('_smoke_test.py'):
        continue
      if not test.endswith('_test.py'):
        continue
      filepath = os.path.join(root, test)
      print filepath[offset:]
      subprocess.call(
          [
            'coverage', 'run',
            '--append',
            '--omit', ','.join(omitted),
            filepath,
          ],
          cwd=SERVICES_DIR, env=env)


def main():
  modules = sorted(
      i for i in os.listdir(SERVICES_DIR)
      if os.path.isdir(os.path.join(SERVICES_DIR, i)))
  modules.remove('tools')
  parser = optparse.OptionParser(description=sys.modules['__main__'].__doc__)
  parser.add_option(
      '-m', dest='modules', default=[], action='append',
      help='Which modules to test. Default to everything in services/. '
           'Available: %s' % ','.join(modules))
  options, args = parser.parse_args()
  if args:
    parser.error('Unknow arguments %s' % args)
  if not options.modules:
    options.modules = modules
  elif not set(modules).issuperset(options.modules):
    parser.error('Valid -m values are %s' % ', '.join(modules))

  gae_dir = find_gae_sdk.find_gae_sdk()
  env = os.environ.copy()
  env['PYTHONPATH'] = gae_dir

  coverage_file = os.path.join(SERVICES_DIR, '.coverage')
  if os.path.isfile(coverage_file):
    os.remove(coverage_file)

  omitted = [
    os.path.join(gae_dir, '*'),
    '/usr/*',
    '*third_party*',
  ]
  for module in options.modules:
    coverage_module(os.path.join(SERVICES_DIR, module), omitted, env)

  print('')
  subprocess.call(['coverage', 'report', '-m'], cwd=SERVICES_DIR)
  print('')
  print('Run \'coverage html -d html\' to generate a nice HTML report in '
        'directory html.')
  return 0


if __name__ == '__main__':
  sys.exit(main())
