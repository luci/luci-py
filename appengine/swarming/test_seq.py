#!/usr/bin/env python
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import subprocess
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_test(test_file, python3=False):
  # vpython
  vpython = 'vpython'
  if python3:
    vpython += '3'

  cmd = [vpython, os.path.join(ROOT_DIR, test_file), '-v']

  print('Running test script: %r' % cmd)
  return subprocess.call(cmd)


def main():
  exit_code = 0

  # These tests need to be run as executable
  # because they don't pass when running in parallel
  # or run via test runner
  test_files = [
      'server/bot_groups_config_test.py',
      'swarming_bot/bot_code/singleton_test.py',
      'local_smoke_test.py',
  ]

  for test_file in test_files:
    exit_code = run_test(test_file) or exit_code

  return exit_code


if __name__ == '__main__':
  sys.exit(main())
