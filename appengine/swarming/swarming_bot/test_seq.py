#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys

import six

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
APPENGINE_DIR = os.path.dirname(os.path.dirname(THIS_DIR))
COMPONENTS_DIR = os.path.join(APPENGINE_DIR, 'components')


def main():
  # These tests need to be run as executable
  # because they don't pass when running in parallel
  # or run via test runner
  test_files = [
      'api/os_utilities_test.py',
      'bot_code/singleton_test.py',
      'bot_code/bot_main_test.py',
  ]
  abs_test_files = [os.path.join(THIS_DIR, t) for t in test_files]

  # execute test runner
  sys.path.insert(0, COMPONENTS_DIR)
  from test_support import sequential_test_runner
  return sequential_test_runner.run_tests(abs_test_files, python3=six.PY3)


if __name__ == '__main__':
  sys.exit(main())
