#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys

import six

from test_support import parallel_test_runner, sequential_test_runner, test_env

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
  return run_tests_parralel() or run_tests_sequential()


def run_tests_parralel():
  if six.PY2:
    test_env.setup_test_env()

  # append attribute filter option "--attribute '!no_run'"
  # https://nose2.readthedocs.io/en/latest/plugins/attrib.html
  sys.argv.extend(['--attribute', '!no_run'])

  return parallel_test_runner.run_tests(python3=six.PY3)


def run_tests_sequential():
  # These tests need to be run as executable
  # because they don't pass when running in parallel
  # or run via test runner
  test_files = [
      'components/auth/delegation_test.py',
      'components/auth/project_tokens_test.py',
      'components/auth/service_account_test.py',
      'components/utils_test.py',
      'components/endpoints_webapp2/discovery_test.py',
  ]
  abs_test_files = [os.path.join(THIS_DIR, t) for t in test_files]

  # execute test runner
  return sequential_test_runner.run_tests(abs_test_files, python3=six.PY3)


if __name__ == '__main__':
  sys.exit(main())
