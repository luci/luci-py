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
  return run_tests_parralel() or run_tests_sequential()


def run_tests_parralel():
  # TODO(jwata): add only root level directory to
  # avoid adhoc sys.path insertions
  # add path for test_support
  sys.path.insert(0, THIS_DIR)

  import test_env_bot
  test_env_bot.setup_test_env()

  # append attribute filter option "--attribute '!no_run'"
  # https://nose2.readthedocs.io/en/latest/plugins/attrib.html
  sys.argv.extend(['--attribute', '!no_run'])

  # enable plugins only on linux
  plugins = []
  if sys.platform.startswith('linux'):
    plugins.append('nose2.plugins.mp')

  # execute test runner
  sys.path.insert(0, COMPONENTS_DIR)
  from test_support import parallel_test_runner
  return parallel_test_runner.run_tests(python3=six.PY3, plugins=plugins)


def run_tests_sequential():
  # These tests need to be run as executable.
  # because they don't pass when running in parallel
  # or run via test runner.
  test_files = [
      'bot_code/singleton_test.py',
  ]
  abs_test_files = [os.path.join(THIS_DIR, t) for t in test_files]

  # execute test runner
  from test_support import sequential_test_runner
  return sequential_test_runner.run_tests(abs_test_files, python3=six.PY3)


if __name__ == '__main__':
  sys.exit(main())
