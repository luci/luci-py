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
  sys.path.insert(0, THIS_DIR)
  import test_env_bot
  test_env_bot.setup_test_env()

  # execute test runner
  sys.path.insert(0, COMPONENTS_DIR)
  from test_support import parallel_test_runner
  return parallel_test_runner.run_tests(python3=six.PY3)


if __name__ == '__main__':
  main()
