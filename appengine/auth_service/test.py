#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys

import six

import test_env
test_env.setup_test_env()

from test_support import parallel_test_runner


def main():
  return parallel_test_runner.run_tests(python3=six.PY3)


if __name__ == '__main__':
  sys.exit(main())
