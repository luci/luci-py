#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import six

from test_support import test_env, parallel_test_runner


def main():
  if six.PY2:
    test_env.setup_test_env()
  parallel_test_runner.run_tests(python3=six.PY3)


if __name__ == '__main__':
  main()
