#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unittest to exercise the code in start_slave.py."""

import os
import subprocess
import sys
import unittest

START_SLAVE = os.path.join(os.path.dirname(__file__), 'start_slave.py')


class TestStartSlave(unittest.TestCase):
  def testRunStartSlave(self):  # pylint: disable=R0201
    # Simply check that an unmodified start_slave script runs and returns.
    subprocess.check_call([sys.executable, START_SLAVE])


if __name__ == '__main__':
  unittest.main()
