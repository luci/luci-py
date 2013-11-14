#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unittest to exercise the code in start_slave.py."""


import subprocess
import sys
import unittest


class TestStartSlave(unittest.TestCase):
  def testRunStartSlave(self):
    # Simply check that an unmodified start_slave script runs and returns.
    subprocess.check([sys.executable, 'start_slave.py'])
