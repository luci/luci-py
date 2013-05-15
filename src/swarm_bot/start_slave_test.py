#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unittest to exercise the code in start_slave.py."""



import subprocess
import sys
import unittest


class TestStartSlave(unittest.TestCase):
  def testRunStartSlave(self):
    # Simply check that an unmodified start_slave script runs and returns.
    subprocess.check([sys.executable, 'start_slave.py'])
