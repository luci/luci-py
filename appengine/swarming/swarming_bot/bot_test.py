#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

__version__ = '23'

import os
import sys
import unittest

import bot

THIS_FILE = os.path.abspath(__file__)


class TestBot(unittest.TestCase):
  def test_bot(self):
    obj = bot.Bot(
        None,
        {'dimensions': {'foo': 'bar'}},
        '1234-1a2b3c4-tainted-joe',
        'base_dir')
    self.assertEqual({'foo': 'bar'}, obj.dimensions)
    self.assertEqual(THIS_FILE, obj.swarming_bot_zip)
    self.assertEqual('1234-1a2b3c4-tainted-joe', obj.server_version)
    self.assertEqual('base_dir', obj.base_dir)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
