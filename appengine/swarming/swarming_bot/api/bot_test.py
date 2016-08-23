#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys
import unittest

THIS_FILE = os.path.abspath(__file__)

import test_env_api
test_env_api.setup_test_env()

import bot


class TestBot(unittest.TestCase):
  def test_bot(self):
    obj = bot.Bot(
        None,
        {'dimensions': {'foo': ['bar']}},
        'https://localhost:1',
        '1234-1a2b3c4-tainted-joe',
        'base_dir',
        None)
    self.assertEqual({'foo': ['bar']}, obj.dimensions)
    self.assertEqual(
        os.path.join(os.path.dirname(THIS_FILE), 'swarming_bot.zip'),
        obj.swarming_bot_zip)
    self.assertEqual('1234-1a2b3c4-tainted-joe', obj.server_version)
    self.assertEqual('base_dir', obj.base_dir)

  def test_attribute_updates(self):
    obj = bot.Bot(
        None,
        {'dimensions': {'foo': ['bar']}},
        'https://localhost:1',
        '1234-1a2b3c4-tainted-joe',
        'base_dir',
        None)
    obj._update_bot_group_cfg('cfg_ver', {'dimensions': {'pool': ['A']}})
    self.assertEqual({'foo': ['bar'], 'pool': ['A']}, obj.dimensions)
    self.assertEqual({'bot_group_cfg_version': 'cfg_ver'}, obj.state)

    # Dimension in bot_group_cfg ('A') wins over custom one ('B').
    obj._update_dimensions({'foo': ['baz'], 'pool': ['B']})
    self.assertEqual({'foo': ['baz'], 'pool': ['A']}, obj.dimensions)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
