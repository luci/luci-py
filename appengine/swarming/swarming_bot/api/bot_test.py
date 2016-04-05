#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys
import unittest
import threading

THIS_FILE = os.path.abspath(__file__)

import test_env_api
test_env_api.setup_test_env()

import bot


class TestBot(unittest.TestCase):
  def test_bot(self):
    obj = bot.Bot(
        None,
        {'dimensions': {'foo': 'bar'}},
        'https://localhost:1/',
        '1234-1a2b3c4-tainted-joe',
        'base_dir',
        None)
    self.assertEqual({'foo': 'bar'}, obj.dimensions)
    self.assertEqual(
        os.path.join(os.path.dirname(THIS_FILE), 'swarming_bot.zip'),
        obj.swarming_bot_zip)
    self.assertEqual('1234-1a2b3c4-tainted-joe', obj.server_version)
    self.assertEqual('base_dir', obj.base_dir)

  def test_bot_call_later(self):
    obj = bot.Bot(None, {}, 'https://localhost:1/', '1234-1a2b3c4-tainted-joe',
                  'base_dir', None)
    ev = threading.Event()
    obj.call_later(0.001, ev.set)
    self.assertTrue(ev.wait(1))

  def test_bot_call_later_cancel(self):
    obj = bot.Bot(None, {}, 'https://localhost:1/', '1234-1a2b3c4-tainted-joe',
                  'base_dir', None)
    ev = threading.Event()
    obj.call_later(0.1, ev.set)
    obj.cancel_all_timers()
    self.assertFalse(ev.wait(0.3))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
