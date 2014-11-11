#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import hashlib
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from server import bot_management
from server import task_result
from support import test_case


class BotManagementTest(test_case.TestCase):
  def test_get_bot_key(self):
    self.assertEqual(
        "Key('BotRoot', 'f-a:1', 'Bot', 'f-a:1')",
        str(bot_management.get_bot_key('f-a:1')))

  def test_tag_bot_seen(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'},
        hashlib.sha1().hexdigest(), False, {'ram': 65})
    bot.put()
    expected = {
      'created_ts': now,
      'dimensions': {u'foo': u'bar'},
      'external_ip': u'8.8.4.4',
      'hostname': u'localhost',
      'id': 'id1',
      'internal_ip': u'127.0.0.1',
      'last_seen_ts': now,
      'quarantined': False,
      'state': {u'ram': 65},
      'task': None,
      'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    }
    self.assertEqual(expected, bot.to_dict())
    bot.task = task_result.unpack_run_result_key('12301')
    bot.put()
    expected['task'] = '12301'
    self.assertEqual(expected, bot.to_dict())

  def test_should_restart_bot_no(self):
    state = {
      'running_time': 0,
      'started_ts': 1410989556.174,
    }
    self.assertEqual(
        (False, ''), bot_management.should_restart_bot('id', {}, state))

  def test_should_restart_bot_yes(self):
    state = {
      'running_time': bot_management.BOT_REBOOT_PERIOD_SECS * 5,
      'started_ts': 1410989556.174,
    }
    needs_reboot, message = bot_management.should_restart_bot('id', {}, state)
    self.assertTrue(needs_reboot)
    self.assertTrue(message)

  def test_get_bot_reboot_period(self):
    # Mostly for code coverage.
    self.mock(bot_management, 'BOT_REBOOT_PERIOD_SECS', 1000)
    self.mock(bot_management, 'BOT_REBOOT_PERIOD_RANDOMIZATION_MARGIN', 0.1)
    self.assertEqual(
        935,
        bot_management.get_bot_reboot_period('bot', {'started_ts': 1234}))
    # Make sure the margin is respected.
    periods = set()
    for i in xrange(0, 1350):
      period = bot_management.get_bot_reboot_period('bot', {'started_ts': i})
      self.assertTrue(900 <= period < 1100)
      periods.add(period)
    # Make sure it's really random and covers all expected range. (This check
    # relies on number of iterations above to be high enough).
    self.assertEqual(200, len(periods))


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
