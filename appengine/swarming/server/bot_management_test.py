#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import hashlib
import logging
import os
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from test_support import test_case

from server import bot_management


class BotManagementTest(test_case.TestCase):
  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(bot_management)
        if i[0] != '_' and hasattr(getattr(bot_management, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_bot_event(self):
    # connected.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha1().hexdigest(), quarantined=False, task_id=None,
        task_name=None)

    expected = {
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'external_ip': u'8.8.4.4',
      'first_seen_ts': now,
      'id': 'id1',
      'last_seen_ts': now,
      'quarantined': False,
      'state': {u'ram': 65},
      'task_id': None,
      'task_name': None,
      'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    }
    self.assertEqual(
        expected, bot_management.get_info_key('id1').get().to_dict())

  def test_get_events_query(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha1().hexdigest(), quarantined=False, task_id=None,
        task_name=None)
    expected = [
      {
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'event_type': u'bot_connected',
      'external_ip': u'8.8.4.4',
      'message': None,
      'quarantined': False,
      'state': {u'ram': 65},
      'task_id': None,
      'ts': now,
      'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
      },
    ]
    self.assertEqual(
        expected,
        [i.to_dict() for i in bot_management.get_events_query('id1', True)])

  def test_bot_event_poll_sleep(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot_management.bot_event(
        event_type='request_sleep', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha1().hexdigest(), quarantined=True, task_id=None,
        task_name=None)

    # Assert that BotInfo was updated too.
    expected = {
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'external_ip': u'8.8.4.4',
      'first_seen_ts': now,
      'id': 'id1',
      'last_seen_ts': now,
      'quarantined': True,
      'state': {u'ram': 65},
      'task_id': None,
      'task_name': None,
      'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    }
    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())
    self.assertEqual(False, bot_info.is_busy)

    # No BotEvent is registered for 'poll'.
    self.assertEqual([], bot_management.get_events_query('id1', True).fetch())

  def test_bot_event_busy(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot_management.bot_event(
        event_type='request_task', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha1().hexdigest(), quarantined=False, task_id='12311',
        task_name='yo')

    expected = {
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'external_ip': u'8.8.4.4',
      'first_seen_ts': now,
      'id': 'id1',
      'last_seen_ts': now,
      'quarantined': False,
      'state': {u'ram': 65},
      'task_id': u'12311',
      'task_name': u'yo',
      'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    }
    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())
    self.assertEqual(True, bot_info.is_busy)

    expected = [
      {
        'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
        'event_type': u'request_task',
        'external_ip': u'8.8.4.4',
        'message': None,
        'quarantined': False,
        'state': {u'ram': 65},
        'task_id': u'12311',
        'ts': now,
        'version': u'da39a3ee5e6b4b0d3255bfef95601890afd80709',
      },
    ]
    self.assertEqual(
        expected,
        [e.to_dict() for e in bot_management.get_events_query('id1', True)])

  def test_should_restart_bot_not_set(self):
    state = {
      'running_time': 0,
      'started_ts': 1410989556.174,
    }
    self.assertEqual(
        (False, ''), bot_management.should_restart_bot('id', state))

  def test_should_restart_bot_bad_type(self):
    state = {
      'periodic_reboot_secs': '100',
      'running_time': 105,
      'started_ts': 1410989556.174,
    }
    self.assertEqual(
        (False, ''), bot_management.should_restart_bot('id', state))

  def test_should_restart_bot_no(self):
    state = {
      'periodic_reboot_secs': 100,
      'running_time': 0,
      'started_ts': 1410989556.174,
    }
    self.assertEqual(
        (False, ''), bot_management.should_restart_bot('id', state))

  def test_should_restart_bot(self):
    state = {
      'periodic_reboot_secs': 100,
      'running_time': 105,
      'started_ts': 1410989556.174,
    }
    needs_reboot, message = bot_management.should_restart_bot('id', state)
    self.assertTrue(needs_reboot)
    self.assertTrue(message)

  def test_get_bot_reboot_period(self):
    # Mostly for code coverage.
    self.mock(bot_management, 'BOT_REBOOT_PERIOD_RANDOMIZATION_MARGIN', 0.1)
    state = {'periodic_reboot_secs': 1000, 'started_ts': 1234}
    self.assertEqual(935, bot_management.get_bot_reboot_period('bot', state))
    # Make sure the margin is respected.
    periods = set()
    for i in xrange(0, 1350):
      state = {'periodic_reboot_secs': 1000, 'started_ts': i}
      period = bot_management.get_bot_reboot_period('bot', state)
      self.assertTrue(900 <= period < 1100)
      periods.add(period)
    # Make sure it's really random and covers all expected range. (This check
    # relies on number of iterations above to be high enough).
    self.assertEqual(200, len(periods))

  def test_get_info_key(self):
    self.assertEqual(
        ndb.Key(bot_management.BotRoot, 'foo', bot_management.BotInfo, 'info'),
        bot_management.get_info_key('foo'))

  def test_get_root_key(self):
    self.assertEqual(
        ndb.Key(bot_management.BotRoot, 'foo'),
        bot_management.get_root_key('foo'))

  def test_get_settings_key(self):
    expected = ndb.Key(
        bot_management.BotRoot, 'foo', bot_management.BotSettings, 'settings')
    self.assertEqual(expected, bot_management.get_settings_key('foo'))

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
