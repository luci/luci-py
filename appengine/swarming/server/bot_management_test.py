#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import hashlib
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from test_support import test_case

from server import bot_management


_VERSION = unicode(hashlib.sha256().hexdigest())


class BotManagementTest(test_case.TestCase):
  def setUp(self):
    super(BotManagementTest, self).setUp()
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now)

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(bot_management)
        if i[0] != '_' and hasattr(getattr(bot_management, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def _gen_bot_info(self, **kwargs):
    out = {
      'authenticated_as': u'bot:id1.domain',
      'composite': [
        bot_management.BotInfo.NOT_IN_MAINTENANCE,
        bot_management.BotInfo.ALIVE,
        bot_management.BotInfo.NOT_MACHINE_PROVIDER,
        bot_management.BotInfo.HEALTHY,
        bot_management.BotInfo.IDLE,
      ],
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'external_ip': u'8.8.4.4',
      'first_seen_ts': self.now,
      'id': 'id1',
      'is_dead': False,
      'last_seen_ts': self.now,
      'lease_id': None,
      'lease_expiration_ts': None,
      'machine_lease': None,
      'machine_type': None,
      'quarantined': False,
      'maintenance_msg': None,
      'state': {u'ram': 65},
      'task_id': None,
      'task_name': None,
      'version': _VERSION,
    }
    out.update(kwargs)
    return out

  def _gen_bot_event(self, **kwargs):
    out = {
      'authenticated_as': u'bot:id1.domain',
      'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      'external_ip': u'8.8.4.4',
      'lease_id': None,
      'lease_expiration_ts': None,
      'machine_lease': None,
      'machine_type': None,
      'message': None,
      'quarantined': False,
      'maintenance_msg': None,
      'state': {u'ram': 65},
      'task_id': None,
      'ts': self.now,
      'version': _VERSION,
      }
    out.update(kwargs)
    return out

  def test_dimensions_to_flat(self):
    self.assertEqual(
        ['a:b', 'c:d'], bot_management.dimensions_to_flat({'a': 'b', 'c': 'd'}))

  def test_bot_event(self):
    # connected.
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:id1.domain',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=False,
        maintenance_msg=None, task_id=None, task_name=None)

    expected = self._gen_bot_info()
    self.assertEqual(
        expected, bot_management.get_info_key('id1').get().to_dict())

  def test_get_events_query(self):
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:id1.domain',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=False,
        maintenance_msg=None, task_id=None, task_name=None)
    expected = [self._gen_bot_event(event_type=u'bot_connected')]
    self.assertEqual(
        expected,
        [i.to_dict() for i in bot_management.get_events_query('id1', True)])

  def test_bot_event_poll_sleep(self):
    bot_management.bot_event(
        event_type='request_sleep', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:id1.domain',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=True,
        maintenance_msg=None, task_id=None, task_name=None)

    # Assert that BotInfo was updated too.
    expected = self._gen_bot_info(
        composite=[
          bot_management.BotInfo.NOT_IN_MAINTENANCE,
          bot_management.BotInfo.ALIVE,
          bot_management.BotInfo.NOT_MACHINE_PROVIDER,
          bot_management.BotInfo.QUARANTINED,
          bot_management.BotInfo.IDLE,
        ],
        quarantined=True)
    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())

    # No BotEvent is registered for 'poll'.
    self.assertEqual([], bot_management.get_events_query('id1', True).fetch())

  def test_bot_event_busy(self):
    bot_management.bot_event(
        event_type='request_task', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:id1.domain',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=False,
        maintenance_msg=None, task_id='12311', task_name='yo')

    expected = self._gen_bot_info(
        composite=[
          bot_management.BotInfo.NOT_IN_MAINTENANCE,
          bot_management.BotInfo.ALIVE,
          bot_management.BotInfo.NOT_MACHINE_PROVIDER,
          bot_management.BotInfo.HEALTHY,
          bot_management.BotInfo.BUSY,
        ],
        task_id=u'12311',
        task_name=u'yo')
    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())

    expected = [
      self._gen_bot_event(event_type=u'request_task', task_id=u'12311'),
    ]
    self.assertEqual(
        expected,
        [e.to_dict() for e in bot_management.get_events_query('id1', True)])

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

  def test_cron_update_bot_info(self):
    # Create two bots, one becomes dead, updating the cron job fixes composite.
    timeout = bot_management.config.settings().bot_death_timeout_secs
    def check(dead, alive):
      q = bot_management.filter_availability(
          bot_management.BotInfo.query(), quarantined=None, in_maintenance=None,
          is_dead=True, is_busy=None, is_mp=None)
      self.assertEqual(dead, [t.to_dict() for t in q])
      q = bot_management.filter_availability(
          bot_management.BotInfo.query(), quarantined=None, in_maintenance=None,
          is_dead=False, is_busy=None, is_mp=None)
      self.assertEqual(alive, [t.to_dict() for t in q])

    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:id1.domain',
        dimensions={'id': ['id1'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=False,
        maintenance_msg=None, task_id=None, task_name=None)
    # One second before the timeout value.
    then = self.mock_now(self.now, timeout-1)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:id2.domain',
        dimensions={'id': ['id2'], 'foo': ['bar']}, state={'ram': 65},
        version=hashlib.sha256().hexdigest(), quarantined=False,
        maintenance_msg=None, task_id=None, task_name=None)

    bot1_alive = self._gen_bot_info()
    bot1_dead = self._gen_bot_info(
        composite=[
          bot_management.BotInfo.NOT_IN_MAINTENANCE,
          bot_management.BotInfo.DEAD,
          bot_management.BotInfo.NOT_MACHINE_PROVIDER,
          bot_management.BotInfo.HEALTHY,
          bot_management.BotInfo.IDLE,
        ],
        is_dead=True)
    bot2_alive = self._gen_bot_info(
        authenticated_as=u'bot:id2.domain',
        dimensions={u'foo': [u'bar'], u'id': [u'id2']},
        first_seen_ts=then,
        id='id2',
        last_seen_ts=then)
    check([], [bot1_alive, bot2_alive])
    self.assertEqual(0, bot_management.cron_update_bot_info())
    check([], [bot1_alive, bot2_alive])

    # Just stale enough to trigger the dead logic.
    then = self.mock_now(self.now, timeout)
    # The cron job didn't run yet, so it still has ALIVE bit.
    check([], [bot1_alive, bot2_alive])
    self.assertEqual(1, bot_management.cron_update_bot_info())
    # The cron job ran, so it's now correct.
    check([bot1_dead], [bot2_alive])

  def test_filter_dimensions(self):
    pass # Tested in handlers_endpoints_test

  def test_filter_availability(self):
    pass # Tested in handlers_endpoints_test


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
