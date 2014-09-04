#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from server import stats
from support import test_case


# pylint: disable=W0212


class StatsPrivateTest(test_case.TestCase):
  def _gen_data(self):
    dimensions = {'os': 'Amiga', 'hostname': 'host3'}
    # Description of the log entries:
    # - host3 is idle
    # - 100 was enqueue
    # - 101 was started by host2
    # - 101 was completed by host2
    # - 201 had bot host1 died on it
    # - 300 expired
    # - 402 is running on host4
    data = (
      stats._pack_entry(action='bot_active', bot_id='host3', dimensions={}),

      stats._pack_entry(
          action='task_enqueued', task_id='100', dimensions={}, user='me'),
      stats._pack_entry(
          action='run_started', run_id='101', bot_id='host2',
          dimensions={}, pending_ms=1500, user='me'),
      stats._pack_entry(
          action='run_completed', run_id='101', bot_id='host2',
          dimensions={}, runtime_ms=6000, user='me'),
      stats._pack_entry(
          action='task_completed', task_id='100',
          dimensions={}, pending_ms=6000, user='me'),

      stats._pack_entry(
          action='run_bot_died', run_id='201', bot_id='host1',
          dimensions=dimensions, user='joe'),

      stats._pack_entry(
          action='task_request_expired', task_id='300', dimensions={},
          user='you'),

      stats._pack_entry(
          action='run_updated', run_id='402', bot_id='host4',
          dimensions={}),
    )
    actions_tested = sorted(stats._unpack_entry(i)['action'] for i in data)
    self.assertEqual(sorted(map(unicode, stats._VALID_ACTIONS)), actions_tested)

    snapshot = stats._Snapshot()
    bots_active = {}
    tasks_active = {}
    for line in data:
      actual = stats._parse_line(
          line, snapshot, bots_active, tasks_active)
      self.assertIs(True, actual, line)

    stats._post_process(snapshot, bots_active, tasks_active)
    return snapshot

  def test_parse_summary(self):
    snapshot = self._gen_data()
    expected = {
      'bots_active': 3,
      'http_failures': 0,
      'http_requests': 0,
      'tasks_active': 2,
      'tasks_avg_pending_secs': 1.5,
      'tasks_avg_runtime_secs': 6.0,
      'tasks_bot_died': 1,
      'tasks_completed': 1,
      'tasks_enqueued': 1,
      'tasks_pending_secs': 1.5,
      'tasks_request_expired': 1,
      'tasks_total_runtime_secs': 6.0,
      'tasks_started': 1,
    }
    self.assertEqual(expected, snapshot.to_dict())
    self.assertEqual(['host2', 'host3', 'host4'], snapshot.bot_ids)

  def test_parse_dimensions(self):
    snapshot = self._gen_data()
    expected = [
      {
        'bots_active': 0,
        'dimensions': '{"os":"Amiga"}',
        'tasks_active': 0,
        'tasks_avg_pending_secs': 0.0,
        'tasks_avg_runtime_secs': 0.0,
        'tasks_bot_died': 1,
        'tasks_completed': 0,
        'tasks_enqueued': 0,
        'tasks_pending_secs': 0,
        'tasks_request_expired': 0,
        'tasks_total_runtime_secs': 0,
        'tasks_started': 0,
      },
      {
        'bots_active': 3,
        'dimensions': '{}',
        'tasks_active': 2,
        'tasks_avg_pending_secs': 1.5,
        'tasks_avg_runtime_secs': 6.0,
        'tasks_bot_died': 0,
        'tasks_completed': 1,
        'tasks_enqueued': 1,
        'tasks_pending_secs': 1.5,
        'tasks_request_expired': 1,
        'tasks_total_runtime_secs': 6.0,
        'tasks_started': 1,
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in snapshot.buckets])
    expected = [
      [],
      [u'host2', u'host3', u'host4'],
    ]
    self.assertEqual(expected, [i.bot_ids for i in snapshot.buckets])

  def test_parse_user(self):
    snapshot = self._gen_data()
    expected = [
      {
        'tasks_active': 0,
        'tasks_avg_pending_secs': 0.0,
        'tasks_avg_runtime_secs': 0.0,
        'tasks_bot_died': 1,
        'tasks_completed': 0,
        'tasks_enqueued': 0,
        'tasks_pending_secs': 0,
        'tasks_request_expired': 0,
        'tasks_total_runtime_secs': 0,
        'tasks_started': 0,
        'user': u'joe',
      },
      {
        'tasks_active': 0,
        'tasks_avg_pending_secs': 1.5,
        'tasks_avg_runtime_secs': 6.0,
        'tasks_bot_died': 0,
        'tasks_completed': 1,
        'tasks_enqueued': 1,
        'tasks_pending_secs': 1.5,
        'tasks_request_expired': 0,
        'tasks_total_runtime_secs': 6.0,
        'tasks_started': 1,
        'user': u'me',
      },
      {
        'tasks_active': 0,
        'tasks_avg_pending_secs': 0.0,
        'tasks_avg_runtime_secs': 0.0,
        'tasks_bot_died': 0,
        'tasks_completed': 0,
        'tasks_enqueued': 0,
        'tasks_pending_secs': 0,
        'tasks_request_expired': 1,
        'tasks_total_runtime_secs': 0,
        'tasks_started': 0,
        'user': u'you',
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in snapshot.users])

  def test_parse_task_active(self):
    # It is important to note that it is the request properties that are logged,
    # not the bot properties.
    data = (
      stats._pack_entry(
          action='run_updated', run_id='201', bot_id='host1',
          dimensions={'os': 'Linux'}),
      stats._pack_entry(
          action='run_updated', run_id='201', bot_id='host1',
          dimensions={'os': 'Linux'}),
      stats._pack_entry(
          action='run_updated', run_id='301', bot_id='host2',
          dimensions={'os': 'Windows'}),
      stats._pack_entry(
          action='bot_active', bot_id='host3',
          dimensions={'os': ['Windows', 'Windows-3.1']}),
      stats._pack_entry(
          action='bot_active', bot_id='host4',
          dimensions={'os': ['Linux', 'Linux-12.04']}),
    )

    snapshot = stats._Snapshot()
    bots_active = {}
    tasks_active = {}
    for line in data:
      actual = stats._parse_line(line, snapshot, bots_active, tasks_active)
      self.assertEqual(True, actual)
    stats._post_process(snapshot, bots_active, tasks_active)

    expected = [
      '{"os":"Linux"}',
      '{"os":"Windows"}',
    ]
    self.assertEqual(expected, [i.dimensions for i in snapshot.buckets])
    self.assertEqual(0, len(snapshot.users))


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
