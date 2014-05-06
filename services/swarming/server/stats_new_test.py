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

from server import stats_new as stats
from support import test_case


# pylint: disable=W0212


class StatsPrivateTest(test_case.TestCase):
  def test_parse(self):
    dimensions = {'os': 'Amiga', 'hostname': 'host3'}
    data = (
      'bot_active; bot_id=host3; dimensions=%s' %
          stats.pack_dimensions(dimensions),
      'request_enqueued; req_id=100; dimensions=; shards=3; user=me',
      'request_completed; dimensions=%s; req_id=200; user=you' %
          stats.pack_dimensions(dimensions),
      'shard_bot_died; shard_id=101-1; bot_id=host1; dimensions=%s; user=me' %
          stats.pack_dimensions(dimensions),
      'shard_completed; shard_id=102-1; bot_id=host2; dimensions=; '
        'runtime_ms=6000; user=me',
      'shard_request_expired; shard_id=103-1; dimensions=; user=me',
      'shard_started; shard_id=102-1; bot_id=host2; dimensions=; '
        'pending_ms=1500; user=me',
      'shard_updated; shard_id=202-1; bot_id=host4; dimensions=',
    )
    actions_tested = sorted(i.split(';', 1)[0] for i in data)
    self.assertEqual(sorted(stats._VALID_ACTIONS), actions_tested)

    snapshot = stats._Snapshot()
    bots_active = {}
    shards_active = {}
    for line in data:
      actual = stats._parse_line(
          line, snapshot, bots_active, shards_active)
      self.assertIs(True, actual)

    stats._post_process(snapshot, bots_active, shards_active)

    expected = {
      'bots_active': 3,
      'dimensions': ['', 'os_Amiga'],
      'http_failures': 0,
      'http_requests': 0,
      'requests_completed': 1,
      'requests_enqueued': 1,
      'shards_active': 4,  # due to request_enqueued=3 and 202-1
      'shards_avg_pending_secs': 1.5,
      'shards_avg_runtime_secs': 6.0,
      'shards_bot_died': 1,
      'shards_completed': 1,
      'shards_enqueued': 3,
      'shards_pending_secs': 1.5,
      'shards_request_expired': 1,
      'shards_total_runtime_secs': 6.0,
      'shards_started': 1,
      'users': ['me', 'you'],
    }
    self.assertEqual(expected, snapshot.to_dict())
    self.assertEqual(['host2', 'host3', 'host4'], snapshot.bot_ids)

    # Note: this data is not yet accessible externally but starts collecting it
    # right away.
    expected = [
      {
        'bots_active': 0,
        'dimensions': '',
        'requests_completed': 0,
        'requests_enqueued': 1,
        'shards_active': 4,
        'shards_avg_pending_secs': 1.5,
        'shards_avg_runtime_secs': 6.0,
        'shards_bot_died': 0,
        'shards_completed': 1,
        'shards_enqueued': 3,
        'shards_pending_secs': 1.5,
        'shards_request_expired': 1,
        'shards_total_runtime_secs': 6.0,
        'shards_started': 1,
      },
      {
        'bots_active': 0,
        'dimensions': 'os_Amiga',
        'requests_completed': 1,
        'requests_enqueued': 0,
        'shards_active': 0,
        'shards_avg_pending_secs': 0.0,
        'shards_avg_runtime_secs': 0.0,
        'shards_bot_died': 1,
        'shards_completed': 0,
        'shards_enqueued': 0,
        'shards_pending_secs': 0,
        'shards_request_expired': 0,
        'shards_total_runtime_secs': 0,
        'shards_started': 0,
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in snapshot.buckets])
    expected = [
      {
        'requests_completed': 0,
        'requests_enqueued': 1,
        'shards_active': 0,
        'shards_avg_pending_secs': 1.5,
        'shards_avg_runtime_secs': 6.0,
        'shards_bot_died': 1,
        'shards_completed': 1,
        'shards_enqueued': 3,
        'shards_pending_secs': 1.5,
        'shards_request_expired': 1,
        'shards_total_runtime_secs': 6.0,
        'shards_started': 1,
        'user': 'me',
      },
      {
        'requests_completed': 1,
        'requests_enqueued': 0,
        'shards_active': 0,
        'shards_avg_pending_secs': 0.0,
        'shards_avg_runtime_secs': 0.0,
        'shards_bot_died': 0,
        'shards_completed': 0,
        'shards_enqueued': 0,
        'shards_pending_secs': 0,
        'shards_request_expired': 0,
        'shards_total_runtime_secs': 0,
        'shards_started': 0,
        'user': 'you',
      },
    ]
    self.assertEqual(expected, [i.to_dict() for i in snapshot.users])

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
