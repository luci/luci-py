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
    snapshot = stats._Snapshot()
    for i in stats._VALID_ACTIONS:
      self.assertEqual(True, stats._parse_line('%s; 0' % i, snapshot))

    expected = {
      'http_failures': 0,
      'http_requests': 0,
      'shards_assigned': 1,
      'shards_bot_died': 1,
      'shards_completed': 1,
      'shards_enqueued': 1,
      'shards_request_expired': 1,
      'shards_updated': 1,
    }
    self.assertEqual(expected, snapshot.to_dict())

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
