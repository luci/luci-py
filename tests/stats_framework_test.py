#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import calendar
import datetime
import os
import sys
import unittest

import test_env

test_env.setup_test_env()

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.ext import ndb
from google.appengine.ext import testbed
# pylint: enable=E0611,F0401

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import stats_framework


class Snapshot(ndb.Model):
  """Fake statistics."""
  a = ndb.IntegerProperty(default=0)
  b = ndb.FloatProperty(default=0)

  def accumulate(self, rhs):
    return stats_framework.accumulate(self, rhs)


class StatsFrameworkTest(unittest.TestCase):
  def setUp(self):
    super(StatsFrameworkTest, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()
    super(StatsFrameworkTest, self).tearDown()

  def test_framework(self):
    # Ensures the processing will run for 120 minutes starting
    # StatisticsFramework.MAX_BACKTRACK days ago.
    called = []

    def gen_data(start, end):
      """Returns fake statistics."""
      self.assertEqual(start + 60, end)
      called.append(start)
      return Snapshot(a=1, b=1)

    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, gen_data)

    now = datetime.datetime.utcnow()
    start_date = now - datetime.timedelta(
        days=stats_framework.StatisticsFramework.MAX_BACKTRACK)
    limit = handler.MAX_MINUTES_PER_PROCESS

    i = handler.process_next_chunk(5, lambda: now)
    self.assertEqual(limit, i)

    # Fresh new stats gathering always starts at midnight.
    midnight = datetime.datetime(*start_date.date().timetuple()[:3])
    expected_calls = [
      calendar.timegm((midnight + datetime.timedelta(minutes=i)).timetuple())
      for i in range(limit)
    ]
    self.assertEqual(expected_calls, called)

    expected = [{'hours_bitmap': 3, 'values': {'a': limit, 'b': float(limit)}}]
    actual = [d.to_dict() for d in handler.stats_day_cls.query().fetch()]
    for i in actual:
      i.pop('created')
      i.pop('modified')
    self.assertEqual(expected, actual)

    expected = [
      {'minutes_bitmap': (1<<60)-1, 'values': {'a': 60, 'b': 60.}}
      for _ in range(limit / 60)
    ]
    actual = [d.to_dict() for d in handler.stats_hour_cls.query().fetch()]
    for i in actual:
      i.pop('created')
    self.assertEqual(expected, actual)

    expected = [{'values': {'a': 1, 'b': 1.}} for i in range(limit)]
    actual = [d.to_dict() for d in handler.stats_minute_cls.query().fetch()]
    for i in actual:
      i.pop('created')
    self.assertEqual(expected, actual)

  def test_keys(self):
    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, None)
    date = datetime.datetime(2010, 1, 2)
    self.assertEqual(
        ndb.Key('StatsRoot', 'test_framework', 'StatsDay', '2010-01-02'),
        handler.day_key(date.date()))

    self.assertEqual(
        ndb.Key(
          'StatsRoot', 'test_framework',
          'StatsDay', '2010-01-02',
          'StatsHour', '00'),
        handler.hour_key(date))
    self.assertEqual(
        ndb.Key(
          'StatsRoot', 'test_framework',
          'StatsDay', '2010-01-02',
          'StatsHour', '00',
          'StatsMinute', '00'),
        handler.minute_key(date))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
