#!/usr/bin/env python
# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
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
    class Data(ndb.Model):
      a = ndb.IntegerProperty(default=0)
      b = ndb.FloatProperty(default=0)

      def accumulate(self, rhs):
        return stats_framework.accumulate(self, rhs)

    called = []
    def gen_data(start, end):
      self.assertEqual(start + 60, end)
      called.append(start)
      return Data(a=1, b=1)

    handler = stats_framework.StatisticsFramework(
        'test_framework', Data, gen_data)
    # Start at midnight of the current day, otherwise the test case is just too
    # slow, as in 20+ seconds.
    # TODO(maruel): Have the code only backtrack for X hours.
    handler.MAX_BACKTRACK = 0

    # Calculate the number of minutes since midnight.
    now = datetime.datetime(2008, 9, 3, 3, 12)
    midnight = datetime.datetime(now.year, now.month, now.day)
    minutes = int((now - midnight).total_seconds() / 60)
    i = handler.process_next_chunk(1, lambda: now)
    self.assertEqual(minutes, i)
    expected_calls = [
      calendar.timegm((midnight + datetime.timedelta(minutes=i)).timetuple())
      for i in range(minutes)
    ]
    self.assertEqual(expected_calls, called)

    # Now ensure the aggregate values for the day is the exact sum of every
    # minutes up to the last completed hour.
    last_hour = datetime.datetime(now.year, now.month, now.day, now.hour)
    hours_minutes = int((last_hour - midnight).total_seconds() / 60)
    day_obj = handler.day_key(now.date()).get()
    expected = {'a': hours_minutes, 'b': float(hours_minutes)}
    self.assertEqual(expected, day_obj.values.to_dict())

    # Ensure DailyStats.hours_bitmap is valid.
    expected_hours_bitmap = (1 << last_hour.hour) - 1
    self.assertEqual(expected_hours_bitmap, day_obj.hours_bitmap)

    # Ensure HourlyStats.hours_bitmap is valid.
    last_hour_obj = handler.hour_key(now).get()
    expected_minutes_bitmap = (1 << now.minute) - 1
    self.assertEqual(expected_minutes_bitmap, last_hour_obj.minutes_bitmap)
    expected = {'a': now.minute, 'b': float(now.minute)}
    self.assertEqual(expected, last_hour_obj.values.to_dict())

    # Check the last minute.
    last_minute = now - datetime.timedelta(minutes=1)
    last_minute_obj = handler.minute_key(last_minute).get()
    expected = {'a': 1, 'b': 1.}
    self.assertEqual(expected, last_minute_obj.values.to_dict())

    # Ensure the minute after the last minute is not present.
    self.assertEqual(None, handler.minute_key(now).get())


if __name__ == '__main__':
  unittest.main()
