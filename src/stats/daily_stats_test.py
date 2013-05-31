#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for DailyStats class."""



import datetime
import unittest


from google.appengine.ext import testbed
from stats import daily_stats
from stats import runner_stats


def _AddRunner(end_time, success, timeout):
  runner = runner_stats.RunnerStats(
      test_case_name='name', dimensions='xp', num_instances=0,
      instance_index=0, created_time=datetime.datetime.now(), end_time=end_time,
      success=success, timed_out=timeout, automatic_retry_count=0)
  runner.put()


class DailyStatsTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

  def tearDown(self):
    self.testbed.deactivate()

  def testGenerateDailyStatsTwice(self):
    current_day = datetime.date.today()

    self.assertTrue(daily_stats.GenerateDailyStats(current_day))
    self.assertFalse(daily_stats.GenerateDailyStats(current_day))

  def testGenerateDailyStatsWithBasicInfo(self):
    current_day = datetime.date.today()

    # Add 1 sucess, 2 regular failures, and 1 failure due to a timeout.
    _AddRunner(datetime.datetime.combine(current_day, datetime.time()),
               success=True, timeout=False)
    _AddRunner(datetime.datetime.combine(current_day, datetime.time()),
               success=False, timeout=False)
    _AddRunner(datetime.datetime.combine(current_day, datetime.time()),
               success=False, timeout=False)
    _AddRunner(datetime.datetime.combine(current_day, datetime.time()),
               success=False, timeout=True)

    # Add a runner from yesterday and tomorrow and ensure they are ignored.
    _AddRunner(datetime.datetime.combine(
        current_day + datetime.timedelta(days=1), datetime.time()),
               success=True, timeout=False)
    _AddRunner(datetime.datetime.combine(
        current_day - datetime.timedelta(days=1), datetime.time()),
               success=True, timeout=False)

    self.assertTrue(daily_stats.GenerateDailyStats(current_day))
    self.assertEqual(1, daily_stats.DailyStats.all().count())

    daily_stat = daily_stats.DailyStats.all().get()
    self.assertEqual(4, daily_stat.shards_finished)
    self.assertEqual(2, daily_stat.shards_failed)
    self.assertEqual(1, daily_stat.shards_timed_out)

  def testGetDailyStats(self):
    current_day = datetime.date.today()
    days_to_add = 7
    for i in range(days_to_add):
      day = current_day - datetime.timedelta(days=i)
      daily_stat = daily_stats.DailyStats(date=day)
      daily_stat.put()

    # Check just getting one day.
    stats = daily_stats.GetDailyStats(current_day)
    self.assertEqual(1, len(stats))
    self.assertEqual(current_day, stats[0].date)

    # Check getting all days.
    stats = daily_stats.GetDailyStats(
        current_day - datetime.timedelta(days=days_to_add))
    self.assertEqual(days_to_add, len(stats))
    self.assertEqual(current_day - datetime.timedelta(days=days_to_add - 1),
                     stats[0].date)
    self.assertEqual(current_day, stats[-1].date)


if __name__ == '__main__':
  unittest.main()
