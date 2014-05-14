#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tests for the RequestorDailyStats classes."""

import datetime
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from server import task_glue
from server import test_helper
from stats import requestor_daily_stats
from support import test_case

# The expected wait and run times for all runners.
EXPECTED_WAIT_TIME = 5
EXPECTED_RUN_TIME = 10

# Requestors for testing.
REQUESTOR = [
  'user@domain.com',
  'user2@domain.com',
]


class RequestorStatsTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(RequestorStatsTest, self).setUp()
    # Support for this code was not implemented in the new DB.
    self.mock(task_glue, 'USE_OLD_API', True)

  def testAddStatsWithNoRequestor(self):
    self.assertEqual(0,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    runner = test_helper.CreateRunner(machine_id='1', exit_codes='0',
        requestor='')
    requestor_daily_stats.UpdateDailyStats(runner)

    self.assertEqual(0,
                     requestor_daily_stats.RequestorDailyStats.query().count())

  def testAddStat(self):
    runner = test_helper.CreateRunner(
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[0])
    requestor_daily_stats.UpdateDailyStats(runner)

    self.assertEqual(1,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    daily_stats = requestor_daily_stats.RequestorDailyStats.query().get()
    expected_id = '%s-%s' % (REQUESTOR[0], str(runner.started.date()))
    self.assertEqual(expected_id, daily_stats.key.id())
    self.assertEqual(1, daily_stats.tests_run)
    self.assertEqual(EXPECTED_WAIT_TIME, daily_stats.time_waiting)
    self.assertEqual(EXPECTED_RUN_TIME, daily_stats.time_running_tests)

  def TestAddMultipleStats(self):
    runner = test_helper.CreateRunner(
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[0])

    requestor_daily_stats.UpdateDailyStats(runner)

    # Record the runner a second time to double the usage on the same day.
    requestor_daily_stats.UpdateDailyStats(runner)

    self.assertEqual(1,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    daily_stats = requestor_daily_stats.RequestorDailyStats.query().get()
    self.assertEqual(str(runner.started.date()), daily_stats.key.id())
    self.assertEqual(2, daily_stats.tests_run)
    self.assertEqual(2 * EXPECTED_WAIT_TIME, daily_stats.time_waiting)
    self.assertEqual(2 * EXPECTED_RUN_TIME, daily_stats.time_running_tests)

  def testAddMultipleRequestors(self):
    # Add the first requestor.
    runner_1 = test_helper.CreateRunner(
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[0])

    requestor_daily_stats.UpdateDailyStats(runner_1)

    # Add the other requestor
    runner_2 = test_helper.CreateRunner(
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[1])
    requestor_daily_stats.UpdateDailyStats(runner_2)

    self.assertEqual(2,
                     requestor_daily_stats.RequestorDailyStats.query().count())
    daily_stats_list = requestor_daily_stats.RequestorDailyStats.query().fetch(
        2)

    # Ensure the 2 models have the same usage stats, but with different headers.
    self.assertNotEqual(daily_stats_list[0].requestor,
                        daily_stats_list[1].requestor)
    self.assertEqual(daily_stats_list[0].date,
                     daily_stats_list[1].date)

    for daily_stats in daily_stats_list:
      self.assertEqual(1, daily_stats.tests_run)
      self.assertEqual(EXPECTED_WAIT_TIME, daily_stats.time_waiting)
      self.assertEqual(EXPECTED_RUN_TIME, daily_stats.time_running_tests)

  def testMultipleDays(self):
    runner_1 = test_helper.CreateRunner(
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[0])
    requestor_daily_stats.UpdateDailyStats(runner_1)

    # Add the usage for the next day.
    runner_2 = test_helper.CreateRunner(
        created=datetime.timedelta(days=1),
        started=datetime.timedelta(minutes=EXPECTED_WAIT_TIME),
        ended=datetime.timedelta(minutes=EXPECTED_RUN_TIME),
        requestor=REQUESTOR[0])
    requestor_daily_stats.UpdateDailyStats(runner_2)

    self.assertEqual(2,
                     requestor_daily_stats.RequestorDailyStats.query().count())
    daily_stats_list = requestor_daily_stats.RequestorDailyStats.query().fetch(
        2)

    self.assertEqual(daily_stats_list[0].requestor,
                     daily_stats_list[1].requestor)
    self.assertNotEqual(daily_stats_list[0].date,
                        daily_stats_list[1].date)

    for daily_stats in daily_stats_list:
      self.assertEqual(1, daily_stats.tests_run)
      self.assertEqual(EXPECTED_WAIT_TIME, daily_stats.time_waiting)
      self.assertEqual(EXPECTED_RUN_TIME, daily_stats.time_running_tests)

  def testDeleteOldRequestorStats(self):
    current_day = datetime.datetime.utcnow().date()

    # Add a model that shouldn't get deleted.
    daily_stat = requestor_daily_stats.RequestorDailyStats(date=current_day)
    daily_stat.put()

    ndb.delete_multi(requestor_daily_stats.QueryOldRequestorDailyStats())
    self.assertEqual(1,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    # Add a model that should get deleted.
    daily_stat = requestor_daily_stats.RequestorDailyStats(date=(
        current_day -
        datetime.timedelta(
            days=requestor_daily_stats.REQUESTOR_DAILY_STATS_LIFE_IN_DAYS + 1)))
    daily_stat.put()
    self.assertEqual(2,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    # Ensure the correct model was deleted.
    ndb.delete_multi(requestor_daily_stats.QueryOldRequestorDailyStats())
    self.assertEqual(1,
                     requestor_daily_stats.RequestorDailyStats.query().count())

    remaining_model = requestor_daily_stats.RequestorDailyStats.query().get()
    self.assertEqual(current_day, remaining_model.date)


if __name__ == '__main__':
  unittest.main()
