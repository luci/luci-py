#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from server import test_helper
from stats import runner_stats
from support import test_case
from third_party.mox import mox

# pylint: disable=W0212


def _CreateRunnerStats():
  return runner_stats.RunnerStats(test_case_name='test',
                                  dimensions='dimensions',
                                  num_instances=1,
                                  instance_index=0,
                                  created_time=datetime.datetime.utcnow(),
                                  assigned_time=datetime.datetime.utcnow(),
                                  end_time=datetime.datetime.utcnow(),
                                  machine_id='id',
                                  success=True,
                                  aborted=False,
                                  automatic_retry_count=0)


class StatManagerTest(test_case.TestCase):
  def setUp(self):
    super(StatManagerTest, self).setUp()
    self._mox = mox.Mox()

    self.config_name = 'c1'

  def tearDown(self):
    self._mox.UnsetStubs()
    super(StatManagerTest, self).tearDown()

  def testRecordRunnerStats(self):
    r_stats = runner_stats.RunnerStats.query().get()
    self.assertEqual(None, r_stats)

    runner = test_helper.CreatePendingRunner(ran_successfully=True)
    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertFalse(r_stats.aborted)
    self.assertTrue(r_stats.success)

  def testSwarmDeleteOldRunnerStats(self):
    self._mox.StubOutWithMock(runner_stats, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS - 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future after the start time, but
    # still close enough to the end time.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 5))
    runner_stats._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    r_stats = _CreateRunnerStats()
    r_stats.assigned_time = r_stats.created_time
    r_stats.put()
    self.assertEqual(1, runner_stats.RunnerStats.query().count())

    # Make sure that runners aren't deleted if they don't have an ended_time.
    ndb.delete_multi(runner_stats.QueryOldRunnerStats())
    self.assertEqual(1, runner_stats.RunnerStats.query().count())

    # Make sure that new runner stats aren't deleted (even if they started
    # long ago).
    r_stats.end_time = r_stats.assigned_time + datetime.timedelta(days=3)
    r_stats.put()
    ndb.delete_multi(runner_stats.QueryOldRunnerStats())
    self.assertEqual(1, runner_stats.RunnerStats.query().count())

    # Make sure that old runner stats are deleted.
    ndb.delete_multi(runner_stats.QueryOldRunnerStats())
    self.assertEqual(0, runner_stats.RunnerStats.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
