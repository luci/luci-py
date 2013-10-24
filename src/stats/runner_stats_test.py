#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for RunnerStats class."""



import datetime
import logging
import unittest


from google.appengine.ext import testbed
from google.appengine.ext import ndb

from server import test_helper
from stats import runner_stats
from third_party.mox import mox


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
                                  timed_out=False,
                                  automatic_retry_count=0)


class StatManagerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    self.config_name = 'c1'

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testRecordRunnerStats(self):
    r_stats = runner_stats.RunnerStats.query().get()
    self.assertEqual(None, r_stats)

    # Create stats from a runner that didn't timeout.
    runner = test_helper.CreatePendingRunner()
    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertFalse(r_stats.timed_out)

    # Record stats from a runner that did timeout.
    runner.errors = 'Runner has become stale'
    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertTrue(r_stats.timed_out)

  def testRecordInvalidRunnerStats(self):
    # Create stats from a runner timed out, but was also successful.
    runner = test_helper.CreatePendingRunner()
    runner.errors = 'Runner has become stale'
    runner.ran_successfully = True
    runner.put()

    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertTrue(r_stats.timed_out)
    self.assertFalse(r_stats.success)

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
    ndb.Future.wait_all(runner_stats.DeleteOldRunnerStats())
    self.assertEqual(1, runner_stats.RunnerStats.query().count())

    # Make sure that new runner stats aren't deleted (even if they started
    # long ago).
    r_stats.end_time = r_stats.assigned_time + datetime.timedelta(days=3)
    r_stats.put()
    ndb.Future.wait_all(runner_stats.DeleteOldRunnerStats())
    self.assertEqual(1, runner_stats.RunnerStats.query().count())

    # Make sure that old runner stats are deleted.
    ndb.Future.wait_all(runner_stats.DeleteOldRunnerStats())
    self.assertEqual(0, runner_stats.RunnerStats.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
