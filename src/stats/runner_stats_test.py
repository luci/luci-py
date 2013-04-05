#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for RunnerStats class."""



import datetime
import logging
import unittest


from google.appengine.ext import testbed
from stats import runner_stats
from third_party.mox import mox


class StatManagerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testGetStatsForOneRunnerStats(self):
    self.assertEqual({}, runner_stats.GetRunnerWaitStats())

    dimensions = 'machine_dimensions'
    wait = 10

    r_stats = runner_stats.RunnerStats(
        dimensions=dimensions, wait_time=wait)
    r_stats.put()

    expected_waits = {dimensions: (wait, wait, wait)}
    self.assertEqual(expected_waits, runner_stats.GetRunnerWaitStats())

  def testGetStatsForMultipleRunners(self):
    config_dimensions = '{"os": "windows"}'
    median_time = 500
    max_time = 1000

    time_count_tuples = ((250, 5), (median_time, 1), (max_time, 5))
    for time, count in time_count_tuples:
      for _ in range(count):
        r_stats = runner_stats.RunnerStats(
            dimensions=config_dimensions, wait_time=time)
        r_stats.put()

    mean_wait = (sum(time * count for time, count in time_count_tuples) /
                 sum(count for _, count in time_count_tuples))

    expected_waits = {config_dimensions: (mean_wait, median_time, max_time)}
    self.assertEqual(expected_waits, runner_stats.GetRunnerWaitStats())

  def testSwarmDeleteOldRunnerStats(self):
    self._mox.StubOutWithMock(runner_stats, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS - 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    r_stats = runner_stats.RunnerStats(
        dimensions='dimensions', wait_time=3,
        started=datetime.date.today())
    r_stats.put()
    self.assertEqual(1, runner_stats.RunnerStats.all().count())

    # Make sure that new runner stats aren't deleted.
    runner_stats.DeleteOldRunnerStats()
    self.assertEqual(1, runner_stats.RunnerStats.all().count())

    # Make sure that old runner stats are deleted.
    runner_stats.DeleteOldRunnerStats()
    self.assertEqual(0, runner_stats.RunnerStats.all().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
