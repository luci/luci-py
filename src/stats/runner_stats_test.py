#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for RunnerStats class."""



import datetime
import hashlib
import logging
import unittest


from google.appengine.ext import testbed
from common import test_request_message
from server import test_request
from server import test_runner
from stats import runner_stats
from third_party.mox import mox


def _AddSecondsToDateTime(date_time, seconds):
  return date_time + datetime.timedelta(seconds=seconds)


def _CreateRunnerStats(dimensions='dimensions'):
  return runner_stats.RunnerStats(test_case_name='test',
                                  dimensions=dimensions,
                                  num_instances=1,
                                  instance_index=0,
                                  created_time=datetime.datetime.now(),
                                  assigned_time=datetime.datetime.now(),
                                  end_time=datetime.datetime.now(),
                                  machine_id='id',
                                  success=True,
                                  timed_out=False,
                                  automatic_retry_count=0)


class StatManagerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    self.config_name = 'c1'

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def _GetRequestMessage(self):
    test_case = test_request_message.TestCase()
    test_case.test_case_name = 'test_case'
    test_case.configurations = [
        test_request_message.TestConfiguration(
            config_name=self.config_name, os='win-xp',)]
    return test_request_message.Stringize(test_case, json_readable=True)

  def _CreateRunner(self):
    """Creates a new runner.

    Returns:
      The newly created runner.
    """
    request = test_request.TestRequest(message=self._GetRequestMessage(),
                                       name='name')
    request.put()
    request.GetTestCase()

    runner = test_runner.TestRunner(
        request=request,
        config_hash=hashlib.sha1().hexdigest(),
        config_name=self.config_name,
        config_instance_index=0,
        num_config_instances=1,
        machine_id='id',
        started=datetime.datetime.now(),
        ended=datetime.datetime.now(),
        ran_successfully=True,
        )
    runner.put()

    return runner

  def testRecordRunnerStats(self):
    r_stats = runner_stats.RunnerStats.all().get()
    self.assertEqual(None, r_stats)

    # Create stats from a runner that didn't timeout.
    runner = self._CreateRunner()
    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertFalse(r_stats.timed_out)

    # Record stats from a runner that did timeout.
    runner.errors = 'Runner has become stale'
    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertTrue(r_stats.timed_out)

  def testRecordInvalidRunnerStats(self):
    # Create stats from a runner timed out, but was also successful.
    runner = self._CreateRunner()
    runner.errors = 'Runner has become stale'
    runner.ran_successfully = True
    runner.put()

    r_stats = runner_stats.RecordRunnerStats(runner)
    self.assertTrue(r_stats.timed_out)
    self.assertFalse(r_stats.success)

  def testGetStatsForOneRunnerStats(self):
    self.assertEqual({}, runner_stats.GetRunnerWaitStats())

    dimensions = 'machine_dimensions'
    wait = 10
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait)
    r_stats.put()

    wait_timedelta = datetime.timedelta(seconds=wait)
    expected_waits = {dimensions: (wait_timedelta, wait_timedelta,
                                   wait_timedelta)}
    self.assertEqual(expected_waits, runner_stats.GetRunnerWaitStats())

  def testGetStatsForMultipleRunners(self):
    config_dimensions = '{"os": "windows"}'
    median_time = 500
    max_time = 1000

    time_count_tuples = ((250, 5), (median_time, 1), (max_time, 5))
    for time, count in time_count_tuples:
      for _ in range(count):
        r_stats = _CreateRunnerStats(dimensions=config_dimensions)
        r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time,
                                                      time)
        r_stats.put()

    mean_wait = int(round(
        float(sum(time * count for time, count in time_count_tuples)) /
        sum(count for _, count in time_count_tuples)))

    expected_waits = {config_dimensions: (
        datetime.timedelta(seconds=mean_wait),
        datetime.timedelta(seconds=median_time),
        datetime.timedelta(seconds=max_time))}
    self.assertEqual(expected_waits, runner_stats.GetRunnerWaitStats())

  def testGetStatsAbortedRunners(self):
    r_stats = _CreateRunnerStats()
    # Set assign_time and end_time to None to imitate an aborted runner.
    r_stats.assigned_time = None
    r_stats.end_time = None
    r_stats.put()

    self.assertEqual({}, runner_stats.GetRunnerWaitStats())

  def testSwarmDeleteOldRunnerStats(self):
    self._mox.StubOutWithMock(runner_stats, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS - 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future after the start time, but
    # still close enough to the end time.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS + 5))
    runner_stats._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    r_stats = _CreateRunnerStats(dimensions='dimensions')
    r_stats.assigned_time = r_stats.created_time
    r_stats.put()
    self.assertEqual(1, runner_stats.RunnerStats.all().count())

    # Make sure that runners aren't deleted if they don't have an ended_time.
    runner_stats.DeleteOldRunnerStats().get_result()
    self.assertEqual(1, runner_stats.RunnerStats.all().count())

    # Make sure that new runner stats aren't deleted (even if they started
    # long ago).
    r_stats.end_time = r_stats.assigned_time + datetime.timedelta(days=3)
    r_stats.put()
    runner_stats.DeleteOldRunnerStats().get_result()
    self.assertEqual(1, runner_stats.RunnerStats.all().count())

    # Make sure that old runner stats are deleted.
    runner_stats.DeleteOldRunnerStats().get_result()
    self.assertEqual(0, runner_stats.RunnerStats.all().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
