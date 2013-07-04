#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for RunnerStats class."""



import datetime
import hashlib
import json
import logging
import unittest


from google.appengine.ext import testbed
from google.appengine.ext import ndb

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


def _CreateWaitSummary(start_time=None, end_time=None):
  start_time = start_time or datetime.datetime.now()
  end_time = end_time or datetime.datetime.now()

  wait_summary = runner_stats.WaitSummary(start_time=start_time,
                                          end_time=end_time)
  wait_summary.put()

  return wait_summary


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
        request=request.key,
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
    r_stats = runner_stats.RunnerStats.query().get()
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

  def testGenerateStatsForOneRunnerStats(self):
    dimensions = 'machine_dimensions'
    wait = 10
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait)
    r_stats.put()

    runner_stats.GenerateStats()
    self.assertEqual(1, runner_stats.WaitSummary.all().count())

    runner_waits = runner_stats.WaitSummary.all().get()
    self.assertEqual(1, runner_waits.children.count())
    self.assertEqual(datetime.datetime.min, runner_waits.start_time)
    self.assertEqual(r_stats.assigned_time, runner_waits.end_time)

    dimension_wait = runner_waits.children.get()
    self.assertEqual(dimensions, dimension_wait.dimensions)
    self.assertEqual(1, dimension_wait.num_runners)
    self.assertEqual(wait, dimension_wait.mean_wait)
    self.assertEqual({'0': 1}, json.loads(dimension_wait.median_buckets))

  def testGenerateStatsForMultipleRunners(self):
    config_dimensions = '{"os": "windows"}'
    median_time = 500
    max_time = 1000

    time_counts = ((250, 5), (median_time, 1), (max_time, 5))
    expected_median_buckets = {}
    num_runners = 0
    # Create the number of runner stats that time_counts specifies.
    for time, count in time_counts:
      expected_median_buckets[str(int(round(time/60.0)))] = count
      num_runners += count
      for _ in range(count):
        r_stats = _CreateRunnerStats(dimensions=config_dimensions)
        r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time,
                                                      time)
        r_stats.put()

    mean_wait = int(round(
        float(sum(time * count for time, count in time_counts)) /
        sum(count for _, count in time_counts)))

    runner_stats.GenerateStats()
    self.assertEqual(1, runner_stats.WaitSummary.all().count())

    runner_waits = runner_stats.WaitSummary.all().get()
    self.assertEqual(1, runner_waits.children.count())
    self.assertEqual(datetime.datetime.min, runner_waits.start_time)
    self.assertEqual(r_stats.assigned_time, runner_waits.end_time)

    dimension_wait = runner_waits.children.get()
    self.assertEqual(config_dimensions, dimension_wait.dimensions)
    self.assertEqual(num_runners, dimension_wait.num_runners)
    self.assertEqual(mean_wait, dimension_wait.mean_wait)
    self.assertEqual(expected_median_buckets,
                     json.loads(dimension_wait.median_buckets))

  def testGenerateStatsMultipleTimes(self):
    dimensions = 'machine_dimensions'
    wait = 10
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait)
    r_stats.put()

    runner_stats.GenerateStats()
    self.assertEqual(1, runner_stats.WaitSummary.all().count())

    # Now create a second set of stats, which should get stored in its own
    # model.
    wait_2 = wait * 5
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait_2)
    r_stats.put()

    runner_stats.GenerateStats()
    self.assertEqual(2, runner_stats.WaitSummary.all().count())

    runner_waits = list(runner_stats.WaitSummary.all())
    self.assertNotEqual(runner_waits[0].start_time, runner_waits[1].start_time)
    if runner_waits[0].start_time < runner_waits[1].start_time:
      oldest = 0
      youngest = 1
    else:
      oldest = 1
      youngest = 0

    self.assertEqual(datetime.datetime.min, runner_waits[oldest].start_time)
    self.assertEqual(runner_waits[oldest].end_time,
                     runner_waits[youngest].start_time)

    for i, wait_summary in enumerate(runner_waits):
      self.assertEqual(1, wait_summary.children.count())
      dimension_wait = wait_summary.children.get()
      self.assertEqual(dimensions, dimension_wait.dimensions)
      self.assertEqual(1, dimension_wait.num_runners)

      if i == oldest:
        self.assertEqual(wait, dimension_wait.mean_wait)
        self.assertEqual({'0': 1}, json.loads(dimension_wait.median_buckets))
      else:
        self.assertEqual(wait_2, dimension_wait.mean_wait)
        self.assertEqual({'1': 1}, json.loads(dimension_wait.median_buckets))

  def testEnsureCorrectStartTime(self):
    # Create a set of summaries and ensure they all have unique start and end
    # times.
    dimensions = 'machine_dimensions'
    start_time = datetime.datetime.now()
    for i in range(5):
      r_stats = _CreateRunnerStats(dimensions=dimensions)
      r_stats.assigned_time = _AddSecondsToDateTime(start_time, i)
      r_stats.put()

      runner_stats.GenerateStats()

    start_times = [wait.start_time for wait in runner_stats.WaitSummary.all()]
    self.assertEqual(len(start_times), len(set(start_times)), start_times)

    end_times = [wait.end_time for wait in runner_stats.WaitSummary.all()]
    self.assertEqual(len(end_times), len(set(end_times)), end_times)

  def testGetStatsForOneRunnerWaits(self):
    self.assertEqual({}, runner_stats.GetRunnerWaitStats())

    wait = 10
    dimensions = 'machine_dimensions'

    wait_summary = _CreateWaitSummary()
    dimension_wait = runner_stats.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=wait,
        median_buckets=json.dumps({'0': 1}),
        longest_wait=wait,
        summary_parent=wait_summary)
    dimension_wait.put()

    expected_waits = {
        dimensions: [datetime.timedelta(seconds=wait),
                     datetime.timedelta(minutes=round(wait / 60.0)),
                     datetime.timedelta(minutes=round(wait / 60.0))]}
    self.assertEqual(expected_waits, runner_stats.GetRunnerWaitStats())

  def testGetStatsForMultipleRunners(self):
    dimensions = '{"os": "windows"}'
    times = [1, 1, 2, 60, 240, 240, 240]
    for i in times:
      wait_summary = _CreateWaitSummary()
      dimension_wait = runner_stats.DimensionWaitSummary(
          dimensions=dimensions,
          num_runners=1,
          mean_wait=i,
          median_buckets=json.dumps({str(int(round(i / 60.0))): 1}),
          longest_wait=i,
          summary_parent=wait_summary)
      dimension_wait.put()

    mean_wait = float(sum(times)) / len(times)
    median_wait = times[len(times) / 2]
    max_time = times[-1]

    expected_waits = {dimensions: [
        datetime.timedelta(seconds=mean_wait),
        datetime.timedelta(minutes=round(median_wait / 60.0)),
        datetime.timedelta(minutes=round(max_time / 60.0))]}
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

  def testDeleteOldWaitStats(self):
    self._mox.StubOutWithMock(runner_stats, '_GetCurrentTime')

    # Set the current time to the future, but not too much, so the model
    # isn't deleted.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.WAIT_SUMMARY_LIFE_IN_DAYS - 1))
    runner_stats._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future so the model is deleted.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=runner_stats.WAIT_SUMMARY_LIFE_IN_DAYS + 5))
    runner_stats._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    wait_summary = _CreateWaitSummary(end_time=datetime.datetime.now())
    dimension_wait = runner_stats.DimensionWaitSummary(
        summary_parent=wait_summary)
    dimension_wait.put()
    self.assertEqual(1, runner_stats.WaitSummary.all().count())
    self.assertEqual(1, runner_stats.DimensionWaitSummary.all().count())

    # Make sure that stats aren't deleted if they aren't old.
    runner_stats.DeleteOldWaitSummaries()
    self.assertEqual(1, runner_stats.WaitSummary.all().count())
    self.assertEqual(1, runner_stats.DimensionWaitSummary.all().count())

    # Make sure that old runner stats are deleted.
    runner_stats.DeleteOldWaitSummaries()
    self.assertEqual(0, runner_stats.WaitSummary.all().count())
    self.assertEqual(0, runner_stats.DimensionWaitSummary.all().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
