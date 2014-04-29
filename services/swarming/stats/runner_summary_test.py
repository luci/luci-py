#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from common import test_request_message
from server import dimension_mapping
from server import test_helper
from stats import runner_stats
from stats import runner_summary
from support import test_case
from third_party.mox import mox

# pylint: disable=W0212


def _AddSecondsToDateTime(date_time, seconds):
  return date_time + datetime.timedelta(seconds=seconds)


def _CreateRunnerStats(dimensions='dimensions'):
  return runner_stats.RunnerStats(test_case_name='test',
                                  dimensions=dimensions,
                                  num_instances=1,
                                  instance_index=0,
                                  created_time=datetime.datetime.utcnow(),
                                  assigned_time=datetime.datetime.utcnow(),
                                  end_time=datetime.datetime.utcnow(),
                                  machine_id='id',
                                  success=True,
                                  aborted=False,
                                  automatic_retry_count=0)


def _CreateWaitSummary(start_time=None, end_time=None):
  start_time = start_time or datetime.datetime.utcnow()
  end_time = end_time or datetime.datetime.utcnow()

  wait_summary = runner_summary.WaitSummary(start_time=start_time,
                                            end_time=end_time)
  wait_summary.put()

  return wait_summary


class RunnerSummaryTest(test_case.TestCase):
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(RunnerSummaryTest, self).setUp()
    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()
    super(RunnerSummaryTest, self).tearDown()

  def testGenerateSnapshotSummary(self):
    runner_summary.GenerateSnapshotSummary()

    self.assertEqual(0, runner_summary.RunnerSummary.query().count())

    # Add a single pending runner.
    pending_runner = test_helper.CreatePendingRunner()
    dimensions = pending_runner.dimensions
    dimension_mapping.DimensionMapping(dimensions=dimensions).put()

    runner_summary.GenerateSnapshotSummary()

    self.assertEqual(1, runner_summary.RunnerSummary.query().count())

    summary = runner_summary.RunnerSummary.query().get()
    self.assertEqual(test_request_message.Stringize(pending_runner.dimensions),
                     summary.dimensions)
    self.assertEqual(1, summary.pending)
    self.assertEqual(0, summary.running)

  def testGenerateWaitSummaryForOneRunnerStats(self):
    dimensions = 'machine_dimensions'
    wait = 10
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait)
    r_stats.put()

    runner_summary.GenerateWaitSummary()
    self.assertEqual(1, runner_summary.WaitSummary.query().count())

    runner_waits = runner_summary.WaitSummary.query().get()
    self.assertEqual(1, runner_waits.children.count())
    self.assertEqual(datetime.datetime.min, runner_waits.start_time)
    self.assertEqual(r_stats.assigned_time, runner_waits.end_time)

    dimension_wait = runner_waits.children.get()
    self.assertEqual(dimensions, dimension_wait.dimensions)
    self.assertEqual(1, dimension_wait.num_runners)
    self.assertEqual(wait, dimension_wait.mean_wait)
    self.assertEqual({'0': 1}, json.loads(dimension_wait.median_buckets))

  def testGenerateWaitSummaryForMultipleRunners(self):
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

    runner_summary.GenerateWaitSummary()
    self.assertEqual(1, runner_summary.WaitSummary.query().count())

    runner_waits = runner_summary.WaitSummary.query().get()
    self.assertEqual(1, runner_waits.children.count())
    self.assertEqual(datetime.datetime.min, runner_waits.start_time)
    self.assertEqual(r_stats.assigned_time, runner_waits.end_time)

    dimension_wait = runner_waits.children.get()
    self.assertEqual(config_dimensions, dimension_wait.dimensions)
    self.assertEqual(num_runners, dimension_wait.num_runners)
    self.assertEqual(mean_wait, dimension_wait.mean_wait)
    self.assertEqual(expected_median_buckets,
                     json.loads(dimension_wait.median_buckets))

  def testGenerateWaitSummaryMultipleTimes(self):
    dimensions = 'machine_dimensions'
    wait = 10
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait)
    r_stats.put()

    runner_summary.GenerateWaitSummary()
    self.assertEqual(1, runner_summary.WaitSummary.query().count())

    # Now create a second set of stats, which should get stored in its own
    # model.
    wait_2 = wait * 5
    r_stats = _CreateRunnerStats(dimensions=dimensions)
    r_stats.assigned_time = _AddSecondsToDateTime(r_stats.created_time, wait_2)
    r_stats.put()

    runner_summary.GenerateWaitSummary()
    self.assertEqual(2, runner_summary.WaitSummary.query().count())

    runner_waits = list(runner_summary.WaitSummary.query())
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
    start_time = datetime.datetime.utcnow()
    for i in range(5):
      r_stats = _CreateRunnerStats(dimensions=dimensions)
      r_stats.assigned_time = _AddSecondsToDateTime(start_time, i)
      r_stats.put()

      runner_summary.GenerateWaitSummary()

    start_times = [
        wait.start_time for wait in runner_summary.WaitSummary.query()]
    self.assertEqual(len(start_times), len(set(start_times)), start_times)

    end_times = [wait.end_time for wait in runner_summary.WaitSummary.query()]
    self.assertEqual(len(end_times), len(set(end_times)), end_times)

  def testGetRunnerSummaryByDimension(self):
    self.assertEqual({}, runner_summary.GetRunnerSummaryByDimension())

    # Add a pending runner and its dimensions.
    pending_runner = test_helper.CreatePendingRunner()
    dimensions = pending_runner.dimensions
    dimension_mapping.DimensionMapping(dimensions=dimensions).put()

    self.assertEqual({dimensions: (1, 0)},
                     runner_summary.GetRunnerSummaryByDimension())

    # Add a running runner.
    running_runner = test_helper.CreatePendingRunner()
    running_runner.started = datetime.datetime.utcnow()
    running_runner.put()

    self.assertEqual({dimensions: (1, 1)},
                     runner_summary.GetRunnerSummaryByDimension())

    # Add a runner with a new dimension
    new_dimensions = running_runner.dimensions + 'extra dimensions'
    dimension_mapping.DimensionMapping(dimensions=new_dimensions).put()

    new_dimension_runner = test_helper.CreatePendingRunner()
    new_dimension_runner.dimensions = new_dimensions
    new_dimension_runner.put()

    self.assertEqual({dimensions: (1, 1),
                      new_dimensions: (1, 0)},
                     runner_summary.GetRunnerSummaryByDimension())

    # Make both the runners on the original dimensions as done.
    pending_runner.done = True
    pending_runner.put()
    running_runner.done = True
    running_runner.put()

    self.assertEqual({dimensions: (0, 0),
                      new_dimensions: (1, 0)},
                     runner_summary.GetRunnerSummaryByDimension())

  def testGetStatsForOneRunnerWaits(self):
    self.assertEqual({}, runner_summary.GetRunnerWaitStats(1))

    # Add some old wait stats that should be ignored.
    wait = 10
    dimensions = 'machine_dimensions'

    wait_summary = _CreateWaitSummary(end_time=(datetime.datetime.utcnow() -
                                                datetime.timedelta(days=2)))
    dimension_wait = runner_summary.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=wait,
        median_buckets=json.dumps({'0': 1}),
        summary_parent=wait_summary.key)
    dimension_wait.put()

    self.assertEqual({}, runner_summary.GetRunnerWaitStats(1))

    # Add some current stats that should be retrieved.
    wait_summary = _CreateWaitSummary()
    dimension_wait = runner_summary.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=wait,
        median_buckets=json.dumps({'0': 1}),
        summary_parent=wait_summary.key)
    dimension_wait.put()

    expected_waits = {
        dimensions: [datetime.timedelta(seconds=wait),
                     datetime.timedelta(minutes=round(wait / 60.0)),
                     datetime.timedelta(minutes=round(wait / 60.0))]}
    self.assertEqual(expected_waits, runner_summary.GetRunnerWaitStats(1))

  def testGetStatsForMultipleRunners(self):
    dimensions = '{"os": "windows"}'
    times = [1, 1, 2, 60, 240, 240, 240]
    for i in times:
      wait_summary = _CreateWaitSummary()
      dimension_wait = runner_summary.DimensionWaitSummary(
          dimensions=dimensions,
          num_runners=1,
          mean_wait=i,
          median_buckets=json.dumps({str(int(round(i / 60.0))): 1}),
          summary_parent=wait_summary.key)
      dimension_wait.put()

    mean_wait = float(sum(times)) / len(times)
    median_wait = times[len(times) / 2]
    max_time = times[-1]

    expected_waits = {dimensions: [
        datetime.timedelta(seconds=mean_wait),
        datetime.timedelta(minutes=round(median_wait / 60.0)),
        datetime.timedelta(minutes=round(max_time / 60.0))]}
    self.assertEqual(expected_waits, runner_summary.GetRunnerWaitStats(1))

  def testGetStatsAbortedRunners(self):
    r_stats = _CreateRunnerStats()
    # Set assign_time and end_time to None to imitate an aborted runner.
    r_stats.assigned_time = None
    r_stats.end_time = None
    r_stats.put()

    self.assertEqual({}, runner_summary.GetRunnerWaitStats(1))

  def testGetRunnerWaitStatsBreakdown(self):
    self.assertEqual({}, runner_summary.GetRunnerWaitStatsBreakdown(1))

    # Add data that is too old and ensure it is ignored.
    wait_summary = _CreateWaitSummary(end_time=(datetime.datetime.utcnow() -
                                                datetime.timedelta(days=2)))
    dimensions = 'machine_dimensions'
    dimension_wait = runner_summary.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=1,
        median_buckets=json.dumps({'0': 1}),
        summary_parent=wait_summary.key)
    dimension_wait.put()

    self.assertEqual({}, runner_summary.GetRunnerWaitStatsBreakdown(1))

    # Add data that should be returned.
    wait_summary = _CreateWaitSummary()
    dimension_wait = runner_summary.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=1,
        median_buckets=json.dumps({'0': 1, '5': 3}),
        summary_parent=wait_summary.key)
    dimension_wait.put()

    expected_output = {dimensions: [1, 0, 0, 0, 0, 3]}
    self.assertEqual(expected_output,
                     runner_summary.GetRunnerWaitStatsBreakdown(1))

    # Add a second set of data to ensure it is properly added to the first one.
    wait_summary = _CreateWaitSummary()
    dimension_wait = runner_summary.DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=1,
        mean_wait=1,
        median_buckets=json.dumps({'0': 1, '3': 2}),
        summary_parent=wait_summary.key)
    dimension_wait.put()

    expected_output = {dimensions: [2, 0, 0, 2, 0, 3]}
    self.assertEqual(expected_output,
                     runner_summary.GetRunnerWaitStatsBreakdown(1))

  def testDeleteOldWaitStats(self):
    self._mox.StubOutWithMock(runner_summary, '_GetCurrentTime')

    # Set the current time to the future, but not too much, so the model
    # isn't deleted.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=runner_summary.WAIT_SUMMARY_LIFE_IN_DAYS - 1))
    runner_summary._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future so the model is deleted.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=runner_summary.WAIT_SUMMARY_LIFE_IN_DAYS + 5))
    runner_summary._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    wait_summary = _CreateWaitSummary(end_time=datetime.datetime.utcnow())
    dimension_wait = runner_summary.DimensionWaitSummary(
        summary_parent=wait_summary.key)
    dimension_wait.put()
    self.assertEqual(1, runner_summary.WaitSummary.query().count())
    self.assertEqual(1, runner_summary.DimensionWaitSummary.query().count())

    # Make sure that stats aren't deleted if they aren't old.
    ndb.delete_multi(runner_summary.QueryOldWaitSummaries())
    self.assertEqual(1, runner_summary.WaitSummary.query().count())
    self.assertEqual(1, runner_summary.DimensionWaitSummary.query().count())

    # Make sure that old runner stats are deleted.
    ndb.delete_multi(runner_summary.QueryOldWaitSummaries())
    self.assertEqual(0, runner_summary.WaitSummary.query().count())
    self.assertEqual(0, runner_summary.DimensionWaitSummary.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
