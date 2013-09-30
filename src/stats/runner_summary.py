#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Runner Summary.

Contains the RunnerSummary class and helper functions. These functions focus on
stats releated to multiple runners at once (i.e. what the average runner wait
time is).
"""


import collections
import datetime
import json
import logging

from google.appengine.ext import ndb

from common import test_request_message
from server import dimension_mapping
from server import test_runner
from stats import runner_stats

# The number of days to keep WaitSummaries before deleting them.
WAIT_SUMMARY_LIFE_IN_DAYS = 28


class RunnerSummary(ndb.Model):
  """Stores a basic summary of runners at a given point in time."""

  # The time of this snapshot.
  time = ndb.DateTimeProperty()

  # The dimensions for this snapshot.
  dimensions = ndb.StringProperty()

  # The number of pending runners.
  pending = ndb.IntegerProperty(indexed=False)

  # The number of running runners.
  running = ndb.IntegerProperty(indexed=False)


def GenerateSnapshotSummary():
  """Store a snapshot of the current runner summary."""
  # Store the time now so all the models will have the same time.
  current_time = datetime.datetime.now()

  for dimensions, summary in GetRunnerSummaryByDimension().iteritems():
    RunnerSummary(time=current_time,
                  dimensions=test_request_message.Stringize(dimensions),
                  pending=summary[0],
                  running=summary[1]).put()


class DimensionWaitSummary(ndb.Model):
  """Store the wait summaries for a single dimension."""

  # The dimension for this summary.
  dimensions = ndb.TextProperty(indexed=False)

  # The number of runners in this summary.
  num_runners = ndb.IntegerProperty(indexed=False)

  # The average wait for these runners to start (in seconds).
  mean_wait = ndb.IntegerProperty(indexed=False)

  # The start times are recorded with minute precision and then recorded in a
  # dictionary where the key is the time in minutes and the value is the number
  # of runners that waited that long.
  # Store the json.dump of the dictionary, since app engine doesn't have a
  # dictionary property.
  median_buckets = ndb.TextProperty(indexed=False)

  # The parent summary, which contains the starting and ending times that this
  # summary spans.
  summary_parent = ndb.KeyProperty(kind='WaitSummary')

  # Get the end time of the parent. This is very useful for getting ranges of
  # summaries.
  end_time = ndb.ComputedProperty(
      lambda self: self.summary_parent.get().end_time)


class WaitSummary(ndb.Model):
  """Stores a summary of wait times from the set of runners."""
  # The start time of the range of this summary.
  start_time = ndb.DateTimeProperty(required=True, indexed=False)

  # The end time of the range of this summary, this will be the latest assigned
  # time of any runner in this summary.
  end_time = ndb.DateTimeProperty(required=True)

  @property
  def children(self):
    return DimensionWaitSummary.query(
        DimensionWaitSummary.summary_parent == self.key)

  @classmethod
  def _pre_delete_hook(cls, key):  # pylint: disable=g-bad-name
    """Delete any children of this wait summary."""
    wait_summary = key.get()
    if not wait_summary:
      return

    if wait_summary:
      for child in wait_summary.children:
        child.key.delete()


class DimensionWaitSums(object):
  """A basic class to hold a sum of DimensionWaitSummaries."""

  def __init__(self, dimensions):
    self.dimensions = dimensions
    self.num_runners = 0
    self.mean_wait = 0
    self.median_buckets = collections.Counter()

  def Add(self, summary_to_add):
    """Adds the input summary to the current sum, if the dimensions match.

    All the values (mean_wait, median_buckets and longest_wait) are updated
    after each add to contain the correct values from the two sets together.

    Args:
      summary_to_add: A DimensionWaitSummary to add to the current total.
    """
    if self.dimensions != summary_to_add.dimensions:
      logging.error('Only able to add summaries that have the same dimensions')
      return

    self.mean_wait = (
        (self.mean_wait * self.num_runners +
         summary_to_add.mean_wait * summary_to_add.num_runners) /
        float(self.num_runners + summary_to_add.num_runners))

    self.num_runners += summary_to_add.num_runners
    self.median_buckets.update(json.loads(summary_to_add.median_buckets))


def _TotalSeconds(time_delta):
  """Get the number of seconds in the given time delta.

  Args:
    time_delta: The time delta to convert.

  Returns:
    The number of seconds in time_delta.
  """
  return int(round(time_delta.total_seconds()))


def _CreateMedianBuckets(times):
  """Convert the given set of time to a Counter of how often each minute occurs.

  Args:
    times: A list of times in seconds.

  Returns:
    A Counter object where the keys are the wait (in minutes) and the value is
    the number of occurences.
  """
  times_in_minutes = [int(round(item.total_seconds() / 60.0)) for item in times]

  return collections.Counter(times_in_minutes)


def GenerateWaitSummary():
  """Generate a stats summary to cover all new runner stats.

  If we have RunnerStats with times (1, 2, 3), it is possible to skip the second
  runner if it isn't visible during generation, and then never examine it since
  it falls in a time range that we already examined (this is possible because
  app engine is only eventually data consistent). The potentially loss of a
  runner's stats shouldn't cause any major problems.
  """
  newest_runner_waits = ndb.gql(
      'SELECT end_time FROM WaitSummary ORDER BY end_time DESC').get()
  start_time = (newest_runner_waits.end_time if newest_runner_waits else
                datetime.datetime.min)
  end_time = datetime.datetime.now()

  time_mappings = {}
  query = runner_stats.RunnerStats.gql('WHERE assigned_time != :1 and '
                                       'assigned_time > :2 ', None, start_time)

  def RecordRunnerDuration(runner_stat):
    # The query doesn't seem to examine milliseconds, so it can return models
    # that have already had their stats generated.
    if runner_stat.assigned_time <= start_time:
      return

    time_mappings.setdefault(runner_stat.dimensions, []).append(
        runner_stat.assigned_time - runner_stat.created_time)
    return runner_stat.assigned_time

  runner_end_times = query.map_async(RecordRunnerDuration).get_result()

  if runner_end_times:
    end_time = max(runner_end_times)

  wait_summary = WaitSummary(start_time=start_time,
                             end_time=end_time)
  wait_summary.put()

  for (dimensions, times) in time_mappings.iteritems():
    mean = sum(times, datetime.timedelta()) / len(times)

    # Store second level precision for mean and longest, but only minute
    # precision for median.
    dimension_summary = DimensionWaitSummary(
        dimensions=dimensions,
        num_runners=len(times),
        mean_wait=_TotalSeconds(mean),
        median_buckets=json.dumps(_CreateMedianBuckets(times)),
        summary_parent=wait_summary.key)
    dimension_summary.put()


def GetRunnerSummaryByDimension():
  """Returns a high level summary of the current runners per dimension.

  Returns:
    A dictionary where the key is the dimension and the value is a tuple of
    (pending runners, running runners) for the dimension.
  """
  def GetRunnerSummary(mapping):
    # Since the following commands are part of a GQL query, we can't use
    # the pythonic "is None", "is not None" or the explicit boolean comparison.
    # pylint: disable=g-equals-none,g-explicit-bool-comparison
    pending_runners_future = test_runner.TestRunner.query(
        test_runner.TestRunner.dimensions == mapping.dimensions,
        test_runner.TestRunner.started == None,
        test_runner.TestRunner.done == False).count_async()

    running_runners_future = test_runner.TestRunner.query(
        test_runner.TestRunner.dimensions == mapping.dimensions,
        test_runner.TestRunner.started != None,
        test_runner.TestRunner.done == False).count_async()
    # pylint: enable=g-equals-none,g-explicit-bool-comparison

    return (mapping.dimensions,
            (pending_runners_future.get_result(),
             running_runners_future.get_result()))

  dimension_query = dimension_mapping.DimensionMapping.query()
  return dict(dimension_query.map(GetRunnerSummary))


def GetRunnerWaitStats(days_to_show):
  """Returns the stats for how long runners are waiting.

  Args:
    days_to_show: The number of days to return the runner stats for.

  Returns:
    A dictionary where the key is the dimension, and the value is
    (mean wait, median wait, longest wait) for getting an assigned
    machine.
  """
  # Merge the various wait summaries.
  dimension_summaries = {}
  cutoff_date = datetime.datetime.today() - datetime.timedelta(
      days=days_to_show)

  dimension_wait_summary_query = DimensionWaitSummary.query(
      DimensionWaitSummary.end_time > cutoff_date)
  for dimension_wait in dimension_wait_summary_query:
    dimension_summaries.setdefault(
        dimension_wait.dimensions,
        DimensionWaitSums(dimension_wait.dimensions)
        ).Add(dimension_wait)

  # Convert the dimension results to the desire formats.
  results = {}
  for dimensions, summary in dimension_summaries.iteritems():
    sorted_elements = sorted(map(int, summary.median_buckets.elements()))
    median_time = int(sorted_elements[len(sorted_elements) / 2])
    longest_wait = sorted_elements[-1]
    results[dimensions] = [datetime.timedelta(seconds=summary.mean_wait),
                           datetime.timedelta(minutes=median_time),
                           datetime.timedelta(minutes=longest_wait)]

  return results


def GetRunnerWaitStatsBreakdown(days_to_show):
  """Returns the runner wait times, broken down by minute.

  Args:
    days_to_show: The number of days to return the wait times for.

  Returns:
    A dictionary where the key is the dimenion, and the value is a list of
    how many runners waited that many minutes (index 0 waited 0 minute, index 1
    waited 1 minute, etc).
  """
  dimension_median_buckets = {}
  cutoff_date = datetime.datetime.today() - datetime.timedelta(
      days=days_to_show)

  # Sum up all the collections.
  dimension_wait_summary_query = DimensionWaitSummary.query(
      DimensionWaitSummary.end_time > cutoff_date)
  for dimension_wait in dimension_wait_summary_query:
    dimension_median_buckets.setdefault(
        dimension_wait.dimensions, collections.Counter()).update(
            json.loads(dimension_wait.median_buckets))

  # Convert the collections to the correct output format.
  results = {}
  for dimensions, median_bucket in dimension_median_buckets.iteritems():
    for minute, count in median_bucket.iteritems():
      median_list = results.setdefault(dimensions, [])
      minute = int(minute)

      if len(median_list) <= minute:
        median_list.extend([0] * (minute - len(median_list) + 1))

      median_list[minute] += count

  return results


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOldWaitSummaries():
  """Clean up all the wait summaries that are older than a certain age.

  Returns:
    The rpc for the async delete call.
  """
  logging.debug('DeleteOldWaitSummaries starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=WAIT_SUMMARY_LIFE_IN_DAYS))

  old_wait_summary_query = WaitSummary.query(
      WaitSummary.end_time < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  rpc = ndb.delete_multi_async(old_wait_summary_query)

  logging.debug('DeleteOldWaitSummaries done')

  return rpc
