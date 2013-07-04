#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Runner Stats.

Contains the RunnerStats class and helper functions.
"""


import collections
import datetime
import json
import logging


from google.appengine.ext import db
from google.appengine.ext import ndb


# Number of days to evaluate when considering runner stats.
RUNNER_STATS_EVALUATION_CUTOFF_DAYS = 7

# The number of days to keep WaitSummaries before deleting them.
WAIT_SUMMARY_LIFE_IN_DAYS = 28


class RunnerStats(ndb.Model):
  """Stores basic stats about a runner.

  If a runner is restarted for any reason, such as an automatic retry, a new
  RunnerStats is created.
  """
  # The name of the test case run by the runner.
  test_case_name = ndb.StringProperty(required=True)

  # The dimensions of the runner.
  dimensions = ndb.TextProperty(required=True)

  # The number of runner instances for this test case and config combo.
  num_instances = ndb.IntegerProperty(required=True)

  # The 0 based instance index of the runner.
  instance_index = ndb.IntegerProperty(required=True)

  # The time that the runner is created and begins to look for a machine.
  created_time = ndb.DateTimeProperty(required=True)

  # The time that the runner is assigned a machine to run on. If the runner
  # never ran, this value can be empty.
  assigned_time = ndb.DateTimeProperty()

  # The time that the runner ended (either by the machine returning or through
  # timing out). If the runner never ran, this value can be empty.
  end_time = ndb.DateTimeProperty()

  # The machine id of the machine that ran this runner. Only valid after the
  # runner has been assigned. If the runner never ran, this value can be empty.
  machine_id = ndb.StringProperty()

  # Indicates if the runner tasks were successful . This is valid only once the
  # runner has finished or timed out.
  success = ndb.BooleanProperty(required=True)

  # Indicates if the runner timed out. This is valid only once the runner has
  # finished or timed out.
  timed_out = ndb.BooleanProperty(required=True)

  # The number of times the runner for this stats has been automatically retried
  # (each retry has its own RunnerStats).
  automatic_retry_count = ndb.IntegerProperty(required=True)


class WaitSummary(db.Model):
  """Stores a summary of wait times from the set of runners."""
  # The start time of the range of this summary.
  start_time = db.DateTimeProperty(required=True, indexed=False)

  # The end time of the range of this summary, this will be the latest assigned
  # time of any runner in this summary.
  end_time = db.DateTimeProperty(required=True)

  def delete(self):  # pylint: disable=g-bad-name
    """Delete any children of this wait summary."""
    # Even if there are no children, self.children is still defined.
    for child in self.children:
      child.delete()

    db.Model.delete(self)


class DimensionWaitSummary(db.Model):
  """Store the wait summaries for a single dimension."""

  # The dimension for this summary.
  dimensions = db.TextProperty(indexed=False)

  # The number of runners in this summary.
  num_runners = db.IntegerProperty(indexed=False)

  # The average wait for these runners to start (in seconds).
  mean_wait = db.IntegerProperty(indexed=False)

  # The start times are recorded with minute precision and then recorded in a
  # dictionary where the key is the time in minutes and the value is the number
  # of runners that waited that long.
  # Store the json.dump of the dictionary, since app engine doesn't have a
  # dictionary property.
  median_buckets = db.TextProperty(indexed=False)

  # The parent summary, which contains the starting and ending times that this
  # summary spans.
  summary_parent = db.ReferenceProperty(WaitSummary,
                                        collection_name='children')


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


def RecordRunnerStats(runner):
  """Record all the stats when a runner has finished running.

  Args:
    runner: The runner that has just finished running.

  Returns:
    The newly created RunnerStats.
  """
  timed_out = bool(runner.errors and 'Runner has become stale' in runner.errors)

  runner_stats = RunnerStats(
      test_case_name=runner.GetName(),
      dimensions=runner.GetDimensionsString(),
      num_instances=runner.num_config_instances,
      instance_index=runner.config_instance_index,
      created_time=runner.created,
      assigned_time=runner.started,
      end_time=runner.ended,
      machine_id=runner.machine_id,
      success=runner.ran_successfully or False,
      timed_out=timed_out,
      automatic_retry_count=runner.automatic_retry_count)

  if runner_stats.timed_out and runner_stats.success:
    logging.error('Runner, %s, was sucessful and timed out, trying as failure',
                  runner.GetName())
    runner_stats.success = False
  runner_stats.put()

  return runner_stats


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


def GenerateStats():
  """Generate a stats summary to cover all new runner stats.

  If we have RunnerStats with times (1, 2, 3), it is possible to skip the second
  runner if it isn't visible during generation, and then never examine it since
  it falls in a time range that we already examined (this is possible because
  app engine is only eventually data consistent). The potentially loss of a
  runner's stats shouldn't cause any major problems.
  """
  newest_runner_waits = db.GqlQuery(
      'SELECT end_time FROM WaitSummary ORDER BY end_time DESC').get()
  start_time = (newest_runner_waits.end_time if newest_runner_waits else
                datetime.datetime.min)
  end_time = datetime.datetime.now()

  time_mappings = {}
  query = RunnerStats.gql('WHERE assigned_time != :1 and assigned_time > :2 ',
                          None, start_time)

  def RecordRunnerDuration(runner_stats):
    # The query doesn't seem to examine milliseconds, so it can return models
    # that have already had their stats generated.
    if runner_stats.assigned_time <= start_time:
      return

    time_mappings.setdefault(runner_stats.dimensions, []).append(
        runner_stats.assigned_time - runner_stats.created_time)
    return runner_stats.assigned_time

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
        summary_parent=wait_summary)
    dimension_summary.put()


def GetRunnerWaitStats():
  """Returns the stats for how long runners are waiting.

  Returns:
    A dictionary where the key is the dimension, and the value is
    (mean wait, median wait, longest wait) for getting an assigned
    machine. Only values from the last RUNNER_STATS_EVALUATION_CUTOFF_DAYS
    are consider.
  """
  # Merge the various wait summaries.
  dimension_summaries = {}
  for dimension_wait in DimensionWaitSummary.all():
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


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOldRunnerStats():
  """Clean up all runners that are older than a certain age and done.

  Returns:
    The rpc for the async delete call (mainly meant for tests).
  """
  logging.debug('DeleteOldRunnersStats starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=RUNNER_STATS_EVALUATION_CUTOFF_DAYS))

  # '!= None' must be used instead of 'is not None' because these arguments
  # become part of a GQL query, where 'is not None' is invalid syntax.
  old_runner_stats_query = RunnerStats.query(
      RunnerStats.end_time != None,  # pylint: disable-msg=g-equals-none
      RunnerStats.end_time < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  rpc = ndb.delete_multi_async(old_runner_stats_query)

  logging.debug('DeleteOldRunnersStats done')

  return rpc


def DeleteOldWaitSummaries():
  """Clean up all the wait summaries that are older than a certain age."""
  logging.debug('DeleteOldWaitSummaries starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=WAIT_SUMMARY_LIFE_IN_DAYS))

  for wait in WaitSummary.gql('WHERE end_time < :1', old_cutoff):
    wait.delete()

  logging.debug('DeleteOldWaitSummaries done')
