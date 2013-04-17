#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Runner Stats.

Contains the RunnerStats class and helper functions.
"""


import datetime
import logging


from google.appengine.ext import db


# Number of days to evaluate when considering runner stats.
RUNNER_STATS_EVALUATION_CUTOFF_DAYS = 7


class RunnerStats(db.Model):
  """Stores basic stats about a runner.

  If a runner is restarted for any reason, such as an automatic retry, a new
  RunnerStats is created.
  """
  # The name of the test case run by the runner.
  test_case_name = db.StringProperty(required=True)

  # The dimensions of the runner.
  dimensions = db.TextProperty(required=True)

  # The number of runner instances for this test case and config combo.
  num_instances = db.IntegerProperty(required=True)

  # The 0 based instance index of the runner.
  instance_index = db.IntegerProperty(required=True)

  # The time that the runner is created and begins to look for a machine.
  created_time = db.DateTimeProperty(required=True)

  # The time that the runner is assigned a machine to run on. If the runner
  # never ran, this value can be empty.
  assigned_time = db.DateTimeProperty()

  # The time that the runner ended (either by the machine returning or through
  # timing out). If the runner never ran, this value can be empty.
  end_time = db.DateTimeProperty()

  # The machine id of the machine that ran this runner. Only valid after the
  # runner has been assigned. If the runner never ran, this value can be empty.
  machine_id = db.StringProperty()

  # Indicates if the runner tasks were successful . This is valid only once the
  # runner has finished or timed out.
  success = db.BooleanProperty(required=True)

  # Indicates if the runner timed out. This is valid only once the runner has
  # finished or timed out.
  timed_out = db.BooleanProperty(required=True)

  # The number of times the runner for this stats has been automatically retried
  # (each retry has its own RunnerStats).
  automatic_retry_count = db.IntegerProperty(required=True)


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


def _DropMicroseconds(time_delta):
  """Remove the microsecond precision from the time delta."""
  return datetime.timedelta(
      seconds=int(round(time_delta.total_seconds())))


def GetRunnerWaitStats():
  """Returns the stats for how long runners are waiting.

  Returns:
    A dictionary where the key is the dimension, and the value is
    (mean wait, median wait, longest wait) for getting an assigned
    machine. Only values from the last RUNNER_STATS_EVALUATION_CUTOFF_DAYS
    are consider.
  """
  # TODO(user): This should probably get just generated once every x hours
  # as a cron job and this function just returns the cached value.

  time_mappings = {}
  for runner_stats in RunnerStats.gql('WHERE assigned_time != :1', None):
    time_mappings.setdefault(runner_stats.dimensions, []).append(
        runner_stats.assigned_time - runner_stats.created_time)

  results = {}
  for (dimension, times) in time_mappings.iteritems():
    sorted_times = sorted(times)
    mean = sum(sorted_times, datetime.timedelta()) / len(sorted_times)
    median = sorted_times[len(sorted_times) / 2]

    # Drop the microseconds from the results, since we don't need that level of
    # precision.
    results[dimension] = (
        _DropMicroseconds(mean),
        _DropMicroseconds(median),
        _DropMicroseconds(sorted_times[-1]))

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

  rpc = db.delete_async(RunnerStats.gql('WHERE end_time != :1 and end_time < '
                                        ':2', None, old_cutoff))

  logging.debug('DeleteOldRunnersStats done')

  return rpc
