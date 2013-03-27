#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Stats Manager.

The stats manager contains all the logic to generate any stats that a user
might want to know about the swarm system, such as failure rates or currently
dead bots.
"""


import datetime
import logging


from google.appengine.ext import db


# Number of days to evaluate when considering runner stats.
RUNNER_STATS_EVALUATION_CUTOFF_DAYS = 7


class RunnerAssignment(db.Model):
  """Stores how long a runner's assignment took and its dimensions."""
  # The dimensions of the runner.
  dimensions = db.TextProperty()

  # The time in seconds that passed between swarm creating this runner and
  # it beginning execution.
  wait_time = db.IntegerProperty()

  # The start date of the runner, used to identify older data to be cleared.
  started = db.DateProperty()


def RecordRunnerAssignment(runner):
  """Records when a runner is assigned.

  Args:
    runner: The runner that is assigned.
  """
  runner_assignment = RunnerAssignment(
      dimensions=runner.GetDimensionsString(),
      wait_time=runner.GetWaitTime(),
      started=runner.started.date())
  runner_assignment.put()


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
  for runner_assignment in RunnerAssignment.all():
    time_mappings.setdefault(runner_assignment.dimensions, []).append(
        runner_assignment.wait_time)

  results = {}
  for (dimension, times) in time_mappings.iteritems():
    sorted_times = sorted(times)
    mean = sum(sorted_times) / len(sorted_times)
    median = sorted_times[len(sorted_times) / 2]

    results[dimension] = (mean, median, sorted_times[-1])

  return results


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOldRunnerStats():
  """Clean up all runners that are older than a certain age and done."""
  logging.debug('DeleteOldRunnersStats starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=RUNNER_STATS_EVALUATION_CUTOFF_DAYS))

  db.delete(RunnerAssignment.gql('WHERE started < :1', old_cutoff))

  logging.debug('DeleteOldRunnersStats done')
