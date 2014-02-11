# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runner Stats.

Contains the RunnerStats class and helper functions. These functions focus on
stats that are related to just a single runner (i.e. how long a runner waited).
"""


import datetime
import logging


from google.appengine.ext import ndb

# Number of days to evaluate when considering runner stats.
RUNNER_STATS_EVALUATION_CUTOFF_DAYS = 7


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


def RecordRunnerStats(runner):
  """Record all the stats when a runner has finished running.

  Args:
    runner: The runner that has just finished running.

  Returns:
    The newly created RunnerStats.
  """
  timed_out = bool(runner.errors and 'Runner has become stale' in runner.errors)

  runner_stats = RunnerStats(
      test_case_name=runner.name,
      dimensions=runner.dimensions,
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
                  runner.name)
    runner_stats.success = False
  runner_stats.put()

  return runner_stats


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.utcnow()


def QueryOldRunnerStats():
  """Returns keys for runners that are done and older than
  RUNNER_STATS_EVALUATION_CUTOFF_DAYS.
  """
  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=RUNNER_STATS_EVALUATION_CUTOFF_DAYS))
  return RunnerStats.query(
      RunnerStats.end_time < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))
