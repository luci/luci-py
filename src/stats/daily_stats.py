#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Daily Stats.

Contains the DailyStats class and helper functions.
"""


import datetime
import logging

from google.appengine.ext import ndb

from stats import runner_stats

# The number of days to keep daily stats around before deleteing them.
DAILY_STATS_LIFE_IN_DAYS = 28


class DailyStats(ndb.Model):
  """Stores a summary of information generated each day."""

  # The date this summary is covering
  date = ndb.DateProperty(required=True)

  # The number of shards that finished (or timed out) during this day.
  shards_finished = ndb.IntegerProperty(default=0, indexed=False)

  # The number of shards that failed to terminate successfully (excluding
  # failures due to internal timeouts).
  shards_failed = ndb.IntegerProperty(default=0, indexed=False)

  # The number of shards that failed due to internal timeouts.
  shards_timed_out = ndb.IntegerProperty(default=0, indexed=False)

  # The total amount of time (in minutes) that all the runners waited to run.
  total_wait_time = ndb.IntegerProperty(default=0, indexed=False)

  # The total amount of time (in minutes) that runners were running on machines.
  total_running_time = ndb.IntegerProperty(default=0, indexed=False)


def _TimeDeltaToMinutes(delta):
  """Return the number of minutes (rounded) in a timedelta.

  Args:
    delta: The timedelta to convert.

  Returns:
    The number of minutes in the given timedelta.
  """
  return int(round(delta.total_seconds() / 60.0))


def GenerateDailyStats(day):
  """Generate the daily summary stats.

  Args:
    day: The day to generate stats for.

  Returns:
    True if the daily stats were successfully generated.
  """
  if ndb.gql('SELECT __key__ FROM DailyStats WHERE date = :1 LIMIT 1',
             day).get():
    logging.warning('Daily stats for %s already exist, skipping '
                    'GenerateDailyStats', day)
    return False

  day_midnight = datetime.datetime.combine(day, datetime.time())
  next_day_midnight = datetime.datetime.combine(
      day + datetime.timedelta(days=1),
      datetime.time())

  daily_stats = DailyStats(date=day)

  def ComputeRunnerStats(runner):
    """Add the stats from the given runner into the daily stats."""
    # If there is no assigned time, the runner never ran, so ignore it.
    if not runner.assigned_time:
      return

    # Update the time spent waiting and running.
    daily_stats.total_wait_time += _TimeDeltaToMinutes(
        runner.assigned_time - runner.created_time)
    daily_stats.total_running_time += _TimeDeltaToMinutes(
        runner.end_time - runner.assigned_time)

    # Update the raw counts.
    daily_stats.shards_finished += 1
    if runner.timed_out:
      daily_stats.shards_timed_out += 1
    elif not runner.success:
      daily_stats.shards_failed += 1

  # Find the number of shards that ran, as well as how many failed, during the
  # day.
  query = runner_stats.RunnerStats.query(
      runner_stats.RunnerStats.end_time >= day_midnight,
      runner_stats.RunnerStats.end_time < next_day_midnight,
      default_options=ndb.QueryOptions(
          projection=('assigned_time', 'created_time', 'end_time', 'success',
                      'timed_out')))

  query.map_async(ComputeRunnerStats).get_result()
  daily_stats.put()

  return True


def GetDailyStats(oldest_day):
  """Return all daily stats that are younger or equal to oldest_day.

  Args:
    oldest_day: The day to use as a cutoff to determine what stat to show.

  Returns:
    A sorted list (ascending order) of the daily stats.
  """
  return [stat for stat in
          DailyStats.gql('WHERE date >= :1 ORDER BY date ASC', oldest_day)]


def DeleteOldDailyStats():
  """Clean up all daily stats that are more than DAILY_STATS_LIFE_IN_DAYS old.

  Returns:
    The rpc for the async delete call (mainly meant for tests).
  """
  logging.debug('DeleteOldDailyStats starting')

  old_cutoff = (datetime.date.today() - datetime.timedelta(
      days=DAILY_STATS_LIFE_IN_DAYS))

  old_daily_stats_query = DailyStats.query(
      DailyStats.date < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  rpc = ndb.delete_multi_async(old_daily_stats_query)

  logging.debug('DeleteOldDailyStats done')

  return rpc
