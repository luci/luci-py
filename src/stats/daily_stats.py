#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Daily Stats.

Contains the DailyStats class and helper functions.
"""


import datetime
import logging

from google.appengine.ext import db


class DailyStats(db.Model):
  """Stores a summary of information generated each day."""

  # The date this summary is covering
  date = db.DateProperty(required=True)

  # The number of shards that finished (or timed out) during this day.
  shards_finished = db.IntegerProperty(default=0, indexed=False)

  # The number of shards that failed to terminate successfully (excluding
  # failures due to internal timeouts).
  shards_failed = db.IntegerProperty(default=0, indexed=False)

  # The number of shards that failed due to internal timeouts.
  shards_timed_out = db.IntegerProperty(default=0, indexed=False)

  # The total amount of time (in minutes) that all the runners waited to run.
  total_wait_time = db.IntegerProperty(default=0, indexed=False)

  # The total amount of time (in minutes) that runners were running on machines.
  total_running_time = db.IntegerProperty(default=0, indexed=False)


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
  if db.GqlQuery('SELECT __key__ FROM DailyStats WHERE date = :1 LIMIT 1',
                 day).get():
    logging.warning('Daily stats for %s already exist, skipping '
                    'GenerateDailyStats', day)
    return False

  day_midnight = datetime.datetime.combine(day, datetime.time())
  next_day_midnight = datetime.datetime.combine(
      day + datetime.timedelta(days=1),
      datetime.time())

  # Find the number of shards that ran, as well as how many failed, during the
  # day.
  query = db.GqlQuery('SELECT success, timed_out, created_time, assigned_time, '
                      'end_time FROM RunnerStats WHERE end_time >= :1 AND '
                      'end_time < :2', day_midnight,
                      next_day_midnight)

  daily_stats = DailyStats(date=day)
  for runner in query:
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
