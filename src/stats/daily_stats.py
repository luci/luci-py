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
  shards_finished = db.IntegerProperty(indexed=False)

  # The number of shards that failed to terminated successfully (excluding
  # failures due to internal timeouts).
  shards_failed = db.IntegerProperty(indexed=False)

  # The number of shards that failed due to internal timeouts.
  shards_timed_out = db.IntegerProperty(indexed=False)


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
  shard_count = 0
  failures = 0
  timeouts = 0
  query = db.GqlQuery('SELECT success, timed_out FROM RunnerStats WHERE '
                      'end_time >= :1 AND end_time < :2', day_midnight,
                      next_day_midnight)
  for runner in query:
    shard_count += 1
    if runner.timed_out:
      timeouts += 1
    elif not runner.success:
      failures += 1

  daily_stats = DailyStats(date=day, shards_finished=shard_count,
                           shards_failed=failures, shards_timed_out=timeouts)
  daily_stats.put()

  return True


def GetDailyStats(oldest_day):
  """Return all daily stats that are younger or equal to oldest_day.

  Args:
    oldest_day: The day to use as a cutoff to determine what stat to show.

  Returns:
    A sorted list (descending order) of the daily stats.
  """
  return [stat for stat in
          DailyStats.gql('WHERE date >= :1 ORDER BY date DESC', oldest_day)]
