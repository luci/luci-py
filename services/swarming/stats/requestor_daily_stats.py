# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Requestor Daily Stats.

The RequestorDailyStats class is used to monitor the usage of Swarm per user.
"""

import datetime
import hashlib
import logging

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

# The number of days to keep requestor stats alive for
REQUESTOR_DAILY_STATS_LIFE_IN_DAYS = 90

# The number of hexdigits to use when determining the RequestParent.
# TODO(csharp): Look at generalizing the code used here and in test_request.
HEXDIGEST_DIGITS_TO_USE = 2

class RequestorParent(ndb.Model):
  """A simple parent for RequestorDailyStats.

  For a given RequestorParent, all the children will have the same value for:
  hashlib.sha1(child.requestor).hexdigest()[:HEXDIGEST_DIGITS_TO_USE].
  This value will also be the id for the parent.
  """


class RequestorDailyStats(ndb.Model):
  # The requestor's id.
  requestor = ndb.StringProperty()

  # The date these stats refer to.
  date = ndb.DateProperty()

  # The number of tests run by this requestor.
  tests_run = ndb.IntegerProperty(default=0, indexed=False)

  # The time spent waiting by all the different tests from this requestor,
  # in minutes.
  time_waiting = ndb.IntegerProperty(default=0, indexed=False)

  # The time spent running tests by this requestor, in minutes.
  time_running_tests = ndb.IntegerProperty(default=0, indexed=False)


@ndb.transactional
def UpdateDailyStatsTransaction(parent_key, model_id, requestor, date,
                                time_waiting, time_running_tests):
  """Update the daily stats atomically.

  Args:
   parent: The parent model.
   model_id: The id of the RequestorDailyStats model to update.
   requestor: The requestor who owns the model.
   date: The date of the model to update.
   time_waiting: The amount to update the time_waiting by.
   time_running_tests: The amount to update the time_running_tests by.
  """
  daily_stats = RequestorDailyStats.get_by_id(model_id, parent=parent_key)
  if not daily_stats:
    daily_stats = RequestorDailyStats(parent=parent_key, id=model_id,
                                      requestor=requestor, date=date)

  # Add the usage from the runner.
  daily_stats.tests_run += 1
  daily_stats.time_waiting += time_waiting
  daily_stats.time_running_tests += time_running_tests
  daily_stats.put()


def UpdateDailyStats(runner):
  """Add the new stats to the daily stats for this runner.

  Args:
    runner: The finished runner to record usage for.
  """
  # We should never try to include the usages of automatically retried machines,
  # because it wasn't the users fault, so they shouldn't be blamed for the
  # additional usage.
  if runner.automatic_retry_count:
    logging.error('Tried to add an automatically retried runner to the usage '
                  'stats.')
    return

  requestor = runner.requestor
  # TODO(csharp): Remove this check once requestors are required.
  if not requestor:
    return

  parent = RequestorParent.get_or_insert(
      hashlib.sha1(requestor).hexdigest()[:HEXDIGEST_DIGITS_TO_USE])

  model_id = '%s-%s' % (requestor, runner.created.date().isoformat())
  time_waiting = int(round(
      (runner.started - runner.created).total_seconds() / 60))
  time_running_tests = int(round(
      (runner.ended - runner.started).total_seconds() / 60))

  try:
    UpdateDailyStatsTransaction(parent.key, model_id, requestor,
                                runner.created.date(), time_waiting,
                                time_running_tests)
  except datastore_errors.TransactionFailedError:
    # Don't worry about failing to record the usage, it just means we might
    # under report user usage.
    logging.warning('Unable to update daily user usage.')


def DeleteOldRequestorDailyStats():
  """Clean up all RequestorDailyStats older than REQUESTOR_STATS_LIFE_IN_DAYS.

  Returns:
    The list of Futures for all the async delete calls.
  """
  logging.debug('DeleteOldRequestorDailyStats starting')

  old_cutoff = datetime.datetime.utcnow().date() - datetime.timedelta(
      days=REQUESTOR_DAILY_STATS_LIFE_IN_DAYS)

  old_requestor_daily_stats_query = RequestorDailyStats.query(
      RequestorDailyStats.date < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  futures = ndb.delete_multi_async(old_requestor_daily_stats_query)

  logging.debug('DeleteOldRequestorDailyStats done')

  return futures
