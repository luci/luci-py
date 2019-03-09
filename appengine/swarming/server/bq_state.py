# coding: utf-8
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Common code to stream (and backfill) rows to BigQuery."""

import datetime
import logging

from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import net
from components import utils
import bqh


# Oldest entity to backfill.
#
# This must match the BigQuery partitioned table expiration.
# TODO(maruel): Switch back to 365+183 once quota issues are fixed.
# https://crbug.com/939204
_OLDEST_BACKFILL = datetime.timedelta(days=32)


### Models


class BqState(ndb.Model):
  """Stores the last BigQuery successful writes.

  Key id: table_name.

  By storing the successful writes, this enables not having to read from BQ. Not
  having to sync state *from* BQ means one less RPC that could fail randomly.
  """
  # Disable memcache, so that deleting the entity takes effect without having to
  # clear memcache.
  _use_memcache = False

  # Last time this entity was updated.
  ts = ndb.DateTimeProperty(indexed=False)

  # Deprecated.
  # db_key and bq_key of the last row uploaded. Some engines uses db_key, others
  # use bq_key, so simply save both.
  last_db_key = ndb.StringProperty(indexed=False)
  last_bq_key = ndb.StringProperty(indexed=False)

  # Deprecated.
  # db_key and bq_key of the rows previously uploaded that had failed and should
  # be retried.
  failed_db_keys = ndb.StringProperty(repeated=True, indexed=False)
  failed_bq_keys = ndb.StringProperty(repeated=True, indexed=False)

  # When in backfill mode, the time of the next item that should be processed.
  # If it's over _OLDEST_BACKFILL old, don't look at it.
  # Exclusive.
  oldest = ndb.DateTimeProperty(indexed=False)
  # When in streaming mode, the most recent item that should be processed.
  # Exclusive.
  recent = ndb.DateTimeProperty(indexed=False)

  def _pre_put_hook(self):
    super(BqState, self)._pre_put_hook()
    if bool(self.recent) != bool(self.oldest):
      raise datastore_errors.BadValueError(
          'Internal error; recent and oldest must both be set')
    if self.oldest:
      if self.oldest >= self.recent:
        raise datastore_errors.BadValueError('Internal error; oldest >= recent')
      if self.oldest.second or self.oldest.microsecond:
        raise datastore_errors.BadValueError(
            'Internal error; oldest has seconds')
      if self.recent.second or self.recent.microsecond:
        raise datastore_errors.BadValueError(
            'Internal error; recent has seconds')


### Private APIs.


def _send_to_bq_raw(dataset, table_name, rows):
  """Sends the rows to BigQuery.

  Arguments:
    dataset: BigQuery dataset name that contains the table.
    table_name: BigQuery table to stream the rows to.
    rows: list of (row_id, row) rows to sent to BQ.

  Returns:
    indexes of rows that failed to be sent.
  """
  # BigQuery API doc:
  # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
  url = (
      'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/'
      '%s/insertAll') % (app_identity.get_application_id(), dataset, table_name)
  payload = {
    'kind': 'bigquery#tableDataInsertAllRequest',
    # Do not fail entire request because of one bad row.
    # We handle invalid rows below.
    'skipInvalidRows': True,
    'ignoreUnknownValues': False,
    'rows': [
      {'insertId': row_id, 'json': bqh.message_to_dict(row)}
      for row_id, row in rows
    ],
  }
  res = net.json_request(
      url=url, method='POST', payload=payload, scopes=bqh.INSERT_ROWS_SCOPE,
      deadline=600)

  dropped = 0
  failed = []
  # Use this error message string to detect the error where we're pushing data
  # that is too old. This can occasionally happen as a cron job looks for old
  # entity and by the time it's sending them BigQuery doesn't accept them, just
  # skip these and log a warning.
  out_of_time = (
      'You can only stream to date range within 365 days in the past '
      'and 183 days in the future relative to the current date')
  # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response
  for line in res.get('insertErrors', []):
    i = line['index']
    err = line['errors'][0]
    if err['reason'] == 'invalid' and out_of_time in err['message']:
      # Silently drop it. The rationale is that if it is not skipped, the loop
      # will get stuck on it.
      dropped += 1
      continue
    if not failed:
      # Log the error for the first entry, useful to diagnose schema failure.
      logging.error('Failed to insert row %s: %r', i, err)
    failed.append(i)
  if dropped:
    logging.warning('%d old rows silently dropped', dropped)
  return failed


def _send_to_bq(dataset, table_name, rows):
  """Sends the rows to BigQuery.

  Deprecated.

  Arguments:
    dataset: BigQuery dataset name that contains the table.
    table_name: BigQuery table to stream the rows to.
    rows: list of tuple(db_key, bq_key, row); (bq_key, row) is to sent to BQ.
        db_key and bq_key must be strings, row must be a protobuf message.

  Returns:
    Datastore keys and BQ keys of rows that failed to be sent.
  """
  failed = _send_to_bq_raw(dataset, table_name, [i[1:] for i in rows])
  failed_db_keys = []
  failed_bq_keys = []
  for index in failed:
    db_key, bq_key, _row = rows[index]
    failed_db_keys.append(db_key)
    failed_bq_keys.append(bq_key)
  return failed_db_keys, failed_bq_keys


### Public API.


def cron_send_to_bq(table_name, get_oldest_key, get_rows, fetch_rows):
  """Sends rows to a BigQuery table.

  To ensure no items are missing, we query the last item in the table, then look
  up the last item in the DB, and stream these.

  Deprecated.

  Logs insert errors and returns a list of timestamps of row that could
  not be inserted.

  Arguments:
    table_name: BigQuery table name. Also used as the key id to use for the
        BqState entity.
    get_oldest_key: returns the bq_key of the oldest row to fetch. Used only
        when there is not state stored. Return tuple(db_key, bq_key). Accepts no
        argument.
    get_rows: returns a list of tuple(db_key, bq_key, row) to send to BigQuery
        into table table_name. Accepts (db_key, bq_key, size).
    fetch_rows: returns a list of tuple(db_key, bq_key, row) data for individual
        rows that had to be retried. Accepts a list of db_key.

  Returns:
    total number of bot events sent to BQ.
  """
  total = 0
  start = utils.utcnow()
  state = BqState.get_by_id(table_name)
  if state and not state.last_db_key:
    logging.info('Skipping, new style task was initiated')
    return

  if not state:
    # No saved state found. Find the oldest entity to send.
    db_key, bq_key = get_oldest_key()
    logging.info('get_oldest_key() = %s, %s', db_key, bq_key)
    if not db_key or not bq_key:
      return total
    state = BqState(
        id=table_name, ts=start, last_db_key=db_key, last_bq_key=bq_key)
    state.put()

  # At worst if it dies, the cron job will run for a while.
  # At worst if memcache is cleared, two cron job will run concurrently. It's
  # inefficient but it's not going to break.
  namespace = 'bq_state.' + table_name
  if not memcache.add('running', 'yep', time=400, namespace=namespace):
    logging.debug('Other cron already running')
    return total

  try:
    should_stop = start + datetime.timedelta(seconds=300)
    while utils.utcnow() < should_stop:
      if not memcache.get('running', namespace=namespace):
        logging.info('memcache was cleared')
        return total

      # Send at most 500 items at a time to reduce the risks of failure.
      max_batch = 500
      # There cannot be more than 500 failed pending send.
      size = max_batch - len(state.failed_db_keys)

      if size:
        rows = get_rows(state.last_db_key, state.last_bq_key, size)
        if not rows:
          logging.info(
              'get_rows(%s, %s, %s) returned 0 rows',
              state.last_db_key, state.last_bq_key, size)
          if not state.failed_db_keys:
            # We're done!
            return total
        else:
          # Save the last row bq_key from tuple(db_key, bq_key, row).
          db_key, bq_key, _row = rows[-1]
          logging.info(
              'get_rows(%s, %s, %s) returned %d rows; last is now %s, %s',
              state.last_db_key, state.last_bq_key, size, len(rows),
              db_key, bq_key)
          state.last_db_key = db_key
          state.last_bq_key = bq_key
      else:
        # Failed is full. Retry again.
        rows = []

      # If some rows were removed, remove them from the failed_db_keys field.
      if state.failed_db_keys:
        backlog = fetch_rows(state.failed_db_keys, state.failed_bq_keys)
        logging.info(
            'fetch_rows(%d rows) returned %d rows',
            len(state.failed_db_keys), len(backlog))
        rows.extend(backlog)
      elif not rows:
        # We've hit the end.
        return total

      # We continue if state.failed_db_keys was set, so BqState.failed_db_keys
      # can be zapped below.

      if rows:
        logging.info('Sending %d rows', len(rows))
        state.failed_db_keys, state.failed_bq_keys = _send_to_bq(
            'swarming', table_name, rows)
        if state.failed_db_keys:
          logging.error('Failed to insert %s rows', len(state.failed_db_keys))
        total += len(rows) - len(state.failed_db_keys)

      # The next cron job round will retry the ones that failed, and last_bq_key
      # is updated.
      state.ts = utils.utcnow()
      state.put()
  finally:
    memcache.delete('running', namespace=namespace)


def cron_trigger_tasks(
    table_name, baseurl, task_name, max_seconds, max_taskqueues):
  """Triggers tasks to send rows to BigQuery via time based slicing.

  It triggers one task queue task per 1 minute slice of time to process. It will
  process up to 2 minutes before now, and up to _OLDEST_BACKFILL time ago. It
  tries to go both ways, both keeping up with new items, and backfilling.

  This function is expected to be called once per minute.

  This function stores in BqState the timestamps of last enqueued events.

  Arguments:
    table_name: BigQuery table name. Also used as the key id to use for the
        BqState entity.
    baseurl: url for the task queue, which the timestamp will be appended to.
    task_name: task name the URL represents.
    max_seconds: the maximum amount of time to run; after which it should stop
        early even if there is still work to do.
    max_items: the maximum number of task queue triggered; to limit parallel
        execution.

  Returns:
    total number of task queue tasks triggered.
  """
  RECENT_OFFSET = datetime.timedelta(seconds=120)
  minute = datetime.timedelta(seconds=60)

  start = utils.utcnow()
  start_rounded = datetime.datetime(*start.timetuple()[:5])
  recent_cutoff = start_rounded - RECENT_OFFSET
  oldest_cutoff = start_rounded - _OLDEST_BACKFILL

  total = 0
  state = BqState.get_by_id(table_name)
  if not state or not state.oldest:
    # Flush the previous state, especially if it was the deprecated way, and
    # start over.
    state = BqState(
        id=table_name, ts=start,
        oldest=recent_cutoff - minute,
        recent=recent_cutoff)
    state.put()

  # First trigger recent row(s).
  while total < max_taskqueues:
    if (state.recent >= recent_cutoff or
        (utils.utcnow() - start).total_seconds() >= max_seconds):
      break
    t = state.recent.strftime(u'%Y-%m-%dT%H:%M')
    if not utils.enqueue_task(baseurl + t, task_name):
      logging.warning('Enqueue for %t failed')
      break
    state.recent += minute
    state.ts = utils.utcnow()
    state.put()
    total += 1

  # Then trigger for backfill of old rows.
  while total < max_taskqueues:
    if (state.oldest <= oldest_cutoff or
        (utils.utcnow() - start).total_seconds() >= max_seconds):
      break
    t = state.oldest.strftime(u'%Y-%m-%dT%H:%M')
    if not utils.enqueue_task(baseurl + t, task_name):
      logging.warning('Enqueue for %t failed')
      break
    state.oldest -= minute
    state.ts = utils.utcnow()
    state.put()
    total += 1

  logging.info('Triggered %d tasks for %s', total, table_name)
  return total


def send_to_bq(table_name, rows):
  """Sends rows to a BigQuery table.

  Iterates until all rows are sent.
  """
  failures = 0
  if rows:
    logging.info('Sending %d rows', len(rows))
    while rows:
      failed = _send_to_bq_raw('swarming', table_name, rows)
      if not failed:
        break
      failures += len(failed)
      logging.warning('Failed to insert %s rows', len(failed))
      rows = [rows[i] for i in failed]
  return failures
