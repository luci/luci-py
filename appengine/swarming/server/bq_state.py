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


# This is to prevent PayloadTooLargeError when inserting to BQ.
RAW_LIMIT = 100


class BQError(Exception):
  """Raised if failed to insert rows."""

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

  # When in streaming mode, the most recent item that should be processed.
  # Exclusive.
  recent = ndb.DateTimeProperty(indexed=False)


class BqMigrationState(ndb.Model):
  """Used during Python => Go BQ export migration.

  ID is the table name being exported.
  """
  # Disable caching to allow writing from Go.
  _use_memcache = False

  # When to switch BQ exports from Python to Go.
  python_to_go = ndb.DateTimeProperty(indexed=False)


### Private APIs.


def _send_to_bq_raw(dataset, table_name, rows):
  """Sends the rows to BigQuery.

  Arguments:
    dataset: BigQuery dataset name that contains the table.
    table_name: BigQuery table to stream the rows to.
    rows: list of (row_id, row) rows to sent to BQ.
  """
  # BigQuery API doc:
  # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
  url = (
      'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/'
      '%s/insertAll') % (app_identity.get_application_id(), dataset, table_name)
  payload = {
      'kind':
          'bigquery#tableDataInsertAllRequest',
      # Do not fail entire request because of one bad row.
      # We handle invalid rows below.
      'skipInvalidRows':
          True,
      'ignoreUnknownValues':
          False,
      'rows': [{
          'insertId': row_id,
          'json': bqh.message_to_dict(row)
      } for row_id, row in rows],
  }
  res = net.json_request(
      url=url,
      method='POST',
      payload=payload,
      scopes=bqh.INSERT_ROWS_SCOPE,
      deadline=600)

  dropped = 0
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

    logging.error('Failed to insert row %s: %r', i, err)
    # TODO(crbug.com/1139745): exclude retryable error.
    raise BQError("failed to insert rows: %s" % err)
  if dropped:
    logging.warning('%d old rows silently dropped', dropped)


### Public API.


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
  RECENT_OFFSET = datetime.timedelta(seconds=240)
  minute = datetime.timedelta(seconds=60)

  start = utils.utcnow()
  start_rounded = datetime.datetime(*start.timetuple()[:5])
  recent_cutoff = start_rounded - RECENT_OFFSET

  total = 0
  state = BqState.get_by_id(table_name)
  if not state:
    state = BqState(
        id=table_name, ts=start,
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

  logging.info('Triggered %d tasks for %s', total, table_name)
  return total


def send_to_bq(table_name, rows):
  """Sends rows to a BigQuery table.

  Iterates until all rows are sent.
  """

  if rows:
    logging.info('Sending %d rows', len(rows))
    _send_to_bq_raw('swarming', table_name, rows)


def should_export(table_name, ts):
  """True if Python code should perform this export task.

  Arguments:
    table_name: a table name being exported e.g. "task_requests".
    ts: datetime.datetime of the start of the exported interval.

  Returns:
    True to do exports from Python, False to silently skip them.
  """
  state = BqMigrationState.get_by_id(table_name)
  if state and ts >= state.python_to_go:
    logging.info('Skipping, exports are done from Go')
    return False
  return True
