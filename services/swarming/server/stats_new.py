# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates statistics out of logs. Contains the backend code.

It's important to keep logs concise for general performance concerns. Each http
handler should strive to do only one stats log entry per request.
"""

import logging

import webapp2
from google.appengine.api import logservice
from google.appengine.ext import ndb
from google.appengine.runtime import DeadlineExceededError

from components import decorators
from components import stats_framework


### Models


class _Snapshot(ndb.Model):
  """A snapshot of statistics, to be embedded in another entity."""
  # General HTTP details.
  http_requests = ndb.IntegerProperty(default=0)
  http_failures = ndb.IntegerProperty(default=0)

  # Actions.
  shards_assigned = ndb.IntegerProperty(default=0)
  shards_bot_died = ndb.IntegerProperty(default=0)
  shards_completed = ndb.IntegerProperty(default=0)
  shards_enqueued = ndb.IntegerProperty(default=0)
  shards_request_expired = ndb.IntegerProperty(default=0)
  shards_updated = ndb.IntegerProperty(default=0)

  def accumulate(self, rhs):
    stats_framework.accumulate(self, rhs, [])


### Utility


_VALID_ACTIONS = frozenset(
  [
    'shard_assigned',
    'shard_bot_died',
    'shard_completed',
    'shard_enqueued',
    'shard_request_expired',
    'shard_updated',
  ])


def _parse_line(line, values):
  """Updates a Snapshot instance with a processed statistics line if relevant.
  """
  try:
    # TODO(maruel): Use measurement when it makes sense, like categorizing
    # shards into dimensions.
    action, _measurement = line.split('; ', 1)
  except ValueError:
    logging.error('Failed to parse stats line\n%s', line)
    return False

  if action == 'shard_assigned':
    values.shards_assigned += 1
  elif action == 'shard_bot_died':
    values.shards_bot_died += 1
  elif action == 'shard_completed':
    values.shards_completed += 1
  elif action == 'shard_enqueued':
    values.shards_enqueued += 1
  elif action == 'shard_request_expired':
    values.shards_request_expired += 1
  elif action == 'shard_updated':
    values.shards_updated += 1
  else:
    logging.error('Unknown stats action\n%s', line)
    return False
  return True


def _extract_snapshot_from_logs(start_time, end_time):
  """Returns a _Snapshot from the processed logs for the specified interval.

  The data is retrieved from logservice via stats_framework.
  """
  values = _Snapshot()
  total_lines = 0
  parse_errors = 0
  for entry in stats_framework.yield_entries(start_time, end_time):
    values.http_requests += 1
    if entry.request.status >= 400:
      values.http_failures += 1

    for l in entry.entries:
      if _parse_line(l, values):
        total_lines += 1
      else:
        parse_errors += 1

  logging.debug('Parsed %d lines, %d errors', total_lines, parse_errors)
  return values


### Public API


STATS_HANDLER = stats_framework.StatisticsFramework(
    'global_stats', _Snapshot, _extract_snapshot_from_logs)


def add_entry(action, value):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  assert action in _VALID_ACTIONS
  stats_framework.add_entry('%s; %s' % (action, value))


### Handlers


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  @decorators.require_cronjob
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    try:
      i = STATS_HANDLER.process_next_chunk(stats_framework.TOO_RECENT)
    except (DeadlineExceededError, logservice.Error):
      # The job will be retried.
      self.response.status_code = 500
      return

    msg = 'Processed %d minutes' % i
    logging.info(msg)
    self.response.write(msg)
