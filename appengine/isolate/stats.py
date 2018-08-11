# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Generates statistics out of logs. Contains the backend code.

The first 100mb of logs read is free. It's important to keep logs concise also
for general performance concerns. Each http handler should strive to do only one
log entry at info level per request.
"""

import logging

from google.appengine.ext import ndb

from components import stats_framework
from components.stats_framework import stats_logs
from components import utils


# Class has no __init__ method - pylint: disable=W0232


### Models


class _Snapshot(ndb.Model):
  """A snapshot of statistics, to be embedded in another entity."""
  # Number of individual uploads and total amount of bytes. Same for downloads.
  uploads = ndb.IntegerProperty(default=0, indexed=False)
  uploads_bytes = ndb.IntegerProperty(default=0, indexed=False)
  downloads = ndb.IntegerProperty(default=0, indexed=False)
  downloads_bytes = ndb.IntegerProperty(default=0, indexed=False)

  # Number of /contains requests and total number of items looked up.
  contains_requests = ndb.IntegerProperty(default=0, indexed=False)
  contains_lookups = ndb.IntegerProperty(default=0, indexed=False)

  # Total number of requests to calculate QPS
  requests = ndb.IntegerProperty(default=0, indexed=False)
  # Number of non-200 requests.
  failures = ndb.IntegerProperty(default=0, indexed=False)

  def to_dict(self):
    """Mangles to make it easier to graph."""
    out = super(_Snapshot, self).to_dict()
    out['other_requests'] = (
        out['requests'] - out['downloads'] - out['contains_requests'] -
        out['uploads'])
    return out

  def accumulate(self, rhs):
    return stats_framework.accumulate(self, rhs, [])

  def requests_as_text(self):
    return '%s (%s failed)' % (
      utils.to_units(self.requests),
      utils.to_units(self.failures))

  def downloads_as_text(self):
    return '%s (%sb)' % (
        utils.to_units(self.downloads),
        utils.to_units(self.downloads_bytes))

  def uploads_as_text(self):
    return '%s (%sb)' % (
        utils.to_units(self.uploads),
        utils.to_units(self.uploads_bytes))

  def lookups_as_text(self):
    return '%s (%s items)' % (
        utils.to_units(self.contains_requests),
        utils.to_units(self.contains_lookups))


### Utility


# Text to store for the corresponding actions.
_ACTION_NAMES = ['store', 'return', 'lookup', 'dupe']


def _parse_line(line, values):
  """Updates a _Snapshot instance with a processed statistics line if relevant.
  """
  if line.count(';') < 2:
    return False
  action_id, measurement, _rest = line.split('; ', 2)
  action = _ACTION_NAMES.index(action_id)
  measurement = int(measurement)

  if action == STORE:
    values.uploads += 1
    values.uploads_bytes += measurement
    return True
  elif action == RETURN:
    values.downloads += 1
    values.downloads_bytes += measurement
    return True
  elif action == LOOKUP:
    values.contains_requests += 1
    values.contains_lookups += measurement
    return True
  elif action == DUPE:
    return True
  else:
    return False


def _extract_snapshot_from_logs(start_time, end_time):
  """Returns a _Snapshot from the processed logs for the specified interval.

  The data is retrieved from logservice via stats_framework.
  """
  values = _Snapshot()
  total_lines = 0
  parse_errors = 0
  for entry in stats_logs.yield_entries(start_time, end_time):
    values.requests += 1
    if entry.request.status >= 400:
      values.failures += 1
    for l in entry.entries:
      if _parse_line(l, values):
        total_lines += 1
      else:
        parse_errors += 1
  logging.debug(
      '_extract_snapshot_from_logs(%s, %s): %d lines, %d errors',
      start_time, end_time, total_lines, parse_errors)
  return values


### Public API


STATS_HANDLER = stats_framework.StatisticsFramework(
    'global_stats', _Snapshot, _extract_snapshot_from_logs)


# Action to log.
STORE, RETURN, LOOKUP, DUPE = range(4)


def add_entry(action, number, where):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  stats_logs.add_entry('%s; %d; %s' % (_ACTION_NAMES[action], number, where))


def generate_stats():
  """Returns the number of minutes processed."""
  return STATS_HANDLER.process_next_chunk(stats_framework.TOO_RECENT)
