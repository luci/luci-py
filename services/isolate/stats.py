# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates statistics out of logs.

The first 100mb of logs read is free. It's important to keep logs concise also
for general performance concerns. Each http handler should strive to do only one
log entry at info level per request.
"""

import logging

import webapp2
from google.appengine.api import logservice
from google.appengine.ext import ndb
from google.appengine.runtime import DeadlineExceededError

import config
import template
from components import stats_framework
from components import utils


### Public API


# Action to log.
STORE, RETURN, LOOKUP, DUPE = range(4)


def log(action, number, where):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  logging.info(
      '%s%s; %d; %s', stats_framework.PREFIX, _ACTION_NAMES[action], number,
      where)


### Models


class Snapshot(ndb.Model):
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

  def accumulate(self, rhs):
    return stats_framework.accumulate(self, rhs)

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


@utils.cache
def get_stats_handler():
  """Returns a global stats bookkeeper, lazily initializing it."""
  return stats_framework.StatisticsFramework(
    'global_stats', Snapshot, _extract_snapshot_from_logs)


# Text to store for the corresponding actions.
_ACTION_NAMES = ['store', 'return', 'lookup', 'dupe']


def _parse_line(line, values):
  """Updates a Snapshot instance with a processed statistics line if relevant.
  """
  if not line.startswith(stats_framework.PREFIX):
    return
  line = line[len(stats_framework.PREFIX):]
  if line.count(';') < 2:
    return
  action_id, measurement, _rest = line.split('; ', 2)
  action = _ACTION_NAMES.index(action_id)
  measurement = int(measurement)

  if action == STORE:
    values.uploads += 1
    values.uploads_bytes += measurement
  elif action == RETURN:
    values.downloads += 1
    values.downloads_bytes += measurement
  elif action == LOOKUP:
    values.contains_requests += 1
    values.contains_lookups += measurement
  elif action == DUPE:
    pass
  else:
    assert False


def _extract_snapshot_from_logs(start_time, end_time):
  """Processes the logs to harvest data and return a Snapshot instance."""
  values = Snapshot()
  module_versions = utils.get_module_version_list(config.STATS_MODULES, True)
  for entry in logservice.fetch(
      start_time=start_time,
      end_time=end_time,
      minimum_log_level=logservice.LOG_LEVEL_INFO,
      include_incomplete=True,
      include_app_logs=True,
      module_versions=module_versions):
    # Ignore other urls.
    if not entry.resource.startswith(config.STATS_REQUEST_PATHS):
      continue

    values.requests += 1
    if entry.status >= 400:
      values.failures += 1

    for log_line in entry.app_logs:
      if log_line.level != logservice.LOG_LEVEL_INFO:
        continue
      _parse_line(log_line.message, values)
  return values


def _generate_stats_data(request):
  return stats_framework.generate_stats_data_from_request(
      request, get_stats_handler())



### Handlers


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    try:
      i = get_stats_handler().process_next_chunk(stats_framework.TOO_RECENT)
    except (DeadlineExceededError, logservice.Error):
      # The job will be retried.
      self.response.status_code = 500
      return
    msg = 'Processed %d minutes' % i
    logging.info(msg)
    self.response.write(msg)


class StatsHandler(webapp2.RequestHandler):
  """Returns the statistics page."""
  def get(self):
    """Presents nice recent statistics."""
    data = _generate_stats_data(self.request)
    to_render = template.get('stats.html')
    self.response.write(to_render.render(data))


class StatsJsonHandler(webapp2.RequestHandler):
  """Returns the statistic data."""
  def get(self):
    data = _generate_stats_data(self.request)
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.headers['Access-Control-Allow-Origin'] = '*'
    self.response.write(utils.encode_to_json(data))
