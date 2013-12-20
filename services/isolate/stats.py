# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates statistics out of logs.

The first 100mb of logs read is free. It's important to keep logs concise also
for general performance concerns. Each http handler should strive to do only one
log entry at info level per request.
"""

import datetime
import json
import logging

import webapp2
from google.appengine.api import logservice
from google.appengine.ext import ndb
from google.appengine.runtime import DeadlineExceededError

from components import stats_framework
import config
import template
import utils


### Public API


# Action to log.
STORE, RETURN, LOOKUP, DUPE = range(4)


def log(action, number, where):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  logging.info('%s%s; %d; %s', _PREFIX, _ACTION_NAMES[action], number, where)


def to_units(number):
  """Convert a string to numbers."""
  UNITS = ('', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y')
  unit = 0
  while number >= 1024.:
    unit += 1
    number = number / 1024.
    if unit == len(UNITS) - 1:
      break
  if unit:
    return '%.2f%s' % (number, UNITS[unit])
  return '%d' % number


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
      to_units(self.requests), to_units(self.failures))

  def downloads_as_text(self):
    return '%s (%sb)' % (
        to_units(self.downloads), to_units(self.downloads_bytes))

  def uploads_as_text(self):
    return '%s (%sb)' % (to_units(self.uploads), to_units(self.uploads_bytes))

  def lookups_as_text(self):
    return '%s (%s items)' % (
        to_units(self.contains_requests), to_units(self.contains_lookups))


### Utility


@utils.cache
def get_stats_handler():
  """Returns a global stats bookkeeper, lazily initializing it."""
  return stats_framework.StatisticsFramework(
    'global_stats', Snapshot, _extract_snapshot_from_logs)


# Text to store for the corresponding actions.
_ACTION_NAMES = ['store', 'return', 'lookup', 'dupe']


# Logs prefix.
_PREFIX = 'Stats: '


# Number of minutes to ignore because they are too fresh. This is done so that
# eventual log consistency doesn't have to be managed explicitly. On the dev
# server, there's no eventual inconsistency so process up to the last minute.
_TOO_RECENT = 5 if not config.is_local_dev_server() else 1


def _parse_line(line, values):
  """Updates a Snapshot instance with a processed statistics line if relevant.
  """
  if not line.startswith(_PREFIX):
    return
  line = line[len(_PREFIX):]
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
  module_versions = config.get_module_version_list(config.STATS_MODULES, True)
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


def _get_request_as_int(request, key, default, min_value, max_value):
  """Returns a request value as int."""
  value = request.params.get(key, '')
  try:
    value = int(value)
  except ValueError:
    return default
  return min(max_value, max(min_value, value))


def _generate_stats_data(request):
  """Returns a dict for data requested in the query."""
  DEFAULT_DAYS = 5
  DEFAULT_HOURS = 48
  DEFAULT_MINUTES = 120
  MIN_VALUE = 0
  MAX_DAYS = 367
  MAX_HOURS = 480
  MAX_MINUTES = 480

  limit_days = _get_request_as_int(
      request, 'days', DEFAULT_DAYS, MIN_VALUE, MAX_DAYS)
  limit_hours = _get_request_as_int(
      request, 'hours', DEFAULT_HOURS, MIN_VALUE, MAX_HOURS)
  limit_minutes = _get_request_as_int(
      request, 'minutes', DEFAULT_MINUTES, MIN_VALUE, MAX_MINUTES)

  now_text = request.params.get('now')
  now = None
  if now_text:
    FORMATS = ('%Y-%m-%d', '%Y-%m-%d %H:%M')
    for f in FORMATS:
      try:
        now = datetime.datetime.strptime(now_text, f)
        break
      except ValueError:
        continue
  if not now:
    now = datetime.datetime.utcnow()
  today = now.date()

  # Fire up all the datastore requests.
  stats_handler = get_stats_handler()

  future_days = []
  if limit_days:
    future_days = ndb.get_multi_async(
        stats_handler.day_key(today - datetime.timedelta(days=i))
        for i in range(limit_days + 1))

  future_hours = []
  if limit_hours:
    future_hours = ndb.get_multi_async(
        stats_handler.hour_key(now - datetime.timedelta(hours=i))
        for i in range(limit_hours + 1))

  future_minutes = []
  if limit_minutes:
    future_minutes = ndb.get_multi_async(
        stats_handler.minute_key(now - datetime.timedelta(minutes=i))
        for i in range(limit_minutes + 1))

  def filterout(futures):
    """Filters out inexistent entities."""
    intermediary = (i.get_result() for i in futures)
    return [i for i in intermediary if i]

  return {
    'days': filterout(future_days),
    'hours': filterout(future_hours),
    'minutes': filterout(future_minutes),
    'now': now.strftime(stats_framework.TIME_FORMAT),
  }


def _to_json(data):
  """Converts data into json-compatible data."""
  if isinstance(data, unicode):
    return data
  elif isinstance(data, str):
    return data.decode('utf-8')
  if isinstance(data, dict):
    return dict((_to_json(k), _to_json(v)) for k, v in data.iteritems())
  elif isinstance(data, ndb.Model):
    return _to_json(data.to_dict())
  elif isinstance(data, (list, tuple)):
    return [_to_json(i) for i in data]
  elif isinstance(data, datetime.datetime):
    return unicode(str(data))
  elif isinstance(data, (int, float, long)):
    # Note: overflowing is an issue with int and long.
    return data
  else:
    assert False, 'Don\'t know how to handle %r' % data


### Handlers


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    try:
      i = get_stats_handler().process_next_chunk(_TOO_RECENT)
    except DeadlineExceededError:
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
    self.response.headers['Content-Type'] = 'text/html'


class StatsJsonHandler(webapp2.RequestHandler):
  """Returns the statistic data."""
  def get(self):
    data = _to_json(_generate_stats_data(self.request))

    # Make it json-compatible.
    self.response.write(json.dumps(data, separators=(',',':')))
    self.response.headers['Content-Type'] = 'application/json'
    self.response.headers['Access-Control-Allow-Origin'] = '*'
