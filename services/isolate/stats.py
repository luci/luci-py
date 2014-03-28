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

import template
from components import stats_framework
from components import utils
from gviz import gviz_api


### Public API


# Action to log.
STORE, RETURN, LOOKUP, DUPE = range(4)


def add_entry(action, number, where):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  stats_framework.add_entry(
      '%s; %d; %s' % (_ACTION_NAMES[action], number, where))


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
  """Returns a Snapshot from the processed logs for the specified interval.

  The data is retrieved from logservice via stats_framework.
  """
  values = Snapshot()
  for entry in stats_framework.yield_entries(start_time, end_time):
    values.requests += 1
    if entry.request.status >= 400:
      values.failures += 1
    for l in entry.entries:
      _parse_line(l, values)
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
  """Returns the statistics web page."""
  def get(self):
    """Presents nice recent statistics.

    It fetches data from the 'JSON' API.
    """
    self.response.write(template.get('stats.html').render({}))


class ApiStatsGvizHandlerBase(webapp2.RequestHandler):
  """Returns the statistic data as a Google Visualization compatible reply.

  The return can be either JSON or JSONP, depending if the header
  'X-DataSource-Auth' is set in the request.

  Note that this is not real JSON, as explained in
  developers.google.com/chart/interactive/docs/dev/implementing_data_source
  """
  DATA_DESCRIPTION_COMMON = {
    'failures': ('number', 'Failures'),
    'requests': ('number', 'Total'),
    'other_requests': ('number', 'Other'),
    'uploads': ('number', 'Uploads'),
    'uploads_bytes': ('number', 'Uploaded'),
    'downloads': ('number', 'Downloads'),
    'downloads_bytes': ('number', 'Downloaded'),
    'contains_requests': ('number', 'Lookups'),
    'contains_lookups': ('number', 'Items looked up'),
  }

  # TODO(maruel): Modifying the order here must update templates/stats.html.
  # Make it smart.
  REQUESTS_COLUMNS_ORDER = (
    'key',
    'requests',
    'other_requests',
    'failures',
    'uploads',
    'downloads',
    'contains_requests',
    'uploads_bytes',
    'downloads_bytes',
    'contains_lookups',
  )

  # To be overriden in subclass.
  KEY = None

  def get(self):
    """Exposes as described at
    https://developers.google.com/chart/interactive/docs/reference#dataparam
    and
    https://developers.google.com/chart/interactive/docs/querylanguage
    """
    kwargs = {}
    for arg in self.request.params.get('tqx', '').split(';'):
      if ':' not in arg:
        continue
      key, value = arg.split(':', 1)
      if key == 'out':
        if value != 'json':
          self.abort(400, 'Unsupported \'out\' argument')
      elif key == 'reqId':
        kwargs['req_id'] = value
      elif key == 'responseHandler':
        kwargs['response_handler'] = value
      else:
        logging.warning('Unknown query parameter %s', arg)

    # TODO(maruel): Implement: self.request.params.get('tq')
    duration = utils.get_request_as_int(self.request, 'duration', 120, 1, 256)
    now = utils.get_request_as_datetime(self.request, 'now')

    def fix_up(d):
      """Mangles to make it easier to graph."""
      i = d.to_dict()
      i['values']['key'] = i['key']
      o = i['values']
      o['other_requests'] = (
          o['requests'] - o['downloads'] - o['contains_requests'] -
          o['uploads'])
      return o

    table = [fix_up(d) for d in self.get_data(now, duration)]
    description = self.DATA_DESCRIPTION_COMMON.copy()
    description.update(self.KEY)

    self.response.headers['Access-Control-Allow-Origin'] = '*'
    if self.request.headers.get('X-DataSource-Auth'):
      # Client requested JSON.
      self.response.headers['Content-Type'] = (
          'application/json; charset=utf-8')
      # TODO(maruel): This manual packaging is annoying, figure out a way to
      # make this cleaner.
      # pylint: disable=W0212
      response_obj = {
        'reqId': str(kwargs.get('req_id', 0)),
        'status': 'ok',
        'table': gviz_api.DataTable(description, table)._ToJSonObj(
            columns_order=self.REQUESTS_COLUMNS_ORDER),
        'version': '0.6',
      }
      encoder = gviz_api.DataTableJSONEncoder()
      self.response.write(encoder.encode(response_obj).encode('utf-8'))
    else:
      # Client requested JSONP.
      self.response.headers['Content-Type'] = (
          'application/javascript; charset=utf-8')
      self.response.write(
          gviz_api.DataTable(description, table).ToJSonResponse(
              columns_order=self.REQUESTS_COLUMNS_ORDER, **kwargs))

  def get_data(self, now, duration):
    raise NotImplementedError()


class ApiStatsGvizDaysHandler(ApiStatsGvizHandlerBase):
  KEY = {
    'key': ('date', 'Day'),
  }
  def get_data(self, now, duration):
    return stats_framework.get_days(get_stats_handler(), now, duration)


class ApiStatsGvizHoursHandler(ApiStatsGvizHandlerBase):
  KEY = {
    'key': ('datetime', 'Time'),
  }
  def get_data(self, now, duration):
    return stats_framework.get_hours(get_stats_handler(), now, duration)


class ApiStatsGvizMinutesHandler(ApiStatsGvizHandlerBase):
  KEY = {
    'key': ('datetime', 'Time'),
  }
  def get_data(self, now, duration):
    return stats_framework.get_minutes(get_stats_handler(), now, duration)
