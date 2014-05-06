# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Frontend handlers for statistics."""

import webapp2

import template
from components import stats_framework
from components import stats_framework_gviz
from components import utils
from gviz import gviz_api
from server import stats_new as stats


_GVIZ_DESCRIPTION_SUMMARY = {
  'http_failures': ('number', 'HTTP Failures'),
  'http_requests': ('number', 'Total HTTP requests'),

  'bots_active': ('number', 'Bots active'),
  'shards_active': ('number', 'Shards active'),

  'request_enqueued': ('number', 'Requests enqueued'),
  'shards_enqueued': ('number', 'Shards enqueued'),

  'shards_started': ('number', 'Shards started'),
  'shards_avg_pending_secs': ('number', 'Average shard pending time (s)'),

  'shards_completed': ('number', 'Shards completed'),
  'shards_total_runtime_secs': ('number', 'Shards total runtime (s)'),
  'shards_avg_runtime_secs': ('number', 'Average shard runtime (s)'),

  'shards_bot_died': ('number', 'Shards where the bot died'),
  'shards_request_expired': ('number', 'Shards requests expired'),
}


# Warning: modifying the order here requires updating templates/stats.html.
_GVIZ_COLUMNS_ORDER_SUMMARY = (
  'key',
  'http_requests',
  'http_failures',

  'bots_active',
  'shards_active',

  'request_enqueued',
  'shards_enqueued',

  'shards_started',

  'shards_completed',
  'shards_avg_pending_secs',
  'shards_total_runtime_secs',
  'shards_avg_runtime_secs',

  'shards_bot_died',
  'shards_request_expired',
)


### Handlers


class StatsHandler(webapp2.RequestHandler):
  """Returns the statistics web page."""
  def get(self):
    """Presents nice recent statistics.

    It fetches data from the 'JSON' API.
    """
    # Preloads the data to save a complete request.
    resolution = self.request.params.get('resolution', 'hours')
    duration = utils.get_request_as_int(self.request, 'duration', 120, 1, 1000)

    description = _GVIZ_DESCRIPTION_SUMMARY.copy()
    description.update(stats_framework_gviz.get_description_key(resolution))
    table = stats_framework.get_stats(
        stats.STATS_HANDLER, resolution, None, duration, True)
    params = {
      'duration': duration,
      'initial_data': gviz_api.DataTable(description, table).ToJSon(
          columns_order=_GVIZ_COLUMNS_ORDER_SUMMARY),
      'resolution': resolution,
    }
    self.response.write(template.render('stats_new.html', params))


class StatsGvizHandlerBase(webapp2.RequestHandler):
  RESOLUTION = None

  def get(self):
    description = _GVIZ_DESCRIPTION_SUMMARY.copy()
    description.update(
        stats_framework_gviz.get_description_key(self.RESOLUTION))
    try:
      stats_framework_gviz.get_json(
          self.request,
          self.response,
          stats.STATS_HANDLER,
          self.RESOLUTION,
          description,
          _GVIZ_COLUMNS_ORDER_SUMMARY)
    except ValueError as e:
      self.abort(400, str(e))


class StatsGvizDaysHandler(StatsGvizHandlerBase):
  RESOLUTION = 'days'


class StatsGvizHoursHandler(StatsGvizHandlerBase):
  RESOLUTION = 'hours'


class StatsGvizMinutesHandler(StatsGvizHandlerBase):
  RESOLUTION = 'minutes'
