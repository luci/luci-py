# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Frontend handlers for statistics."""

import webapp2

import stats
import template
from components import stats_framework
from components import stats_framework_gviz
from components import utils
from gviz import gviz_api


# GViz data description.
_GVIZ_DESCRIPTION = {
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

# Warning: modifying the order here requires updating templates/stats.html.
_GVIZ_COLUMNS_ORDER = (
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

    description = _GVIZ_DESCRIPTION.copy()
    description.update(stats_framework_gviz.get_description_key(resolution))
    table = stats_framework.get_stats(
        stats.STATS_HANDLER, resolution, None, duration, True)
    params = {
      'duration': duration,
      'initial_data': gviz_api.DataTable(description, table).ToJSon(
          columns_order=_GVIZ_COLUMNS_ORDER),
      'resolution': resolution,
    }
    self.response.write(template.get('stats.html').render(params))


class StatsGvizHandlerBase(webapp2.RequestHandler):
  RESOLUTION = None

  def get(self):
    description = _GVIZ_DESCRIPTION.copy()
    description.update(
        stats_framework_gviz.get_description_key(self.RESOLUTION))
    try:
      stats_framework_gviz.get_json(
          self.request,
          self.response,
          stats.STATS_HANDLER,
          self.RESOLUTION,
          description,
          _GVIZ_COLUMNS_ORDER)
    except ValueError as e:
      self.abort(400, str(e))


class StatsGvizDaysHandler(StatsGvizHandlerBase):
  RESOLUTION = 'days'


class StatsGvizHoursHandler(StatsGvizHandlerBase):
  RESOLUTION = 'hours'


class StatsGvizMinutesHandler(StatsGvizHandlerBase):
  RESOLUTION = 'minutes'
