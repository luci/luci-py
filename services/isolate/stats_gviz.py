# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Frontend handlers for statistics."""

import webapp2

import stats
import template
from components import stats_framework_gviz


### Handlers


class StatsHandler(webapp2.RequestHandler):
  """Returns the statistics web page."""
  def get(self):
    """Presents nice recent statistics.

    It fetches data from the 'JSON' API.
    """
    self.response.write(template.get('stats.html').render({}))


class StatsGvizHandlerBase(webapp2.RequestHandler):
  RESOLUTION = None

  # GViz data description.
  GVIZ_DESCRIPTION = {
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
  GVIZ_COLUMNS_ORDER = (
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

  def get(self):
    description = self.GVIZ_DESCRIPTION.copy()
    description.update(
        stats_framework_gviz.get_description_key(self.RESOLUTION))
    try:
      stats_framework_gviz.get_json(
          self.request,
          self.response,
          stats.get_stats_handler(),
          self.RESOLUTION,
          description,
          self.GVIZ_COLUMNS_ORDER)
    except ValueError as e:
      self.abort(400, str(e))


class StatsGvizDaysHandler(StatsGvizHandlerBase):
  RESOLUTION = 'days'


class StatsGvizHoursHandler(StatsGvizHandlerBase):
  RESOLUTION = 'hours'


class StatsGvizMinutesHandler(StatsGvizHandlerBase):
  RESOLUTION = 'minutes'
