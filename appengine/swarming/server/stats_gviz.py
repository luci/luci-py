# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Frontend handlers for statistics."""

import datetime
import itertools
import json

import template
from components import auth
from components import natsort
from components import stats_framework
from components import stats_framework_gviz
from components import utils
from gviz import gviz_api
from server import acl
from server import stats

### Private Stuff.


class _Dimensions(object):
  TEMPLATE = 'swarming/stats_bucket.html'

  DESCRIPTION = {
    'bots_active': ('number', 'Bots active'),
    'tasks_active': ('number', 'Tasks active'),

    'tasks_enqueued': ('number', 'Tasks enqueued'),
    'tasks_started': ('number', 'Tasks started'),
    'tasks_completed': ('number', 'Tasks completed'),

    'tasks_avg_pending_secs': ('number', 'Average shard pending time (s)'),
    'tasks_total_runtime_secs': ('number', 'Tasks total runtime (s)'),
    'tasks_avg_runtime_secs': ('number', 'Average shard runtime (s)'),

    'tasks_bot_died': ('number', 'Tasks where the bot died'),
    'tasks_request_expired': ('number', 'Tasks requests expired'),
  }

  # Warning: modifying the order here requires updating cls.TEMPLATE.
  ORDER = (
    'key',

    'bots_active',
    'tasks_active',

    'tasks_enqueued',
    'tasks_started',
    'tasks_completed',  # 5th element.

    'tasks_avg_pending_secs',
    'tasks_total_runtime_secs',
    'tasks_avg_runtime_secs',

    'tasks_bot_died',
    'tasks_request_expired',  # 10th element.
  )


class _Summary(object):
  TEMPLATE = 'swarming/stats.html'

  DESCRIPTION = {
    'http_failures': ('number', 'HTTP Failures'),
    'http_requests': ('number', 'Total HTTP requests'),

    'bots_active': ('number', 'Bots active'),
    'tasks_active': ('number', 'Tasks active'),

    'tasks_enqueued': ('number', 'Tasks enqueued'),

    'tasks_started': ('number', 'Tasks started'),
    'tasks_avg_pending_secs': ('number', 'Average shard pending time (s)'),

    'tasks_completed': ('number', 'Tasks completed'),
    'tasks_total_runtime_secs': ('number', 'Tasks total runtime (s)'),
    'tasks_avg_runtime_secs': ('number', 'Average shard runtime (s)'),

    'tasks_bot_died': ('number', 'Tasks where the bot died'),
    'tasks_request_expired': ('number', 'Tasks requests expired'),
  }

  # Warning: modifying the order here requires updating cls.TEMPLATE.
  ORDER = (
    'key',
    'http_requests',
    'http_failures',

    'bots_active',
    'tasks_active',

    'tasks_enqueued',  # 5th element.
    'tasks_started',
    'tasks_completed',

    'tasks_avg_pending_secs',
    'tasks_total_runtime_secs',
    'tasks_avg_runtime_secs',  # 10th element.

    'tasks_bot_died',
    'tasks_request_expired',
  )


def _stats_data_to_summary(stats_data):
  """Converts StatsMinute/StatsHour/StatsDay into a dict."""
  return (i.to_dict() for i in stats_data)


def _stats_data_to_dimensions(stats_data, dimensions):
  """Converts StatsMinute/StatsHour/StatsDay into a dict for the particular
  dimensions.
  """
  def fix(line):
    for bucket in line.values.buckets:
      if dimensions == bucket.dimensions:
        item = bucket
        break
    else:
      # pylint: disable=W0212
      item = stats._SnapshotForDimensions(dimensions=dimensions)
    out = item.to_dict()
    out['key'] = line.get_timestamp()
    return out

  return (j for j in (fix(i) for i in stats_data) if j)


### Handlers


class StatsHandlerBase(auth.AuthenticatingHandler):
  """Returns the statistics web page."""

  def send_response(self, res_type_info):
    """Presents nice recent statistics.

    It preloads data in the template for maximum responsiveness and
    interactively fetches data from the JSON API.
    """
    # Preloads the data to save a complete request.
    resolution = self.request.params.get('resolution', 'hours')
    if resolution not in stats_framework.RESOLUTIONS:
      self.abort(404)
    duration = utils.get_request_as_int(
        self.request, 'duration', default=120, min_value=1, max_value=1000)
    now = utils.get_request_as_datetime(self.request, 'now')
    now = now or datetime.datetime.utcnow()

    description = res_type_info.DESCRIPTION.copy()
    description.update(stats_framework_gviz.get_description_key(resolution))
    stats_data = stats_framework.get_stats(
        stats.STATS_HANDLER, resolution, now, duration, False)
    template_data = self.process_data(description, stats_data)
    template_data['duration'] = duration
    template_data['now'] = now
    template_data['resolution'] = resolution
    self.response.write(template.render(res_type_info.TEMPLATE, template_data))


class StatsSummaryHandler(StatsHandlerBase):
  @auth.require(acl.is_user)
  def get(self):
    self.send_response(_Summary)

  @staticmethod
  def process_data(description, stats_data):
    def sorted_unique_list_from_itr(i):
      return natsort.natsorted(set(itertools.chain.from_iterable(i)))

    dimensions = sorted_unique_list_from_itr(
        (i.dimensions for i in line.values.buckets) for line in stats_data)

    table = _stats_data_to_summary(stats_data)
    # TODO(maruel): 'dimensions' should be updated when the user changes the
    # resolution at which the data is displayed.
    return {
      'dimensions': json.dumps(dimensions),
      'initial_data': gviz_api.DataTable(description, table).ToJSon(
          columns_order=_Summary.ORDER),
    }


class StatsGvizHandlerBase(auth.AuthenticatingHandler):
  def send_response(self, res_type_info, resolution):
    if resolution not in stats_framework.RESOLUTIONS:
      self.abort(404)

    duration = utils.get_request_as_int(
        self.request, 'duration', default=120, min_value=1, max_value=1000)
    now = utils.get_request_as_datetime(self.request, 'now')
    description = res_type_info.DESCRIPTION.copy()
    description.update(
        stats_framework_gviz.get_description_key(resolution))
    stats_data = stats_framework.get_stats(
        stats.STATS_HANDLER, resolution, now, duration, False)
    tqx_args = tqx_args = stats_framework_gviz.process_tqx(
        self.request.params.get('tqx', ''))
    try:
      stats_framework_gviz.get_json_raw(
          self.request,
          self.response,
          self.get_table(stats_data),
          description,
          res_type_info.ORDER,
          tqx_args)
    except ValueError as e:
      self.abort(400, str(e))


class StatsGvizSummaryHandler(StatsGvizHandlerBase):
  @auth.require(acl.is_user)
  def get(self, resolution):
    self.send_response(_Summary, resolution)

  @staticmethod
  def get_table(stats_data):
    return _stats_data_to_summary(stats_data)


class StatsGvizDimensionsHandler(StatsGvizHandlerBase):
  dimensions = None

  @auth.require(acl.is_user)
  def get(self, dimensions, resolution):
    # Save it for later use in self.process_data().
    self.dimensions = dimensions
    self.send_response(_Dimensions, resolution)

  def get_table(self, stats_data):
    return _stats_data_to_dimensions(stats_data, self.dimensions)
