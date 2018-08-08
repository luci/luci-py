# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""GViz connector code for stats_framework."""

import logging

from components import stats_framework
from components import utils
from gviz import gviz_api


### Public API.


def process_tqx(tqx):
  """Processes the tqx gviz arguments."""
  kwargs = {}
  for arg in tqx.split(';'):
    if ':' not in arg:
      continue
    key, value = arg.split(':', 1)
    if key == 'out':
      if value != 'json':
        raise ValueError('Unsupported \'out\' argument')
    elif key == 'reqId':
      kwargs['req_id'] = value
    elif key == 'responseHandler':
      kwargs['response_handler'] = value
    elif key == 'tq':
      # TODO(maruel): Implement.
      raise ValueError('Unsupported flag tq=%s' % value)
    else:
      logging.warning('Unknown query parameter %s', arg)
  return kwargs


def get_description_key(resolution):
  """Returns GVIZ description key for the specific resolution."""
  if resolution == 'days':
    # It permits Google Viz to properly list days instead of midnight every day.
    return {'key': ('date', 'Day')}
  elif resolution in ('hours', 'minutes'):
    return {'key': ('datetime', 'Time')}
  raise ValueError('Unexpected resolution')


def get_json(request, response, handler, resolution, description, order):
  """Returns the statistic data as a Google Visualization compatible reply.

  The return can be either JSON or JSONP, depending if the header
  'X-DataSource-Auth' is set in the request.

  Note that this is not real JSON, as explained in
  developers.google.com/chart/interactive/docs/dev/implementing_data_source

  Exposes the data in the format described at
  https://developers.google.com/chart/interactive/docs/reference#dataparam
  and
  https://developers.google.com/chart/interactive/docs/querylanguage

  Arguments:
  - request: A webapp2.Request.
  - response: A webapp2.Response.
  - handler: A StatisticsFramework.
  - resolution: One of 'days', 'hours' or 'minutes'.
  - description: Dict describing the columns.
  - order: List describing the order to use for the columns.

  Raises:
    ValueError if a 400 should be returned.
  """
  tqx_args = process_tqx(request.params.get('tqx', ''))
  duration = utils.get_request_as_int(request, 'duration', 120, 1, 256)
  now = None
  now_text = request.params.get('now')
  if now_text:
    now = utils.parse_datetime(now_text)

  table = stats_framework.get_stats(handler, resolution, now, duration, True)
  return get_json_raw(request, response, table, description, order, tqx_args)


def get_json_raw(request, response, table, description, order, tqx_args):
  """Returns the statistic data as a Google Visualization compatible reply.

  The return can be either JSON or JSONP, depending if the header
  'X-DataSource-Auth' is set in the request.

  Note that this is not real JSON, as explained in
  developers.google.com/chart/interactive/docs/dev/implementing_data_source

  Exposes the data in the format described at
  https://developers.google.com/chart/interactive/docs/reference#dataparam
  and
  https://developers.google.com/chart/interactive/docs/querylanguage

  Arguments:
  - request: A webapp2.Request.
  - response: A webapp2.Response.
  - table: Raw data ready to be sent back.
  - description: Dict describing the columns.
  - order: List describing the order to use for the columns.
  - tqx_args: Dict of flags used in the tqx query parameter.

  Raises:
    ValueError if a 400 should be returned.
  """
  if request.headers.get('X-DataSource-Auth'):
    # Client requested JSON.
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    # TODO(maruel): This manual packaging is annoying, figure out a way to
    # make this cleaner.
    # pylint: disable=W0212
    response_obj = {
      'reqId': str(tqx_args.get('req_id', 0)),
      'status': 'ok',
      'table': gviz_api.DataTable(description, table)._ToJSonObj(
          columns_order=order),
      'version': '0.6',
    }
    encoder = gviz_api.DataTableJSONEncoder()
    response.write(encoder.encode(response_obj).encode('utf-8'))
  else:
    # Client requested JSONP.
    response.headers['Content-Type'] = (
        'application/javascript; charset=utf-8')
    response.write(
        gviz_api.DataTable(description, table).ToJSonResponse(
            columns_order=order, **tqx_args))
