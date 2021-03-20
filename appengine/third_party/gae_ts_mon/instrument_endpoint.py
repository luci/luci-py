# Copyright 2019 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import time
import functools

# Not all apps enable endpoints. If the import fails, the app will not
# use @instrument_endpoint() decorator, so it is safe to ignore it.
try:
  import endpoints
except ImportError:  # pragma: no cover
  pass

from infra_libs.ts_mon import exporter
from infra_libs.ts_mon.common import http_metrics


def instrument(time_fn=time.time):
  """Decorator to instrument Cloud Endpoint methods."""

  def decorator(fn):
    method_name = fn.__name__
    assert method_name

    @functools.wraps(fn)
    def decorated(service, *args, **kwargs):
      service_name = service.__class__.__name__
      endpoint_name = '/_ah/spi/%s.%s' % (service_name, method_name)
      start_time = time_fn()
      response_status = 0
      time_now = time_fn()

      try:
        with exporter.parallel_flush(time_now):
          ret = fn(service, *args, **kwargs)
          response_status = 200
          return ret
      except endpoints.ServiceException as e:
        response_status = e.http_status
        raise
      except Exception:
        response_status = 500
        raise
      finally:
        elapsed_ms = int((time_fn() - start_time) * 1000)
        http_metrics.update_http_server_metrics(endpoint_name, response_status,
                                                elapsed_ms)

    return decorated

  return decorator
