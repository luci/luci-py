# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import logging
import os
import sys
import time

import webapp2

from google.appengine.api import modules
from google.appengine.api.app_identity import app_identity

from infra_libs.ts_mon import deferred_metric_store
from infra_libs.ts_mon import memcache_metric_store
from infra_libs.ts_mon.common import http_metrics
from infra_libs.ts_mon.common import interface
from infra_libs.ts_mon.common import metric_store
from infra_libs.ts_mon.common import monitors
from infra_libs.ts_mon.common import targets

REGION = 'appengine'
PUBSUB_PROJECT = 'chrome-infra-mon-pubsub'
PUBSUB_TOPIC = 'monacq'


def initialize(app=None, is_local_unittest=None):
  if is_local_unittest is None:  # pragma: no cover
    # Since gae_ts_mon.initialize is called at module-scope by appengine apps,
    # AppengineTestCase.setUp() won't have run yet and none of the appengine
    # stubs will be initialized, so accessing memcache or even getting the
    # application ID will fail.
    is_local_unittest = ('expect_tests' in sys.argv[0])

  if app is not None:
    instrument_wsgi_application(app)

  if interface.state.global_monitor is not None:
    # Even if ts_mon was already initialized in this instance we should update
    # the metric index in case any new metrics have been registered.
    if hasattr(interface.state.store,
               'update_metric_index'):  # pragma: no cover
      interface.state.store.update_metric_index()
    return

  # Use the application ID as the service name and the module name as the job
  # name.
  if is_local_unittest:  # pragma: no cover
    service_name = 'unittest'
    job_name = 'unittest'
    hostname = 'unittest'
  else:
    service_name = app_identity.get_application_id()
    job_name = modules.get_current_module_name()
    hostname = modules.get_current_version_name()

  interface.state.target = targets.TaskTarget(
      service_name, job_name, REGION, hostname)
  interface.state.flush_mode = 'auto'

  if is_local_unittest:  # pragma: no cover
    interface.state.store = metric_store.InProcessMetricStore(interface.state)
  else:
    memcache_store = memcache_metric_store.MemcacheMetricStore(
        interface.state, report_module_versions=not is_local_unittest)
    interface.state.store = deferred_metric_store.DeferredMetricStore(
        interface.state, memcache_store)


  # Don't send metrics when running on the dev appserver.
  if (is_local_unittest or
      os.environ.get('SERVER_SOFTWARE', '').startswith('Development')):
    logging.info('Using debug monitor')
    interface.state.global_monitor = monitors.DebugMonitor()
  else:
    logging.info('Using pubsub monitor %s/%s', PUBSUB_PROJECT, PUBSUB_TOPIC)
    interface.state.global_monitor = monitors.PubSubMonitor(
        monitors.APPENGINE_CREDENTIALS, PUBSUB_PROJECT, PUBSUB_TOPIC)

  logging.info('Initialized ts_mon with service_name=%s, job_name=%s, '
               'hostname=%s',
               service_name, job_name, hostname)


def _instrumented_dispatcher(dispatcher, request, response, time_fn=time.time):
  start_time = time_fn()
  response_status = 0
  interface.state.store.initialize_context()
  try:
    ret = dispatcher(request, response)
  except webapp2.HTTPException as ex:
    response_status = ex.code
    raise
  except Exception:
    response_status = 500
    raise
  else:
    if isinstance(ret, webapp2.Response):
      response = ret
    response_status = response.status_int
  finally:
    elapsed_ms = int((time_fn() - start_time) * 1000)

    fields = {'status': response_status, 'name': '', 'is_robot': False}
    if request.route is not None:
      # Use the route template regex, not the request path, to prevent an
      # explosion in possible field values.
      fields['name'] = request.route.template
    if request.user_agent is not None:
      # We shouldn't log user agents, but we can store whether or not the
      # user agent string indicates that the requester was a Google bot.
      fields['is_robot'] = (
          'GoogleBot' in request.user_agent or
          'GoogleSecurityScanner' in request.user_agent)

    http_metrics.server_durations.add(elapsed_ms, fields=fields)
    http_metrics.server_response_status.increment(fields=fields)
    if request.content_length is not None:
      http_metrics.server_request_bytes.add(request.content_length,
                                            fields=fields)
    if response.content_length is not None:  # pragma: no cover
      http_metrics.server_response_bytes.add(response.content_length,
                                             fields=fields)

    try:
      interface.state.store.finalize_context()
    except Exception:  # pragma: no cover
      logging.exception('metric store finalize failed')

  return ret


def instrument_wsgi_application(app, time_fn=time.time):
  # Don't instrument the same router twice.
  if hasattr(app.router, '__instrumented_by_ts_mon'):
    return

  old_dispatcher = app.router.dispatch

  def dispatch(router, request, response):
    return _instrumented_dispatcher(old_dispatcher, request, response,
                                    time_fn=time_fn)

  app.router.set_dispatcher(dispatch)
  app.router.__instrumented_by_ts_mon = True
