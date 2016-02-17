# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import copy
import datetime
import logging
import os
import sys
import time
import threading

import webapp2

from google.appengine.api import modules
from google.appengine.api.app_identity import app_identity
from google.appengine.api import runtime
from google.appengine.ext import ndb

from infra_libs.ts_mon import handlers
from infra_libs.ts_mon import shared
from infra_libs.ts_mon.common import http_metrics
from infra_libs.ts_mon.common import interface
from infra_libs.ts_mon.common import metric_store
from infra_libs.ts_mon.common import monitors
from infra_libs.ts_mon.common import targets


def _reset_cumulative_metrics():
  """Clear the state when an instance loses its task_num assignment."""
  logging.warning('Instance %s got purged from Datastore, but is still alive. '
                  'Clearing cumulative metrics.', shared.instance_key_id())
  for _target, metric, start_time, _fields in interface.state.store.get_all():
    if metric.is_cumulative():
      metric.reset()


_flush_metrics_lock = threading.Lock()


def flush_metrics_if_needed(time_fn=datetime.datetime.utcnow):
  time_now = time_fn()
  minute_ago = time_now - datetime.timedelta(seconds=60)
  if interface.state.last_flushed > minute_ago:
    return False
  # Do not hammer Datastore if task_num is not yet assigned.
  interface.state.last_flushed = time_now
  with _flush_metrics_lock:
    return _flush_metrics_if_needed_locked(time_now)


def _flush_metrics_if_needed_locked(time_now):
  """Return True if metrics were actually sent."""
  entity = shared.get_instance_entity()
  if entity.task_num < 0:
    if interface.state.target.task_num >= 0:
      _reset_cumulative_metrics()
    interface.state.target.task_num = -1
    interface.state.last_flushed = entity.last_updated
    updated_sec_ago = (time_now - entity.last_updated).total_seconds()
    if updated_sec_ago > shared.INSTANCE_EXPECTED_TO_HAVE_TASK_NUM_SEC:
      logging.warning('Instance %s is %n seconds old with no task_num.',
                      shared.instance_key_id(), updated_sec_ago)
    return False
  interface.state.target.task_num = entity.task_num

  entity.last_updated = time_now
  entity_deferred = entity.put_async()

  interface.flush()

  for metric in shared.global_metrics.itervalues():
    metric.reset()

  entity_deferred.get_result()
  return True


def _shutdown_hook():
  shared.shutdown_counter.increment()
  if flush_metrics_if_needed():
    logging.info('Shutdown hook: deleting %s, metrics were flushed.',
                 shared.instance_key_id())
  else:
    logging.warning('Shutdown hook: deleting %s, metrics were NOT flushed.',
                    shared.instance_key_id())
  with shared.instance_namespace_context():
    ndb.Key('Instance', shared.instance_key_id()).delete()


def _internal_callback():
  for module_name in modules.get_modules():
    target_fields = {
        'task_num': 0,
        'hostname': '',
        'job_name': module_name,
    }
    shared.appengine_default_version.set(
        modules.get_default_version(module_name), target_fields=target_fields)


def initialize(app=None, enable=True, cron_module='default',
               is_local_unittest=None):
  """Instruments webapp2 `app` with gae_ts_mon metrics.

  Instruments all the endpoints in `app` with basic metrics.

  Args:
    app (webapp2 app): the app to instrument.
    enable (bool): enables sending the actual metrics.
      This allows apps to turn monitoring on or off dynamically, per app.
    cron_module (str): the name of the module handling the
      /internal/cron/ts_mon/send endpoint. This allows moving the cron job
      to any module the user wants.
    is_local_unittest (bool or None): whether we are running in a unittest.
  """
  if is_local_unittest is None:  # pragma: no cover
    # Since gae_ts_mon.initialize is called at module-scope by appengine apps,
    # AppengineTestCase.setUp() won't have run yet and none of the appengine
    # stubs will be initialized, so accessing Datastore or even getting the
    # application ID will fail.
    is_local_unittest = ('expect_tests' in sys.argv[0])

  if enable and app is not None:
    instrument_wsgi_application(app)
    if is_local_unittest or modules.get_current_module_name() == cron_module:
      instrument_wsgi_application(handlers.app)

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
    shared.get_instance_entity()  # Create an Instance entity.
    runtime.set_shutdown_hook(_shutdown_hook)

  interface.state.target = targets.TaskTarget(
      service_name, job_name, shared.REGION, hostname, task_num=-1)
  interface.state.flush_mode = 'manual'
  interface.state.last_flushed = datetime.datetime.utcnow()

  # Don't send metrics when running on the dev appserver.
  if (is_local_unittest or
      os.environ.get('SERVER_SOFTWARE', '').startswith('Development')):
    logging.info('Using debug monitor')
    interface.state.global_monitor = monitors.DebugMonitor()
  else:
    logging.info('Using pubsub monitor %s/%s', shared.PUBSUB_PROJECT,
                 shared.PUBSUB_TOPIC)
    interface.state.global_monitor = monitors.PubSubMonitor(
        monitors.APPENGINE_CREDENTIALS, shared.PUBSUB_PROJECT,
        shared.PUBSUB_TOPIC)

  shared.register_global_metrics([shared.appengine_default_version])
  shared.register_global_metrics_callback(
      shared.INTERNAL_CALLBACK_NAME, _internal_callback)

  logging.info('Initialized (%s) ts_mon with service_name=%s, job_name=%s, '
               'hostname=%s',
               'enabled' if enable else 'disabled',
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
      # We must not log user agents, but we can store whether or not the
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
    flush_metrics_if_needed()

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


def reset_for_unittest():
  shared.reset_for_unittest()
  interface.reset_for_unittest()
