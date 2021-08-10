# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import time

# Not all apps enable flask. If the import fails, the app wouldn't be able to
# pass an instance of Flask() to gae_ts_mon.initialize(). If flask is not
# importable by gae_ts_mon, but by a business application, then
# gae_ts_mon.initialize() will raise Exception("Unsupported wsgi application").
try:
  import flask
except ImportError:  # pragma: no cover
  flask = None
else:
  from infra_libs.ts_mon import instrument_flask  # pylint: disable=ungrouped-imports

# webapp2 won't be available in Chrome Infra Python3 SDK.
try:
  import webapp2
except ImportError:  # pragma: no cover
  webapp2 = None
else:
  from infra_libs.ts_mon import instrument_webapp2  # pylint: disable=ungrouped-imports

from google.appengine.api import modules
from google.appengine.api.app_identity import app_identity
from google.appengine.api import runtime
from google.appengine.ext import ndb

from infra_libs.ts_mon import exporter
from infra_libs.ts_mon import handlers
from infra_libs.ts_mon import shared
from infra_libs.ts_mon.common import interface
from infra_libs.ts_mon.common import monitors
from infra_libs.ts_mon.common import standard_metrics
from infra_libs.ts_mon.common import targets


def _shutdown_hook(time_fn=time.time):
  shared.shutdown_counter.increment()
  if exporter.flush_metrics_if_needed(time_fn()):
    logging.info('Shutdown hook: deleting %s, metrics were flushed.',
                 shared.instance_key_id())
  else:
    logging.warning('Shutdown hook: deleting %s, metrics were NOT flushed.',
                    shared.instance_key_id())
  with shared.instance_namespace_context():
    ndb.Key(shared.Instance._get_kind(), shared.instance_key_id()).delete()


def _internal_callback():
  # TODO(crbug.com/monorail/8841) This can only be replaced by the Admin API,
  # which isn't a drop-in replacement; requires each project enabling
  # "App Engine Admin API", installing a client library, and making REST call.
  for module_name in modules.get_modules():
    target_fields = {
        'task_num': 0,
        'hostname': '',
        'job_name': module_name,
    }
    shared.appengine_default_version.set(
        modules.get_default_version(module_name), target_fields=target_fields)


def initialize(
    app,
    is_enabled_fn=None,
    cron_module='default',  # pylint: disable=unused-argument
    is_local_unittest=None):
  """Instruments webapp2 `app` with gae_ts_mon metrics.

  Instruments all the endpoints in `app` with basic metrics.

  Args:
    app (webapp2 app): the app to instrument.
    is_enabled_fn (function or None): a function returning bool if ts_mon should
      send the actual metrics. None (default) is equivalent to lambda: True.
      This allows apps to turn monitoring on or off dynamically, per app.
    cron_module (str): DEPRECATED. This param is noop.
    is_local_unittest (bool or None): whether we are running in a unittest.
  """
  if is_local_unittest is None:  # pragma: no cover
    # Since gae_ts_mon.initialize is called at module-scope by appengine apps,
    # AppengineTestCase.setUp() won't have run yet and none of the appengine
    # stubs will be initialized, so accessing Datastore or even getting the
    # application ID will fail.
    is_local_unittest = ('expect_tests' in sys.argv[0])

  if is_enabled_fn is not None:
    interface.state.flush_enabled_fn = is_enabled_fn

  if app is None:
    raise Exception('app cannot be None')
  instrument_wsgi_application(app)

  # Use the application ID as the service name and the module name as the job
  # name.
  if is_local_unittest:  # pragma: no cover
    service_name = 'unittest'
    job_name = 'unittest'
    hostname = 'unittest'
  else:
    if shared.is_python3_env():
      service_name = os.getenv('GOOGLE_CLOUD_PROJECT', '')
      job_name = os.getenv('GAE_SERVICE', '')
      hostname = os.getenv('GAE_VERSION', '')
    else:
      service_name = app_identity.get_application_id()
      job_name = modules.get_current_module_name()
      hostname = modules.get_current_version_name()
    # TODO(crbug.com/monorail/8841): follow up on whether
    # runtime.set_shutdown_hook is supported in python 3 env.
    runtime.set_shutdown_hook(_shutdown_hook)

  interface.state.target = targets.TaskTarget(
      service_name, job_name, shared.REGION, hostname, task_num=-1)
  interface.state.flush_mode = 'manual'
  interface.state.last_flushed = datetime.datetime.utcnow()

  # pylint: disable=line-too-long
  # Don't send metrics when running on the dev appserver.
  # : https://cloud.google.com/appengine/docs/standard/python/tools/using-local-server#detecting_application_runtime_environment
  # pylint: enable=line-too-long
  if (is_local_unittest or
      os.environ.get('SERVER_SOFTWARE', '').startswith('Development')):
    logging.debug('Using debug monitor')
    interface.state.global_monitor = monitors.DebugMonitor()
  else:
    logging.debug('Using https monitor %s with %s', shared.PRODXMON_ENDPOINT,
                  shared.PRODXMON_SERVICE_ACCOUNT_EMAIL)
    interface.state.global_monitor = monitors.HttpsMonitor(
        shared.PRODXMON_ENDPOINT,
        monitors.DelegateServiceAccountCredentials(
            shared.PRODXMON_SERVICE_ACCOUNT_EMAIL,
            monitors.AppengineCredentials()))

  interface.register_global_metrics([shared.appengine_default_version])
  interface.register_global_metrics_callback(
      shared.INTERNAL_CALLBACK_NAME, _internal_callback)

  # We invoke global callbacks once for the whole application in the cron
  # handler.  Leaving this set to True would invoke them once per task.
  interface.state.invoke_global_callbacks_on_flush = False

  standard_metrics.init()

  logging.debug(
      'Initialized ts_mon with service_name=%s, job_name=%s, '
      'hostname=%s', service_name, job_name, hostname)


def instrument_wsgi_application(app, time_fn=time.time):
  """Instrument a given WSGI app."""
  if webapp2 is not None and isinstance(app, webapp2.WSGIApplication):
    return instrument_webapp2.instrument(app, time_fn)

  if flask is not None and isinstance(app, flask.Flask):
    return instrument_flask.instrument(app, time_fn)

  raise NotImplementedError("Unsupported wsgi application")


def reset_for_unittest(disable=False):
  interface.reset_for_unittest(disable=disable)
