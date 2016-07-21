# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This modules is imported by AppEngine and defines the 'app' object.

It is a separate file so that application bootstrapping code like ereporter2,
that shouldn't be done in unit tests, can be done safely. This file must be
tested via a smoke test.
"""

import os
import sys

import endpoints
import gae_ts_mon

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

from components import ereporter2

import handlers_endpoints
import handlers_frontend
import ts_mon_metrics
from server import config


def create_application():
  ereporter2.register_formatter()

  def is_enabled_callback():
    return config.settings().enable_ts_monitoring

  # App that serves HTML pages and old API.
  frontend_app = handlers_frontend.create_application(False)
  gae_ts_mon.initialize(frontend_app, is_enabled_fn=is_enabled_callback)
  # Local import, because it instantiates the mapreduce app.
  from mapreduce import main
  gae_ts_mon.initialize(main.APP, is_enabled_fn=is_enabled_callback)

  # TODO(maruel): Remove this once there is no known client anymore.
  api = endpoints.api_server([
    handlers_endpoints.swarming_api,
    # components.config endpoints for validation and configuring of luci-config
    # service URL.
    config.ConfigApi,
  ])

  ts_mon_metrics.initialize()
  return frontend_app, api, main.APP


app, endpoints_app, mapreduce_app = create_application()
