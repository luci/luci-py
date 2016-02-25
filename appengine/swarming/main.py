# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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
from server import config


def create_application():
  ereporter2.register_formatter()
  # TODO(sergeyberezin): Fix today.
  #enable_ts_mon = config.settings().enable_ts_monitoring
  enable_ts_mon = False
  # App that serves HTML pages and old API.
  frontend_app = handlers_frontend.create_application(False)
  gae_ts_mon.initialize(frontend_app, enable=enable_ts_mon)
  # Local import, because it instantiates the mapreduce app.
  from mapreduce import main
  gae_ts_mon.initialize(main.APP, enable=enable_ts_mon)
  # App that serves new endpoints API.
  api = endpoints.api_server([handlers_endpoints.swarming_api])
  return frontend_app, api, main.APP


app, endpoints_app, mapreduce_app = create_application()
