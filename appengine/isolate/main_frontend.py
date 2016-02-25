# Copyright 2014 The Swarming Authors. All rights reserved.
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

from components import auth
from components import ereporter2
from components import utils

import config
import handlers_frontend
import handlers_endpoints_v1
import handlers_endpoints_v2


def create_application():
  ereporter2.register_formatter()

  # App that serves HTML pages and old API.
  frontend = handlers_frontend.create_application(False)
  # TODO(sergeyberezin): Fix today.
  #enable_ts_mon = config.settings().enable_ts_monitoring
  enable_ts_mon = False
  gae_ts_mon.initialize(frontend, enable=enable_ts_mon)
  # App that serves new endpoints API.
  api = endpoints.api_server([handlers_endpoints_v1.IsolateService,
                              handlers_endpoints_v2.IsolateServiceV2])
  return frontend, api


frontend_app, endpoints_app = create_application()
