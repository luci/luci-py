# Copyright 2014 The LUCI Authors. All rights reserved.
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
import webapp2

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

from components import utils
utils.fix_protobuf_package()

from components import auth
from components import ereporter2
from components import endpoints_webapp2

import gae_ts_mon

import config
import handlers_endpoints_v1
import handlers_frontend
import handlers_prpc


def create_application():
  ereporter2.register_formatter()

  # App that serves HTML pages and old API.
  frontend = handlers_frontend.create_application(False)

  def is_enabled_callback():
    return config.settings().enable_ts_monitoring

  gae_ts_mon.initialize(frontend, is_enabled_fn=is_enabled_callback)
  # App that serves new endpoints API.
  endpoints_api = endpoints_webapp2.api_server([
      handlers_endpoints_v1.IsolateService,
      # components.config endpoints for validation and configuring of
      # luci-config service URL.
      config.ConfigApi,
  ])

  prpc_api = webapp2.WSGIApplication(handlers_prpc.get_routes())
  return frontend, endpoints_api, prpc_api


frontend_app, endpoints_app, prpc_app = create_application()
