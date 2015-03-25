# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module is the entry point to load the AppEngine instance."""

import endpoints

from google.appengine.ext.appstats import recording

from components import auth
from components import ereporter2
from components import utils

from frontend import handlers


def create_applications():
  """Bootstraps the app and creates the url router."""
  ereporter2.register_formatter()

  # App that serves HTML pages and old API.
  frontend = handlers.create_application()
  # Doing it here instead of appengine_config.py reduce the scope of appstats
  # recording. To clarify, this means mapreduces started with map_reduce_jobs.py
  # won't be instrumented, which is actually what we want in practice.
  if utils.is_canary():
    frontend = recording.appstats_wsgi_middleware(frontend)

  # App that serves new endpoints API.
  api = endpoints.api_server([auth.AuthService])

  return frontend, api


frontend_app, endpoints_app = create_applications()
