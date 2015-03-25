# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module is the entry point to load the AppEngine instance."""

from google.appengine.ext.appstats import recording

from components import ereporter2
from components import utils

from backend import handlers


def create_application():
  """Bootstraps the app and creates the url router."""
  ereporter2.register_formatter()
  a = handlers.create_application()
  # Doing it here instead of appengine_config.py reduce the scope of appstats
  # recording. To clarify, this means mapreduces started with map_reduce_jobs.py
  # won't be instrumented, which is actually what we want in practice.
  if utils.is_canary():
    a = recording.appstats_wsgi_middleware(a)
  return a


app = create_application()
