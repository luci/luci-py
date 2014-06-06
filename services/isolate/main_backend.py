# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module is the entry point to load the AppEngine instance."""

import os
import sys

from google.appengine.ext.appstats import recording

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))

from components import ereporter2
from components import utils
import handlers_backend


def create_application():
  """Bootstraps the app and creates the url router."""
  ereporter2.register_formatter()
  a = handlers_backend.create_application()
  # In theory we'd want to take the output of app_identity.get_application_id().
  # Sadly, this function does an RPC call and may contribute to cause time out
  # on the initial load.
  # Doing it here instead of appengine_config.py reduce the scope of appstats
  # recording. To clarify, this means mapreduces started with map_reduce_jobs.py
  # won't be instrumented, which is actually what we want in practice.
  if utils.is_canary():
    a = recording.appstats_wsgi_middleware(a)
  return a


app = create_application()
