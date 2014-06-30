# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the application."""

import os
import sys

from google.appengine.api import logservice
from google.appengine.ext.appstats import recording

APP_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

from components import ereporter2
from components import utils
import handlers_frontend


# Aggressively flush the logs, on the canary appstats gets in the way and
# crashes right when the handler is about to return. Saw it happen on prod too,
# especially when there's a fest of "suspended generator transaction" log
# entries.
logservice.AUTOFLUSH_EVERY_LINES = 1


def CreateApplication():
  ereporter2.register_formatter()
  a = handlers_frontend.create_application(False)
  # In theory we'd want to take the output of app_identity.get_application_id().
  # Sadly, this function does an RPC call and may contribute to cause time out
  # on the initial load.
  if utils.is_canary():
    a = recording.appstats_wsgi_middleware(a)
  return a


app = CreateApplication()
