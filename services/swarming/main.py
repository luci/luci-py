# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the application."""

import os

from google.appengine.ext.appstats import recording

from components import ereporter2
import handlers


def CreateApplication():
  ereporter2.register_formatter()
  a = handlers.CreateApplication()
  # In theory we'd want to take the output of app_identity.get_application_id().
  # Sadly, this function does an RPC call and may contribute to cause time out
  # on the initial load.
  if os.environ['APPLICATION_ID'].endswith('-dev'):
    a = recording.appstats_wsgi_middleware(a)
  return a


app = CreateApplication()
