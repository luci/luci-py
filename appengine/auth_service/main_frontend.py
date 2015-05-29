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

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

import endpoints

from components import auth
from components import config
from components import ereporter2
from components import utils

import handlers_frontend


def create_applications():
  ereporter2.register_formatter()

  # App that serves HTML pages and old API.
  frontend = handlers_frontend.create_application(False)
  # App that serves endpoints APIs.
  api = endpoints.api_server([auth.AuthService, config.ConfigApi])
  return frontend, api


frontend_app, endpoints_app = create_applications()
