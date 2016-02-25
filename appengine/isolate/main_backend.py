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

import gae_ts_mon

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

from components import ereporter2
from components import utils

import config
import handlers_backend


def create_application():
  ereporter2.register_formatter()
  backend = handlers_backend.create_application(False)

  def is_enabled_callback():
    return config.settings().enable_ts_monitoring

  gae_ts_mon.initialize(backend, is_enabled_fn=is_enabled_callback)
  return backend


app = create_application()
