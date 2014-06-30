# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server backend url handlers."""

import webapp2

from components import ereporter2
from common import ereporter2_config


def get_routes():
  routes = []
  routes.extend(ereporter2.get_backend_routes())
  return routes


def create_application(debug=False):
  ereporter2_config.configure()
  return webapp2.WSGIApplication(get_routes(), debug=debug)
