# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main (frontend) entry point for the GCE Backend service"""

import endpoints
import webapp2

from components import config
from components import endpoints_webapp2

import handlers_endpoints


def create_frontend_app():
  return webapp2.WSGIApplication(handlers_endpoints.get_routes())
