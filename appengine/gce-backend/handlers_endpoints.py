# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Cloud endpoints for the GCE Backend API."""

import endpoints

from components import config
from components import endpoints_webapp2


def get_routes():
  return endpoints_webapp2.api_server([config.ConfigApi])


def create_endpoints_app():
  return endpoints.api_server([config.ConfigApi])
