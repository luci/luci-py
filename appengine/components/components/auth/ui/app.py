# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Exports a function that creates WSGI app with Auth routes (API and UI)."""

import webapp2

from . import rest_api
from . import ui

# Part of public API of 'auth' component, exposed by this module.
__all__ = ['create_wsgi_application']


def create_wsgi_application(debug=False):
  routes = []
  routes.extend(rest_api.get_rest_api_routes())
  routes.extend(ui.get_ui_routes())
  return webapp2.WSGIApplication(routes, debug=debug)
