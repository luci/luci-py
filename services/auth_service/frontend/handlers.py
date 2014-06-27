# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server frontend url handlers."""

import os
import webapp2

from components import auth
from components import utils

from components.auth.ui import rest_api
from components.auth.ui import ui


# Path to search for jinja templates.
TEMPLATES_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'templates')


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


class ServicesHandler(ui.UINavbarTabHandler):
  """Page with management UI for linking services."""
  navbar_tab_url = '/auth/services'
  navbar_tab_id = 'services'
  navbar_tab_title = 'Services'
  js_file_url = '/auth_service/static/js/services.js'
  template_file = 'services.html'


def get_routes():
  # Auth service extends the basic UI and API provided by Auth component.
  routes = []
  routes.extend(rest_api.get_rest_api_routes())
  routes.extend(ui.get_ui_routes())
  routes.extend([
    webapp2.Route(
        r'/', webapp2.RedirectHandler, defaults={'_uri': '/auth/groups'}),
    webapp2.Route(r'/_ah/warmup', WarmupHandler),
  ])
  return routes


def create_application(debug=False):
  # Configure UI appearance, add all custom tabs.
  ui.configure_ui(
      app_name='Auth Service',
      ui_tabs=[
        # Standard tabs provided by auth component.
        ui.GroupsHandler,
        ui.OAuthConfigHandler,
        # Additional tabs available only on auth service.
        ServicesHandler,
      ],
      template_paths=[TEMPLATES_DIR])

  # Add a fake admin for local dev server.
  if utils.is_local_dev_server():
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'test@example.com'),
        'Users that can manage groups')
  return webapp2.WSGIApplication(get_routes(), debug=debug)
