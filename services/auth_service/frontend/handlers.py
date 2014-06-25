# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server frontend url handlers."""

import re
import webapp2

from components import auth
from components import auth_ui
from components import utils


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def get_routes():
  routes = []
  routes.extend(auth_ui.get_rest_api_routes())
  routes.extend(auth_ui.get_ui_routes())
  routes.extend([
    webapp2.Route(
        r'/', webapp2.RedirectHandler, defaults={'_uri': '/auth/groups'}),
    webapp2.Route(
        r'/_ah/warmup', WarmupHandler),
  ])
  return routes


def create_application(debug=False):
  # Supported authentication mechanisms.
  auth.configure([
    auth.oauth_authentication,
    auth.cookie_authentication,
    auth.service_to_service_authentication,
  ])

  # URL of a commit that corresponds to currently running version.
  rev = re.match(r'\d+-([a-f0-9]+)$', utils.get_app_version())
  if rev:
    app_revision_url = (
        'https://code.google.com/p/swarming/source/detail?r=%s' % rev)
  else:
    app_revision_url = None

  # Customize auth UI to show that it's running on Auth Service.
  auth_ui.configure_ui(
      app_name='Auth Service',
      app_version=utils.get_app_version(),
      app_revision_url=app_revision_url)

  # Add a fake admin for local dev server.
  if utils.is_local_dev_server():
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'test@example.com'),
        'Users that can manage groups')

  return webapp2.WSGIApplication(get_routes(), debug=debug)
