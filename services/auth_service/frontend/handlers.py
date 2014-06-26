# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server frontend url handlers."""

import webapp2

from components import auth
from components import utils


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def get_routes():
  # Most routes are provided via 'auth' component included in app.yaml.
  return [
    webapp2.Route(
        r'/', webapp2.RedirectHandler, defaults={'_uri': '/auth/groups'}),
    webapp2.Route(
        r'/_ah/warmup', WarmupHandler),
  ]


def create_application(debug=False):
 # Add a fake admin for local dev server.
  if utils.is_local_dev_server():
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'test@example.com'),
        'Users that can manage groups')
  return webapp2.WSGIApplication(get_routes(), debug=debug)
