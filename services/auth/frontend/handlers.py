# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server frontend url handlers."""

import webapp2


class RootHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('Hello, world')


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def get_routes():
  return [
    # Public URLs.
    webapp2.Route(r'/', RootHandler),

    # AppEngine-specific URLs.
    webapp2.Route(r'/_ah/warmup', WarmupHandler),
  ]


def create_application(debug=False):
  return webapp2.WSGIApplication(get_routes(), debug=debug)
