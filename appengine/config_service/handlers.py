# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import webapp2

from components import decorators

import gitiles_import


class CronGitilesImport(webapp2.RequestHandler):
  """Imports configs from Gitiles."""
  @decorators.require_cronjob
  def get(self):
    gitiles_import.cron_run_import()


class MainPageHandler(webapp2.RequestHandler):
  """Redirects to API Explorer."""
  def get(self):
    self.redirect('_ah/api/explorer')


def get_frontend_routes():  # pragma: no cover
  return [
      webapp2.Route(r'/', MainPageHandler),
  ]


def get_backend_routes():  # pragma: no cover
  return [
      webapp2.Route(
          r'/internal/cron/luci-config/gitiles_import',
          CronGitilesImport),
  ]
