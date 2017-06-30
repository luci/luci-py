# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import httplib

import webapp2

from components import decorators
from components.config.proto import service_config_pb2
from google.appengine.ext.webapp import template

import common
import gitiles_import
import notifications
import os
import storage


class CronGitilesImport(webapp2.RequestHandler):
  """Imports configs from Gitiles."""
  @decorators.require_cronjob
  def get(self):
    gitiles_import.cron_run_import()


class MainPageHandler(webapp2.RequestHandler):
  """Redirects to API Explorer."""

  def get(self):
    self.redirect('_ah/api/explorer')

class UIHandler(webapp2.RequestHandler):
  """ Serves the UI with the proper client ID. """

  def get(self):
    # TODO(cwpayton): put the client_id in a proto file so that it is
    # configurable and can be read dynamically by this file.
    template_values = {
      'client_id':
          '247108661754-svmo17vmk1j5hlt388gb45qblgvg2h98.apps.googleusercontent.com'
    }
    path = os.path.join(os.path.dirname(__file__), 'ui/static/index.html')
    self.response.out.write(template.render(path, template_values))


class SchemasHandler(webapp2.RequestHandler):
  """Redirects to a known schema definition."""

  def get(self, name):
    cfg = storage.get_self_config_async(
        common.SCHEMAS_FILENAME, service_config_pb2.SchemasCfg).get_result()
    # Assume cfg was validated by validation.py
    if cfg:
      for schema in cfg.schemas:
        if schema.name == name:
          # Convert from unicode.
          assert schema.url
          self.redirect(str(schema.url))
          return

    self.response.write('Schema %s not found\n' % name)
    self.response.set_status(httplib.NOT_FOUND)


def get_frontend_routes():  # pragma: no cover
  return [
    webapp2.Route(r'/', MainPageHandler),
    webapp2.Route(r'/newui', UIHandler),
    webapp2.Route(r'/schemas/<name:.+>', SchemasHandler),
    webapp2.Route(r'/_ah/bounce', notifications.BounceHandler),
  ]


def get_backend_routes():  # pragma: no cover
  return [
      webapp2.Route(
          r'/internal/cron/luci-config/gitiles_import',
          CronGitilesImport),
  ]
