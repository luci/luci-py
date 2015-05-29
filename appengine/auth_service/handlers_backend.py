# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server backend url handlers."""

import webapp2

from components import decorators

import config
import importer
import replication


class InternalUpdateConfigCronHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    config.refetch_config()


class InternalImportGroupsCronHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    importer.import_external_groups()


class InternalReplicationTaskHandler(webapp2.RequestHandler):
  @decorators.require_taskqueue('replication')
  def post(self, auth_db_rev):
    success = replication.update_replicas_task(int(auth_db_rev))
    self.response.set_status(200 if success else 500)


def get_routes():
  return [
    webapp2.Route(
        r'/internal/cron/update_config',
        InternalUpdateConfigCronHandler),
    webapp2.Route(
        r'/internal/cron/import_groups',
        InternalImportGroupsCronHandler),
    webapp2.Route(
        r'/internal/taskqueue/replication/<auth_db_rev:\d+>',
        InternalReplicationTaskHandler),
  ]


def create_application(debug):
  replication.configure_as_primary()
  return webapp2.WSGIApplication(get_routes(), debug=debug)
