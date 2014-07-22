# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server backend url handlers."""

import webapp2

from components import decorators
from components import ereporter2

from common import replication


class InternalReplicationTaskHandler(webapp2.RequestHandler):
  @decorators.require_taskqueue('replication')
  def post(self, auth_db_rev):
    success = replication.update_replicas_task(int(auth_db_rev))
    self.response.set_status(200 if success else 500)


def get_routes():
  routes = [
    webapp2.Route(
        r'/internal/taskqueue/replication/<auth_db_rev:\d+>',
        InternalReplicationTaskHandler),
  ]
  routes.extend(ereporter2.get_backend_routes())
  return routes


def create_application(debug=False):
  ereporter2.configure()
  replication.configure_as_primary()
  return webapp2.WSGIApplication(get_routes(), debug=debug)
