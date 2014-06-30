# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import webapp2
from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from components import datastore_utils
from components import decorators
from components import ereporter2
from server import errors
from server import result_helper
from server import stats
from server import task_scheduler


class CronAbortBotDiedHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_abort_bot_died()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronAbortExpiredShardToRunHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_abort_expired_task_to_run()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronTriggerCleanupDataHandler(webapp2.RequestHandler):
  """Triggers task to delete orphaned blobs."""

  @decorators.require_cronjob
  def get(self):
    taskqueue.add(method='POST', url='/internal/taskqueue/cleanup_data',
                  queue_name='cleanup')
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class TaskCleanupDataHandler(webapp2.RequestHandler):
  """Deletes orphaned blobs."""

  @decorators.silence(datastore_errors.Timeout)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    # All the things that need to be deleted.
    queries = [
        errors.QueryOldErrors(),
        result_helper.QueryOldResults(),
        result_helper.QueryOldResultChunks(),
    ]
    datastore_utils.incremental_map(
        queries, ndb.delete_multi_async, max_inflight=50)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


def get_routes():
  """Returns internal urls that should only be accessible via the backend."""
  routes = [
    # Cron jobs.
    ('/internal/cron/abort_bot_died', CronAbortBotDiedHandler),
    ('/internal/cron/abort_expired_task_to_run',
        CronAbortExpiredShardToRunHandler),

    ('/internal/cron/stats/update', stats.InternalStatsUpdateHandler),
    ('/internal/cron/trigger_cleanup_data', CronTriggerCleanupDataHandler),

    # Task queues.
    ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),
  ]
  routes = [webapp2.Route(*a) for a in routes]
  routes.extend(ereporter2.get_backend_routes())
  return routes
