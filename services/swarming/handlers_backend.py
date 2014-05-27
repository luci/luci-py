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
from server import admin_user
from server import result_helper
from server import errors
from server import task_scheduler
from server import stats_new as stats

import handlers_common


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


class CronSendEreporter2MailHandler(webapp2.RequestHandler):
  """Sends email containing the errors found in logservice."""

  @decorators.require_cronjob
  def get(self):
    request_id_url = self.request.host_url + '/secure/ereporter2/request/'
    report_url = self.request.host_url + '/secure/ereporter2/report'
    result = ereporter2.generate_and_email_report(
        None,
        handlers_common.should_ignore_error_record,
        admin_user.GetAdmins(),
        request_id_url,
        report_url,
        ereporter2.REPORT_TITLE_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        {})
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if result:
      self.response.write('Success.')
    else:
      # Do not HTTP 500 since we do not want it to be retried.
      self.response.write('Failed.')


def get_routes():
  """Returns internal urls that should only be accessible via the backend."""
  return [
    # Cron jobs.
    ('/internal/cron/abort_bot_died', CronAbortBotDiedHandler),
    ('/internal/cron/abort_expired_task_to_run',
        CronAbortExpiredShardToRunHandler),

    ('/internal/cron/ereporter2/mail', CronSendEreporter2MailHandler),
    ('/internal/cron/stats/update', stats.InternalStatsUpdateHandler),
    ('/internal/cron/trigger_cleanup_data', CronTriggerCleanupDataHandler),

    # Task queues.
    ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),
  ]
