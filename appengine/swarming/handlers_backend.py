# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import json
import logging

import webapp2
from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue

import mapreduce_jobs
from components import decorators
from components import machine_provider
from server import lease_management
from server import stats
from server import task_scheduler


class CronBotDiedHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    try:
      task_scheduler.cron_handle_bot_died(self.request.host_url)
    except datastore_errors.NeedIndexError as e:
      # When a fresh new instance is deployed, it takes a few minutes for the
      # composite indexes to be created even if they are empty. Ignore the case
      # where the index is defined but still being created by AppEngine.
      if not str(e).startswith(
          'NeedIndexError: The index for this query is not ready to serve.'):
        raise
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronAbortExpiredShardToRunHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_abort_expired_task_to_run(self.request.host_url)
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


class CronMachineProviderBotHandler(webapp2.RequestHandler):
  """Handles bots leased from the Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    swarming_server = 'https://%s' % app_identity.get_default_version_hostname()
    # TODO(smut): Parallelize when there are lots of machine types.
    for machine_type_key in lease_management.MachineType.query().fetch(
        keys_only=True):
      lease_requests = lease_management.get_lease_requests(
          machine_type_key, swarming_server)
      if lease_requests:
        responses = machine_provider.lease_machines(lease_requests)
        lease_management.update_leases(machine_type_key, responses)


class TaskCleanupDataHandler(webapp2.RequestHandler):
  """Deletes orphaned blobs."""

  @decorators.silence(datastore_errors.Timeout)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    # TODO(maruel): Clean up old TaskRequest after a cut-off date.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class TaskSendPubSubMessage(webapp2.RequestHandler):
  """Sends PubSub notification about task completion."""

  # Add task_id to the URL for better visibility in request logs.
  @decorators.require_taskqueue('pubsub')
  def post(self, task_id):  # pylint: disable=unused-argument
    task_scheduler.task_handle_pubsub_task(json.loads(self.request.body))
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


### Mapreduce related handlers


class InternalLaunchMapReduceJobWorkerHandler(webapp2.RequestHandler):
  """Called via task queue or cron to start a map reduce job."""
  @decorators.require_taskqueue(mapreduce_jobs.MAPREDUCE_TASK_QUEUE)
  def post(self, job_id):  # pylint: disable=R0201
    mapreduce_jobs.launch_job(job_id)


###


def get_routes():
  """Returns internal urls that should only be accessible via the backend."""
  routes = [
    # Cron jobs.
    # TODO(maruel): Rename cron.yaml job url. Doing so is a bit annoying since
    # the app version has to be running an already compatible version already.
    ('/internal/cron/abort_bot_died', CronBotDiedHandler),
    ('/internal/cron/handle_bot_died', CronBotDiedHandler),
    ('/internal/cron/abort_expired_task_to_run',
        CronAbortExpiredShardToRunHandler),

    ('/internal/cron/stats/update', stats.InternalStatsUpdateHandler),
    ('/internal/cron/trigger_cleanup_data', CronTriggerCleanupDataHandler),
    ('/internal/cron/machine_provider', CronMachineProviderBotHandler),

    # Task queues.
    ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),
    (r'/internal/taskqueue/pubsub/<task_id:[0-9a-f]+>', TaskSendPubSubMessage),

    # Mapreduce related urls.
    (r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
      InternalLaunchMapReduceJobWorkerHandler),
  ]
  return [webapp2.Route(*a) for a in routes]
