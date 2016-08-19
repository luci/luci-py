# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import datetime
import json
import logging

import webapp2
from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue

from components import utils

import mapreduce_jobs
from components import decorators
from components import datastore_utils
from components import machine_provider
from server import bot_management
from server import config
from server import lease_management
from server import stats
from server import task_result
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
    BATCH_SIZE = 50

    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    app_id = app_identity.get_application_id()
    swarming_server = 'https://%s' % app_identity.get_default_version_hostname()
    found = 0
    for machine_type_key in lease_management.MachineType.query().fetch(
        keys_only=True):
      lease_requests = lease_management.generate_lease_requests(
          machine_type_key, app_id, swarming_server)
      found += len(lease_requests)
      responses = []
      while lease_requests:
        response = machine_provider.lease_machines(lease_requests[:BATCH_SIZE])
        responses.extend(response.get('responses', []))
        lease_requests = lease_requests[BATCH_SIZE:]
      if responses:
        lease_management.update_leases(machine_type_key, responses)
    logging.info('Updated %d', found)


class CronMachineProviderCleanUpHandler(webapp2.RequestHandler):
  """Cleans up leftover BotInfo entities."""

  @decorators.require_cronjob
  def get(self):
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.clean_up_bots()


class CronBotsDimensionAggregationHandler(webapp2.RequestHandler):
  """Aggregates all bots dimensions (except id) in the fleet."""

  @decorators.require_cronjob
  def get(self):
    seen = {}
    now = utils.utcnow()
    for b in bot_management.BotInfo.query():
      for i in b.dimensions_flat:
        k, v = i.split(':', 1)
        if k != 'id':
          seen.setdefault(k, set()).add(v)
    dims = [
      bot_management.DimensionValues(dimension=k,values=sorted(values))
      for k, values in sorted(seen.iteritems())
    ]

    logging.info('Saw dimensions %s', dims)
    bot_management.DimensionAggregation(
        key=bot_management.DimensionAggregation.KEY,
        dimensions=dims,
        ts=now).put()


class CronTasksTagsAggregationHandler(webapp2.RequestHandler):
  """Aggregates all task tags from the last 12 hours."""

  @decorators.require_cronjob
  def get(self):
    seen = {}
    now = utils.utcnow()
    count = 0
    q = task_result.get_result_summaries_query(
        now - datetime.timedelta(hours=12), None, 'created_ts', 'all', None)
    cursor = None
    while True:
      tasks, cursor = datastore_utils.fetch_page(q, 1000, cursor)
      count += len(tasks)
      for t in tasks:
        for i in t.tags:
          k, v = i.split(':', 1)
          s = seen.setdefault(k, set())
          if s is not None:
            s.add(v)
            # 128 is arbitrary large number to avoid OOM
            if len(s) >= 128:
              logging.info('Stripping tag %s because there are too many', k)
              seen[k] = None
      if not cursor or len(tasks) == 0:
        break

    tags = [
      task_result.TagValues(tag=k, values=sorted(values))
      for k, values in sorted(seen.iteritems()) if values is not None
    ]

    logging.info('From %d tasks, saw tags %s', count, tags)
    task_result.TagAggregation(
        key=task_result.TagAggregation.KEY,
        tags=tags,
        ts=now).put()


class CronMachineProviderPubSubHandler(webapp2.RequestHandler):
  """Listens for Pub/Sub communication from Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    taskqueue.add(
        method='POST', url='/internal/taskqueue/pubsub/machine_provider',
        queue_name='machine-provider-pubsub')
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class TaskCleanupDataHandler(webapp2.RequestHandler):
  """Deletes orphaned blobs."""

  @decorators.silence(datastore_errors.Timeout)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    # TODO(maruel): Clean up old TaskRequest after a cut-off date.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class TaskMachineProviderPubSubHandler(webapp2.RequestHandler):
  """Handles Pub/Sub messages from the Machine Provider."""

  @decorators.require_taskqueue('machine-provider-pubsub')
  def post(self):
    app_id = app_identity.get_application_id()
    lease_management.process_pubsub(app_id)


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
    ('/internal/cron/aggregate_bots_dimensions',
        CronBotsDimensionAggregationHandler),
    ('/internal/cron/aggregate_tasks_tags',
        CronTasksTagsAggregationHandler),
    ('/internal/cron/machine_provider', CronMachineProviderBotHandler),
    ('/internal/cron/machine_provider_cleanup',
        CronMachineProviderCleanUpHandler),
    ('/internal/cron/machine_provider_pubsub',
        CronMachineProviderPubSubHandler),

    # Task queues.
    ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),
    (r'/internal/taskqueue/pubsub/<task_id:[0-9a-f]+>', TaskSendPubSubMessage),
    ('/internal/taskqueue/pubsub/machine_provider',
        TaskMachineProviderPubSubHandler),

    # Mapreduce related urls.
    (r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
      InternalLaunchMapReduceJobWorkerHandler),
  ]
  return [webapp2.Route(*a) for a in routes]
