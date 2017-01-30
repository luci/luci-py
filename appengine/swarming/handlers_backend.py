# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import datetime
import json
import logging

import webapp2
from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from components import utils

import mapreduce_jobs
from components import decorators
from components import datastore_utils
from components import machine_provider
from server import bot_management
from server import config
from server import lease_management
from server import stats
from server import task_pack
from server import task_result
from server import task_scheduler
import ts_mon_metrics


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


class CronMachineProviderConfigHandler(webapp2.RequestHandler):
  """Configures entities to lease bots from the Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    if config.settings().mp.server:
      new_server = config.settings().mp.server
      current_config = machine_provider.MachineProviderConfiguration().cached()
      if new_server != current_config.instance_url:
        logging.info('Updating Machine Provider server to %s', new_server)
        current_config.modify(instance_url=new_server)

    lease_management.ensure_entities_exist()
    lease_management.drain_excess()


class CronMachineProviderManagementHandler(webapp2.RequestHandler):
  """Manages leases for bots from the Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.schedule_lease_management()


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
  """Aggregates all task tags from the last hour."""

  @decorators.require_cronjob
  def get(self):
    seen = {}
    now = utils.utcnow()
    count = 0
    q = task_result.TaskResultSummary.query(
        task_result.TaskResultSummary.modified_ts >
        now - datetime.timedelta(hours=1))
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
              logging.info('Limiting tag %s because there are too many', k)
              seen[k] = None
      if not cursor or len(tasks) == 0:
        break

    tags = [
      task_result.TagValues(tag=k, values=sorted(values or []))
      for k, values in sorted(seen.iteritems())
    ]
    logging.info('From %d tasks, saw %d tags', count, len(tags))
    task_result.TagAggregation(
        key=task_result.TagAggregation.KEY,
        tags=tags,
        ts=now).put()


class CancelTasksHandler(webapp2.RequestHandler):
  """Cancels tasks given a list of their ids."""
  @decorators.require_taskqueue('cancel-tasks')
  def post(self):
    ids = self.request.body.split(',')
    logging.info('Cancelling tasks with ids %s', ids)
    for task_id in ids:
      if not task_id:
        logging.error('Cannot cancel a blank task')
        continue
      request_key, result_key = task_pack.get_request_and_result_keys(task_id)
      if not request_key or not result_key:
        logging.error('Cannot search for a falsey key. Request: %s Result: %s',
                      request_key, result_key)
        continue
      request_obj = request_key.get()
      if not request_obj:
        logging.error('Request for %s was not found.', request_key.id())
        continue
      ok, was_running = task_scheduler.cancel_task(request_obj, result_key)
      logging.info('task %s canceled: %s was running: %s',
                   task_id, ok, was_running)


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


class TaskMachineProviderManagementHandler(webapp2.RequestHandler):
  """Manages a lease for a Machine Provider bot."""

  @decorators.require_taskqueue('machine-provider-manage')
  def post(self):
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'MachineLease', key
    lease_management.manage_lease(key)


class TaskGlobalMetrics(webapp2.RequestHandler):
  """Compute global metrics for timeseries monitoring."""

  @decorators.require_taskqueue('tsmon')
  def post(self, kind):
    ts_mon_metrics.set_global_metrics(kind, payload=self.request.body)


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

    # Machine Provider.
    ('/internal/cron/machine_provider_config',
        CronMachineProviderConfigHandler),
    ('/internal/cron/machine_provider_manage',
        CronMachineProviderManagementHandler),

    # Task queues.
    ('/internal/taskqueue/cancel-tasks', CancelTasksHandler),
    ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),
    (r'/internal/taskqueue/pubsub/<task_id:[0-9a-f]+>', TaskSendPubSubMessage),
    ('/internal/taskqueue/machine-provider-manage',
        TaskMachineProviderManagementHandler),
    (r'/internal/taskqueue/tsmon/<kind:[0-9A-Za-z]+>', TaskGlobalMetrics),

    # Mapreduce related urls.
    (r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
      InternalLaunchMapReduceJobWorkerHandler),
  ]
  return [webapp2.Route(*a) for a in routes]
