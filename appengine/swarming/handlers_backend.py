# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import collections
import datetime
import json
import logging

import webapp2
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import utils

import mapreduce_jobs
from components import decorators
from components import datastore_utils
from components import machine_provider
from server import bot_groups_config
from server import bot_management
from server import config
from server import lease_management
from server import named_caches
from server import task_pack
from server import task_queues
from server import task_result
from server import task_scheduler
import ts_mon_metrics


class CronBotDiedHandler(webapp2.RequestHandler):
  """Sets running tasks where the bot is not sending ping updates for several
  minutes as BOT_DIED.
  """

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
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
  """Set tasks that haven't started before their expiration_ts timestamp as
  EXPIRED.

  Most of the tasks will be expired 'inline' as bots churn through the queue,
  but tasks where the bots are not polling will be expired by this cron job.
  """

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    task_scheduler.cron_abort_expired_task_to_run(self.request.host_url)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronTidyTaskQueues(webapp2.RequestHandler):
  """Removes unused tasks queues, the 'dimensions sets' without active task
  flows.
  """

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    task_queues.cron_tidy_stale()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronUpdateBotInfoComposite(webapp2.RequestHandler):
  """Updates BotInfo.composite if needed, e.g. the bot became dead because it
  hasn't pinged for a while.
  """

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    bot_management.cron_update_bot_info()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronDeleteOldBotEvents(webapp2.RequestHandler):
  """Deletes old BotEvent entities."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    bot_management.cron_delete_old_bot_events()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronMachineProviderBotsUtilizationHandler(webapp2.RequestHandler):
  """Determines Machine Provider bot utilization."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.compute_utilization()


class CronMachineProviderConfigHandler(webapp2.RequestHandler):
  """Configures entities to lease bots from the Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    if config.settings().mp.server:
      new_server = config.settings().mp.server
      current_config = machine_provider.MachineProviderConfiguration().cached()
      if new_server != current_config.instance_url:
        logging.info('Updating Machine Provider server to %s', new_server)
        current_config.modify(updated_by='', instance_url=new_server)

    lease_management.ensure_entities_exist()
    lease_management.drain_excess()


class CronMachineProviderManagementHandler(webapp2.RequestHandler):
  """Manages leases for bots from the Machine Provider."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.schedule_lease_management()


class CronNamedCachesUpdate(webapp2.RequestHandler):
  """Updates named caches hints."""

  @decorators.require_cronjob
  def get(self):
    named_caches.cron_update_named_caches()


class CronCountTaskBotDistributionHandler(webapp2.RequestHandler):
  """Counts how many runnable bots per task for monitoring."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)

    # Step one: build a dictionary mapping dimensions to a count of how many
    # tasks have those dimensions (exclude id from dimensions).
    n_tasks_by_dimensions = collections.Counter()
    q = task_result.TaskResultSummary.query(
        task_result.TaskResultSummary.state.IN(
            task_result.State.STATES_RUNNING))
    for result in q:
      # Make dimensions immutable so they can be used to index a key.
      req = result.request
      for i in xrange(req.num_task_slices):
        t = req.task_slice(i)
        dimensions = tuple(sorted(
              (k, tuple(sorted(v)))
              for k, v in t.properties.dimensions.iteritems()))
        n_tasks_by_dimensions[dimensions] += 1

    # Count how many bots have those dimensions for each set.
    n_bots_by_dimensions = {}
    for dimensions, n_tasks in n_tasks_by_dimensions.iteritems():
      filter_dimensions = []
      for k, values in dimensions:
        for v in values:
          filter_dimensions.append(u'%s:%s' % (k, v))
      q = bot_management.BotInfo.query()
      try:
        q = bot_management.filter_dimensions(q, filter_dimensions)
      except ValueError as e:
        # If there's a problem getting dimensions here, we just don't add the
        # async result to n_bots_by_dimensions, then below treat it as zero
        # (no bots could run this task).
        # This results in overly-pessimistic monitoring, which means someone
        # might look into it to and find the actual error here.
        logging.error('%s', e)
        continue
      n_bots_by_dimensions[dimensions] = q.count_async()

    # Multiply out, aggregating by fixed dimensions
    for dimensions, n_tasks in n_tasks_by_dimensions.iteritems():
      n_bots = 0
      if dimensions in n_bots_by_dimensions:
        n_bots = n_bots_by_dimensions[dimensions].get_result()

      dimensions = dict(dimensions)
      fields = {'pool': dimensions.get('pool', [''])[0]}
      for _ in range(n_tasks):
        ts_mon_metrics._task_bots_runnable.add(n_bots, fields)


class CronBotsDimensionAggregationHandler(webapp2.RequestHandler):
  """Aggregates all bots dimensions (except id) in the fleet."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    seen = {}
    now = utils.utcnow()
    for b in bot_management.BotInfo.query():
      for i in b.dimensions_flat:
        k, v = i.split(':', 1)
        if k != 'id':
          seen.setdefault(k, set()).add(v)
    dims = [
      bot_management.DimensionValues(dimension=k, values=sorted(values))
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
    ndb.get_context().set_cache_policy(lambda _: False)
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


class CronBotGroupsConfigHandler(webapp2.RequestHandler):
  """Fetches bots.cfg with all includes, assembles the final config."""

  @decorators.require_cronjob
  def get(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    ok = True
    try:
      bot_groups_config.refetch_from_config_service()
    except bot_groups_config.BadConfigError:
      ok = False
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.' if ok else 'Fail.')


class CancelTasksHandler(webapp2.RequestHandler):
  """Cancels tasks given a list of their ids."""

  @decorators.require_taskqueue('cancel-tasks')
  def post(self):
    payload = json.loads(self.request.body)
    logging.info('Cancelling tasks with ids: %s', payload['tasks'])
    kill_running = payload['kill_running']
    # TODO(maruel): Parallelize.
    for task_id in payload['tasks']:
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
      ok, was_running = task_scheduler.cancel_task(
          request_obj, result_key, kill_running)
      logging.info('task %s canceled: %s was running: %s',
                   task_id, ok, was_running)


class TaskDimensionsHandler(webapp2.RequestHandler):
  """Refreshes the active task queues."""

  @decorators.require_taskqueue('rebuild-task-cache')
  def post(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if not task_queues.rebuild_task_cache(self.request.body):
      # The task needs to be retried.
      self.response.set_status(500)
    else:
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
    ndb.get_context().set_cache_policy(lambda _: False)
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'MachineLease', key
    lease_management.manage_lease(key)


class TaskNamedCachesPool(webapp2.RequestHandler):
  """Update named caches cache for a pool."""

  @decorators.require_taskqueue('named-cache-task')
  def post(self):
    params = json.loads(self.request.body)
    named_caches.task_update_pool(params['pool'])


class TaskGlobalMetrics(webapp2.RequestHandler):
  """Compute global metrics for timeseries monitoring."""

  @decorators.require_taskqueue('tsmon')
  def post(self, kind):
    if kind == 'machine_types':
      # Avoid a circular dependency. lease_management imports task_scheduler
      # which imports ts_mon_metrics, so invoke lease_management directly to
      # calculate Machine Provider-related global metrics.
      lease_management.set_global_metrics()
    else:
      ts_mon_metrics.set_global_metrics(kind, payload=self.request.body)


### Mapreduce related handlers


class InternalLaunchMapReduceJobWorkerHandler(webapp2.RequestHandler):
  """Called via task queue or cron to start a map reduce job."""

  @decorators.require_taskqueue(mapreduce_jobs.MAPREDUCE_TASK_QUEUE)
  def post(self, job_id):  # pylint: disable=R0201
    ndb.get_context().set_cache_policy(lambda _: False)
    mapreduce_jobs.launch_job(job_id)


###


def get_routes():
  """Returns internal urls that should only be accessible via the backend."""
  routes = [
    # Cron jobs.
    ('/internal/cron/abort_bot_died', CronBotDiedHandler),
    ('/internal/cron/abort_expired_task_to_run',
        CronAbortExpiredShardToRunHandler),
    ('/internal/cron/task_queues_tidy', CronTidyTaskQueues),
    ('/internal/cron/update_bot_info', CronUpdateBotInfoComposite),
    ('/internal/cron/delete_old_bot_events', CronDeleteOldBotEvents),

    ('/internal/cron/count_task_bot_distribution',
        CronCountTaskBotDistributionHandler),

    ('/internal/cron/aggregate_bots_dimensions',
        CronBotsDimensionAggregationHandler),
    ('/internal/cron/aggregate_tasks_tags',
        CronTasksTagsAggregationHandler),

    ('/internal/cron/bot_groups_config', CronBotGroupsConfigHandler),

    # Machine Provider.
    ('/internal/cron/machine_provider_bot_usage',
        CronMachineProviderBotsUtilizationHandler),
    ('/internal/cron/machine_provider_config',
        CronMachineProviderConfigHandler),
    ('/internal/cron/machine_provider_manage',
        CronMachineProviderManagementHandler),
    ('/internal/cron/named_caches_update', CronNamedCachesUpdate),

    # Task queues.
    ('/internal/taskqueue/cancel-tasks', CancelTasksHandler),
    ('/internal/taskqueue/rebuild-task-cache', TaskDimensionsHandler),
    (r'/internal/taskqueue/pubsub/<task_id:[0-9a-f]+>', TaskSendPubSubMessage),
    ('/internal/taskqueue/machine-provider-manage',
        TaskMachineProviderManagementHandler),
    (r'/internal/taskqueue/update_named_cache', TaskNamedCachesPool),
    (r'/internal/taskqueue/tsmon/<kind:[0-9A-Za-z_]+>', TaskGlobalMetrics),

    # Mapreduce related urls.
    (r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
      InternalLaunchMapReduceJobWorkerHandler),
  ]
  return [webapp2.Route(*a) for a in routes]


def create_application(debug):
  return webapp2.WSGIApplication(get_routes(), debug=debug)
