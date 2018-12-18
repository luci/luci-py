# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main entry point for Swarming backend handlers."""

import json
import logging

import webapp2
from google.appengine.ext import ndb

import mapreduce_jobs
from components import decorators
from server import bot_groups_config
from server import bot_management
from server import config
from server import lease_management
from server import named_caches
from server import stats_bots
from server import stats_tasks
from server import task_queues
from server import task_request
from server import task_result
from server import task_scheduler
import ts_mon_metrics


## Cron jobs.


class _CronHandlerBase(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    # Disable the in-process cache for all handlers; most of the cron jobs load
    # a tons of instances, causing excessive memory usage if the entities are
    # kept in memory.
    ndb.get_context().set_cache_policy(lambda _: False)
    self.run_cron()

  def run_cron(self):
    raise NotImplementedError()


class CronBotDiedHandler(_CronHandlerBase):
  """Sets running tasks where the bot is not sending ping updates for several
  minutes as BOT_DIED.
  """

  def run_cron(self):
    task_scheduler.cron_handle_bot_died()


class CronAbortExpiredShardToRunHandler(_CronHandlerBase):
  """Set tasks that haven't started before their expiration_ts timestamp as
  EXPIRED.

  Most of the tasks will be expired 'inline' as bots churn through the queue,
  but tasks where the bots are not polling will be expired by this cron job.
  """

  def run_cron(self):
    task_scheduler.cron_abort_expired_task_to_run()


class CronTidyTaskQueues(_CronHandlerBase):
  """Removes unused tasks queues, the 'dimensions sets' without active task
  flows.
  """

  def run_cron(self):
    task_queues.cron_tidy_stale()


class CronUpdateBotInfoComposite(_CronHandlerBase):
  """Updates BotInfo.composite if needed, e.g. the bot became dead because it
  hasn't pinged for a while.
  """

  def run_cron(self):
    bot_management.cron_update_bot_info()


class CronDeleteOldBots(_CronHandlerBase):
  """Deletes old BotRoot entity groups."""

  def run_cron(self):
    bot_management.cron_delete_old_bot()


class CronDeleteOldBotEvents(_CronHandlerBase):
  """Deletes old BotEvent entities."""

  def run_cron(self):
    bot_management.cron_delete_old_bot_events()


class CronDeleteOldTasks(_CronHandlerBase):
  """Deletes old TaskRequest entities and all their decendants."""

  def run_cron(self):
    task_request.cron_delete_old_task_requests()


class CronMachineProviderBotsUtilizationHandler(_CronHandlerBase):
  """Determines Machine Provider bot utilization."""

  def run_cron(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.cron_compute_utilization()


class CronMachineProviderConfigHandler(_CronHandlerBase):
  """Configures entities to lease bots from the Machine Provider."""

  def run_cron(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.cron_sync_config(config.settings().mp.server)


class CronMachineProviderManagementHandler(_CronHandlerBase):
  """Manages leases for bots from the Machine Provider."""

  def run_cron(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not config.settings().mp.enabled:
      logging.info('MP support is disabled')
      return

    lease_management.cron_schedule_lease_management()


class CronNamedCachesUpdate(_CronHandlerBase):
  """Updates named caches hints."""

  def run_cron(self):
    named_caches.cron_update_named_caches()


class CronCountTaskBotDistributionHandler(_CronHandlerBase):
  """Counts how many runnable bots per task for monitoring."""

  def run_cron(self):
    task_scheduler.cron_task_bot_distribution()


class CronBotsDimensionAggregationHandler(_CronHandlerBase):
  """Aggregates all bots dimensions (except id) in the fleet."""

  def run_cron(self):
    bot_management.cron_aggregate_dimensions()


class CronTasksTagsAggregationHandler(_CronHandlerBase):
  """Aggregates all task tags from the last hour."""

  def run_cron(self):
    task_result.cron_update_tags()


class CronBotGroupsConfigHandler(_CronHandlerBase):
  """Fetches bots.cfg with all includes, assembles the final config."""

  def run_cron(self):
    try:
      bot_groups_config.refetch_from_config_service()
    except bot_groups_config.BadConfigError:
      pass


class CronExternalSchedulerCancellationsHandler(_CronHandlerBase):
  """Fetches cancelled tasks from external scheulers, and cancels them."""

  def run_cron(self):
    task_scheduler.cron_handle_external_cancellations()


class CronBotsStats(_CronHandlerBase):
  """Update bots monitoring statistics."""

  def run_cron(self):
    stats_bots.cron_generate_stats()


class CronTasksStats(_CronHandlerBase):
  """Update tasks monitoring statistics."""

  def run_cron(self):
    stats_tasks.cron_generate_stats()


class CronBotsSendToBQ(_CronHandlerBase):
  """Streams BotEvent to BigQuery."""

  def run_cron(self):
    bot_management.cron_send_to_bq()


## Task queues.


class CancelTasksHandler(webapp2.RequestHandler):
  """Cancels tasks given a list of their ids."""

  @decorators.require_taskqueue('cancel-tasks')
  def post(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    payload = json.loads(self.request.body)
    logging.info('Cancelling tasks with ids: %s', payload['tasks'])
    kill_running = payload['kill_running']
    # TODO(maruel): Parallelize.
    for task_id in payload['tasks']:
      ok, was_running = task_scheduler.cancel_task_with_id(
          task_id, kill_running, None)
      logging.info('task %s canceled: %s was running: %s',
                   task_id, ok, was_running)


class CancelTaskOnBotHandler(webapp2.RequestHandler):
  """Cancels a given task if it is running on the given bot.

  If bot is not specified, cancel task unconditionally.
  If bot is specified, and task is not running on bot, then do nothing.
  """

  @decorators.require_taskqueue('cancel-task-on-bot')
  def post(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    payload = json.loads(self.request.body)
    task_id = payload.get('task_id')
    if not task_id:
      logging.error('Missing task_id.')
      return
    bot_id = payload.get('bot_id')
    try:
      ok, was_running = task_scheduler.cancel_task_with_id(
          task_id, True, bot_id)
      logging.info('task %s canceled: %s was running: %s',
                   task_id, ok, was_running)
    except ValueError:
      # Ignore errors that may be due to missing or invalid tasks.
      logging.warning('Ignoring a task cancellation due to exception.',
          exc_info=True)


class TaskDimensionsHandler(webapp2.RequestHandler):
  """Refreshes the active task queues."""

  @decorators.require_taskqueue('rebuild-task-cache')
  def post(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    if not task_queues.rebuild_task_cache(self.request.body):
      # The task needs to be retried. Reply that the service is unavailable
      # (503) instead of an internal server error (500) to help differentiating
      # in the logs, even if it is not technically correct.
      self.response.set_status(503)


class TaskSendPubSubMessage(webapp2.RequestHandler):
  """Sends PubSub notification about task completion."""

  # Add task_id to the URL for better visibility in request logs.
  @decorators.require_taskqueue('pubsub')
  def post(self, task_id):  # pylint: disable=unused-argument
    ndb.get_context().set_cache_policy(lambda _: False)
    task_scheduler.task_handle_pubsub_task(json.loads(self.request.body))


class TaskMachineProviderManagementHandler(webapp2.RequestHandler):
  """Manages a lease for a Machine Provider bot."""

  @decorators.require_taskqueue('machine-provider-manage')
  def post(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'MachineLease', key
    lease_management.task_manage_lease(key)


class TaskNamedCachesPool(webapp2.RequestHandler):
  """Update named caches cache for a pool."""

  @decorators.require_taskqueue('named-cache-task')
  def post(self):
    ndb.get_context().set_cache_policy(lambda _: False)
    params = json.loads(self.request.body)
    logging.info('Handling pool: %s', params['pool'])
    named_caches.task_update_pool(params['pool'])


class TaskGlobalMetrics(webapp2.RequestHandler):
  """Compute global metrics for timeseries monitoring."""

  @decorators.require_taskqueue('tsmon')
  def post(self, kind):
    ndb.get_context().set_cache_policy(lambda _: False)
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
    ('/internal/cron/delete_old_bot', CronDeleteOldBots),
    ('/internal/cron/delete_old_bot_events', CronDeleteOldBotEvents),
    ('/internal/cron/delete_old_tasks', CronDeleteOldTasks),
    ('/internal/cron/bots/stats', CronBotsStats),
    ('/internal/cron/tasks/stats', CronTasksStats),
    ('/internal/cron/bots/send_to_bq', CronBotsSendToBQ),

    ('/internal/cron/count_task_bot_distribution',
        CronCountTaskBotDistributionHandler),

    ('/internal/cron/aggregate_bots_dimensions',
        CronBotsDimensionAggregationHandler),
    ('/internal/cron/aggregate_tasks_tags',
        CronTasksTagsAggregationHandler),

    ('/internal/cron/bot_groups_config', CronBotGroupsConfigHandler),

    ('/internal/cron/external_scheduler_cancellations',
        CronExternalSchedulerCancellationsHandler),

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
    ('/internal/taskqueue/cancel-task-on-bot', CancelTaskOnBotHandler),
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
