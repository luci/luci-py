# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Timeseries metrics."""

from collections import defaultdict
import itertools
import json
import logging

from google.appengine.ext import ndb
from google.appengine.datastore.datastore_query import Cursor

from components import utils
import gae_ts_mon

from server import bot_management
from server import task_result

IGNORED_DIMENSIONS = ('id', 'android_devices')
# Real timeout is 60s, keep it slightly under to bail out early.
REQUEST_TIMEOUT_SEC = 50
# Cap the max number of items per taskqueue task, to keep the total
# number of collected streams managable within each instance.
EXECUTORS_PER_SHARD = 500
JOBS_PER_SHARD = 500

# Override default target fields for app-global metrics.
TARGET_FIELDS = {
    'job_name':  '',  # module name
    'hostname': '',  # version
    'task_num':  0,  # instance ID
}

# A custom bucketer with 12% resolution in the range of 1..10**5.
# Used for job cycle times.
_bucketer = gae_ts_mon.GeometricBucketer(growth_factor=10**0.05,
                                     num_finite_buckets=100)

# Regular (instance-local) metrics: jobs/completed and jobs/durations.
# Both have the following metric fields:
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# - result: one of 'success', 'failure', or 'infra-failure'.
jobs_completed = gae_ts_mon.CounterMetric(
    'jobs/completed',
    description='Number of completed jobs.')


jobs_durations = gae_ts_mon.CumulativeDistributionMetric(
    'jobs/durations', bucketer=_bucketer,
    description='Cycle times of completed jobs, in seconds.')


# Similar to jobs/completed and jobs/duration, but with a dedup field.
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# - deduped: boolean describing whether the job was deduped or not.
jobs_requested = gae_ts_mon.CounterMetric(
    'jobs/requested',
    description='Number of requested jobs over time.')


# Swarming-specific metric. Metric fields:
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
tasks_expired = gae_ts_mon.CounterMetric(
    'swarming/tasks/expired',
    description='Number of expired tasks')

# Global metric. Metric fields:
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# Override target field:
# - hostname: 'autogen:<executor_id>': name of the bot that executed a job,
#     or an empty string. e.g. 'autogen:swarm42-m4'.
# Value should be 'pending' or 'running'. Completed / canceled jobs should not
# send this metric.
jobs_running = gae_ts_mon.BooleanMetric(
    'jobs/running',
    description='Presence metric for a running job.')

# Global metric. Metric fields:
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# - status: 'pending' or 'running'.
jobs_active = gae_ts_mon.GaugeMetric(
    'jobs/active',
    description='Number of running, pending or otherwise active jobs.')


# Global metric. Target field: hostname = 'autogen:<executor_id>' (bot id).
executors_pool = gae_ts_mon.StringMetric(
    'executors/pool',
    description='Pool name for a given job executor.')


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
executors_status = gae_ts_mon.StringMetric(
    'executors/status',
    description=('Status of a job executor.'))


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
# Note that 'running' will report data as long as the job is running,
# so it is best to restrict data to status == 'pending.'
jobs_pending_durations = gae_ts_mon.NonCumulativeDistributionMetric(
    'jobs/pending_durations', bucketer=_bucketer,
    description='Pending times of active jobs, in seconds.')


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
# Note that 'running' will report data as long as the job is running,
# so it is best to restrict data to status == 'pending.'
jobs_max_pending_duration = gae_ts_mon.FloatMetric(
    'jobs/max_pending_duration',
    description='Maximum pending seconds of pending jobs.')


def pool_from_dimensions(dimensions):
  """Return a canonical string of flattened dimensions."""
  iterables = (map(lambda x: '%s:%s' % (key, x), values)
               for key, values in dimensions.iteritems()
               if key not in IGNORED_DIMENSIONS)
  return '|'.join(sorted(itertools.chain(*iterables)))


def extract_job_fields(tags):
  """Extracts common job's metric fields from TaskResultSummary.

  Args:
    tags (list of str): list of 'key:value' strings.
  """
  tags_dict = {}
  for tag in tags:
    try:
      key, value = tag.split(':', 1)
      tags_dict[key] = value
    except ValueError:
      pass

  spec_name = tags_dict.get('spec_name')
  if not spec_name:
    spec_name = '%s:%s:%s' % (
        tags_dict.get('master', ''),
        tags_dict.get('buildername', ''),
        tags_dict.get('name', ''))

  fields = {
      'project_id': tags_dict.get('project', ''),
      'subproject_id': tags_dict.get('subproject', ''),
      'spec_name': spec_name,
  }
  return fields


def update_jobs_completed_metrics(task_result_summary):
  fields = extract_job_fields(task_result_summary.tags)
  if task_result_summary.internal_failure:
    fields['result'] = 'infra-failure'
  elif task_result_summary.failure:
    fields['result'] = 'failure'
  else:
    fields['result'] = 'success'
  jobs_completed.increment(fields=fields)
  if task_result_summary.duration is not None:
    jobs_durations.add(task_result_summary.duration, fields=fields)


def update_jobs_requested_metrics(task_request, deduped):
  fields = extract_job_fields(task_request.tags)
  fields['deduped'] = deduped
  jobs_requested.increment(fields=fields)


def _start_task(payload):
  """Common boilerplate for a task in a chain of tasks."""
  start_time = utils.utcnow()
  params = {
      'cursor': None,
      'task_start': start_time,
      'task_count': 0,
      'count': 0,
  }
  if payload:
    try:
      params = json.loads(payload)
    except ValueError as e:
      logging.error('_set_executors_metrics: bad JSON: %s: %s',
                    payload, e)
      return

  params['task_count'] += 1
  cursor = Cursor(urlsafe=params['cursor']) if params['cursor'] else None
  return start_time, params, cursor


@ndb.tasklet
def _set_jobs_metrics(payload):
  start_time, params, cursor = _start_task(payload)

  state_map = {task_result.State.RUNNING: 'running',
               task_result.State.PENDING: 'pending'}
  jobs_counts = defaultdict(lambda: 0)
  jobs_total = 0
  jobs_pending_distributions = defaultdict(
      lambda: gae_ts_mon.Distribution(_bucketer))
  jobs_max_pending_durations = defaultdict(
      lambda: 0.0)

  query_iter = task_result.get_result_summaries_query(
      None, None, 'created_ts', 'pending_running', None).iter(
      produce_cursors=True, start_cursor=cursor)

  while (yield query_iter.has_next_async()):
    if (jobs_total >= JOBS_PER_SHARD or
        (utils.utcnow() - start_time).total_seconds() > REQUEST_TIMEOUT_SEC):
      cursor = query_iter.cursor_after()
      params['cursor'] = cursor.urlsafe()
      utils.enqueue_task(url='/internal/taskqueue/tsmon/jobs',
                         queue_name='tsmon',
                         payload=json.dumps(params))
      break

    params['count'] += 1
    jobs_total += 1
    summary = query_iter.next()
    status = state_map.get(summary.state, '')
    fields = extract_job_fields(summary.tags)
    target_fields = dict(TARGET_FIELDS)
    if summary.bot_id:
      target_fields['hostname'] = 'autogen:' + summary.bot_id
    if summary.bot_id and status == 'running':
      jobs_running.set(True, target_fields=target_fields, fields=fields)
    fields['status'] = status

    key = tuple(sorted(fields.iteritems()))

    jobs_counts[key] += 1

    pending_duration = summary.pending_now(utils.utcnow())
    if pending_duration is not None:
      jobs_pending_distributions[key].add(pending_duration.total_seconds())
      jobs_max_pending_durations[key] = max(
          jobs_max_pending_durations[key],
          pending_duration.total_seconds())

  logging.debug(
      '_set_jobs_metrics: task %d started at %s, processed %d jobs (%d total)',
      params['task_count'], params['task_start'], jobs_total, params['count'])

  # Global counts are sharded by task_num and aggregated in queries.
  target_fields = dict(TARGET_FIELDS)
  target_fields['task_num'] = params['task_count']

  for key, count in jobs_counts.iteritems():
    jobs_active.set(count, target_fields=target_fields, fields=dict(key))

  for key, distribution in jobs_pending_distributions.iteritems():
    jobs_pending_durations.set(
        distribution, target_fields=target_fields, fields=dict(key))

  for key, val in jobs_max_pending_durations.iteritems():
    jobs_max_pending_duration.set(
        val, target_fields=target_fields, fields=dict(key))


@ndb.tasklet
def _set_executors_metrics(payload):
  start_time, params, cursor = _start_task(payload)
  query_iter = bot_management.BotInfo.query().iter(
      produce_cursors=True, start_cursor=cursor)

  executors_count = 0
  while (yield query_iter.has_next_async()):
    if (executors_count >= EXECUTORS_PER_SHARD or
        (utils.utcnow() - start_time).total_seconds() > REQUEST_TIMEOUT_SEC):
      cursor = query_iter.cursor_after()
      params['cursor'] = cursor.urlsafe()
      utils.enqueue_task(url='/internal/taskqueue/tsmon/executors',
                         queue_name='tsmon',
                         payload=json.dumps(params))
      break

    params['count'] += 1
    executors_count += 1
    bot_info = query_iter.next()
    status = 'ready'
    if bot_info.task_id:
      status = 'running'
    elif bot_info.quarantined:
      status = 'quarantined'
    elif bot_info.is_dead(utils.utcnow()):
      status = 'dead'

    target_fields = dict(TARGET_FIELDS)
    target_fields['hostname'] = 'autogen:' + bot_info.id

    executors_status.set(status, target_fields=target_fields)
    executors_pool.set(
        pool_from_dimensions(bot_info.dimensions),
        target_fields=target_fields)

  logging.debug(
      '%s: task %d started at %s, processed %d bots (%d total)',
      '_set_executors_metrics', params['task_count'], params['task_start'],
       executors_count, params['count'])


def set_global_metrics(kind, payload=None):
  if kind == 'jobs':
    _set_jobs_metrics(payload).get_result()
  elif kind == 'executors':
    _set_executors_metrics(payload).get_result()
  else:
    logging.error('set_global_metrics(kind=%s): unknown kind.', kind)


def _set_global_metrics():
  utils.enqueue_task(url='/internal/taskqueue/tsmon/jobs', queue_name='tsmon')
  utils.enqueue_task(url='/internal/taskqueue/tsmon/executors',
                     queue_name='tsmon')


def initialize():
  gae_ts_mon.register_global_metrics([
      executors_pool,
      executors_status,
      jobs_active,
      jobs_max_pending_duration,
      jobs_pending_durations,
      jobs_running,
  ])
  gae_ts_mon.register_global_metrics_callback('callback', _set_global_metrics)
