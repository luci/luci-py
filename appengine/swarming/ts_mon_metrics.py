# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Timeseries metrics."""

from collections import defaultdict
import datetime
import json
import logging

from google.appengine.datastore.datastore_query import Cursor

from components import utils
import gae_ts_mon

from server import bot_management
from server import task_result

# - android_devices is a side effect of the health of each Android devices
#   connected to the bot.
# - caches has an unbounded matrix.
# - server_version is the current server version. It'd be good to have but the
#   current monitoring pipeline is not adapted for this.
# - id is unique for each bot.
# - temp_band is android specific.
# Keep in sync with ../swarming_bot/bot_code/bot_main.py
_IGNORED_DIMENSIONS = ('android_devices', 'caches', 'id', 'server_version',
                       'temp_band')
# Real timeout is 60s, keep it slightly under to bail out early.
_REQUEST_TIMEOUT_SEC = 50
# Cap the max number of items per taskqueue task, to keep the total
# number of collected streams managable within each instance.
_EXECUTORS_PER_SHARD = 500
_JOBS_PER_SHARD = 500

# Override default target fields for app-global metrics.
_TARGET_FIELDS = {
    'job_name': '',  # module name
    'hostname': '',  # version
    'task_num': 0,  # instance ID
}


### All the metrics.


# Custom bucketer with 12% resolution in the range of 1..10**5. Used for job
# cycle times.
_bucketer = gae_ts_mon.GeometricBucketer(growth_factor=10**0.05,
                                         num_finite_buckets=100)

# Custom bucketer with 2% resolution in the range of 100ms...100s. Used for
# pubsub latency measurements.
# Roughly speaking measurements range between 150ms and 300ms. However timeout
# for pubsub notification is 10s.
_pubsub_bucketer = gae_ts_mon.GeometricBucketer(growth_factor=10**0.01,
                                                num_finite_buckets=300,
                                                scale=10)

# Custom bucketer with 2% resolution in the range of 100ms...100000s. Used for
# task scheduling latency measurements.
_scheduler_bucketer = gae_ts_mon.GeometricBucketer(growth_factor=10**0.01,
                                                   num_finite_buckets=600,
                                                   scale=100)

# Custom bucketer with 2% resolution in the range of 100ms...100000s. Used for
# task dead detection latency.
# cron job runs every 60s, but can also fail for a few hours.
_detection_bucketer = gae_ts_mon.GeometricBucketer(growth_factor=10**0.01,
                                                   num_finite_buckets=600,
                                                   scale=100)

# Regular (instance-local) metrics: jobs/completed and jobs/durations.
# Both have the following metric fields:
# - project_id: e.g. 'chromium'.
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - pool: e.g. 'Chrome'.
# - spec_name: name of a job specification.
# - result: one of 'success', 'failure', or 'infra-failure'.
_jobs_completed = gae_ts_mon.CounterMetric(
    'jobs/completed',
    'Number of completed jobs.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('result'),
        gae_ts_mon.StringField('status'),
    ])


_jobs_durations = gae_ts_mon.CumulativeDistributionMetric(
    'jobs/durations',
    'Cycle times of completed jobs, in seconds.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('result'),
    ],
    bucketer=_bucketer)


# Similar to jobs/completed and jobs/duration, but with a dedup field.
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - pool: e.g. 'Chrome'
# - spec_name: name of a job specification.
# - deduped: boolean describing whether the job was deduped or not.
_jobs_requested = gae_ts_mon.CounterMetric(
    'jobs/requested',
    'Number of requested jobs over time.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.BooleanField('deduped'),
    ])


# Swarming-specific metric. Metric fields:
# - project_id: e.g. 'chromium'.
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - pool: e.g. 'Chrome'.
# - spec_name: name of a job specification.
# - priority: priority of a task.
_tasks_expired = gae_ts_mon.CounterMetric(
    'swarming/tasks/expired', 'Number of expired tasks', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.IntegerField('priority'),
    ])


# Swarming-specific metric. Metric fields:
# - project_id: e.g. 'chromium-swarm'
_tasks_expiration_delay = gae_ts_mon.CumulativeDistributionMetric(
    'swarming/tasks/expiration_delay',
    'Delay of task expiration, in seconds.', [
        gae_ts_mon.StringField('project_id'),
    ])


# Swarming-specific metric. Metric fields:
# - project_id: e.g. 'chromium-swarm'
_tasks_slice_expiration_delay = gae_ts_mon.CumulativeDistributionMetric(
    'swarming/tasks/slice_expiration_delay',
    'Delay of task slice expiration, in seconds.',
    [
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.IntegerField('slice_index'),
    ],
    bucketer=gae_ts_mon.FixedWidthBucketer(width=30),
)


# Global metric. Metric fields:.
# - project_id: e.g. 'chromium'.
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - pool: e.g. 'Chrome'.
# - spec_name: name of a job specification.
# Override target field:
# - hostname: 'autogen:<executor_id>': name of the bot that executed a job,
#     or an empty string. e.g. 'autogen:swarm42-m4'.
# Value should be 'pending' or 'running'. Completed / canceled jobs should not
# send this metric.
_jobs_running = gae_ts_mon.BooleanMetric(
    'jobs/running', 'Presence metric for a running job.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
    ])

# Global metric. Metric fields:
# - project_id: e.g. 'chromium'.
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - pool: e.g. 'Chrome'.
# - spec_name: name of a job specification.
# - status: 'pending' or 'running'.
_jobs_active = gae_ts_mon.GaugeMetric(
    'jobs/active', 'Number of running, pending or otherwise active jobs.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status'),
    ])


# Global metric. Target field: hostname = 'autogen:<executor_id>' (bot id).
_executors_pool = gae_ts_mon.StringMetric(
    'executors/pool',
    'Pool name for a given job executor.',
    None)


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
_executors_status = gae_ts_mon.StringMetric(
    'executors/status',
    'Status of a job executor.',
    None)


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
# Note that 'running' will report data as long as the job is running,
# so it is best to restrict data to status == 'pending.'
_jobs_pending_durations = gae_ts_mon.NonCumulativeDistributionMetric(
    'jobs/pending_durations',
    'Pending times of active jobs, in seconds.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status'),
        gae_ts_mon.StringField('device_type'),
    ],
    bucketer=_bucketer)


# Global metric. Target fields:
# - hostname = 'autogen:<executor_id>' (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
# Note that 'running' will report data as long as the job is running,
# so it is best to restrict data to status == 'pending.'
_jobs_max_pending_duration = gae_ts_mon.FloatMetric(
    'jobs/max_pending_duration', 'Maximum pending seconds of pending jobs.', [
        gae_ts_mon.StringField('spec_name'),
        gae_ts_mon.StringField('project_id'),
        gae_ts_mon.StringField('subproject_id'),
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status'),
        gae_ts_mon.StringField('device_type'),
    ])


# Instance metric. Metric fields:
# - auth_method = one of 'luci_token', 'service_account', 'ip_whitelist'.
# - condition = depends on the auth method (e.g. email for 'service_account').
_bot_auth_successes = gae_ts_mon.CounterMetric(
    'swarming/bot_auth/success',
    'Number of successful bot authentication events', [
        gae_ts_mon.StringField('auth_method'),
        gae_ts_mon.StringField('condition'),
    ])

# Instance metric. Metric fields:
# - pool: e.g. 'skia'.
# - status: e.g. 'User canceled'.
# - http_status_code: e.g. 404.
_task_state_change_pubsub_notify_latencies = \
  gae_ts_mon.CumulativeDistributionMetric(
    'swarming/tasks/state_change_pubsub_notify_latencies',
    'Latency (in ms) of PubSub notification when backend receives task_update',
    [
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status'),
        gae_ts_mon.IntegerField('http_status_code')
    ],
    bucketer=_pubsub_bucketer,
)

# Instance metric. Measures the latency for Swarming to recognise a task
# death (KILLED or BOT_DIED) and commit the state to storage.
# Metric fields:
# - pool: e.g. 'skia'.
# - cron: e.g. True if dead task was initiated by cron job, False otherwise
_dead_task_detection_latencies = \
  gae_ts_mon.CumulativeDistributionMetric(
    'swarming/tasks/dead_task_detection_latencies',
    'Latency (in ms) of task scheduling request',
    [
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.BooleanField('cron'),
    ],
    bucketer=_detection_bucketer,
)

# Instance metric. Metric fields:
# - pool: e.g. 'skia'.
# - status: e.g. 'No resource available'.
_task_state_change_schedule_latencies = \
  gae_ts_mon.CumulativeDistributionMetric(
    'swarming/tasks/state_change_scheduling_latencies',
    'Latency (in ms) of task scheduling request',
    [
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status')
    ],
    bucketer=_scheduler_bucketer,
)

# Instance metric. Metric fields:
# - pool: e.g. 'skia'.
# - queue_count: number of queues scanned in parallel (up to 30).
_scheduler_scans = gae_ts_mon.CounterMetric(
    'swarming/scheduler/scans', 'Number of queue scans', [
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.IntegerField('queue_count'),
    ])

# Instance metric. Metric fields:
# - pool: e.g. 'skia'.
# - status: 'claimed', 'expired', etc.
_scheduler_visits = gae_ts_mon.CumulativeDistributionMetric(
    'swarming/scheduler/visits',
    'Distribution of TaskToRunShard visited per scan', [
        gae_ts_mon.StringField('pool'),
        gae_ts_mon.StringField('status'),
    ],
    bucketer=gae_ts_mon.FixedWidthBucketer(width=5))


### Private stuff.


def _pool_from_dimensions(dimensions):
  """Return a canonical string of flattened dimensions."""
  pairs = []
  for key, values in dimensions.items():
    if key in _IGNORED_DIMENSIONS:
      continue
    # Strip all the prefixes of other values. values is already sorted.
    for i, value in enumerate(values):
      if not any(v.startswith(value) for v in values[i+1:]):
        pairs.append(u'%s:%s' % (key, value))
  return u'|'.join(sorted(pairs))


def _set_jobs_metrics(payload):
  params = _ShardParams(payload)

  state_map = {
      task_result.State.RUNNING: 'running',
      task_result.State.PENDING: 'pending'
  }
  jobs_counts = defaultdict(lambda: 0)
  jobs_total = 0
  jobs_pending_distributions = defaultdict(
      lambda: gae_ts_mon.Distribution(_bucketer))
  jobs_max_pending_durations = defaultdict(
      lambda: 0.0)

  query_iter = task_result.get_result_summaries_query(
      None, None, 'created_ts', 'pending_running',
      None).iter(produce_cursors=True, start_cursor=params.cursor)

  while query_iter.has_next():
    runtime = (utils.utcnow() - params.start_time).total_seconds()
    if jobs_total >= _JOBS_PER_SHARD or runtime > _REQUEST_TIMEOUT_SEC:
      params.cursor = query_iter.cursor_after()
      params.task_count += 1
      utils.enqueue_task(
          '/internal/taskqueue/monitoring/tsmon/jobs',
          'tsmon',
          payload=params.json())
      params.task_count -= 1  # For accurate logging below.
      break

    params.count += 1
    jobs_total += 1
    summary = query_iter.next()
    status = state_map.get(summary.state, '')
    tags_dict = _tags_to_dict(summary.tags)
    fields = _extract_job_fields(tags_dict)
    target_fields = dict(_TARGET_FIELDS)
    if summary.bot_id:
      target_fields['hostname'] = 'autogen:' + summary.bot_id
    if summary.bot_id and status == 'running':
      _jobs_running.set(True, target_fields=target_fields, fields=fields)
    fields['status'] = status

    key = tuple(sorted(fields.items()))

    jobs_counts[key] += 1

    fields['device_type'] = tags_dict.get('device_type', '')
    key = tuple(sorted(fields.items()))

    pending_duration = summary.pending_now(utils.utcnow())
    if pending_duration is not None:
      jobs_pending_distributions[key].add(pending_duration.total_seconds())
      jobs_max_pending_durations[key] = max(
          jobs_max_pending_durations[key],
          pending_duration.total_seconds())

  logging.debug(
      '_set_jobs_metrics: task %d started at %s, processed %d jobs (%d total)',
      params.task_count, params.task_start, jobs_total, params.count)

  # Global counts are sharded by task_num and aggregated in queries.
  target_fields = dict(_TARGET_FIELDS)
  target_fields['task_num'] = params.task_count

  for key, count in jobs_counts.items():
    _jobs_active.set(count, target_fields=target_fields, fields=dict(key))

  for key, distribution in jobs_pending_distributions.items():
    _jobs_pending_durations.set(
        distribution, target_fields=target_fields, fields=dict(key))

  for key, val in jobs_max_pending_durations.items():
    _jobs_max_pending_duration.set(
        val, target_fields=target_fields, fields=dict(key))


def _set_executors_metrics(payload):
  params = _ShardParams(payload)
  query_iter = bot_management.BotInfo.query().iter(
      produce_cursors=True, start_cursor=params.cursor)

  executors_count = 0
  while query_iter.has_next():
    runtime = (utils.utcnow() - params.start_time).total_seconds()
    if (executors_count >= _EXECUTORS_PER_SHARD or
        runtime > _REQUEST_TIMEOUT_SEC):
      params.cursor = query_iter.cursor_after()
      params.task_count += 1
      utils.enqueue_task(
          '/internal/taskqueue/monitoring/tsmon/executors',
          'tsmon',
          payload=params.json())
      params.task_count -= 1  # For accurate logging below.
      break

    params.count += 1
    executors_count += 1
    bot_info = query_iter.next()
    status = 'ready'
    if bot_info.task_id:
      status = 'running'
    elif bot_info.quarantined:
      status = 'quarantined'
    elif bot_info.is_dead:
      status = 'dead'
    elif bot_info.state and bot_info.state.get('maintenance', False):
      status = 'maintenance'

    target_fields = dict(_TARGET_FIELDS)
    target_fields['hostname'] = 'autogen:' + bot_info.id

    _executors_status.set(status, target_fields=target_fields)
    _executors_pool.set(
        _pool_from_dimensions(bot_info.dimensions), target_fields=target_fields)

  logging.debug(
      '%s: task %d started at %s, processed %d bots (%d total)',
      '_set_executors_metrics', params.task_count, params.task_start,
       executors_count, params.count)


def _set_global_metrics():
  utils.enqueue_task('/internal/taskqueue/monitoring/tsmon/jobs', 'tsmon')
  utils.enqueue_task('/internal/taskqueue/monitoring/tsmon/executors', 'tsmon')


class _ShardParams(object):
  """Parameters for a chain of taskqueue tasks."""
  def __init__(self, payload):
    self.start_time = utils.utcnow()
    self.cursor = None
    self.task_start = self.start_time
    self.task_count = 0
    self.count = 0
    if not payload:
      return
    try:
      params = json.loads(payload)
      if params['cursor']:
        self.cursor = Cursor(urlsafe=params['cursor'])
      self.task_start = datetime.datetime.strptime(
          params['task_start'], utils.DATETIME_FORMAT)
      self.task_count = params['task_count']
      self.count = params['count']
    except (ValueError, KeyError) as e:
      logging.error('_ShardParams: bad JSON: %s: %s', payload, e)
      # Stop the task chain and let the request fail.
      raise

  def json(self):
    return utils.encode_to_json({
        'cursor': self.cursor.urlsafe() if self.cursor else None,
        'task_start': self.task_start,
        'task_count': self.task_count,
        'count': self.count,
    })


def _tags_to_dict(tags):
  """Converts list of string tags to dict.

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
  return tags_dict


def _extract_given_job_fields(tags, tag_names):
  """Extracts job metric fields given by tag name from TaskResultSummary.

  Args:
    tags: list of tags.
    tags_names: list of tag names to extract.
  """
  tags_dict = _tags_to_dict(tags)
  fields = {}
  for tag in tag_names:
    fields[tag] = tags_dict.get(tag, '')
  return fields


def _extract_job_fields(tags_dict):
  """Extracts common job's metric fields from TaskResultSummary.

  Args:
    tags_dict: tags dictionary.
  """
  spec_name = tags_dict.get('spec_name')
  if not spec_name:
    spec_name = tags_dict.get('buildername', '')
    if tags_dict.get('build_is_experimental') == 'true':
      spec_name += ':experimental'

  fields = {
      'project_id': tags_dict.get('project', ''),
      'subproject_id': tags_dict.get('subproject', ''),
      'pool': tags_dict.get('pool', ''),
      'spec_name': spec_name,
  }
  return fields


### Public API.

def on_task_requested(summary, deduped):
  """When a task is created."""
  fields = _extract_job_fields(_tags_to_dict(summary.tags))
  fields['deduped'] = deduped
  _jobs_requested.increment(fields=fields)


def on_task_completed(summary):
  """When a task is stopped from being processed."""
  fields = _extract_job_fields(_tags_to_dict(summary.tags))
  if summary.state == task_result.State.EXPIRED:
    fields['priority'] = summary.priority
    _tasks_expired.increment(fields=fields)
    return

  if summary.internal_failure:
    fields['result'] = 'infra-failure'
  elif summary.failure:
    fields['result'] = 'failure'
  else:
    fields['result'] = 'success'

  completed_fields = fields.copy()
  completed_fields['status'] = task_result.State.to_string(summary.state)
  _jobs_completed.increment(fields=completed_fields)
  if summary.duration is not None:
    _jobs_durations.add(summary.duration, fields=fields)


def on_task_expired(summary, task_to_run):
  """When a task slice is expired."""
  tags_dict = _tags_to_dict(summary.tags)
  fields = {'project_id': tags_dict.get('project', '')}

  # slice expiration delay
  _tasks_slice_expiration_delay.add(
      task_to_run.expiration_delay,
      fields=dict(fields, slice_index=task_to_run.task_slice_index))

  # task expiration delay
  if summary.expiration_delay:
    _tasks_expiration_delay.add(summary.expiration_delay, fields=fields)


def on_bot_auth_success(auth_method, condition):
  _bot_auth_successes.increment(fields={
      'auth_method': auth_method,
      'condition': condition,
  })


def set_global_metrics(kind, payload=None):
  if kind == 'jobs':
    _set_jobs_metrics(payload)
  elif kind == 'executors':
    _set_executors_metrics(payload)
  else:
    logging.error('set_global_metrics(kind=%s): unknown kind.', kind)


def on_task_status_change_pubsub_latency(tags, state, http_status_code,
                                         latency):
  fields = _extract_given_job_fields(tags, [
      'pool',
  ])
  fields['status'] = task_result.State.to_string(state)
  fields['http_status_code'] = http_status_code
  logging.debug('Incrementing ts_mon PubSub notification count with fields=%s',
                fields)
  _task_state_change_pubsub_notify_latencies.add(latency, fields=fields)


def on_task_status_change_scheduler_latency(summary):
  fields = _extract_given_job_fields(summary.tags, [
      'pool',
  ])
  fields['status'] = task_result.State.to_string(summary.state)
  latency = summary.pending_now(utils.utcnow())
  _task_state_change_schedule_latencies.add(round(latency.total_seconds() *
                                                  1000),
                                            fields=fields)


def on_dead_task_detection_latency(tags, latency, cron):
  fields = _extract_given_job_fields(tags, [
      'pool',
  ])
  fields['cron'] = cron
  _dead_task_detection_latencies.add(round(latency.total_seconds() * 1000),
                                     fields=fields)


def on_scheduler_scan(pool, queue_count):
  _scheduler_scans.increment(
      fields={
          'pool': pool,
          'queue_count': 30 if queue_count > 30 else queue_count,
      })


def on_scheduler_visits(pool, claimed, mismatch, stale, total, visited):
  def add(key, val):
    _scheduler_visits.add(val, fields={'pool': pool, 'status': key})

  add('claimed', claimed)
  add('mismatch', mismatch)
  add('stale', stale)
  add('total', total)
  add('visited', visited)


def initialize():
  # These metrics are the ones that are reset everything they are flushed.
  gae_ts_mon.register_global_metrics([
      _executors_pool,
      _executors_status,
      _jobs_active,
      _jobs_max_pending_duration,
      _jobs_pending_durations,
      _jobs_running,
  ])
  gae_ts_mon.register_global_metrics_callback('callback', _set_global_metrics)
