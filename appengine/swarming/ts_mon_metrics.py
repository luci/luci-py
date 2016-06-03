# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Timeseries metrics."""

from collections import defaultdict
import itertools

from google.appengine.ext import ndb

from components import utils
import gae_ts_mon

from server import bot_management
from server import task_result

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
# - executor_id: name of the bot that executed a job, e.g. 'swarm42-m4'
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# - result: one of 'success', 'failure', or 'infra-failure'.
jobs_completed = gae_ts_mon.CounterMetric(
    'jobs/completed',
    description='Number of completed jobs.')


jobs_durations = gae_ts_mon.CumulativeDistributionMetric(
    'jobs/durations', bucketer=_bucketer,
    description='Cycle times of completed jobs, in seconds.')


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
# - executor_id: name of the bot that executes a job, or empty string.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# Value should be 'pending' or 'running'. Completed / canceled jobs should not
# send this metric.
jobs_status = gae_ts_mon.StringMetric(
    'jobs/status',
    description='Status of an active job.')

# Global metric. Metric fields:
# - project_id: e.g. 'chromium'
# - subproject_id: e.g. 'blink'. Set to empty string if not used.
# - spec_name: name of a job specification, e.g. '<master>:<builder>:<test>'
#     for buildbot jobs.
# - status: 'pending' or 'running'.
jobs_active = gae_ts_mon.GaugeMetric(
    'jobs/active',
    description='Number of running, pending or otherwise active jobs.')


# Global metric. Metric fields: executor_id (bot id).
executors_pool = gae_ts_mon.StringMetric(
    'executors/pool',
    description='Pool name for a given job executor.')


# Global metric. Metric fields:
# - executor_id (bot id).
# Status value must be 'ready', 'running', or anything else, possibly
# swarming-specific, when it cannot run a job. E.g. 'quarantined' or
# 'dead'.
executors_status = gae_ts_mon.StringMetric(
    'executors/status',
    description=('Status of a job executor.'))


def pool_from_dimensions(dimensions):
  """Return a canonical string of flattened dimensions.

  Exclude the 'id' dimension, as it would put each bot in its own pool.
  """
  iterables = (map(lambda x: '%s:%s' % (key, x), values)
               for key, values in dimensions.iteritems()
               if key != 'id')
  return '|'.join(sorted(itertools.chain(*iterables)))


def extract_job_fields(tags, bot_id=None):
  """Extracts common job's metric fields from TaskResultSummary.

  Args:
    tags (list of str): list of 'key:value' strings.
    bot_id (str or None): when present, adds 'executor_id' as a field.
      Note, that a metric always has to have the same set of fields. So
      bot_id must always be present or absent for each particular metric.
  """
  tags_dict = {}
  for tag in tags:
    try:
      key, value = tag.split(':', 1)
      tags_dict[key] = value
    except ValueError:
      pass

  fields = {
      'project_id': tags_dict.get('project', ''),
      'subproject_id': tags_dict.get('subproject', ''),
      'spec_name': '%s:%s:%s' % (tags_dict.get('master', ''),
                                 tags_dict.get('buildername', ''),
                                 tags_dict.get('name', '')),
  }
  if bot_id is not None:
    fields.update({'executor_id': bot_id})
  return fields


def update_jobs_completed_metrics(task_result_summary, bot_id):
  fields = extract_job_fields(task_result_summary.tags, bot_id)
  if task_result_summary.internal_failure:
    fields['result'] = 'infra-failure'
  elif task_result_summary.failure:
    fields['result'] = 'failure'
  else:
    fields['result'] = 'success'
  jobs_completed.increment(fields=fields)
  jobs_durations.add(task_result_summary.duration, fields=fields)


@ndb.tasklet
def _set_jobs_metrics():
  state_map = {task_result.State.RUNNING: 'running',
               task_result.State.PENDING: 'pending'}
  query_iter = task_result.get_result_summaries_query(
      None, None, 'created_ts', 'pending_running', None).iter()
  jobs_counts = defaultdict(lambda: 0)
  while (yield query_iter.has_next_async()):
    summary = query_iter.next()
    status = state_map.get(summary.state, '')
    fields = extract_job_fields(summary.tags, summary.bot_id or '')
    if summary.bot_id:
      jobs_status.set(status, target_fields=TARGET_FIELDS, fields=fields)
    fields.pop('executor_id')
    fields['status'] = status
    jobs_counts[tuple(sorted(fields.iteritems()))] += 1

  for key, count in jobs_counts.iteritems():
    jobs_active.set(count, target_fields=TARGET_FIELDS, fields=dict(key))


@ndb.tasklet
def _set_executors_metrics(now):
  query_iter = bot_management.BotInfo.query().iter()
  while (yield query_iter.has_next_async()):
    bot_info = query_iter.next()
    status = 'ready'
    if bot_info.task_id:
      status = 'running'
    elif bot_info.quarantined:
      status = 'quarantined'
    elif bot_info.is_dead(now):
      status = 'dead'

    executors_status.set(
        status, target_fields=TARGET_FIELDS,
        fields={'executor_id': bot_info.id})
    executors_pool.set(
        pool_from_dimensions(bot_info.dimensions),
        target_fields=TARGET_FIELDS,
        fields={'executor_id': bot_info.id})


@ndb.tasklet
def _set_global_metrics_async(now):
  yield _set_executors_metrics(now), _set_jobs_metrics()


def _set_global_metrics(now=None):
  if now is None:
    now = utils.utcnow()
  _set_global_metrics_async(now).get_result()


def initialize():
  gae_ts_mon.register_global_metrics(
      [jobs_status, executors_pool, executors_status])
  gae_ts_mon.register_global_metrics_callback('callback', _set_global_metrics)
