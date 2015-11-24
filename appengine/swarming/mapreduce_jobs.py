# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the mapreduces, which are used to do one-off mass updates on entities
and other manually triggered maintenance tasks.

Automatically triggered maintenance tasks should use a task queue on the backend
instead.
"""

import datetime
import logging

from google.appengine.ext import ndb

from mapreduce import control
from mapreduce import operation

from components import utils

from server import task_pack
from server import task_result  # Needed for entity.get()


# Task queue name to run all map reduce jobs on.
MAPREDUCE_TASK_QUEUE = 'mapreduce-jobs'


# Registered mapreduce jobs, displayed on admin page.
MAPREDUCE_JOBS = {
  'backfill_tags': {
    'name': 'Backfill tags',
    'mapper_parameters': {
      'entity_kind': 'server.task_result.TaskResultSummary',
    },
  },
  'fix_tags': {
    'name': 'fix_tags',
    'mapper_parameters': {
      'entity_kind': 'server.task_result.TaskResultSummary',
    },
  },
  'delete_old': {
    'name': 'delete_old',
    'mapper_parameters': {
      'entity_kind': 'server.task_request.TaskRequest',
    },
  },
}


def launch_job(job_id):
  """Launches a job given its key from MAPREDUCE_JOBS dict."""
  assert job_id in MAPREDUCE_JOBS, 'Unknown mapreduce job id %s' % job_id
  job_def = MAPREDUCE_JOBS[job_id].copy()
  # 256 helps getting things done faster but it is very easy to burn thousands
  # of $ within a few hours. Don't forget to update queue.yaml accordingly.
  job_def.setdefault('shard_count', 128)
  job_def.setdefault('queue_name', MAPREDUCE_TASK_QUEUE)
  job_def.setdefault(
      'reader_spec', 'mapreduce.input_readers.DatastoreInputReader')
  job_def.setdefault('handler_spec', 'mapreduce_jobs.' + job_id)
  return control.start_map(base_path='/internal/mapreduce', **job_def)


### Actual mappers


OLD_TASKS_CUTOFF = utils.utcnow() - datetime.timedelta(hours=12)


def backfill_tags(entity):
  # Already handled?
  if entity.tags:
    return

  # TaskRequest is immutable, can be fetched outside the transaction.
  task_request = entity.request_key.get(use_cache=False, use_memcache=False)
  if not task_request or not task_request.tags:
    return

  # Fast path for old entries: do not use transaction, assumes old entities are
  # not being concurrently modified outside of this job.
  if entity.created_ts and entity.created_ts < OLD_TASKS_CUTOFF:
    entity.tags = task_request.tags
    yield operation.db.Put(entity)
    return

  # For recent entries be careful and use transaction.
  def fix_task_result_summary():
    task_result_summary = entity.key.get()
    if task_result_summary and not task_result_summary.tags:
      task_result_summary.tags = task_request.tags
      task_result_summary.put()

  ndb.transaction(fix_task_result_summary, use_cache=False, use_memcache=False)


def fix_tags(entity):
  """Backfills missing tags and fix the ones with an invalid value."""
  request = entity.request_key.get(use_cache=False, use_memcache=False)
  # Compare the two lists of tags.
  if entity.tags != request.tags:
    entity.tags = request.tags
    logging.info('Fixed %s', entity.task_id)
    yield operation.db.Put(entity)


def delete_old(entity):
  key_to_delete = None
  if entity.key.parent():
    # It is a TaskRequestShard, it is very old.
    key_to_delete = entity.key.parent()
  elif not task_pack.request_key_to_result_summary_key(entity.key).get(
      use_cache=False, use_memcache=False):
    # There's a TaskRequest without TaskResultSummary, delete it.
    key_to_delete = entity.key

  if key_to_delete:
    logging.info('Deleting %s: %s', entity.task_id, key_to_delete)
    total = 1
    qo = ndb.QueryOptions(keys_only=True)
    for k in ndb.Query(default_options=qo, ancestor=key_to_delete):
      yield operation.db.Delete(k)
      total += 1
    yield operation.db.Delete(key_to_delete)
    logging.info('Deleted %d entities', total)
