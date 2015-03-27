# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the mapreduces, which are used to do one-off mass updates on entities
and other manually triggered maintenance tasks.

Automatically triggered maintenance tasks should use a task queue on the backend
instead.
"""

import logging

from mapreduce import control

import config
import gcs


# Task queue name to run all map reduce jobs on.
MAPREDUCE_TASK_QUEUE = 'mapreduce-jobs'


# Registered mapreduce jobs, displayed on admin page.
MAPREDUCE_JOBS = {
  'find_missing_gs_files': {
    'name': 'Report missing GS files',
    'mapper_parameters': {
      'entity_kind': 'handlers.ContentEntry',
    },
  },
  'delete_broken_entries': {
    'name': 'Delete entries that do not have corresponding GS files',
    'mapper_parameters': {
      'entity_kind': 'handlers.ContentEntry',
    },
  },
}


def launch_job(job_id):
  """Launches a job given its key from MAPREDUCE_JOBS dict."""
  assert job_id in MAPREDUCE_JOBS, 'Unknown mapreduce job id %s' % job_id
  job_def = MAPREDUCE_JOBS[job_id].copy()
  job_def.setdefault('shard_count', 64)
  job_def.setdefault('queue_name', MAPREDUCE_TASK_QUEUE)
  job_def.setdefault(
      'reader_spec', 'mapreduce.input_readers.DatastoreInputReader')
  job_def.setdefault('handler_spec', 'mapreduce_jobs.' + job_id)
  return control.start_map(base_path='/internal/mapreduce', **job_def)


def is_good_content_entry(entry):
  """True if ContentEntry is not broken.

  ContentEntry is broken if it is in old format (before content namespace
  were sharded) or corresponding Google Storage file doesn't exist.
  """
  # New entries use GS file path as ids. File path is always <namespace>/<hash>.
  entry_id = entry.key.id()
  if '/' not in entry_id:
    return False
  # Content is inline, entity doesn't have GS file attached -> it is fine.
  if entry.content is not None:
    return True
  # Ensure GS file exists.
  return bool(gcs.get_file_info(config.settings().gs_bucket, entry_id))


### Actual mappers


def find_missing_gs_files(entry):
  """Mapper that takes ContentEntry and logs to output if GS file is missing."""
  if not is_good_content_entry(entry):
    logging.error('MR: found bad entry\n%s', entry.key.id())


def delete_broken_entries(entry):
  """Mapper that deletes ContentEntry entities that are broken."""
  if not is_good_content_entry(entry):
    # MR framework disables memcache on a context level. Explicitly request
    # to cleanup memcache, otherwise the rest of the isolate service will still
    # think that entity exists.
    entry.key.delete(use_memcache=True)
    logging.error('MR: deleted bad entry\n%s', entry.key.id())
