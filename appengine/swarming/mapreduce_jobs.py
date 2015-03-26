# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the mapreduces, which are used to do one-off mass updates on entities
and other manually triggered maintenance tasks.

Automatically triggered maintenance tasks should use a task queue on the backend
instead.
"""

import logging

from mapreduce import control


# Task queue name to run all map reduce jobs on.
MAPREDUCE_TASK_QUEUE = 'mapreduce-jobs'


# Registered mapreduce jobs, displayed on admin page.
MAPREDUCE_JOBS = {
  'dummy': {
    'name': 'Dummy task',
    'mapper_parameters': {
      'entity_kind': 'server.task_result.TaskResultSummary',
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


### Actual mappers


def dummy(_entry):
  # TODO(maruel): Do something, use:
  #   from mapreduce import operation
  #   yield operation.db.Put(entity)
  pass
