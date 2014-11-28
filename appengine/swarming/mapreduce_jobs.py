# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Map reduce jobs to update the DB schemas or any other maintenance task."""

import logging
import os
import sys

# Map-reduce library expects 'mapreduce' package to be in sys.path.
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(ROOT_DIR, 'third_party'))

from mapreduce import control
from mapreduce import main


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


# Export mapreduce WSGI application as 'app' for *.yaml routes.
app = main.APP
