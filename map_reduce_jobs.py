# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys

# Map-reduce library expects 'mapreduce' package to be in sys.path.
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(ROOT_DIR, 'third_party'))

# pylint: disable=E0611,F0401
from mapreduce import control
from mapreduce import main

import config
import gcs


# Task queue name to run all map reduce jobs on.
MAP_REDUCE_TASK_QUEUE = 'map-reduce-jobs'


# All registered mapreduce jobs, will be displayed on admin page.
# All parameters are passed as is to mapreduce.control.start_map.
MAP_REDUCE_JOBS = {
  'find_missing_gs_files': {
    'name': 'Report missing GS files',
    'handler_spec': 'map_reduce_jobs.detect_missing_gs_file_mapper',
    'reader_spec': 'mapreduce.input_readers.DatastoreInputReader',
    'mapper_parameters': {
      'entity_kind': 'main.ContentEntry',
      'batch_size': 50,
    },
    'shard_count': 32,
    'queue_name': MAP_REDUCE_TASK_QUEUE,
  },
}


def launch_job(job_id):
  """Launches a job given its key from MAP_REDUCE_JOBS dict."""
  assert job_id in MAP_REDUCE_JOBS, 'Unknown mapreduce job id %s' % job_id
  job_def = MAP_REDUCE_JOBS[job_id]
  return control.start_map(base_path='/restricted/mapreduce', **job_def)


### Actual mappers


def detect_missing_gs_file_mapper(entry):
  """Mapper that takes ContentEntry and logs to output if GS file is missing."""
  # Content is inline and entity doesn't have GS file attached -> skip.
  if entry.content is not None:
    return
  # Check whether GS file exists.
  gs_bucket = config.settings().gs_bucket
  exists = bool(gcs.get_file_info(gs_bucket, entry.gs_filepath))
  if not exists:
    logging.warning('MR: missing GS file %s', entry.gs_filepath)


# Export mapreduce WSGI application as 'app'.
# Used in app.yaml and module-backend.yaml for mapreduce/* routes.
app = main.APP
