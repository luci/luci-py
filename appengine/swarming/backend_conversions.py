# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functions that convert internal to/from Backend API's protoc objects."""

import collections
import copy
import posixpath

from google.protobuf import json_format

from components import utils
from server import task_request

from proto.api.internal.bb import swarming_bb_pb2

# This is the path, relative to the swarming run dir, to the directory that
# contains the mounted swarming named caches. It will be prepended to paths of
# caches defined in swarmbucket configs.
_CACHE_DIR = 'cache'

# TODO(crbug/1236848): Replace 'assert's with raised exceptions.

def compute_task_request(run_task_req):
  # type: (backend_pb2.RunTaskRequest) -> Tuple[task_request.TaskRequest,
  #     Optional[task_request.SecretBytes], task_request.BuildToken]

  build_token = task_request.BuildToken(
      build_id=run_task_req.build_id,
      token=run_task_req.backend_token,
      buildbucket_host=run_task_req.buildbucket_host)

  # NOTE: secret_bytes cannot be passed via `-secret_bytes` in `command`
  # because tasks in swarming can view command details of other tasks.
  secret_bytes = None
  if run_task_req.secrets:
    secret_bytes = task_request.SecretBytes(
        secret_bytes=run_task_req.secrets.SerializeToString())

  backend_config = _ingest_backend_config(run_task_req.backend_config)
  slices = _compute_task_slices(
      run_task_req, backend_config, secret_bytes is not None)
  expiration_ms = sum([s.expiration_secs for s in slices]) * 1000000
  # The expiration_ts may be different from run_task_req.start_deadline
  # if the last slice's expiration_secs had to be extended to 60s
  now = utils.utcnow()
  tr = task_request.TaskRequest(
      created_ts=now,
      task_slices=slices,
      expiration_ts=utils.timestamp_to_datetime(
          utils.datetime_to_timestamp(now) + expiration_ms),
      realm=run_task_req.realm,
      name='bb-%d' % run_task_req.build_id,
      priority=backend_config.priority,
      bot_ping_tolerance_secs=backend_config.bot_ping_tolerance,
      service_account=backend_config.service_account,
      has_build_token=True)

  parent_id = backend_config.parent_run_id
  if parent_id:
    tr.parent_task_id = parent_id

  return tr, secret_bytes, build_token


def _ingest_backend_config(req_backend_config):
  # type: (struct_pb2.Struct) -> swarming_bb_pb2.SwarmingBackendConfig
  json_config = json_format.MessageToJson(req_backend_config)
  return json_format.Parse(json_config, swarming_bb_pb2.SwarmingBackendConfig)


def _compute_task_slices(run_task_req, backend_config, has_secret_bytes):
  # type: (backend_pb2.RunTaskRequest, swarming_bb_pb2.SwarmingBackendConfig,
  #     bool) -> Sequence[task_request.TaskSlice]

  # {expiration_secs: {'key1': [value1, ...], 'key2': [value1, ...]}
  dims_by_exp = collections.defaultdict(lambda: collections.defaultdict(list))

  for cache in run_task_req.caches:
    assert not cache.wait_for_warm_cache.nanos
    if cache.wait_for_warm_cache.seconds:
      dims_by_exp[cache.wait_for_warm_cache.seconds]['caches'].append(
          cache.name)

  for dim in run_task_req.dimensions:
    assert not dim.expiration.nanos
    dims_by_exp[dim.expiration.seconds][dim.key].append(dim.value)

  base_dims = dims_by_exp.pop(0, {})
  for key, values in base_dims.iteritems():
    values.sort()

  base_slice = task_request.TaskSlice(
      # In bb-on-swarming, `wait_for_capacity` is only used for the last slice
      # (base_slice) to give named caches some time to show up.
      wait_for_capacity=backend_config.wait_for_capacity,
      expiration_secs=int(run_task_req.start_deadline.seconds -
                          utils.time_time()),
      properties=task_request.TaskProperties(
          caches=[
              task_request.CacheEntry(
                  path=posixpath.join(_CACHE_DIR, cache.path), name=cache.name)
              for cache in run_task_req.caches
          ],
          dimensions_data=base_dims,
          execution_timeout_secs=run_task_req.execution_timeout.seconds,
          grace_period_secs=run_task_req.grace_period.seconds,
          command=_compute_command(
              run_task_req, backend_config.agent_binary_cipd_filename),
          has_secret_bytes=has_secret_bytes,
          cipd_input=task_request.CipdInput(packages=[
              task_request.CipdPackage(
                  package_name=backend_config.agent_binary_cipd_pkg,
                  version=backend_config.agent_binary_cipd_vers)
          ])),
  )

  if not dims_by_exp:
    return [base_slice]

  # Initialize task slices with base properties and computed expiration.
  last_exp = 0
  task_slices = []
  for expiration_secs in sorted(dims_by_exp):
    slice_exp_secs = expiration_secs - last_exp
    task_slices.append(
        task_request.TaskSlice(
            expiration_secs=slice_exp_secs,
            properties=copy.deepcopy(base_slice.properties),
        ))
    last_exp = expiration_secs

  # Add extra dimensions for all slices.
  extra_dims = collections.defaultdict(list)
  for i, (_exp,
          dims) in enumerate(sorted(dims_by_exp.iteritems(), reverse=True)):
    for key, values in dims.iteritems():
      extra_dims[key].extend(values)

    props = task_slices[-1 - i].properties
    for key, values in extra_dims.iteritems():
      props.dimensions.setdefault(key, []).extend(values)
      props.dimensions[key].sort()

  # Adjust expiration on base_slice and add it as the last slice.
  base_slice.expiration_secs = max(base_slice.expiration_secs - last_exp, 60)
  task_slices.append(base_slice)

  return task_slices


def _compute_command(run_task_req, agent_binary_name):
  # type: (backend_pb2.RunTaskRequest, str) -> Sequence[str]
  # TODO(crbug/1236848): Before the command is executed, `${SWARMING_TASK_ID}`
  # should be replaced by the actual task_id.
  args = [agent_binary_name] + run_task_req.agent_args[:]
  args.extend(['-cache-base', _CACHE_DIR, '-task-id', '${SWARMING_TASK_ID}'])
  return args
