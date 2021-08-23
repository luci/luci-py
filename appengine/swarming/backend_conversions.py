# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functions that convert internal to/from Backend API's protoc objects."""

import collections
import copy
import posixpath

from components import utils
from server import task_request

# This is the path, relative to the swarming run dir, to the directory that
# contains the mounted swarming named caches. It will be prepended to paths of
# caches defined in swarmbucket configs.
_CACHE_DIR = 'cache'

# TODO(crbug/1236848): Replace 'assert's with raised exceptions.
# TODO(crbug/1236848): Validate backend_config.fields have the expected
# value types.


def compute_task_request(run_task_req):
  # type: (backend_pb2.RunTaskRequest) ->
  #     Tuple[task_request.TaskRequest, Optional[task_request.SecretBytes]]

  # NOTE: secret_bytes cannot be passed via `-secret_bytes` in `command`
  # because tasks in swarming can view command details of other tasks.
  secret_bytes = None
  if run_task_req.secrets:
    secret_bytes = task_request.SecretBytes(
        secret_bytes=run_task_req.secrets.SerializeToString())

  slices = _compute_task_slices(run_task_req, secret_bytes is not None)
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
      priority=int(run_task_req.backend_config.fields['priority'].number_value),
      bot_ping_tolerance_secs=int(run_task_req.backend_config
                                  .fields['bot_ping_tolerance'].number_value),
      service_account=run_task_req.backend_config.fields['service_account']
      .string_value,
      user=run_task_req.backend_config.fields['user'].string_value)

  parent_id = run_task_req.backend_config.fields['parent_run_id'].string_value
  if parent_id:
    tr.parent_task_id = parent_id
  # TODO(crbug/1236848): Create a task_request.BuildToken and return here.
  return tr, secret_bytes


def _compute_task_slices(run_task_req, has_secret_bytes):
  # type: (backend_pb2.RunTaskRequest, bool)
  #   -> Sequence[task_request.TaskSlice]

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
      wait_for_capacity=bool(
          run_task_req.backend_config.fields['wait_for_capacity']),
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
          command=_compute_command(run_task_req),
          has_secret_bytes=has_secret_bytes,
          cipd_input=task_request.CipdInput(packages=[
              task_request.CipdPackage(
                  package_name=run_task_req.backend_config
                  .fields['agent_binary_cipd_pkg'].string_value,
                  version=run_task_req.backend_config
                  .fields['agent_binary_cipd_vers'].string_value)
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


def _compute_command(run_task_req):
  # type: (backend_pb2.RunTaskRequest) -> Sequence[str]
  # TODO(crbug/1236848): Before the command is executed, `${SWARMING_TASK_ID}`
  # should be replaced by the actual task_id.
  args = [
      run_task_req.backend_config.fields['agent_binary_cipd_filename']
      .string_value
  ] + run_task_req.agent_args[:]
  args.extend(['-cache-base', _CACHE_DIR, '-task-id', '${SWARMING_TASK_ID}'])
  return args
