# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Converts between ndb database entities and pRPC swarming api objects.
"""

from collections import defaultdict
from datetime import datetime
import json
import logging

from google.protobuf.timestamp_pb2 import Timestamp

from components import utils
from proto.api_v2 import swarming_pb2
from server import task_request
from server import task_result
from server import task_pack


def _string_pairs_from_dict(dictionary):
  """Used for items like environment variables.
  Expects a dictionary with the form <string, string> and returns
  a list of swarming.StringPair. Will also sort the list by key.
  """
  return [
      swarming_pb2.StringPair(key=k, value=v)
      for k, v in sorted((dictionary or {}).items())
  ]


def _duplicate_string_pairs_from_dict(dictionary):
  """Expects a dictionary with the type of string, list[string]. It flattens
  dictionary into a list of StringPair.

  For example:
  a: [b, c]
  b: [c, d]

  would be converted to:

  [(a, b), (a, c), (b, c), (b, d)]
  """
  # For compatibility due to legacy swarming_rpcs.TaskProperties.dimensions.
  # TaskProperties dimensions are stored as a list of StringPair instead of
  # deduplicated StringListPair. Having dimensions in this form is legacy
  # behaviour from old protorpc api.
  out = []
  for k, values in (dictionary or {}).items():
    assert isinstance(values, (list, tuple)), dictionary
    out.extend(swarming_pb2.StringPair(key=k, value=v) for v in values)
  return out


def _string_list_pairs_from_dict(dictionary):
  # For key: values items like bot dimensions.
  return [
      swarming_pb2.StringListPair(key=k, value=v)
      for k, v in sorted((dictionary or {}).items())
  ]


def date(ts):
  """Converts datetime.timestamp into google.protobuf.timestamp_pb2.Timestamp

  Args:
    ts: datetime.timestamp or None

  Returns:
    google.protobuf.timestamp_pb2.Timestamp or None if ts is None
  """
  if ts is None:
    return None
  stamp = Timestamp()
  stamp.FromDatetime(ts)
  return stamp


def _state(state_dict):
  # type: (dict) -> str
  return json.dumps(state_dict or {}, sort_keys=True, separators=(',', ':'))


def bot_info_response(bot_info, deleted=False):
  """Converts a ndb BotInfo object into a BotInfoResponse for pRPC api

  Args:
    bot_info: bot_management.BotInfo
    deleted: True if this bot_info object was created from a BotInfo object
      due to earlier ones being deleted.

  Returns:
    swarming_pb2.BotInfo
  """
  return swarming_pb2.BotInfo(external_ip=bot_info.external_ip,
                              authenticated_as=bot_info.authenticated_as,
                              is_dead=bot_info.is_dead,
                              quarantined=bot_info.quarantined,
                              maintenance_msg=bot_info.maintenance_msg,
                              task_id=bot_info.task_id,
                              task_name=bot_info.task_name,
                              version=bot_info.version,
                              first_seen_ts=date(bot_info.first_seen_ts),
                              last_seen_ts=date(bot_info.last_seen_ts),
                              state=_state(bot_info.state),
                              bot_id=bot_info.id,
                              dimensions=_string_list_pairs_from_dict(
                                  bot_info.dimensions),
                              deleted=deleted)


def bots_response(bots, death_timeout, cursor):
  """Converts a list of bot_management.BotInfo ndb entities to
  protobuf objects for pRPC.

  Args:
    bots: List of bot_management.BotInfo ndb entities.
    death_timeout: How long to wait, in seconds, from last communication with
      bot to declare it dead.
    cursor: cursor from previous request.

  Returns:
    swarming_pb2.BotInfoListResponse with bots converted to
      swarming_pb2.BotInfo objects.
  """
  out = swarming_pb2.BotInfoListResponse(
      cursor=cursor,
      items=[bot_info_response(bot) for bot in bots],
      death_timeout=death_timeout,
  )
  out.now.GetCurrentTime()
  return out


def _bot_event_response(event):
  # type: (bot_management.BotEvent) -> swarming_pb2.BotEventResponse
  """Converts a ndb BotEvent entity to a BotEvent response of pRPC"""
  # must have a value because ts is indexed on
  assert event.ts
  return swarming_pb2.BotEventResponse(
      ts=date(event.ts),
      event_type=event.event_type,
      message=event.message,
      external_ip=event.external_ip,
      authenticated_as=event.authenticated_as,
      version=event.version,
      quarantined=event.quarantined,
      maintenance_msg=event.maintenance_msg,
      task_id=event.task_id,
      dimensions=_string_list_pairs_from_dict(event.dimensions),
      state=_state(event.state),
  )


def bot_events_response(items, cursor):
  # type: (List[bot_management.BotEvent], str) -> swarming_pb2.BotEventsResponse
  return swarming_pb2.BotEventsResponse(
      now=date(datetime.utcnow()),
      items=[_bot_event_response(event) for event in items],
      cursor=cursor)


def _cas_op_stats(stat):
  # type: (Optional[task_result.CASOperationStats]) ->
  #   Optional[swarming_pb2.CASOperationStats]
  if stat is None:
    return None
  return swarming_pb2.CASOperationStats(
      duration=stat.duration,
      initial_number_items=stat.initial_number_items,
      initial_size=stat.initial_size,
      items_cold=stat.items_cold,
      items_hot=stat.items_hot,
      num_items_cold=stat.num_items_cold,
      num_items_hot=stat.num_items_hot,
      total_bytes_items_hot=stat.total_bytes_items_hot,
      total_bytes_items_cold=stat.total_bytes_items_cold,
  )


def _op_stats(stat):
  # type: (Optional[task_result.OperationStats]) ->
  #   Optional[swarming_pb2.OperationStats]
  if stat is None:
    return None
  return swarming_pb2.OperationStats(duration=stat.duration)


def _perf_stats(stats):
  # type: (Optional[task_result.PerformanceStats]) ->
  #   Optional[swarming_pb2.PerformanceStats]
  if stats is None:
    return None
  return swarming_pb2.PerformanceStats(
      bot_overhead=stats.bot_overhead,
      isolated_upload=_cas_op_stats(stats.isolated_upload),
      isolated_download=_cas_op_stats(stats.isolated_download),
      package_installation=_op_stats(stats.package_installation),
      cache_trim=_op_stats(stats.cache_trim),
      named_caches_uninstall=_op_stats(stats.named_caches_uninstall),
      named_caches_install=_op_stats(stats.named_caches_install),
      cleanup=_op_stats(stats.cleanup),
  )


def _cas_reference(ref):
  # type: (Optional[task_result.CASReference]) ->
  #   Optional[swarming_pb2.CASReference]
  if ref is None:
    return None
  return swarming_pb2.CASReference(cas_instance=ref.cas_instance,
                                   digest=swarming_pb2.Digest(
                                       hash=ref.digest.hash,
                                       size_bytes=ref.digest.size_bytes))


def _cipd_package(package):
  # type: (Optional[task_request.CipdPackage]) ->
  #   Optional[swarming_pb2.CipdPackage]
  if package is None:
    return None
  return swarming_pb2.CipdPackage(
      package_name=package.package_name,
      version=package.version,
      path=package.path,
  )


def _cipd_pins(cipd_pins):
  # type: (Optional[task_request.CipdPins]) ->
  #   Optional[swarming_pb2.CipdPins]
  if cipd_pins is None:
    return None
  return swarming_pb2.CipdPins(
      client_package=_cipd_package(cipd_pins.client_package),
      packages=[_cipd_package(package) for package in cipd_pins.packages])


def _resultdb_info(info):
  # type: (Optional[task_request.ResultDBInfo]) ->
  #   Optional[swarming_pb2.ResultDBInfo]
  if info is None:
    return None
  return swarming_pb2.ResultDBInfo(hostname=info.hostname,
                                   invocation=info.invocation)


def task_result_response(result, include_performance_stats=True):
  """Converts a TaskRunResult or a TaskResultSummary to a TaskResultResponse.

  Arguments:
    result: TaskRunResult or TaskResultSummary ndb entity.
    include_performance_stats: If true, this will return a non-empty
      swarming_pb2.PerformanceStats for the performance_stats protobuf field.

  Returns:
    task_result.TaskResultResponse.
  """
  out = swarming_pb2.TaskResultResponse(
      bot_id=result.bot_id,
      bot_version=result.bot_version,
      bot_logs_cloud_project=result.bot_logs_cloud_project,
      deduped_from=result.deduped_from,
      duration=result.duration,
      exit_code=result.exit_code,
      failure=result.failure,
      internal_failure=result.internal_failure,
      state=result.state,
      task_id=result.task_id,
      name=result.name,
      current_task_slice=result.current_task_slice,
      completed_ts=date(result.completed_ts),
      bot_idle_since_ts=date(result.bot_idle_since_ts),
      abandoned_ts=date(result.abandoned_ts),
      modified_ts=date(result.modified_ts),
      started_ts=date(result.started_ts),
      created_ts=date(result.created_ts),
      bot_dimensions=_string_list_pairs_from_dict(result.bot_dimensions),
      children_task_ids=result.children_task_ids,
      server_versions=result.server_versions,
      performance_stats=_perf_stats(result.performance_stats) if
      include_performance_stats and result.performance_stats.is_valid else None,
      cas_output_root=_cas_reference(result.cas_output_root),
      missing_cas=[_cas_reference(ref) for ref in result.missing_cas],
      missing_cipd=[_cipd_package(package) for package in result.missing_cipd],
      cipd_pins=_cipd_pins(result.cipd_pins),
      resultdb_info=_resultdb_info(result.resultdb_info))

  if result.__class__ is task_result.TaskRunResult:
    if result.cost_usd is not None:
      out.costs_usd.extend([result.cost_usd])
    if result.task_id:
      out.run_id = result.task_id
  else:
    assert result.__class__ is task_result.TaskResultSummary, result
    k = result.run_result_key
    run_id = task_pack.pack_run_result_key(k) if k else None
    if run_id:
      out.run_id = run_id
    if result.user:
      out.user = result.user
    if result.tags:
      out.tags.extend(result.tags)
    if result.costs_usd:
      out.costs_usd.extend(result.costs_usd)
  return out


def task_list_response(items, cursor, include_performance_stats=True):
  # type(List[task_result.TaskResultResponse], str) -> TaskListResponse
  out = swarming_pb2.TaskListResponse()
  out.cursor = cursor or ''
  out.items.extend(
      [task_result_response(item, include_performance_stats) for item in items])
  out.now.GetCurrentTime()
  return out


def task_request_list_response(items, cursor):
  # type(List[task_request.TaskRequest], str) -> TasksRequestResponse
  out = swarming_pb2.TaskRequestsResponse()
  out.cursor = cursor or ''
  out.items.extend([task_request_response(item) for item in items])
  out.now.GetCurrentTime()
  return out


def _cache_entry(entry):
  # type: (Optional[task_request.CacheEntry]) ->
  #   Optional[swarming_pb2.CipdInput]
  if entry is None:
    return None
  return swarming_pb2.CacheEntry(
      name=entry.name,
      path=entry.path,
  )


def _cipd_input(cipd_input):
  # type: (Optional[task_request.CipdInput]) -> Optional[swarming_pb2.CipdInput]
  if cipd_input is None:
    return None
  return swarming_pb2.CipdInput(
      server=cipd_input.server,
      client_package=_cipd_package(cipd_input.client_package),
      packages=[_cipd_package(package) for package in cipd_input.packages],
  )


def _task_properties(props):
  # type: Optional[task_request.TaskProperties]) ->
  #   Optional[swarming_pb2.TaskProperties]
  if not props:
    return None
  return swarming_pb2.TaskProperties(
      caches=[_cache_entry(entry) for entry in props.caches],
      cas_input_root=_cas_reference(props.cas_input_root),
      containment=swarming_pb2.Containment(
          containment_type=props.containment.containment_type)
      if props.containment else None,
      cipd_input=_cipd_input(props.cipd_input),
      env_prefixes=_string_list_pairs_from_dict(props.env_prefixes),
      env=[
          swarming_pb2.StringPair(key=k, value=v) for k, v in props.env.items()
      ] if props.env else None,
      dimensions=_duplicate_string_pairs_from_dict(props.dimensions),
      command=props.command,
      outputs=props.outputs,
      idempotent=props.idempotent,
      io_timeout_secs=props.io_timeout_secs,
      secret_bytes='<REDACTED>' if props.has_secret_bytes else None,
      execution_timeout_secs=props.execution_timeout_secs,
      grace_period_secs=props.grace_period_secs,
  )


def _task_slice(task_slice):
  # type: (Optional[task_request.TaskSlice]) -> Optional[swarming_pb2.TaskSlice]
  if task_slice is None:
    return None
  return swarming_pb2.TaskSlice(
      properties=_task_properties(task_slice.properties),
      expiration_secs=task_slice.expiration_secs,
      wait_for_capacity=task_slice.wait_for_capacity,
  )


def _resultdb_cfg(resultdb_cfg):
  # type: (Optional[task_request.ResultDBCfg]) ->
  #   Optional[swarming_pb2.ResultDBCfg]
  if resultdb_cfg is None:
    return None
  return swarming_pb2.ResultDBCfg(enable=resultdb_cfg.enable)


def task_request_response(request):
  # type: (task_request.TaskRequest) -> swarming_pb2.TaskRequestResponse
  """Converts a TaskRequest ndb entity into a TaskRequestResponse.
  """
  slices = [
      _task_slice(request.task_slice(i)) for i in range(request.num_task_slices)
  ]
  return swarming_pb2.TaskRequestResponse(
      created_ts=date(request.created_ts),
      properties=slices[0].properties if slices else None,
      resultdb=_resultdb_cfg(request.resultdb),
      tags=request.tags,
      user=request.user,
      authenticated=request.authenticated.to_bytes(),
      service_account=request.service_account,
      realm=request.realm,
      expiration_secs=request.expiration_secs,
      name=request.name,
      task_id=request.task_id,
      parent_task_id=request.parent_task_id,
      priority=request.priority,
      pubsub_topic=request.pubsub_topic,
      pubsub_userdata=request.pubsub_userdata,
      bot_ping_tolerance_secs=request.bot_ping_tolerance_secs,
      task_slices=slices,
  )


def _resultdb_from_rpc(request):
  # type: (swarming_pb2.NewTaskRequest) -> task_request.ResultDBCfg
  return task_request.ResultDBCfg(enable=request.resultdb.enable)


def _cipd_package_from_rpc(package):
  # type: (swarming_pb2.CipdPackage) -> task_request.CipdPackage
  return task_request.CipdPackage(
      # For an ndb.Model, setting a kwarg value to None is the same as leaving
      # it unset. Prefer that behaviour than setting the default empty value.
      # The idea here is to mimic the protorpc implementation which will
      # avoid setting a value if a given attribute is Falsy.
      package_name=package.package_name or None,
      version=package.version or None,
      path=package.path or None,
  )


def _digest_from_rpc(digest):
  # type: (swarming_pb2.Digest) -> task_request.Digest
  return task_request.Digest(
      hash=digest.hash or None,
      size_bytes=digest.size_bytes or None,
  )


def _cache_entry_from_rpc(cache_entry):
  # type: (swarming_pb2.CacheEntry) -> task_request.CacheEntry
  return task_request.CacheEntry(
      name=cache_entry.name or None,
      path=cache_entry.path or None,
  )


def _task_properties_from_rpc(props):
  # type: (swarming_pb2.TaskProperties) ->
  #   tuple(task_request.TaskProperties, task_request.SecretBytes)
  cipd_input = None
  if props.HasField('cipd_input'):
    client_package = None
    if props.cipd_input.HasField('client_package'):
      client_package = _cipd_package_from_rpc(props.cipd_input.client_package)
    cipd_input = task_request.CipdInput(
        client_package=client_package,
        packages=[
            _cipd_package_from_rpc(pack)
            for pack in (props.cipd_input.packages or [])
        ])

  # Default value for swarming_pb2.ContainmentType is NOT_SPECIFIED which is 0.
  containment = task_request.Containment(
      containment_type=int(props.containment.containment_type))

  cas_input_root = None
  if props.HasField('cas_input_root'):
    if (props.cas_input_root.HasField('digest')
        and props.cas_input_root.HasField('cas_instance')):
      digest = _digest_from_rpc(props.cas_input_root.digest)
      cas_input_root = task_request.CASReference(
          cas_instance=props.cas_input_root.cas_instance,
          digest=digest,
      )

  secret_bytes = None
  # if unset, this will be empty str
  if props.secret_bytes:
    secret_bytes = task_request.SecretBytes(secret_bytes=props.secret_bytes)

  if len(set(i.key for i in props.env)) != len(props.env):
    raise ValueError('same environment variable key cannot be specified twice')
  if len(set(i.key for i in props.env_prefixes)) != len(props.env_prefixes):
    raise ValueError('same environment prefix key cannot be specified twice')

  dims = defaultdict(lambda: [])
  for i in props.dimensions:
    dims[i.key].append(i.value)

  caches = [_cache_entry_from_rpc(c) for c in props.caches]
  env = {i.key: i.value for i in props.env}
  env_prefixes = {i.key: list(i.value) for i in props.env_prefixes}

  out = task_request.TaskProperties(
      caches=caches,
      # TaskProperties ndb entity expects a list so we must convert the proto3
      # collection to a list.
      command=list(props.command) or [],
      relative_cwd=props.relative_cwd,
      inputs_ref=None,
      cas_input_root=cas_input_root,
      cipd_input=cipd_input,
      dimensions_data=dict(dims),
      env=env,
      env_prefixes=env_prefixes,
      execution_timeout_secs=props.execution_timeout_secs,
      grace_period_secs=props.grace_period_secs,
      io_timeout_secs=props.io_timeout_secs,
      idempotent=props.idempotent,
      outputs=list(props.outputs),
      has_secret_bytes=secret_bytes is not None,
      containment=containment,
  )
  return out, secret_bytes


def _task_slice_from_rpc(task_slice):
  # type: (swarming_pb2.TaskSlice) ->
  #   tuple(task_request.TaskSlice, task_request.SecretBytes)
  props, secret_bytes = _task_properties_from_rpc(task_slice.properties)
  out = task_request.TaskSlice(
      expiration_secs=task_slice.expiration_secs,
      wait_for_capacity=task_slice.wait_for_capacity,
      properties=props,
  )
  return out, secret_bytes


def new_task_request_from_rpc(request):
  # type: (swarming_pb2.NewTaskRequest) ->
  #   tuple(task_request.TaskRequest,
  #         task_request.SecretBytes,
  #         task_request.TemplateApplyEnum)
  """Converts swarming_pb2.NewTaskRequest object to the ndb entity,
  task_request.TaskRequest required to schedule a new task.

  Args:
    request: swarming_pb2.NewTaskRequest

  Returns:
    task_request.TaskRequest

  Raises:
    ValueError when the task request is invalid.
  """
  if request.task_slices and request.HasField('properties'):
    raise ValueError('Specify one of properties or task_slices, not both')

  # TODO(https://crbug.com/1422082) This code path will likely never occur.
  if request.HasField('properties'):
    logging.info('Properties is still used')
    if not request.expiration_secs:
      raise ValueError('missing expiration_secs')
    props, secret_bytes = _task_properties_from_rpc(request.properties)
    slices = [
        task_request.TaskSlice(properties=props,
                               expiration_secs=request.expiration_secs),
    ]
  elif request.task_slices:
    if request.expiration_secs:
      raise ValueError(
          'When using task_slices, do not specify a global expiration_secs')
    secret_bytes = None
    slices = []
    for t in (request.task_slices or []):
      sl, se = _task_slice_from_rpc(t)
      slices.append(sl)
      if se:
        if secret_bytes and se != secret_bytes:
          raise ValueError(
              'When using secret_bytes multiple times, all values must match')
        secret_bytes = se
  else:
    raise ValueError('Specify one of properties or task_slices')

  pttf = swarming_pb2.NewTaskRequest.PoolTaskTemplateField
  template_apply = {
      pttf.AUTO: task_request.TEMPLATE_AUTO,
      pttf.CANARY_PREFER: task_request.TEMPLATE_CANARY_PREFER,
      pttf.CANARY_NEVER: task_request.TEMPLATE_CANARY_NEVER,
      pttf.SKIP: task_request.TEMPLATE_SKIP,
  }[request.pool_task_template]

  out = task_request.TaskRequest(
      name=request.name,
      created_ts=utils.utcnow(),
      task_slices=slices,
      # 'tags' is now generated from manual_tags plus automatic tags.
      manual_tags=list(request.tags),
      user=request.user,
      priority=request.priority,
      realm=request.realm,
      service_account=request.service_account,
      # prefer to unset this if protobuf default value is used.
      # ndb has its own default of 1200.
      bot_ping_tolerance_secs=request.bot_ping_tolerance_secs
      or task_request.DEFAULT_BOT_PING_TOLERANCE,
      resultdb=_resultdb_from_rpc(request),
      has_build_token=False,
      scheduling_algorithm=None,
      rbe_instance=None,
      txn_uuid=None)

  return out, secret_bytes, template_apply
