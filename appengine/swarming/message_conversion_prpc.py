# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Converts database entities to and from swarming api prpc objects to ndb
database objects.
"""

import json
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
import proto.api_v2.swarming_pb2 as swarming_pb2

from server import task_pack
from server import task_result
from server import task_request


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
  """
  if ts is None:
    return None
  stamp = Timestamp()
  stamp.FromDatetime(ts)
  return stamp


def _state(state_dict):
  return json.dumps(state_dict or {}, sort_keys=True, separators=(',', ':'))


def bot_info_to_proto(bot_info, deleted=False):
  """Converts a ndb BotInfo object into a BotInfoResponse for pRPC api
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


def _bot_event_response(event):
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
  return swarming_pb2.BotEventsResponse(
      now=date(datetime.utcnow()),
      items=[_bot_event_response(event) for event in items],
      cursor=cursor)


def _cas_op_stats(stat):
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
  if stat is None:
    return None
  return swarming_pb2.OperationStats(duration=stat.duration)


def _perf_stats(stats):
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
  if ref is None:
    return None
  return swarming_pb2.CASReference(cas_instance=ref.cas_instance,
                                   digest=swarming_pb2.Digest(
                                       hash=ref.digest.hash,
                                       size_bytes=ref.digest.size_bytes))


def _cipd_package(package):
  if package is None:
    return None
  return swarming_pb2.CipdPackage(
      package_name=package.package_name,
      version=package.version,
      path=package.path,
  )


def _cipd_pins(cipd_pins):
  if cipd_pins is None:
    return None
  return swarming_pb2.CipdPins(
      client_package=_cipd_package(cipd_pins.client_package),
      packages=[_cipd_package(package) for package in cipd_pins.packages])


def _resultdb_info(resultdb_info):
  if resultdb_info is None:
    return None
  return swarming_pb2.ResultDBInfo(
      hostname=resultdb_info.hostname,
      invocation=resultdb_info.invocation,
  )


def task_result_response(result, include_performance_stats=True):
  """Converts a TaskRunResult or a TaskResultSummary to a TaskResultResponse.

  Arguments:
    result: TaskRunResult or TaskResultSummary ndb entity.
    include_performance_stats: If true, this will return a non-empty
      swarming_pb2.PerformanceStats for the performance_stats protobuf field.
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
      performance_stats=_perf_stats(result.performance_stats)
      if include_performance_stats else None,
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
    out.user = result.user
    out.tags.extend(result.tags)
  return out


def bot_tasks_response(items, cursor):
  out = swarming_pb2.TaskListResponse()
  out.cursor = cursor or ''
  out.items.extend([task_result_response(item) for item in items])
  out.now.GetCurrentTime()
  return out
