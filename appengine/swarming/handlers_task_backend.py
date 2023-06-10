# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""This module defines Swarming Server frontend pRPC handlers."""

import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from components import auth

import api_helpers
import backend_conversions
import handlers_exceptions
import prpc_helpers

from bb.go.chromium.org.luci.buildbucket.proto import backend_prpc_pb2
from bb.go.chromium.org.luci.buildbucket.proto import backend_pb2
from bb.go.chromium.org.luci.buildbucket.proto import common_pb2
from bb.go.chromium.org.luci.buildbucket.proto import task_pb2

from server import acl
from server import bot_management
from server import realms
from server import task_pack
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_result

_FETCH_TASKS_LIMIT = 1000
_CANCEL_TASKS_LIMIT = 500


class TaskBackendAPIService(object):
  """Service implements the pRPC service in backend.proto."""

  DESCRIPTION = backend_prpc_pb2.TaskBackendServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_use_task_backend, log_identity=True)
  def RunTask(self, request, _context):
    # type: (backend_pb2.RunTaskRequest, context.ServicerContext)
    #     -> backend_pb2.RunTaskResponse

    api_helpers.validate_backend_configs(
        [backend_conversions.ingest_backend_config(request.backend_config)])
    tr, secret_bytes, build_token = backend_conversions.compute_task_request(
        request)

    api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)

    def _schedule_request():
      try:
        return task_scheduler.schedule_request(tr,
                                               secret_bytes=secret_bytes,
                                               build_token=build_token)
      except (TypeError, ValueError) as e:
        raise handlers_exceptions.BadRequestException(e.message)

    result, is_deduped = api_helpers.cache_request('backend_run_task',
                                                   request.request_id,
                                                   _schedule_request)
    if is_deduped:
      logging.info('Reusing task %s with uuid %s', result.task_id,
                   request.request_id)
    hostname = app_identity.get_default_version_hostname()
    task = task_pb2.Task(id=task_pb2.TaskID(id=result.task_id,
                                            target=request.target),
                         link="https://%s/task?id=%s&o=true&w=true" %
                         (hostname, result.task_id))
    backend_conversions.convert_task_state_to_status(
      result.state, result.failure, task)
    return backend_pb2.RunTaskResponse(task=task)

  @prpc_helpers.method
  @auth.require(acl.can_use_task_backend, log_identity=True)
  def CancelTasks(self, request, _context):
    # type: (backend_pb2.CancelTasksRequest, context.ServicerContext)
    #     -> backend_pb2.CancelTasksResponse
    task_ids = [task_id.id for task_id in request.task_ids]
    if len(task_ids) > _CANCEL_TASKS_LIMIT:
      raise handlers_exceptions.BadRequestException(
          'Requesting %d tasks for cancellation when the allowed max is %d.' %
          (len(task_ids), _CANCEL_TASKS_LIMIT))

    request_keys, result_keys = zip(*[
        task_pack.get_request_and_result_keys(task_id) for task_id in task_ids
    ])

    pools = set()
    for tr in ndb.get_multi(request_keys):
      if tr:
        pools.update(set(bot_management.get_pools_from_dimensions_flat(
            tr.tags)))
    realms.check_tasks_cancel_acl(pools)

    query = task_result.TaskResultSummary.query().filter(
        task_result.TaskResultSummary.key.IN(result_keys))

    query = task_result.filter_query(task_result.TaskResultSummary,
                                     query,
                                     start=None,
                                     end=None,
                                     sort='created_ts',
                                     state='pending_running')

    task_scheduler.cancel_tasks(len(task_ids), query)

    # CancelTasksResponse should return ALL tasks in `request.task_ids`
    # not just the tasks that are actually getting cancelled.
    task_results = task_result.fetch_task_results(task_ids)

    return backend_pb2.CancelTasksResponse(
        tasks=backend_conversions.convert_results_to_tasks(
            task_results, task_ids))

  @prpc_helpers.method
  @auth.require(acl.can_use_task_backend, log_identity=True)
  def FetchTasks(self, request, _context):
    # type: (backend_pb2.FetchTasksRequest, context.ServicerContext)
    #     -> backend_pb2.FetchTaskResponse
    requested_task_ids = [task_id.id for task_id in request.task_ids]
    if len(requested_task_ids) > _FETCH_TASKS_LIMIT:
      raise handlers_exceptions.BadRequestException(
          'Requesting %d tasks when the allowed max is %d.' %
          (len(requested_task_ids), _FETCH_TASKS_LIMIT))

    request_keys = [
        task_pack.get_request_and_result_keys(task_id)[0]
        for task_id in requested_task_ids
    ]
    pools = []
    for tr in ndb.get_multi(request_keys):
      if tr:
        pools += bot_management.get_pools_from_dimensions_flat(tr.tags)
    realms.check_tasks_list_acl(pools)

    task_results = task_result.fetch_task_results(requested_task_ids)

    return backend_pb2.FetchTasksResponse(
        tasks=backend_conversions.convert_results_to_tasks(
            task_results, requested_task_ids))

  @prpc_helpers.method
  @auth.require(acl.can_use_task_backend, log_identity=True)
  def ValidateConfigs(self, request, _context):
    # type: (backend_pb2.ValidateConfigsRequest, context.ServicerContext)
    #     -> backend_pb2.ValidateConfigsResponse

    configs = [
        backend_conversions.ingest_backend_config(config.config_json)
        for config in request.configs
    ]

    errors = api_helpers.validate_backend_configs(configs)

    return backend_pb2.ValidateConfigsResponse(config_errors=[
        backend_pb2.ValidateConfigsResponse.ErrorDetail(index=i, error=error)
        for (i, error) in errors
    ])
