# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""

import datetime
import logging

from google.appengine.api import datastore_errors
from google.protobuf import empty_pb2

from components import auth
from components import ereporter2
from components import prpc
from proto.api_v2 import swarming_pb2
from proto.api_v2 import swarming_prpc_pb2
from proto.internals import rbe_pb2
from proto.internals import rbe_prpc_pb2
from server import acl
from server import realms
from server import task_result
from server import task_scheduler
from server import task_to_run
import api_common
import message_conversion_prpc
import prpc_helpers
import handlers_exceptions
from handlers_task_backend import TaskBackendAPIService

_SORT_MAP = {
    swarming_pb2.QUERY_CREATED_TS: 'created_ts',
    swarming_pb2.QUERY_COMPLETED_TS: 'completed_ts',
    swarming_pb2.QUERY_ABANDONED_TS: 'abandoned_ts',
    swarming_pb2.QUERY_STARTED_TS: 'started_ts',
}

_STATE_MAP = {
    swarming_pb2.QUERY_PENDING: 'pending',
    swarming_pb2.QUERY_RUNNING: 'running',
    swarming_pb2.QUERY_PENDING_RUNNING: 'pending_running',
    swarming_pb2.QUERY_COMPLETED: 'completed',
    swarming_pb2.QUERY_COMPLETED_SUCCESS: 'completed_success',
    swarming_pb2.QUERY_COMPLETED_FAILURE: 'completed_failure',
    swarming_pb2.QUERY_EXPIRED: 'expired',
    swarming_pb2.QUERY_TIMED_OUT: 'timed_out',
    swarming_pb2.QUERY_BOT_DIED: 'bot_died',
    swarming_pb2.QUERY_CANCELED: 'canceled',
    swarming_pb2.QUERY_ALL: 'all',
    swarming_pb2.QUERY_DEDUPED: 'deduped',
    swarming_pb2.QUERY_KILLED: 'killed',
    swarming_pb2.QUERY_NO_RESOURCE: 'no_resource',
    swarming_pb2.QUERY_CLIENT_ERROR: 'client_error',
}


class BotsService(object):
  DESCRIPTION = swarming_prpc_pb2.BotsServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetBot(self, request, _context):
    bot_id = request.bot_id
    bot, deleted = api_common.get_bot(bot_id)
    return message_conversion_prpc.bot_info_to_proto(bot, deleted)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def DeleteBot(self, request, _context):
    bot_id = request.bot_id
    api_common.delete_bot(bot_id)
    return swarming_pb2.DeleteResponse(deleted=True)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListBotEvents(self, request, _context):
    bot_id = request.bot_id
    start = request.start.ToDatetime()
    end = request.end.ToDatetime()
    limit = request.limit
    cursor = request.cursor
    items, cursor = api_common.get_bot_events(bot_id, start, end, limit, cursor)
    return message_conversion_prpc.bot_events_response(items, cursor)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def TerminateBot(self, request, _context):
    bot_id = request.bot_id
    task_id = api_common.terminate_bot(bot_id)
    return swarming_pb2.TerminateResponse(task_id=task_id)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListBotTasks(self, request, _context):
    bot_id = request.bot_id
    start = request.start.ToDatetime()
    if not request.HasField("start"):
      start = None
    end = request.end.ToDatetime()
    if not request.HasField("end"):
      end = None
    sort = _SORT_MAP.get(request.sort)
    state = _STATE_MAP.get(request.state)
    limit = request.limit
    cursor = request.cursor
    items, cursor = api_common.list_bot_tasks(bot_id, start, end, sort, state,
                                              cursor, limit)
    return message_conversion_prpc.bot_tasks_response(items, cursor)


class TasksService(object):
  DESCRIPTION = swarming_prpc_pb2.TasksServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetResult(self, request, _context):
    _, result = api_common.get_request_and_result(request.task_id,
                                                  api_common.VIEW, False)
    return message_conversion_prpc.task_result_response(
        result, request.include_performance_stats)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetRequest(self, request, _context):
    request_key, _ = api_common.to_keys(request.task_id)
    request_obj = api_common.get_task_request_async(
        request.task_id, request_key, api_common.VIEW).get_result()
    return message_conversion_prpc.task_request_response(request_obj)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def CancelTask(self, request, _context):
    canceled, was_running = api_common.cancel_task(request.task_id,
                                                   request.kill_running)
    return swarming_pb2.CancelResponse(canceled=canceled,
                                       was_running=was_running)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetStdout(self, request, _context):
    output, state = api_common.get_output(request.task_id, request.offset,
                                          request.length)
    return swarming_pb2.TaskOutputResponse(output=output, state=state)

  @prpc_helpers.method
  @auth.require(acl.can_create_task,
                'User cannot create tasks.',
                log_identity=True)
  def NewTask(self, request, _context):
    try:
      request_obj, secret_bytes, template_apply = (
          message_conversion_prpc.new_task_request_from_rpc(request))
    except (datastore_errors.BadValueError, ValueError) as e:
      raise handlers_exceptions.BadRequestException(str(e))

    ntr = api_common.new_task(request_obj, secret_bytes, template_apply,
                              request.evaluate_only, request.request_uuid)

    return swarming_pb2.TaskRequestMetadataResponse(
        request=message_conversion_prpc.task_request_response(ntr.request),
        task_id=ntr.task_id,
        task_result=message_conversion_prpc.task_result_response(
            ntr.task_result, False) if ntr.task_result else None,
    )


class InternalsService(object):
  DESCRIPTION = rbe_prpc_pb2.InternalsServiceDescription

  @prpc_helpers.method
  @auth.require(acl.is_swarming_itself, log_identity=True)
  def ExpireSlice(self, request, _context):
    logging.info('%s', request)

    task_request_key, _ = api_common.to_keys(request.task_id)
    to_run_key = task_to_run.task_to_run_key_from_parts(
        task_request_key, request.task_to_run_shard, request.task_to_run_id)

    # Only NO_RESOURCE is expected to happen. All other conditions are internal
    # failures due to server misconfiguration. Unfortunately there's no good
    # task state to represent them, so just use EXPIRED + report the error via
    # ereporter (it eventually results in a notification to admins).
    terminal_state = task_result.State.EXPIRED
    if request.reason == rbe_pb2.ExpireSliceRequest.NO_RESOURCE:
      terminal_state = task_result.State.NO_RESOURCE
    task_scheduler.expire_slice(to_run_key, terminal_state)

    # Submit the report only after expiring the slice, in case this is slow.
    if request.reason != rbe_pb2.ExpireSliceRequest.NO_RESOURCE:
      ereporter2.log(source='rbe',
                     category=rbe_pb2.ExpireSliceRequest.Reason.Name(
                         request.reason),
                     message=request.details,
                     params={
                         'task_id':
                         request.task_id,
                         'slice_index':
                         task_to_run.task_to_run_key_slice_index(to_run_key),
                     })

    return empty_pb2.Empty()


def get_routes():
  s = prpc.Server()
  s.add_service(BotsService())
  s.add_service(TaskBackendAPIService())
  s.add_service(TasksService())
  s.add_service(InternalsService())
  s.add_interceptor(auth.prpc_interceptor)
  return s.get_routes()
