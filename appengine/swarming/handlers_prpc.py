# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""
import datetime

from components import auth
from components import prpc
import proto.api_v2.swarming_pb2 as swarming_pb2
import proto.api_v2.swarming_prpc_pb2 as swarming_prpc_pb2
from server import acl
from server import realms
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


def get_routes():
  s = prpc.Server()
  s.add_service(BotsService())
  s.add_service(TaskBackendAPIService())
  s.add_interceptor(auth.prpc_interceptor)
  return s.get_routes()
