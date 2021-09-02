# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""

import logging

from google.appengine.api import datastore_errors
from google.protobuf import empty_pb2

from components import prpc
from components.prpc.codes import StatusCode

import api_helpers
import backend_conversions
import handlers_exceptions
import prpc_helpers
from components import auth
from components import datastore_utils
from proto.api.internal.bb import backend_prpc_pb2
from proto.api.internal.bb import backend_pb2
from proto.api import swarming_prpc_pb2  # pylint: disable=no-name-in-module
from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from server import acl
from server import bot_management
from server import task_request
from server import task_scheduler


class TaskBackendAPIService(prpc_helpers.SwarmingPRPCService):
  """Service implements the pRPC service in backend.proto."""

  DESCRIPTION = backend_prpc_pb2.TaskBackendServiceDescription

  @prpc_helpers.PRPCMethod
  @auth.require(
      acl.can_create_task, 'User cannot create tasks.', log_identity=True)
  def RunTask(self, request, _context):
    # type: (backend_pb2.RunTaskRequest, context.ServicerContext)
    #     -> empty_pb2.Empty

    tr, secret_bytes, build_token = backend_conversions.compute_task_request(
        request)

    api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)

    # TODO(crbug/1236848): Used request.request_id to dedupe tasks within
    # ten minutes.

    try:
      result_summary = task_scheduler.schedule_request(
          tr, secret_bytes=secret_bytes, build_token=build_token)
      logging.info('Returned ResultSummary: %r', result_summary)
    except (TypeError, ValueError) as e:
      raise handlers_exceptions.BadRequestException(e.message)

    return empty_pb2.Empty()


class BotAPIService(object):
  """Service implements the pRPC service in swarming.proto."""

  DESCRIPTION = swarming_prpc_pb2.BotAPIServiceDescription

  # TODO(maruel): Add implementation. https://crbug.com/913953

  def Events(self, request, context):
    logging.debug('%s', request)
    try:
      if not request.bot_id:
        # TODO(maruel): Allows not specifying one. Or specifying a pool.
        raise ValueError('specify bot_id')

      # Transparently limit to 1000, default to 200.
      page_size = request.page_size or 200
      if page_size > 1000:
        page_size = 1000
      if page_size < 0:
        raise ValueError('page_size must be positive')

      start = None
      end = None
      if request.HasField('start_time'):
        start = request.start_time.ToDatetime()
      if request.HasField('end_time'):
        end = request.end_time.ToDatetime()
      if (start and end) and start >= end:
        raise ValueError('start_time must be before end_time')

      # The BotEvent key is already in the right chronological order, but
      # querying per BotEvent.ts *requires* ordering per BotEvent.ts.
      order = not (start or end)
      q = bot_management.get_events_query(request.bot_id, order)
      if not order:
        q = q.order(-bot_management.BotEvent.ts, bot_management.BotEvent.key)
      if start:
        q = q.filter(bot_management.BotEvent.ts >= start)
      if end:
        q = q.filter(bot_management.BotEvent.ts < end)

      items, cursor = datastore_utils.fetch_page(
          q, page_size, request.page_token)
      if not items:
        # Check if the bot exists, if not, return a 404. We check BotRoot, not
        # BotInfo, so that even deleted bots can be queried. See bot_management
        # for more information.
        if not bot_management.get_root_key(request.bot_id).get():
          context.set_code(StatusCode.NOT_FOUND)
          context.set_details('Bot does not exist')
          return None
    except ValueError as e:
      context.set_code(StatusCode.INVALID_ARGUMENT)
      context.set_details(str(e))
      return None
    logging.info('Returning %d events', len(items))
    out = swarming_pb2.BotEventsResponse(next_page_token=cursor)
    for r in items:
      i = out.events.add()
      r.to_proto(i)
    return out


def get_routes():
  s = prpc.Server()
  s.add_service(BotAPIService())
  # TODO(crbug/1236848): add
  # s.add_service(TaskBackendAPIService())
  # s.add_interceptor(auth.prpc_interceptor)
  return s.get_routes()
