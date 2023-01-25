# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""

from components import prpc
from components import auth
import proto.api_v2.swarming_prpc_pb2 as swarming_prpc_pb2
import proto.api_v2.swarming_pb2 as swarming_pb2
from server import realms
from server import acl
import api_common
import message_conversion_prpc
import prpc_helpers


class BotsService(object):
  DESCRIPTION = swarming_prpc_pb2.BotsServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetBot(self, request, _context):
    bot_id = request.bot_id
    realms.check_bot_get_acl(bot_id)
    bot, deleted = api_common.get_bot(bot_id)
    return message_conversion_prpc.bot_info_to_proto(bot, deleted)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def DeleteBot(self, request, _context):
    bot_id = request.bot_id
    realms.check_bot_delete_acl(bot_id)
    api_common.delete_bot(bot_id)
    return swarming_pb2.DeleteResponse(deleted=True)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListBotEvents(self, request, _context):
    bot_id = request.bot_id
    realms.check_bot_get_acl(bot_id)
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
    realms.check_bot_terminate_acl(bot_id)
    task_id = api_common.terminate_bot(bot_id)
    return swarming_pb2.TerminateResponse(task_id=task_id)

  @prpc_helpers.method
  def ListBotTasks(self, request, _context):
    pass


def get_routes():
  s = prpc.Server()
  s.add_service(BotsService())
  s.add_interceptor(auth.prpc_interceptor)
  return s.get_routes()
