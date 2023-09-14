# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""

import datetime
import logging

from google.appengine.api import datastore_errors
from google.protobuf import empty_pb2
from google.rpc import status_pb2

from components import auth
from components import ereporter2
from components import prpc
from components import utils
from components.prpc import codes
from proto.api_v2 import swarming_pb2
from proto.api_v2 import swarming_prpc_pb2
from proto.internals import rbe_pb2
from proto.internals import rbe_prpc_pb2
from server import acl
from server import bot_code
from server import config
from server import task_pack
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

_NULLABLE_BOOL_MAP = {
    swarming_pb2.TRUE: True,
    swarming_pb2.FALSE: False,
    swarming_pb2.NULL: None,
}


def _get_start_and_end_dates(request):
  """Gets two fields `start` and `end` from  a request proto and converts
  them into datetime.datetime. If neither of the fields are defined in the proto
  then just return None for each.
  """
  start = None
  if request.HasField("start"):
    start = request.start.ToDatetime()
  end = None
  if request.HasField("end"):
    end = request.end.ToDatetime()
  return start, end


def _get_sort_and_state(request):
  """Converts `sort` and `state` into forms which are compatible
  with the previous endpoints API.
  """
  sort = _SORT_MAP.get(request.sort)
  state = _STATE_MAP.get(request.state)
  return sort, state


class BotsService(object):
  """Module implements the Bots service defined in proto/api_v2/swarming.proto
  """
  DESCRIPTION = swarming_prpc_pb2.BotsServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetBot(self, request, _context):
    bot_id = request.bot_id
    bot, deleted = api_common.get_bot(bot_id)
    return message_conversion_prpc.bot_info_response(bot, deleted)

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
    start, end = _get_start_and_end_dates(request)
    items, cursor = api_common.get_bot_events(bot_id, start, end, request.limit,
                                              request.cursor)
    return message_conversion_prpc.bot_events_response(items, cursor)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def TerminateBot(self, request, _context):
    bot_id = request.bot_id
    reason = request.reason
    task_id = api_common.terminate_bot(bot_id, reason)
    return swarming_pb2.TerminateResponse(task_id=task_id)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListBotTasks(self, request, _context):
    bot_id = request.bot_id
    start, end = _get_start_and_end_dates(request)
    sort, state = _get_sort_and_state(request)
    filters = api_common.TaskFilters(
        sort=sort,
        state=state,
        start=start,
        end=end,
        tags=[],
    )
    items, cursor = api_common.list_bot_tasks(bot_id, filters, request.limit,
                                              request.cursor)
    return message_conversion_prpc.task_list_response(items, cursor)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetBotDimensions(self, request, _context):
    dr = api_common.get_dimensions(request.pool)
    ts = message_conversion_prpc.date(dr.ts)
    bd = [
        swarming_pb2.StringListPair(key=d.dimension, value=d.values)
        for d in dr.bots_dimensions
    ]
    return swarming_pb2.BotsDimensions(bots_dimensions=bd, ts=ts)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListBots(self, request, _context):
    quarantined = _NULLABLE_BOOL_MAP.get(request.quarantined)
    in_maintenance = _NULLABLE_BOOL_MAP.get(request.in_maintenance)
    is_dead = _NULLABLE_BOOL_MAP.get(request.is_dead)
    is_busy = _NULLABLE_BOOL_MAP.get(request.is_busy)
    dimensions = [
        "%s:%s" % (pair.key, pair.value) for pair in request.dimensions
    ]
    bf = api_common.BotFilters(
        dimensions=dimensions,
        quarantined=quarantined,
        in_maintenance=in_maintenance,
        is_dead=is_dead,
        is_busy=is_busy,
    )
    bots, cursor = api_common.list_bots(bf, request.limit, request.cursor)
    return message_conversion_prpc.bots_response(
        bots,
        config.settings().bot_death_timeout_secs, cursor)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def CountBots(self, request, _context):
    dimensions = [
        "%s:%s" % (pair.key, pair.value) for pair in request.dimensions
    ]
    bc = api_common.count_bots(dimensions)
    out = swarming_pb2.BotsCount(
        count=bc.count,
        quarantined=bc.quarantined,
        maintenance=bc.maintenance,
        dead=bc.dead,
        busy=bc.busy,
    )
    out.now.GetCurrentTime()
    return out


class TasksService(object):
  """Module implements the Tasks service defined in proto/api_v2/swarming.proto
  """
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
  def BatchGetResult(self, request, _context):
    if not request.task_ids:
      raise handlers_exceptions.BadRequestException('Task IDs list is empty')

    # Must have no dups.
    seen = set()
    for task_id in request.task_ids:
      if task_id in seen:
        raise handlers_exceptions.BadRequestException(
            'Duplicate ID in the task IDs list: %s' % task_id)
      seen.add(task_id)

    # The response being assembled. Each item is ResultOrError or None.
    results = [None] * len(request.task_ids)

    # Writes ResultOrError that carries an error.
    def error(idx, code, message):
      assert results[idx] is None
      results[idx] = swarming_pb2.BatchGetResultResponse.ResultOrError(
          task_id=request.task_ids[idx],
          error=status_pb2.Status(
              code=code.value,
              message=message,
          ),
      )

    # Writes ResultOrError that carries a task result.
    def result(idx, summary):
      assert results[idx] is None
      result_pb = message_conversion_prpc.task_result_response(
          summary, request.include_performance_stats)
      assert result_pb.task_id == request.task_ids[idx]
      results[idx] = swarming_pb2.BatchGetResultResponse.ResultOrError(
          task_id=request.task_ids[idx],
          result=result_pb)

    # Get TaskResultSummary keys of all recognized task IDs.
    keys = []
    indx = []
    for idx, task_id in enumerate(request.task_ids):
      try:
        keys.append(task_pack.unpack_result_summary_key(task_id))
        indx.append(idx)
      except ValueError as exc:
        error(
            idx, codes.StatusCode.INVALID_ARGUMENT,
            'Bad task ID "%s": %s' % (task_id, exc))

    # ACL check predicate.
    if acl.can_view_all_tasks():
      check_visible = lambda _summary: True
    else:
      # TODO(vadimsh): Implement. Depends on propagating `realm` and `pools`
      # into TaskResultSummary.
      check_visible = lambda _summary: False

    # Fetch result summaries of all tasks or discover they are missing.
    summaries = task_result.fetch_task_result_summaries(keys)
    for idx, summary in zip(indx, summaries):
      if not summary:
        error(idx, codes.StatusCode.NOT_FOUND, 'No such task')
      elif not check_visible(summary):
        error(
            idx, codes.StatusCode.PERMISSION_DENIED,
            'No access to see the status of this task')
      else:
        result(idx, summary)

    return swarming_pb2.BatchGetResultResponse(results=results)

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

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def CancelTasks(self, request, _context):
    start, end = _get_start_and_end_dates(request)
    tcr = api_common.cancel_tasks(request.tags, start, end, request.limit,
                                  request.cursor, request.kill_running)
    now = message_conversion_prpc.date(tcr.now)
    return swarming_pb2.TasksCancelResponse(cursor=tcr.cursor,
                                            matched=tcr.matched,
                                            now=now)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def ListTasks(self, request, _context):
    start, end = _get_start_and_end_dates(request)
    sort, state = _get_sort_and_state(request)
    rsf = api_common.TaskFilters(
        start=start,
        end=end,
        sort=sort,
        state=state,
        tags=list(request.tags),
    )
    items, cursor = api_common.list_task_results(rsf, request.limit,
                                                 request.cursor)
    return message_conversion_prpc.task_list_response(
        items, cursor, request.include_performance_stats)

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def CountTasks(self, request, _context):
    now = utils.utcnow()
    start, end = _get_start_and_end_dates(request)
    state = _STATE_MAP.get(request.state)
    sort = 'created_ts'
    trf = api_common.TaskFilters(
        start=start,
        end=end,
        state=state,
        sort=sort,
        tags=request.tags,
    )
    count = api_common.count_tasks(trf, now)
    return swarming_pb2.TasksCount(
        count=count,
        now=message_conversion_prpc.date(now),
    )

  @prpc_helpers.method
  @auth.require(acl.can_view_all_tasks, log_identity=True)
  def ListTaskRequests(self, request, _context):
    start, end = _get_start_and_end_dates(request)
    sort, state = _get_sort_and_state(request)
    trf = api_common.TaskFilters(
        start=start,
        end=end,
        state=state,
        sort=sort,
        tags=list(request.tags),
    )
    items, cursor = api_common.list_task_requests_no_realm_check(
        trf, request.limit, request.cursor)
    return message_conversion_prpc.task_request_list_response(items, cursor)

  @prpc_helpers.method
  @auth.require(acl.can_view_all_tasks, log_identity=True)
  def ListTaskStates(self, request, _context):
    states = api_common.get_states(request.task_id)
    return swarming_pb2.TaskStates(states=states)


class InternalsService(object):
  """Module implements the Internals service defined in
  proto/internals/rbe.proto"""

  DESCRIPTION = rbe_prpc_pb2.InternalsServiceDescription

  # Not all expiration reasons map well to task states. Vague ones are not in
  # this map and we'll use generic EXPIRED for them + report the error via
  # the ereporter (to make it show up in aggregated error reports).
  REASON_TO_TASK_STATE = {
      rbe_pb2.ExpireSliceRequest.NO_RESOURCE: task_result.State.NO_RESOURCE,
      rbe_pb2.ExpireSliceRequest.BOT_INTERNAL_ERROR: task_result.State.BOT_DIED,
      rbe_pb2.ExpireSliceRequest.EXPIRED: task_result.State.EXPIRED,
  }

  @prpc_helpers.method
  @auth.require(acl.is_swarming_itself, log_identity=True)
  def ExpireSlice(self, request, _context):
    logging.info('%s', request)

    task_request_key, _ = api_common.to_keys(request.task_id)
    to_run_key = task_to_run.task_to_run_key_from_parts(
        task_request_key, request.task_to_run_shard, request.task_to_run_id)

    if request.reason in self.REASON_TO_TASK_STATE:
      terminal_state = self.REASON_TO_TASK_STATE[request.reason]
      report_to_log = terminal_state == task_result.State.BOT_DIED
    else:
      terminal_state = task_result.State.EXPIRED
      report_to_log = True

    task_scheduler.expire_slice(to_run_key, terminal_state)

    # Submit the report only after expiring the slice, in case this is slow.
    if report_to_log:
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


class SwarmingService(object):
  DESCRIPTION = swarming_prpc_pb2.SwarmingServiceDescription

  @prpc_helpers.method
  @auth.require(acl.can_access, log_identity=True)
  def GetDetails(self, _request, _context):
    details = api_common.get_server_details()
    return swarming_pb2.ServerDetails(
        bot_version=details.bot_version,
        server_version=details.server_version,
        display_server_url_template=details.display_server_url_template,
        luci_config=details.luci_config,
        cas_viewer_server=details.cas_viewer_server,
    )

  @prpc_helpers.method
  @auth.require(acl.can_create_bot, log_identity=True)
  def GetToken(self, _request, _context):
    return swarming_pb2.BootstrapToken(
        bootstrap_token=bot_code.generate_bootstrap_token())

  @prpc_helpers.method
  @auth.require(acl.can_view_config, log_identity=True)
  def GetBootstrap(self, _request, _context):
    obj = bot_code.get_bootstrap('', '')
    return swarming_pb2.FileContent(content=obj.content.decode('utf-8'),
                                    who=obj.who,
                                    when=obj.when,
                                    version=obj.version)

  @prpc_helpers.method
  @auth.require(acl.can_view_config, log_identity=True)
  def GetBotConfig(self, _request, _context):
    obj, _ = bot_code.get_bot_config()
    return swarming_pb2.FileContent(content=obj.content.decode('utf-8'),
                                    who=obj.who,
                                    when=obj.when,
                                    version=obj.version)

  @prpc_helpers.method
  @auth.public
  def GetPermissions(self, request, _context):
    perms = api_common.get_permissions(request.bot_id, request.task_id,
                                       request.tags)
    return swarming_pb2.ClientPermissions(
        delete_bot=perms.delete_bot,
        delete_bots=perms.delete_bots,
        terminate_bot=perms.terminate_bot,
        get_configs=perms.get_configs,
        put_configs=perms.put_configs,
        cancel_task=perms.cancel_task,
        cancel_tasks=perms.cancel_tasks,
        get_bootstrap_token=perms.get_bootstrap_token,
        list_bots=perms.list_bots,
        list_tasks=perms.list_tasks)


def get_routes():
  s = prpc.Server()
  s.add_service(BotsService())
  s.add_service(TaskBackendAPIService())
  s.add_service(TasksService())
  s.add_service(InternalsService())
  s.add_service(SwarmingService())
  s.add_interceptor(auth.prpc_interceptor)
  return s.get_routes()
