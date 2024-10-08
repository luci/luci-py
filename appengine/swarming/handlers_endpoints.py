# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server endpoints handlers."""

import functools
import logging

from google.appengine.api import datastore_errors

import endpoints
import gae_ts_mon
from protorpc import messages
from protorpc import message_types
from protorpc import protojson
from protorpc import remote

from components import auth
from components import datastore_utils
from components import endpoints_webapp2
from components import utils

import api_common
import handlers_exceptions
import message_conversion
import swarming_rpcs
from server import acl
from server import bot_code
from server import config
from server import task_queues


### Helper Methods

# Add support for BooleanField in protorpc in endpoints GET requests.
_old_decode_field = protojson.ProtoJson.decode_field
def _decode_field(self, field, value):
  if (isinstance(field, messages.BooleanField) and
      isinstance(value, basestring)):
    return value.lower() == 'true'
  return _old_decode_field(self, field, value)
protojson.ProtoJson.decode_field = _decode_field


def _convert_to_endpoints_exception(func):
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except handlers_exceptions.NotFoundException as e:
      raise endpoints.NotFoundException(e.message)
    except handlers_exceptions.BadRequestException as e:
      raise endpoints.BadRequestException(e.message)
    except handlers_exceptions.InternalException as e:
      raise endpoints.InternalServerErrorException(e.message)
    except handlers_exceptions.PermissionException as e:
      raise auth.AuthorizationError(e.message)

  return wrapper


def endpoint(*args, **kwargs):
  def decorator(func):
    instrument = gae_ts_mon.instrument_endpoint()
    endpoints_method = auth.endpoints_method(*args, **kwargs)
    return endpoints_method(instrument(_convert_to_endpoints_exception(func)))

  return decorator


def get_or_raise(key):
  """Returns an entity or raises an endpoints exception if it does not exist."""
  result = key.get()
  if not result:
    raise endpoints.NotFoundException('%s not found.' % key.id())
  return result


### API


swarming_api = auth.endpoints_api(
    name='swarming',
    version='v1',
    description=
        'API to interact with the Swarming service. Permits to create, '
        'view and cancel tasks, query tasks and bots')


PermissionsRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    bot_id=messages.StringField(1),
    task_id=messages.StringField(2),
    tags=messages.StringField(3, repeated=True),
)


@swarming_api.api_class(resource_name='server', path='server')
class SwarmingServerService(remote.Service):
  @endpoint(message_types.VoidMessage,
            swarming_rpcs.ServerDetails,
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def details(self, _request):
    """Returns information about the server."""
    details = api_common.get_server_details()
    return swarming_rpcs.ServerDetails(
        bot_version=details.bot_version,
        server_version=details.server_version,
        display_server_url_template=details.display_server_url_template,
        luci_config=details.luci_config,
        cas_viewer_server=details.cas_viewer_server,
    )

  @endpoint(message_types.VoidMessage, swarming_rpcs.BootstrapToken)
  @auth.require(acl.can_create_bot, log_identity=True)
  def token(self, _request):
    """Returns a token to bootstrap a new bot.

    This may seem strange to be a POST and not a GET, but it's very
    important to make sure GET requests are idempotent and safe
    to be pre-fetched; generating a token is neither of those things.
    """
    return swarming_rpcs.BootstrapToken(
        bootstrap_token = bot_code.generate_bootstrap_token(),
      )

  @endpoint(PermissionsRequest,
            swarming_rpcs.ClientPermissions,
            http_method='GET')
  @auth.public
  def permissions(self, request):
    """Returns the caller's permissions."""
    perms = api_common.get_permissions(request.bot_id, request.task_id,
                                       request.tags)
    return swarming_rpcs.ClientPermissions(
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

  @endpoint(message_types.VoidMessage,
            swarming_rpcs.FileContent,
            http_method='GET')
  @auth.require(acl.can_view_config, log_identity=True)
  def get_bootstrap(self, _request):
    """Retrieves the current version of bootstrap.py."""
    obj = bot_code.get_bootstrap('', '')
    return swarming_rpcs.FileContent(
        content=obj.content.decode('utf-8'),
        who=obj.who,
        when=obj.when,
        version=obj.version)

  @endpoint(message_types.VoidMessage,
            swarming_rpcs.FileContent,
            http_method='GET')
  @auth.require(acl.can_view_config, log_identity=True)
  def get_bot_config(self, _request):
    """Retrieves the current version of bot_config.py."""
    obj, _ = bot_code.get_bot_config()
    return swarming_rpcs.FileContent(
        content=obj.content.decode('utf-8'),
        who=obj.who,
        when=obj.when,
        version=obj.version)


TaskId = endpoints.ResourceContainer(
    message_types.VoidMessage,
    task_id=messages.StringField(1, required=True))


TaskIdWithPerf = endpoints.ResourceContainer(
    message_types.VoidMessage,
    task_id=messages.StringField(1, required=True),
    include_performance_stats=messages.BooleanField(2, default=False))


TaskCancel = endpoints.ResourceContainer(
    swarming_rpcs.TaskCancelRequest,
    task_id=messages.StringField(1, required=True))


TaskIdWithOffset = endpoints.ResourceContainer(
    message_types.VoidMessage,
    task_id=messages.StringField(1, required=True),
    offset=messages.IntegerField(2, default=0),
    length=messages.IntegerField(3, default=0))


@swarming_api.api_class(resource_name='task', path='task')
class SwarmingTaskService(remote.Service):
  """Swarming's task-related API."""

  @endpoint(TaskIdWithPerf,
            swarming_rpcs.TaskResult,
            name='result',
            path='{task_id}/result',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def result(self, request):
    """Reports the result of the task corresponding to a task ID.

    It can be a 'run' ID specifying a specific retry or a 'summary' ID hidding
    the fact that a task may have been retried transparently, when a bot reports
    BOT_DIED.

    A summary ID ends with '0', a run ID ends with '1' or '2'.
    """
    logging.debug('%s', request)
    # Workaround a bug in ndb where if a memcache set fails, a stale copy can be
    # kept in memcache indefinitely. In the case of task result, if this happens
    # on the very last store where the task is saved to NDB to be marked as
    # completed, and that the DB store succeeds *but* the memcache update fails,
    # this API will *always* return the stale version.
    try:
      _, result = api_common.get_request_and_result(request.task_id,
                                                    api_common.VIEW)
    except ValueError:
      raise endpoints.BadRequestException('Invalid task ID')
    return message_conversion.task_result_to_rpc(
        result, request.include_performance_stats)

  @endpoint(TaskId,
            swarming_rpcs.TaskRequest,
            name='request',
            path='{task_id}/request',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def request(self, request):
    """Returns the task request corresponding to a task ID."""
    logging.debug('%s', request)
    request_key, _ = api_common.to_keys(request.task_id)
    request_obj = api_common.get_task_request_async(
        request.task_id, request_key, api_common.VIEW).get_result()
    return message_conversion.task_request_to_rpc(request_obj)

  @endpoint(TaskCancel,
            swarming_rpcs.CancelResponse,
            name='cancel',
            path='{task_id}/cancel')
  @auth.require(acl.can_access, log_identity=True)
  def cancel(self, request):
    """Cancels a task.

    If a bot was running the task, the bot will forcibly cancel the task.
    """
    logging.debug('request %s', request)
    ok, was_running = api_common.cancel_task(request.task_id,
                                             request.kill_running)
    return swarming_rpcs.CancelResponse(ok=ok, was_running=was_running)

  @endpoint(TaskIdWithOffset,
            swarming_rpcs.TaskOutput,
            name='stdout',
            path='{task_id}/stdout',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def stdout(self, request):
    """Returns the output of the task corresponding to a task ID."""
    logging.debug('%s', request)
    output, state = api_common.get_output(request.task_id, request.offset,
                                          request.length)
    if output:
      # The datastore entity (TaskOutputChunk) stores output as a string blob.
      # In python2 `str` objects are basically c-strings with no character
      # encoding required. However, protorpc `StringField` requires that
      # a string must be either:
      # 1. encoded in 7bit ascii
      # 2. have the unicode type
      # Since the output of tasks is allowed to be utf-8 encoded strings,
      # we must decode the string to utf-8 to be a valid StringField.
      output = output.decode('utf-8', 'replace')
    return swarming_rpcs.TaskOutput(output=output,
                                    state=swarming_rpcs.TaskState(state))


TasksRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    limit=messages.IntegerField(1, default=200),
    cursor=messages.StringField(2),
    # These should be DateTimeField but endpoints + protorpc have trouble
    # encoding this message in a GET request, this is due to DateTimeField's
    # special encoding in protorpc-1.0/protorpc/message_types.py that is
    # bypassed when using endpoints-1.0/endpoints/protojson.py to add GET query
    # parameter support.
    end=messages.FloatField(3),
    start=messages.FloatField(4),
    state=messages.EnumField(swarming_rpcs.TaskStateQuery, 5, default='ALL'),
    tags=messages.StringField(6, repeated=True),
    sort=messages.EnumField(swarming_rpcs.TaskSort, 7, default='CREATED_TS'),
    include_performance_stats=messages.BooleanField(8, default=False))


TaskStatesRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    task_id=messages.StringField(1, repeated=True))


TasksCountRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    end=messages.FloatField(3),
    start=messages.FloatField(4),
    state=messages.EnumField(swarming_rpcs.TaskStateQuery, 5, default='ALL'),
    tags=messages.StringField(6, repeated=True))


@swarming_api.api_class(resource_name='tasks', path='tasks')
class SwarmingTasksService(remote.Service):
  """Swarming's tasks-related API."""

  @endpoint(swarming_rpcs.NewTaskRequest, swarming_rpcs.TaskRequestMetadata)
  @auth.require(
      acl.can_create_task, 'User cannot create tasks.', log_identity=True)
  def new(self, request):
    """Creates a new task.

    The task will be enqueued in the tasks list and will be executed at the
    earliest opportunity by a bot that has at least the dimensions as described
    in the task request.
    """
    sb = (request.properties.secret_bytes
          if request.properties is not None else None)
    if sb is not None:
      request.properties.secret_bytes = "HIDDEN"
    logging.debug('%s', request)
    if sb is not None:
      request.properties.secret_bytes = sb

    try:
      request_obj, secret_bytes, template_apply = (
          message_conversion.new_task_request_from_rpc(request, utils.utcnow()))
    except (datastore_errors.BadValueError, ValueError) as e:
      raise endpoints.BadRequestException(e.message)

    ntr = api_common.new_task(request_obj, secret_bytes, template_apply,
                              request.evaluate_only, request.request_uuid)

    return swarming_rpcs.TaskRequestMetadata(
        request=message_conversion.task_request_to_rpc(ntr.request)
        if ntr.request else None,
        task_id=ntr.task_id,
        task_result=message_conversion.task_result_to_rpc(
            ntr.task_result, False) if ntr.task_result else None)


  @endpoint(TasksRequest, swarming_rpcs.TaskList, http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def list(self, request):
    """Returns full task results based on the filters.

    This endpoint is significantly slower than 'count'. Use 'count' when
    possible. If you just want the state of tasks, use 'get_states'.
    """
    # TODO(maruel): Rename 'list' to 'results'.
    # TODO(maruel): Rename 'TaskList' to 'TaskResults'.
    logging.debug('%s', request)

    now = utils.utcnow()
    try:
      rsf = self._query_from_request(request)
    except ValueError as e:
      raise endpoints.BadRequestException("invalid datetime values %s" % str(e))
    items, cursor = api_common.list_task_results(rsf, request.limit,
                                                 request.cursor)
    return swarming_rpcs.TaskList(
        cursor=cursor,
        items=[
          message_conversion.task_result_to_rpc(
              i, request.include_performance_stats)
          for i in items
        ],
        now=now)

  @endpoint(TaskStatesRequest, swarming_rpcs.TaskStates, http_method='GET')
  # TODO(martiniss): users should be able to view their state. This requires
  # looking up each TaskRequest.
  @auth.require(acl.can_view_all_tasks, log_identity=True)
  def get_states(self, request):
    """Returns task state for a specific set of tasks."""
    logging.debug('%s', request)

    states = api_common.get_states(request.task_id)

    return swarming_rpcs.TaskStates(
        states=[swarming_rpcs.TaskState(state) for state in states])

  @endpoint(TasksRequest, swarming_rpcs.TaskRequests, http_method='GET')
  @auth.require(acl.can_view_all_tasks, log_identity=True)
  def requests(self, request):
    """Returns tasks requests based on the filters.

    This endpoint is slightly slower than 'list'. Use 'list' or 'count' when
    possible.
    """
    logging.debug('%s', request)
    if request.include_performance_stats:
      raise endpoints.BadRequestException(
          'Can\'t set include_performance_stats for tasks/list')
    now = utils.utcnow()
    try:
      rsf = self._query_from_request(request)
    except ValueError as e:
      raise endpoints.BadRequestException("invalid datetime values %s" % str(e))
    items, cursor = api_common.list_task_requests_no_realm_check(
        rsf, request.limit, request.cursor)
    return swarming_rpcs.TaskRequests(
        cursor=cursor,
        items=[message_conversion.task_request_to_rpc(i) for i in items],
        now=now)

  @endpoint(swarming_rpcs.TasksCancelRequest,
            swarming_rpcs.TasksCancelResponse,
            http_method='POST')
  @auth.require(acl.can_access, log_identity=True)
  def cancel(self, request):
    """Cancel a subset of pending tasks based on the tags.

    Cancellation happens asynchronously, so when this call returns,
    cancellations will not have completed yet.
    """
    logging.debug('request %s', request)
    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
    except ValueError as e:
      raise endpoints.BadRequestException('Invalid timestamp: %s' % e)
    tcr = api_common.cancel_tasks(request.tags, start, end, request.limit,
                                  request.cursor, request.kill_running)
    return swarming_rpcs.TasksCancelResponse(cursor=tcr.cursor,
                                             matched=tcr.matched,
                                             now=tcr.now)

  @endpoint(TasksCountRequest, swarming_rpcs.TasksCount, http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def count(self, request):
    """Counts number of tasks in a given state."""
    logging.debug('%s', request)
    now = utils.utcnow()
    trf = self._query_from_request(request, 'created_ts')
    count = api_common.count_tasks(trf, now)
    return swarming_rpcs.TasksCount(count=count, now=now)


  def _query_from_request(self, request, sort=None):
    """Returns api_common.TaskFilters object."""
    start = message_conversion.epoch_to_datetime(request.start)
    end = message_conversion.epoch_to_datetime(request.end)
    return api_common.TaskFilters(
        start=start,
        end=end,
        sort=sort or request.sort.name.lower(),
        state=request.state.name.lower(),
        tags=request.tags,
    )


TaskQueuesRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    # Note that it's possible that the RPC returns a tad more or less items than
    # requested limit.
    limit=messages.IntegerField(1, default=200),
    cursor=messages.StringField(2))


@swarming_api.api_class(resource_name='queues', path='queues')
class SwarmingQueuesService(remote.Service):
  @endpoint(TaskQueuesRequest, swarming_rpcs.TaskQueueList, http_method='GET')
  @auth.require(acl.can_view_all_tasks, log_identity=True)
  def list(self, request):
    logging.debug('%s', request)
    now = utils.utcnow()
    q = task_queues.TaskDimensionsInfo.query()
    cursor = request.cursor
    out = []
    count = 0
    # As there can be a lot of terminate tasks, try to loop a few times (max 5)
    # to get more items.
    try:
      while len(out) < request.limit and (cursor or not count) and count < 5:
        items, cursor = datastore_utils.fetch_page(q, request.limit - len(out),
                                                   cursor)
        for i in items:
          for (dims_flat, valid_until_ts) in i.expiry_map().items():
            # Ignore the tasks that are only id specific, since they are
            # termination tasks. There may be one per bot, and it is not really
            # useful for the user, the user may just query the list of bots.
            if len(dims_flat) == 1 and dims_flat[0].startswith('id:'):
              continue
            out.append(
                swarming_rpcs.TaskQueue(dimensions=list(dims_flat),
                                        valid_until_ts=valid_until_ts))
        count += 1
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for tasks/list: %s' % e)
    return swarming_rpcs.TaskQueueList(cursor=cursor, items=out, now=now)


BotId = endpoints.ResourceContainer(
    message_types.VoidMessage,
    bot_id=messages.StringField(1, required=True))


BotEventsRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    bot_id=messages.StringField(1, required=True),
    limit=messages.IntegerField(2, default=200),
    cursor=messages.StringField(3),
    # end, start are seconds since epoch.
    end=messages.FloatField(4),
    start=messages.FloatField(5))


BotTasksRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    bot_id=messages.StringField(1, required=True),
    limit=messages.IntegerField(2, default=200),
    cursor=messages.StringField(3),
    # end, start are seconds since epoch.
    end=messages.FloatField(4),
    start=messages.FloatField(5),
    state=messages.EnumField(swarming_rpcs.TaskStateQuery, 6, default='ALL'),
    sort=messages.EnumField(swarming_rpcs.TaskSort, 7, default='CREATED_TS'),
    include_performance_stats=messages.BooleanField(8, default=False))


@swarming_api.api_class(resource_name='bot', path='bot')
class SwarmingBotService(remote.Service):
  """Bot-related API. Permits querying information about the bot's properties"""

  @endpoint(BotId,
            swarming_rpcs.BotInfo,
            name='get',
            path='{bot_id}/get',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def get(self, request):
    """Returns information about a known bot.

    This includes its state and dimensions, and if it is currently running a
    task.
    """
    logging.debug('%s', request)
    bot_id = request.bot_id
    bot, deleted = api_common.get_bot(bot_id)
    return message_conversion.bot_info_to_rpc(bot, deleted=deleted)

  @endpoint(BotId,
            swarming_rpcs.DeletedResponse,
            name='delete',
            path='{bot_id}/delete')
  @auth.require(acl.can_access, log_identity=True)
  def delete(self, request):
    """Deletes the bot corresponding to a provided bot_id.

    At that point, the bot will not appears in the list of bots but it is still
    possible to get information about the bot with its bot id is known, as
    historical data is not deleted.

    It is meant to remove from the DB the presence of a bot that was retired,
    e.g. the VM was shut down already. Use 'terminate' instead of the bot is
    still alive.
    """
    logging.debug('%s', request)
    bot_id = request.bot_id
    api_common.delete_bot(bot_id)
    return swarming_rpcs.DeletedResponse(deleted=True)

  @endpoint(BotEventsRequest,
            swarming_rpcs.BotEvents,
            name='events',
            path='{bot_id}/events',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def events(self, request):
    """Returns events that happened on a bot."""
    logging.debug('%s', request)
    bot_id = request.bot_id

    try:
      now = utils.utcnow()
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      limit = request.limit
      cursor = request.cursor
      items, cursor = api_common.get_bot_events(bot_id, start, end, limit,
                                                cursor)
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for bot.events: %s' % e)
    return swarming_rpcs.BotEvents(
        cursor=cursor,
        items=[message_conversion.bot_event_to_rpc(r) for r in items],
        now=now)

  @endpoint(BotId,
            swarming_rpcs.TerminateResponse,
            name='terminate',
            path='{bot_id}/terminate')
  @auth.require(acl.can_access, log_identity=True)
  def terminate(self, request):
    """Asks a bot to terminate itself gracefully.

    The bot will stay in the DB, use 'delete' to remove it from the DB
    afterward. This request returns a pseudo-taskid that can be waited for to
    wait for the bot to turn down.

    This command is particularly useful when a privileged user needs to safely
    debug a machine specific issue. The user can trigger a terminate for one of
    the bot exhibiting the issue, wait for the pseudo-task to run then access
    the machine with the guarantee that the bot is not running anymore.
    """
    # TODO(maruel): Disallow a terminate task when there's one currently
    # pending or if the bot is considered 'dead', e.g. no contact since 10
    # minutes.
    logging.debug('%s', request)
    bot_id = unicode(request.bot_id)
    task_id = api_common.terminate_bot(bot_id)
    return swarming_rpcs.TerminateResponse(task_id=task_id)

  @endpoint(BotTasksRequest,
            swarming_rpcs.BotTasks,
            name='tasks',
            path='{bot_id}/tasks',
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def tasks(self, request):
    """Lists a given bot's tasks within the specified date range.

    In this case, the tasks are effectively TaskRunResult since it's individual
    task tries sent to this specific bot.

    It is impossible to search by both tags and bot id. If there's a need,
    TaskRunResult.tags will be added (via a copy from TaskRequest.tags).
    """
    logging.debug('%s', request)

    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
    except ValueError as e:
      raise endpoints.BadRequestException('Invalid timestamp: %s' % e)
    now = utils.utcnow()
    filters = api_common.TaskFilters(
        start=start,
        end=end,
        sort=request.sort.name.lower(),
        state=request.state.name.lower(),
        tags=[],
    )
    items, cursor = api_common.list_bot_tasks(
        request.bot_id,
        filters,
        request.limit,
        request.cursor,
    )
    return swarming_rpcs.BotTasks(
        cursor=cursor,
        items=[
          message_conversion.task_result_to_rpc(
              r, request.include_performance_stats)
          for r in items
        ],
        now=now)


BotsRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    limit=messages.IntegerField(1, default=200),
    cursor=messages.StringField(2),
    # Must be a list of 'key:value' strings to filter the returned list of bots
    # on.
    dimensions=messages.StringField(3, repeated=True),
    quarantined=messages.EnumField(
        swarming_rpcs.ThreeStateBool, 4, default='NONE'),
    in_maintenance=messages.EnumField(
        swarming_rpcs.ThreeStateBool, 8, default='NONE'),
    is_dead=messages.EnumField(swarming_rpcs.ThreeStateBool, 5, default='NONE'),
    is_busy=messages.EnumField(swarming_rpcs.ThreeStateBool, 6, default='NONE'))


BotsCountRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    dimensions=messages.StringField(1, repeated=True))


BotsDimensionsRequest = endpoints.ResourceContainer(
    message_types.VoidMessage, pool=messages.StringField(1))


@swarming_api.api_class(resource_name='bots', path='bots')
class SwarmingBotsService(remote.Service):
  """Bots-related API."""

  @endpoint(BotsRequest, swarming_rpcs.BotList, http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def list(self, request):
    """Provides list of known bots.

    Deleted bots will not be listed.
    """
    logging.debug('%s', request)

    now = utils.utcnow()
    quarantined = swarming_rpcs.to_bool(request.quarantined)
    in_maintenance = swarming_rpcs.to_bool(request.in_maintenance)
    is_dead = swarming_rpcs.to_bool(request.is_dead)
    is_busy = swarming_rpcs.to_bool(request.is_busy)
    bf = api_common.BotFilters(
        dimensions=request.dimensions,
        quarantined=quarantined,
        in_maintenance=in_maintenance,
        is_dead=is_dead,
        is_busy=is_busy,
    )
    bots, cursor = api_common.list_bots(bf, request.limit, request.cursor)
    return swarming_rpcs.BotList(
        cursor=cursor,
        death_timeout=config.settings().bot_death_timeout_secs,
        items=[message_conversion.bot_info_to_rpc(bot) for bot in bots],
        now=now)

  @endpoint(BotsCountRequest, swarming_rpcs.BotsCount, http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def count(self, request):
    """Counts number of bots with given dimensions."""
    logging.debug('%s', request)

    now = utils.utcnow()
    bc = api_common.count_bots(request.dimensions)
    return swarming_rpcs.BotsCount(count=bc.count,
                                   quarantined=bc.quarantined,
                                   maintenance=bc.maintenance,
                                   dead=bc.dead,
                                   busy=bc.busy,
                                   now=now)

  @endpoint(BotsDimensionsRequest,
            swarming_rpcs.BotsDimensions,
            http_method='GET')
  @auth.require(acl.can_access, log_identity=True)
  def dimensions(self, request):
    """Returns the cached set of dimensions currently in use in the fleet."""
    # Implemented only in Go now.
    raise NotImplementedError()


def get_routes():
  return endpoints_webapp2.api_routes([
    SwarmingServerService,
    SwarmingTaskService,
    SwarmingTasksService,
    SwarmingQueuesService,
    SwarmingBotService,
    SwarmingBotsService,
    # components.config endpoints for validation and configuring of luci-config
    # service URL.
    config.ConfigApi], base_path='/api')
