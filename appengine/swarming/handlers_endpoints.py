# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Swarming Server endpoints handlers."""

import datetime
import json
import logging

from google.appengine.api import datastore_errors
import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from components import auth
from components import datastore_utils
from components import utils

import message_conversion
import swarming_rpcs
from server import acl
from server import bot_code
from server import bot_management
from server import config
from server import task_pack
from server import task_request
from server import task_result
from server import task_scheduler


### Helper Methods


def get_result_key(task_id):
  """Provides the key corresponding to a task ID."""
  key = None
  summary_key = None
  try:
    key = task_pack.unpack_result_summary_key(task_id)
    summary_key = key
  except ValueError:
    try:
      key = task_pack.unpack_run_result_key(task_id)
      summary_key = task_pack.run_result_key_to_result_summary_key(key)
    except ValueError:
      raise endpoints.BadRequestException(
          'Task ID %s produces an invalid key.' % task_id)
  return key, summary_key


def get_or_raise(key):
  """Returns an entity or raises an endpoints exception if it does not exist."""
  result = key.get()
  if not result:
    raise endpoints.NotFoundException('%s not found.' % key.id())
  return result


def get_result_entity(task_id):
  """Returns the entity (TaskResultSummary or TaskRunResult) for a given ID."""
  key, _ = get_result_key(task_id)
  return get_or_raise(key)


### API


swarming_api = auth.endpoints_api(
    name='swarming',
    version='v1',
    description=
        'API to interact with the Swarming service. Permits to create, '
        'view and cancel tasks, query tasks and bots')


VersionRequest = endpoints.ResourceContainer(
    message_types.VoidMessage,
    version=messages.IntegerField(1))


@swarming_api.api_class(resource_name='server', path='server')
class SwarmingServerService(remote.Service):
  @auth.endpoints_method(
      message_types.VoidMessage, swarming_rpcs.ServerDetails,
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def details(self, _request):
    """Returns information about the server."""
    return swarming_rpcs.ServerDetails(server_version=utils.get_app_version())

  @auth.endpoints_method(
      VersionRequest, swarming_rpcs.FileContent,
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def get_bootstrap(self, request):
    """Retrieves the current or a previous version of bootstrap.py."""
    obj = bot_code.get_bootstrap('', request.version)
    if not obj:
      return swarming_rpcs.FileContent()
    return swarming_rpcs.FileContent(
        content=obj.content.decode('utf-8'),
        who=obj.who.to_bytes() if obj.who else None,
        when=obj.when,
        version=obj.version)

  @auth.endpoints_method(
      VersionRequest, swarming_rpcs.FileContent,
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def get_bot_config(self, request):
    """Retrieves the current or a previous version of bot_config.py."""
    obj = bot_code.get_bot_config(request.version)
    if not obj:
      return swarming_rpcs.FileContent()
    return swarming_rpcs.FileContent(
        content=obj.content.decode('utf-8'),
        who=obj.who.to_bytes() if obj.who else None,
        when=obj.when,
        version=obj.version)

  @auth.endpoints_method(
      swarming_rpcs.FileContentRequest, swarming_rpcs.FileContent)
  @auth.require(acl.is_admin)
  def put_bootstrap(self, request):
    """Stores a new version of bootstrap.py."""
    key = bot_code.store_bootstrap(request.content.encode('utf-8'))
    obj = key.get()
    return swarming_rpcs.FileContent(
        who=obj.who.to_bytes() if obj.who else None,
        when=obj.created_ts,
        version=obj.version)

  @auth.endpoints_method(
      swarming_rpcs.FileContentRequest, swarming_rpcs.FileContent)
  @auth.require(acl.is_admin)
  def put_bot_config(self, request):
    """Stores a new version of bot_config.py."""
    key = bot_code.store_bot_config(request.content.encode('utf-8'))
    obj = key.get()
    return swarming_rpcs.FileContent(
        who=obj.who.to_bytes() if obj.who else None,
        when=obj.created_ts,
        version=obj.version)


TaskId = endpoints.ResourceContainer(
    message_types.VoidMessage,
    task_id=messages.StringField(1, required=True))


@swarming_api.api_class(resource_name='task', path='task')
class SwarmingTaskService(remote.Service):
  """Swarming's task-related API."""
  @auth.endpoints_method(
      TaskId, swarming_rpcs.TaskResult,
      name='result',
      path='{task_id}/result',
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def result(self, request):
    """Reports the result of the task corresponding to a task ID.

    It can be a 'run' ID specifying a specific retry or a 'summary' ID hidding
    the fact that a task may have been retried transparently, when a bot reports
    BOT_DIED.

    A summary ID ends with '0', a run ID ends with '1' or '2'.
    """
    logging.info('%s', request)
    return message_conversion.task_result_to_rpc(
        get_result_entity(request.task_id))

  @auth.endpoints_method(
      TaskId, swarming_rpcs.TaskRequest,
      name='request',
      path='{task_id}/request',
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def request(self, request):
    """Returns the task request corresponding to a task ID."""
    logging.info('%s', request)
    _, summary_key = get_result_key(request.task_id)
    request_key = task_pack.result_summary_key_to_request_key(summary_key)
    return message_conversion.task_request_to_rpc(get_or_raise(request_key))

  @auth.endpoints_method(
      TaskId, swarming_rpcs.CancelResponse,
      name='cancel',
      path='{task_id}/cancel')
  @auth.require(acl.is_admin)
  def cancel(self, request):
    """Cancels a task.

    If a bot was running the task, the bot will forcibly cancel the task.
    """
    logging.info('%s', request)
    summary_key = task_pack.unpack_result_summary_key(request.task_id)
    ok, was_running = task_scheduler.cancel_task(summary_key)
    return swarming_rpcs.CancelResponse(ok=ok, was_running=was_running)

  @auth.endpoints_method(
      TaskId, swarming_rpcs.TaskOutput,
      name='stdout',
      path='{task_id}/stdout',
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def stdout(self, request):
    """Returns the output of the task corresponding to a task ID."""
    # TODO(maruel): Add streaming. Real streaming is not supported by AppEngine
    # v1.
    # TODO(maruel): Send as raw content instead of encoded. This is not
    # supported by cloud endpoints.
    logging.info('%s', request)
    result = get_result_entity(request.task_id)
    output = result.get_command_output_async(0).get_result()
    if output:
      output = output.decode('utf-8', 'replace')
    return swarming_rpcs.TaskOutput(output=output)


@swarming_api.api_class(resource_name='tasks', path='tasks')
class SwarmingTasksService(remote.Service):
  """Swarming's tasks-related API."""
  @auth.endpoints_method(
      swarming_rpcs.NewTaskRequest, swarming_rpcs.TaskRequestMetadata)
  @auth.require(acl.is_bot_or_user)
  def new(self, request):
    """Creates a new task.

    The task will be enqueued in the tasks list and will be executed at the
    earliest opportunity by a bot that has at least the dimensions as described
    in the task request.
    """
    logging.info('%s', request)
    try:
      request = message_conversion.new_task_request_from_rpc(
          request, utils.utcnow())
      posted_request = task_request.make_request(request, acl.is_bot_or_admin())
    except (datastore_errors.BadValueError, TypeError, ValueError) as e:
      raise endpoints.BadRequestException(e.message)

    result_summary = task_scheduler.schedule_request(posted_request)

    previous_result = None
    if result_summary.deduped_from:
      previous_result = message_conversion.task_result_to_rpc(result_summary)

    return swarming_rpcs.TaskRequestMetadata(
        request=message_conversion.task_request_to_rpc(posted_request),
        task_id=task_pack.pack_result_summary_key(result_summary.key),
        task_result=previous_result)

  @auth.endpoints_method(
      swarming_rpcs.TasksRequest, swarming_rpcs.TaskList,
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def list(self, request):
    """Provides a list of available tasks."""
    logging.info('%s', request)
    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      now = utils.utcnow()
      query = task_result.get_result_summaries_query(
          start, end,
          request.sort.name.lower(),
          request.state.name.lower(),
          request.tags)
      items, cursor = datastore_utils.fetch_page(
          query, request.limit, request.cursor)
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for tasks/list: %s' % e)
    return swarming_rpcs.TaskList(
        cursor=cursor,
        items=[message_conversion.task_result_to_rpc(i) for i in items],
        now=now)

  @auth.endpoints_method(
      swarming_rpcs.TasksCountRequest, swarming_rpcs.TasksCount,
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def count(self, request):
    """Counts number of tasks in a given state."""
    logging.info('%s', request)
    if not request.start:
      raise endpoints.BadRequestException('start (as epoch) is required')
    if not request.end:
      raise endpoints.BadRequestException('end (as epoch) is required')
    try:
      now = utils.utcnow()
      query = task_result.get_result_summaries_query(
          message_conversion.epoch_to_datetime(request.start),
          message_conversion.epoch_to_datetime(request.end),
          'created_ts',
          request.state.name.lower(),
          request.tags)
      count = query.count()
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for tasks/count: %s' % e)
    return swarming_rpcs.TasksCount(count=count, now=now)


BotId = endpoints.ResourceContainer(
    message_types.VoidMessage,
    bot_id=messages.StringField(1, required=True))


BotEventsRequest = endpoints.ResourceContainer(
    swarming_rpcs.BotEventsRequest,
    bot_id=messages.StringField(1, required=True))


BotTasksRequest = endpoints.ResourceContainer(
    swarming_rpcs.BotTasksRequest,
    bot_id=messages.StringField(1, required=True))


@swarming_api.api_class(resource_name='bot', path='bot')
class SwarmingBotService(remote.Service):
  """Bot-related API. Permits querying information about the bot's properties"""
  @auth.endpoints_method(
      BotId, swarming_rpcs.BotInfo,
      name='get',
      path='{bot_id}/get',
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def get(self, request):
    """Returns information about a known bot.

    This includes its state and dimensions, and if it is currently running a
    task.
    """
    logging.info('%s', request)
    bot = get_or_raise(bot_management.get_info_key(request.bot_id))
    return message_conversion.bot_info_to_rpc(bot, utils.utcnow())

  @auth.endpoints_method(
      BotId, swarming_rpcs.DeletedResponse,
      name='delete',
      path='{bot_id}/delete')
  @auth.require(acl.is_admin)
  def delete(self, request):
    """Deletes the bot corresponding to a provided bot_id.

    At that point, the bot will not appears in the list of bots but it is still
    possible to get information about the bot with its bot id is known, as
    historical data is not deleted.

    It is meant to remove from the DB the presence of a bot that was retired,
    e.g. the VM was shut down already. Use 'terminate' instead of the bot is
    still alive.
    """
    logging.info('%s', request)
    bot_key = bot_management.get_info_key(request.bot_id)
    get_or_raise(bot_key)  # raises 404 if there is no such bot
    bot_key.delete()
    return swarming_rpcs.DeletedResponse(deleted=True)

  @auth.endpoints_method(
      BotEventsRequest, swarming_rpcs.BotEvents,
      name='events',
      path='{bot_id}/events',
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def events(self, request):
    """Returns events that happened on a bot."""
    logging.info('%s', request)
    try:
      now = utils.utcnow()
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      order = not (start or end)
      query = bot_management.get_events_query(request.bot_id, order)
      if not order:
        query = query.order(
            -bot_management.BotEvent.ts, bot_management.BotEvent.key)
      if start:
        query = query.filter(bot_management.BotEvent.ts >= start)
      if end:
        query = query.filter(bot_management.BotEvent.ts < end)
      items, cursor = datastore_utils.fetch_page(
          query, request.limit, request.cursor)
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for bot.events: %s' % e)
    return swarming_rpcs.BotEvents(
        cursor=cursor,
        items=[message_conversion.bot_event_to_rpc(r) for r in items],
        now=now)

  @auth.endpoints_method(
      BotId, swarming_rpcs.TerminateResponse,
      name='terminate',
      path='{bot_id}/terminate')
  @auth.require(acl.is_bot_or_admin)
  def terminate(self, request):
    """Asks a bot to terminate itself gracefully.

    The bot will stay in the DB, use 'delete' to remove it from the DB
    afterward. This request returns a pseudo-taskid that can be waited for to
    wait for the bot to turn down.
    """
    # TODO(maruel): Disallow a terminate task when there's one currently
    # pending or if the bot is considered 'dead', e.g. no contact since 10
    # minutes.
    logging.info('%s', request)
    bot_key = bot_management.get_info_key(request.bot_id)
    get_or_raise(bot_key)  # raises 404 if there is no such bot
    try:
      # Craft a special priority 0 task to tell the bot to shutdown.
      properties = task_request.TaskProperties(
          dimensions={u'id': request.bot_id},
          execution_timeout_secs=0,
          grace_period_secs=0,
          io_timeout_secs=0)
      now = utils.utcnow()
      request = task_request.TaskRequest(
          created_ts=now,
          expiration_ts=now + datetime.timedelta(days=1),
          name='Terminate %s' % request.bot_id,
          priority=0,
          properties=properties,
          tags=['terminate:1'],
          user=auth.get_current_identity().to_bytes())
      assert request.properties.is_terminate
      posted_request = task_request.make_request(request, acl.is_bot_or_admin())
    except (datastore_errors.BadValueError, TypeError, ValueError) as e:
      raise endpoints.BadRequestException(e.message)

    result_summary = task_scheduler.schedule_request(posted_request)
    return swarming_rpcs.TerminateResponse(
        task_id=task_pack.pack_result_summary_key(result_summary.key))

  @auth.endpoints_method(
      BotTasksRequest, swarming_rpcs.BotTasks,
      name='tasks',
      path='{bot_id}/tasks',
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def tasks(self, request):
    """Lists a given bot's tasks within the specified date range.

    In this case, the tasks are effectively TaskRunResult since it's individual
    task tries sent to this specific bot.

    It is impossible to search by both tags and bot id. If there's a need,
    TaskRunResult.tags will be added (via a copy from TaskRequest.tags).
    """
    logging.info('%s', request)
    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      now = utils.utcnow()
      query = task_result.get_run_results_query(
          start, end,
          request.sort.name.lower(),
          request.state.name.lower(),
          request.bot_id)
      items, cursor = datastore_utils.fetch_page(
          query, request.limit, request.cursor)
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate filter for bot.tasks: %s' % e)
    return swarming_rpcs.BotTasks(
        cursor=cursor,
        items=[message_conversion.task_result_to_rpc(r) for r in items],
        now=now)


@swarming_api.api_class(resource_name='bots', path='bots')
class SwarmingBotsService(remote.Service):
  """Bots-related API."""
  @auth.endpoints_method(
      swarming_rpcs.BotsRequest, swarming_rpcs.BotList,
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def list(self, request):
    """Provides list of known bots.

    Deleted bots will not be listed.
    """
    logging.info('%s', request)
    now = utils.utcnow()
    q = bot_management.BotInfo.query().order(bot_management.BotInfo.key)
    bots, cursor = datastore_utils.fetch_page(q, request.limit, request.cursor)
    return swarming_rpcs.BotList(
        cursor=cursor,
        death_timeout=config.settings().bot_death_timeout_secs,
        items=[message_conversion.bot_info_to_rpc(bot, now) for bot in bots],
        now=now)
