# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Swarming Server endpoints handlers."""

import datetime
import json
import logging

from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from components import auth
from components import utils

import message_conversion
import swarming_rpcs
from server import acl
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


@swarming_api.api_class(resource_name='server', path='server')
class SwarmingServerService(remote.Service):
  @auth.endpoints_method(
      message_types.VoidMessage, swarming_rpcs.ServerDetails,
      http_method='GET')
  @auth.require(acl.is_bot_or_user)
  def details(self, _request):
    """Returns information about the server."""
    return swarming_rpcs.ServerDetails(server_version=utils.get_app_version())


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
    return swarming_rpcs.TaskRequestMetadata(
        request=message_conversion.task_request_to_rpc(posted_request),
        task_id=task_pack.pack_result_summary_key(result_summary.key))

  @auth.endpoints_method(
      swarming_rpcs.TasksRequest, swarming_rpcs.TaskList,
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def list(self, request):
    """Provides a list of available tasks."""
    logging.info('%s', request)
    state = request.state.name.lower()
    uses = sum([bool(request.tags), state != 'all'])
    if state != 'all':
      raise endpoints.BadRequestException(
          'Querying by state is not yet supported. '
          'Received argument state=%s.' % state)
    if uses > 1:
      raise endpoints.BadRequestException(
          'Only one of tag (1 or many) or state can be used.')

    # get the tasks
    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      items, cursor_str, state = task_result.get_result_summaries(
          request.tags, request.cursor, start, end, state, request.limit)
      return swarming_rpcs.TaskList(
          cursor=cursor_str,
          items=[message_conversion.task_result_to_rpc(i) for i in items])
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate limit for tasks/list: %s' % e)


BotId = endpoints.ResourceContainer(
    message_types.VoidMessage,
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
    """
    logging.info('%s', request)
    bot_key = bot_management.get_info_key(request.bot_id)
    get_or_raise(bot_key)  # raises 404 if there is no such bot
    bot_key.delete()
    return swarming_rpcs.DeletedResponse(deleted=True)

  @auth.endpoints_method(
      BotTasksRequest, swarming_rpcs.BotTasks,
      name='tasks',
      path='{bot_id}/tasks',
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def tasks(self, request):
    """Lists a given bot's tasks within the specified date range."""
    logging.info('%s', request)
    try:
      start = message_conversion.epoch_to_datetime(request.start)
      end = message_conversion.epoch_to_datetime(request.end)
      run_results, cursor, more = task_result.get_run_results(
          request.cursor, request.bot_id, start, end, request.limit)
    except ValueError as e:
      raise endpoints.BadRequestException(
          'Inappropriate limit for bots/list: %s' % e)
    return swarming_rpcs.BotTasks(
        cursor=cursor.urlsafe() if cursor and more else None,
        items=[message_conversion.task_result_to_rpc(r) for r in run_results],
        now=utils.utcnow())


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
    cursor = datastore_query.Cursor(urlsafe=request.cursor)
    q = bot_management.BotInfo.query().order(bot_management.BotInfo.key)
    bots, cursor, more = q.fetch_page(request.limit, start_cursor=cursor)
    return swarming_rpcs.BotList(
        cursor=cursor.urlsafe() if cursor and more else None,
        death_timeout=config.settings().bot_death_timeout_secs,
        items=[message_conversion.bot_info_to_rpc(bot, now) for bot in bots],
        now=now)
