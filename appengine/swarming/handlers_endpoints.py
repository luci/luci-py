# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Swarming Server endpoints handlers."""

import json
import logging

from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
import endpoints
from protorpc import message_types
from protorpc import remote

from components import auth
from components import ereporter2
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
    raise endpoints.NotFoundException('Key %s not found.' % str(key))
  return result


def get_result_entity(task_id):
  """Returns the entity corresponding to a task ID.

  Can be either a TaskResultSummary or a TaskRunResult.
  """
  key, _ = get_result_key(task_id)
  return get_or_raise(key)


def _transform_request(request_dict):
  """Makes a task request compatible with the "old" API."""
  exp_secs = request_dict.pop('expiration_secs', None)
  if exp_secs is not None:
    request_dict['scheduling_expiration_secs'] = exp_secs
  request_dict.setdefault('properties', {})
  command = request_dict['properties'].pop('command', None)
  if command is not None:
    request_dict['properties']['commands'] = [command]
  data = request_dict['properties'].pop('data', None)
  if data is not None:
    request_dict['properties']['data'] = [
      [pair['key'], pair['value']] for pair in data]
  for key in ['dimensions', 'env']:
    old_list = request_dict['properties'].get(key, [])
    request_dict['properties'][key] = {
      pair['key']: pair['value'] for pair in old_list}
  request_dict['properties'].setdefault('data', [])


### API


swarming_api = auth.endpoints_api(name='swarming', version='v1')


@swarming_api.api_class(resource_name='tasks', path='tasks')
class SwarmingTaskService(remote.Service):
  """Swarming's task-related API."""

  ### API Methods

  @auth.endpoints_method(message_types.VoidMessage, swarming_rpcs.ServerDetails)
  @auth.require(acl.is_bot_or_user)
  def server_details(self, _request):
    """Returns information about the server."""
    return swarming_rpcs.ServerDetails(server_version=utils.get_app_version())

  @auth.endpoints_method(swarming_rpcs.TaskId, swarming_rpcs.TaskResultSummary)
  @auth.require(acl.is_bot_or_user)
  def result(self, request):
    """Reports the result of the task corresponding to a task ID."""
    entity = get_result_entity(request.task_id)
    return message_conversion.task_result_summary_from_dict(
        utils.to_json_encodable(entity))

  @auth.endpoints_method(swarming_rpcs.TaskId, swarming_rpcs.TaskRequest)
  @auth.require(acl.is_bot_or_user)
  def request(self, request):
    """Returns the task result corresponding to a task ID."""
    _, summary_key = get_result_key(request.task_id)
    request_key = task_pack.result_summary_key_to_request_key(summary_key)
    entity = get_or_raise(request_key)
    return message_conversion.task_request_from_dict(entity.to_dict())

  @auth.endpoints_method(swarming_rpcs.TaskId, swarming_rpcs.CancelResponse)
  @auth.require(acl.is_admin)
  def cancel(self, request):
    """Cancels a task and indicate success."""
    summary_key = task_pack.unpack_result_summary_key(request.task_id)
    ok, was_running = task_scheduler.cancel_task(summary_key)
    return swarming_rpcs.CancelResponse(ok=ok, was_running=was_running)

  @auth.endpoints_method(swarming_rpcs.TaskId, swarming_rpcs.TaskOutput)
  @auth.require(acl.is_bot_or_user)
  def result_output(self, request):
    """Reports the output of the task corresponding to a task ID."""
    result = get_result_entity(request.task_id)
    output = result.get_command_output_async(0).get_result()
    if output:
      output = output.decode('utf-8', 'replace')
    return swarming_rpcs.TaskOutput(output=output)

  @auth.endpoints_method(
      swarming_rpcs.TaskRequest, swarming_rpcs.TaskRequestMetadata,
      http_method='POST')
  @auth.require(acl.is_bot_or_user)
  def new(self, request):
    """Provides a TaskRequest and receive its metadata."""
    request_dict = json.loads(remote.protojson.encode_message(request))
    _transform_request(request_dict)

    # If the priority is below 100, make the the user has right to do so.
    if request_dict.get('priority', 255) < 100 and not acl.is_bot_or_admin():
      # Silently drop the priority of normal users.
      request_dict['priority'] = 100

    try:
      posted_request = task_request.make_request(request_dict)
    except (datastore_errors.BadValueError, TypeError, ValueError) as e:
      raise endpoints.BadRequestException(e.message)

    result_summary = task_scheduler.schedule_request(posted_request)
    posted_dict = utils.to_json_encodable(posted_request)
    return swarming_rpcs.TaskRequestMetadata(
        request=message_conversion.task_request_from_dict(posted_dict),
        task_id=task_pack.pack_result_summary_key(result_summary.key))

  @auth.endpoints_method(swarming_rpcs.TasksRequest, swarming_rpcs.TaskList)
  @auth.require(acl.is_privileged_user)
  def list(self, request):
    """Provides a list of available tasks."""
    state = request.state.name.lower()
    uses = sum([
        request.name is not None, bool(request.tag), state != 'all'])
    if uses > 1:
      raise endpoints.BadRequestException(
          'Only one of name, tag (1 or many) or state can be used.')

    # get the tasks
    items, cursor_str, sort, state = task_result.get_tasks(
        request.name, request.tag, request.cursor, request.limit, request.sort,
        state)
    return swarming_rpcs.TaskList(
        cursor=cursor_str,
        items=[message_conversion.task_result_summary_from_dict(
            utils.to_json_encodable(item)) for item in items],
        limit=request.limit,
        sort=sort,
        state=state)


@swarming_api.api_class(resource_name='bots')
class SwarmingBotService(remote.Service):
  """Swarming's bot-related API."""

  @auth.endpoints_method(
      swarming_rpcs.BotId, swarming_rpcs.BotInfo, http_method='GET')
  @auth.require(acl.is_privileged_user)
  def get(self, request):
    """Provides BotInfo corresponding to a provided bot_id."""
    bot = get_or_raise(bot_management.get_info_key(request.bot_id))
    entity_dict = bot.to_dict_with_now(utils.utcnow())
    return message_conversion.bot_info_from_dict(entity_dict)

  @auth.endpoints_method(
      swarming_rpcs.BotId, swarming_rpcs.DeletedResponse, http_method='DELETE')
  @auth.require(acl.is_admin)
  def delete(self, request):
    """Deletes the bot corresponding to a provided bot_id."""
    bot_key = bot_management.get_info_key(request.bot_id)
    get_or_raise(bot_key)  # raises 404 if there is no such bot
    bot_key.delete()
    return swarming_rpcs.DeletedResponse(deleted=True)

  @auth.endpoints_method(
      swarming_rpcs.BotTasksRequest, swarming_rpcs.BotTask,
      http_method='GET')
  @auth.require(acl.is_privileged_user)
  def tasks(self, request):
    """Lists all of a given bot's tasks."""
    cursor = datastore_query.Cursor(urlsafe=request.cursor)
    run_results, cursor, more = task_result.TaskRunResult.query(
        task_result.TaskRunResult.bot_id == request.bot_id).order(
            -task_result.TaskRunResult.started_ts).fetch_page(
                request.limit, start_cursor=cursor)
    result_list = [message_conversion.task_run_result_from_dict(
        utils.to_json_encodable(run_result)) for run_result in run_results]
    response = swarming_rpcs.BotTask(
        cursor=cursor.urlsafe() if cursor and more else None,
        items=result_list,
        limit=request.limit,
        now=utils.utcnow())
    return response

  @auth.endpoints_method(
      swarming_rpcs.BotsRequest, swarming_rpcs.BotList, http_method='GET')
  @auth.require(acl.is_privileged_user)
  def list(self, request):
    """Provides list of bots."""
    now = utils.utcnow()
    cursor = datastore_query.Cursor(urlsafe=request.cursor)
    q = bot_management.BotInfo.query().order(bot_management.BotInfo.key)
    bots, cursor, more = q.fetch_page(request.limit, start_cursor=cursor)
    return swarming_rpcs.BotList(
        cursor=cursor.urlsafe() if cursor and more else None,
        death_timeout=config.settings().bot_death_timeout_secs,
        items=[message_conversion.bot_info_from_dict(bot.to_dict_with_now(
            now)) for bot in bots],
        limit=request.limit,
        now=now)


def create_application():
  ereporter2.register_formatter()
  return endpoints.api_server([swarming_api])


app = create_application()
