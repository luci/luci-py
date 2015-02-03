# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming client REST APIs handlers."""

import base64
import json
import logging
import textwrap

import webapp2

from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
from google.appengine import runtime
from google.appengine.ext import ndb

from components import auth
from components import ereporter2
from components import utils
from server import acl
from server import config
from server import bot_code
from server import bot_management
from server import stats
from server import task_pack
from server import task_result
from server import task_scheduler
from server import task_to_run


def has_unexpected_subset_keys(expected_keys, minimum_keys, actual_keys, name):
  """Returns an error if unexpected keys are present or expected keys are
  missing.

  Accepts optional keys.

  This is important to catch typos.
  """
  actual_keys = frozenset(actual_keys)
  superfluous = actual_keys - expected_keys
  missing = minimum_keys - actual_keys
  if superfluous or missing:
    msg_missing = (' missing: %s' % sorted(missing)) if missing else ''
    msg_superfluous = (
        (' superfluous: %s' % sorted(superfluous)) if superfluous else '')
    return 'Unexpected %s%s%s; did you make a typo?' % (
        name, msg_missing, msg_superfluous)


def log_unexpected_subset_keys(
    expected_keys, minimum_keys, actual_keys, request, source, name):
  """Logs an error if unexpected keys are present or expected keys are missing.

  Accepts optional keys.

  This is important to catch typos.
  """
  message = has_unexpected_subset_keys(
    expected_keys, minimum_keys, actual_keys, name)
  if message:
    ereporter2.log_request(request, source=source, message=message)
  return message


def log_unexpected_keys(expected_keys, actual_keys, request, source, name):
  """Logs an error if unexpected keys are present or expected keys are missing.
  """
  return log_unexpected_subset_keys(
      expected_keys, expected_keys, actual_keys, request, source, name)


def process_doc(handler):
  lines = handler.__doc__.rstrip().splitlines()
  rest = textwrap.dedent('\n'.join(lines[1:]))
  return '\n'.join((lines[0], rest)).rstrip()


### New Client APIs.


class ClientApiListHandler(auth.ApiHandler):
  """All query handlers"""

  @auth.public
  def get(self):
    # Hard to make it any simpler.
    prefix = '/swarming/api/v1/client/'
    data = {
      r.template[len(prefix):]: process_doc(r.handler) for r in get_routes()
      if r.template.startswith(prefix) and hasattr(r.handler, 'get')
    }
    self.send_response(data)


class ClientHandshakeHandler(auth.ApiHandler):
  """First request to be called to get initial data like XSRF token.

  Request body is a JSON dict:
    {
      # TODO(maruel): Add useful data.
    }

  Response body is a JSON dict:
    {
      "server_version": "138-193f1f3",
      "xsrf_token": "......",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  EXPECTED_KEYS = frozenset()

  @auth.require_xsrf_token_request
  @auth.require(acl.is_bot_or_user)
  def post(self):
    request = self.parse_body()
    log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'client', 'keys')
    data = {
      # This access token will be used to validate each subsequent request.
      'server_version': utils.get_app_version(),
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.send_response(data)


class ClientTaskResultBase(auth.ApiHandler):
  """Implements the common base code for task related query APIs."""

  def get_result_key(self, task_id):
    # TODO(maruel): Users can only request their own task. Privileged users can
    # request any task.
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
        self.abort_with_error(400, error='Invalid key')
    return key, summary_key

  def get_result_entity(self, task_id):
    key, _ = self.get_result_key(task_id)
    result = key.get()
    if not result:
      self.abort_with_error(404, error='Task not found')
    return result


class ClientTaskResultHandler(ClientTaskResultBase):
  """Task's result meta data"""

  @auth.require(acl.is_bot_or_user)
  def get(self, task_id):
    result = self.get_result_entity(task_id)
    self.send_response(utils.to_json_encodable(result))


class ClientTaskResultRequestHandler(ClientTaskResultBase):
  """Task's request details"""

  @auth.require(acl.is_bot_or_user)
  def get(self, task_id):
    _, summary_key = self.get_result_key(task_id)
    request_key = task_pack.result_summary_key_to_request_key(summary_key)
    self.send_response(utils.to_json_encodable(request_key.get()))


class ClientTaskResultOutputHandler(ClientTaskResultBase):
  """Task's output for a single command"""

  @auth.require(acl.is_bot_or_user)
  def get(self, task_id, command_index):
    result = self.get_result_entity(task_id)
    output = result.get_command_output_async(int(command_index)).get_result()
    if output:
      output = output.decode('utf-8', 'replace')
    # JSON then reencodes to ascii compatible encoded strings, which explodes
    # the size.
    data = {
      'output': output,
    }
    self.send_response(utils.to_json_encodable(data))


class ClientTaskResultOutputAllHandler(ClientTaskResultBase):
  """All output from all commands in a task"""

  @auth.require(acl.is_bot_or_user)
  def get(self, task_id):
    result = self.get_result_entity(task_id)
    # JSON then reencodes to ascii compatible encoded strings, which explodes
    # the size.
    data = {
      'outputs': [
        i.decode('utf-8', 'replace') if i else i
        for i in result.get_outputs()
      ],
    }
    self.send_response(utils.to_json_encodable(data))


class ClientApiTasksHandler(auth.ApiHandler):
  """Requests all TaskResultSummary with filters.

  It is specifically a GET with query parameters for simplicity instead of a
  JSON POST.

  Arguments:
    name: Search by task name; str or None.
    tag: Search by task tag, can be used mulitple times; list(str) or None.
    cursor: Continue a previous query; str or None.
    limit: Maximum number of items to return.
    sort: Ordering: 'created_ts', 'modified_ts', 'completed_ts', 'abandoned_ts'.
        Defaults to 'created_ts'.
    state: Filtering: 'all', 'pending', 'running', 'pending_running',
        'completed', 'completed_success', 'completed_failure', 'bot_died',
        'expired', 'canceled'. Defaults to 'all'.

  In particular, one of `name`, `tag` or `state` can be used
  exclusively.
  """
  EXPECTED = {'cursor', 'limit', 'name', 'sort', 'state', 'tag'}

  @auth.require(acl.is_privileged_user)
  def get(self):
    extra = frozenset(self.request.GET) - self.EXPECTED
    if extra:
      self.abort_with_error(
          400,
          error='Extraneous query parameters. Did you make a typo? %s' %
          ','.join(sorted(extra)))

    # Use a similar query to /user/tasks.
    name = self.request.get('name')
    tags = self.request.get_all('tag')
    cursor_str = self.request.get('cursor')
    limit = int(self.request.get('limit', 100))
    sort = self.request.get('sort', 'created_ts')
    state = self.request.get('state', 'all')

    uses = bool(name) + bool(tags) + bool(state!='all')
    if uses > 1:
      self.abort_with_error(
          400, error='Only one of name, tag (1 or many) or state can be used')

    items, cursor_str, sort, state = task_result.get_tasks(
        name, tags, cursor_str, limit, sort, state)
    data = {
      'cursor': cursor_str,
      'items': items,
      'limit': limit,
      'sort': sort,
      'state': state,
    }
    self.send_response(utils.to_json_encodable(data))


class ClientApiBots(auth.ApiHandler):
  """Bots known to the server"""

  @auth.require(acl.is_privileged_user)
  def get(self):
    now = utils.utcnow()
    limit = int(self.request.get('limit', 1000))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    q = bot_management.BotInfo.query().order(bot_management.BotInfo.key)
    bots, cursor, more = q.fetch_page(limit, start_cursor=cursor)
    data = {
      'cursor': cursor.urlsafe() if cursor and more else None,
      'death_timeout': config.settings().bot_death_timeout_secs,
      'items': [b.to_dict_with_now(now) for b in bots],
      'limit': limit,
      'now': now,
    }
    self.send_response(utils.to_json_encodable(data))


class ClientApiBot(auth.ApiHandler):
  """Bot's meta data"""

  @auth.require(acl.is_privileged_user)
  def get(self, bot_id):
    bot = bot_management.get_info_key(bot_id).get()
    if not bot:
      self.abort_with_error(404, error='Bot not found')
    now = utils.utcnow()
    self.send_response(utils.to_json_encodable(bot.to_dict_with_now(now)))

  @auth.require(acl.is_admin)
  def delete(self, bot_id):
    # Only delete BotInfo, not BotRoot, BotEvent nor BotSettings.
    bot_key = bot_management.get_info_key(bot_id)
    found = False
    if bot_key.get():
      bot_key.delete()
      found = True
    self.send_response({'deleted': bool(found)})


class ClientApiBotTask(auth.ApiHandler):
  """Tasks executed on a specific bot"""

  @auth.require(acl.is_privileged_user)
  def get(self, bot_id):
    limit = int(self.request.get('limit', 100))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    run_results, cursor, more = task_result.TaskRunResult.query(
        task_result.TaskRunResult.bot_id == bot_id).order(
            -task_result.TaskRunResult.started_ts).fetch_page(
                limit, start_cursor=cursor)
    now = utils.utcnow()
    data = {
      'cursor': cursor.urlsafe() if cursor and more else None,
      'items': run_results,
      'limit': limit,
      'now': now,
    }
    self.send_response(utils.to_json_encodable(data))


class ClientApiServer(auth.ApiHandler):
  """Server details"""

  @auth.require(acl.is_privileged_user)
  def get(self):
    data = {
      'bot_version': bot_code.get_bot_version(self.request.host_url),
    }
    self.send_response(utils.to_json_encodable(data))


class ClientRequestHandler(auth.ApiHandler):
  """Creates a new request, returns all the meta data about the request."""
  @auth.require(acl.is_bot_or_user)
  def post(self):
    request = self.parse_body()
    # If the priority is below 100, make the the user has right to do so.
    if request.get('priority', 255) < 100 and not acl.is_bot_or_admin():
      # Silently drop the priority of normal users.
      request['priority'] = 100

    try:
      request, result_summary = task_scheduler.make_request(request)
    except (datastore_errors.BadValueError, TypeError, ValueError) as e:
      self.abort_with_error(400, error=str(e))

    data = {
      'request': request.to_dict(),
      'task_id': task_pack.pack_result_summary_key(result_summary.key),
    }
    self.send_response(utils.to_json_encodable(data))


class ClientCancelHandler(auth.ApiHandler):
  """Cancels a task."""

  # TODO(maruel): Allow privileged users to cancel, and users to cancel their
  # own task.
  @auth.require(acl.is_admin)
  def post(self):
    request = self.parse_body()
    task_id = request.get('task_id')
    summary_key = task_pack.unpack_result_summary_key(task_id)

    ok, was_running = task_scheduler.cancel_task(summary_key)
    out = {
      'ok': ok,
      'was_running': was_running,
    }
    self.send_response(out)


def get_routes():
  routes = [
      ('/swarming/api/v1/client/bots', ClientApiBots),
      ('/swarming/api/v1/client/bot/<bot_id:[^/]+>', ClientApiBot),
      ('/swarming/api/v1/client/bot/<bot_id:[^/]+>/tasks', ClientApiBotTask),
      ('/swarming/api/v1/client/cancel', ClientCancelHandler),
      ('/swarming/api/v1/client/handshake', ClientHandshakeHandler),
      ('/swarming/api/v1/client/list', ClientApiListHandler),
      ('/swarming/api/v1/client/request', ClientRequestHandler),
      ('/swarming/api/v1/client/server', ClientApiServer),
      ('/swarming/api/v1/client/task/<task_id:[0-9a-f]+>',
          ClientTaskResultHandler),
      ('/swarming/api/v1/client/task/<task_id:[0-9a-f]+>/request',
          ClientTaskResultRequestHandler),
      ('/swarming/api/v1/client/task/<task_id:[0-9a-f]+>/output/'
        '<command_index:[0-9]+>',
          ClientTaskResultOutputHandler),
      ('/swarming/api/v1/client/task/<task_id:[0-9a-f]+>/output/all',
          ClientTaskResultOutputAllHandler),
      ('/swarming/api/v1/client/tasks', ClientApiTasksHandler),
  ]
  return [webapp2.Route(*i) for i in routes]
