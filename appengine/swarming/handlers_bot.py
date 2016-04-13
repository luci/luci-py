# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Internal bot API handlers."""

import base64
import json
import logging
import textwrap

import webob
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
from server import bot_code
from server import bot_management
from server import stats
from server import task_pack
from server import task_request
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


def has_unexpected_keys(expected_keys, actual_keys, name):
  """Return an error if unexpected keys are present or expected keys are
  missing.
  """
  return has_unexpected_subset_keys(
      expected_keys, expected_keys, actual_keys, name)


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


def has_missing_keys(minimum_keys, actual_keys, name):
  """Returns an error if expected keys are not present.

  Do not warn about unexpected keys.
  """
  actual_keys = frozenset(actual_keys)
  missing = minimum_keys - actual_keys
  if missing:
    msg_missing = (' missing: %s' % sorted(missing)) if missing else ''
    return 'Unexpected %s%s; did you make a typo?' % (name, msg_missing)


class BootstrapHandler(auth.AuthenticatingHandler):
  """Returns python code to run to bootstrap a swarming bot."""

  @auth.require(acl.is_bot)
  def get(self):
    self.response.headers['Content-Type'] = 'text/x-python'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot_bootstrap.py"')
    self.response.out.write(
        bot_code.get_bootstrap(self.request.host_url).content)


class BotCodeHandler(auth.AuthenticatingHandler):
  """Returns a zip file with all the files required by a bot.

  Optionally specify the hash version to download. If so, the returned data is
  cacheable.
  """

  @auth.require(acl.is_bot)
  def get(self, version=None):
    if version:
      expected = bot_code.get_bot_version(self.request.host_url)
      if version != expected:
        # This can happen when the server is rapidly updated.
        logging.error('Requested Swarming bot %s, have %s', version, expected)
        self.abort(404)
      self.response.headers['Cache-Control'] = 'public, max-age=3600'
    else:
      self.response.headers['Cache-Control'] = 'no-cache, no-store'
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot.zip"')
    self.response.out.write(
        bot_code.get_swarming_bot_zip(self.request.host_url))


class _BotBaseHandler(auth.ApiHandler):
  """
  Request body is a JSON dict:
    {
      "dimensions": <dict of properties>,
      "state": <dict of properties>,
      "version": <sha-1 of swarming_bot.zip uncompressed content>,
    }
  """

  EXPECTED_KEYS = {u'dimensions', u'state', u'version'}
  REQUIRED_STATE_KEYS = {u'running_time', u'sleep_streak'}

  # TODO(vadimsh): Remove once bots use X-Whitelisted-Bot-Id or OAuth.
  xsrf_token_enforce_on = ()

  def _process(self):
    """Returns True if the bot has invalid parameter and should be automatically
    quarantined.

    Does one DB synchronous GET.

    Returns:
      tuple(request, bot_id, version, state, dimensions, quarantined_msg)
    """
    request = self.parse_body()
    version = request.get('version', None)

    dimensions = request.get('dimensions', {})
    state = request.get('state', {})
    bot_id = None
    if dimensions.get('id'):
      dimension_id = dimensions['id']
      if (isinstance(dimension_id, list) and len(dimension_id) == 1
          and isinstance(dimension_id[0], unicode)):
        bot_id = dimensions['id'][0]

    # The bot may decide to "self-quarantine" itself. Accept both via
    # dimensions or via state. See bot_management._BotCommon.quarantined for
    # more details.
    if (bool(dimensions.get('quarantined')) or
        bool(state.get('quarantined'))):
      return request, bot_id, version, state, dimensions, 'Bot self-quarantined'

    quarantined_msg = None
    # Use a dummy 'for' to be able to break early from the block.
    for _ in [0]:

      quarantined_msg = has_unexpected_keys(
          self.EXPECTED_KEYS, request, 'keys')
      if quarantined_msg:
        break

      quarantined_msg = has_missing_keys(
          self.REQUIRED_STATE_KEYS, state, 'state')
      if quarantined_msg:
        break

      if not bot_id:
        quarantined_msg = 'Missing bot id'
        break

      if not all(
          isinstance(key, unicode) and
          isinstance(values, list) and
          all(isinstance(value, unicode) for value in values)
          for key, values in dimensions.iteritems()):
        quarantined_msg = (
            'Invalid dimensions type:\n%s' % json.dumps(dimensions,
              sort_keys=True, indent=2, separators=(',', ': ')))
        break

      dimensions_count = task_to_run.dimensions_powerset_count(dimensions)
      if dimensions_count > task_to_run.MAX_DIMENSIONS:
        quarantined_msg = 'Dimensions product %d is too high' % dimensions_count
        break

      if not isinstance(
          state.get('lease_expiration_ts'), (None.__class__, int)):
        quarantined_msg = (
            'lease_expiration_ts (%r) must be int or None' % (
                state['lease_expiration_ts']))
        break

    if quarantined_msg:
      line = 'Quarantined Bot\nhttps://%s/restricted/bot/%s\n%s' % (
          app_identity.get_default_version_hostname(), bot_id,
          quarantined_msg)
      ereporter2.log_request(self.request, source='bot', message=line)
      return request, bot_id, version, state, dimensions, quarantined_msg

    # Look for admin enforced quarantine.
    bot_settings = bot_management.get_settings_key(bot_id).get()
    if bool(bot_settings and bot_settings.quarantined):
      return request, bot_id, version, state, dimensions, 'Quarantined by admin'

    return request, bot_id, version, state, dimensions, None


class BotHandshakeHandler(_BotBaseHandler):
  """First request to be called to get initial data like XSRF token.

  The bot is server-controled so the server doesn't have to support multiple API
  version. When running a task, the bot sync the the version specific URL. Once
  abot finished its currently running task, it'll be immediately be upgraded
  after on its next poll.

  This endpoint does not return commands to the bot, for example to upgrade
  itself. It'll be told so when it does its first poll.

  Response body is a JSON dict:
    {
      "bot_version": <sha-1 of swarming_bot.zip uncompressed content>,
      "server_version": "138-193f1f3",
      "xsrf_token": "......",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  @auth.require_xsrf_token_request
  @auth.require(acl.is_bot)
  def post(self):
    (_request, bot_id, version, state,
        dimensions, quarantined_msg) = self._process()
    bot_management.bot_event(
        event_type='bot_connected', bot_id=bot_id,
        external_ip=self.request.remote_addr, dimensions=dimensions,
        state=state, version=version, quarantined=bool(quarantined_msg),
        task_id='', task_name=None, message=quarantined_msg)

    data = {
      # This access token will be used to validate each subsequent request.
      'bot_version': bot_code.get_bot_version(self.request.host_url),
      'expiration_sec': auth.handler.XSRFToken.expiration_sec,
      'server_version': utils.get_app_version(),
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.send_response(data)


class BotPollHandler(_BotBaseHandler):
  """The bot polls for a task; returns either a task, update command or sleep.

  In case of exception on the bot, this is enough to get it just far enough to
  eventually self-update to a working version. This is to ensure that coding
  errors in bot code doesn't kill all the fleet at once, they should still be up
  just enough to be able to self-update again even if they don't get task
  assigned anymore.
  """

  @auth.require(acl.is_bot)
  def post(self):
    """Handles a polling request.

    Be very permissive on missing values. This can happen because of errors
    on the bot, *we don't want to deny them the capacity to update*, so that the
    bot code is eventually fixed and the bot self-update to this working code.

    It makes recovery of the fleet in case of catastrophic failure much easier.
    """
    (_request, bot_id, version, state,
        dimensions, quarantined_msg) = self._process()
    sleep_streak = state.get('sleep_streak', 0)
    quarantined = bool(quarantined_msg)

    # Note bot existence at two places, one for stats at 1 minute resolution,
    # the other for the list of known bots.
    action = 'bot_inactive' if quarantined else 'bot_active'
    stats.add_entry(action=action, bot_id=bot_id, dimensions=dimensions)

    def bot_event(event_type, task_id=None, task_name=None):
      bot_management.bot_event(
          event_type=event_type, bot_id=bot_id,
          external_ip=self.request.remote_addr, dimensions=dimensions,
          state=state, version=version, quarantined=quarantined,
          task_id=task_id, task_name=task_name, message=quarantined_msg)

    # Bot version is host-specific because the host URL is embedded in
    # swarming_bot.zip
    expected_version = bot_code.get_bot_version(self.request.host_url)
    if version != expected_version:
      bot_event('request_update')
      self._cmd_update(expected_version)
      return
    if quarantined:
      bot_event('request_sleep')
      self._cmd_sleep(sleep_streak, quarantined)
      return

    #
    # At that point, the bot should be in relatively good shape since it's
    # running the right version. It is still possible that invalid code was
    # pushed to the server, so be diligent about it.
    #

    # Bot may need a reboot if it is running for too long. We do not reboot
    # quarantined bots.
    needs_restart, restart_message = bot_management.should_restart_bot(
        bot_id, state)
    if needs_restart:
      bot_event('request_restart')
      self._cmd_restart(restart_message)
      return

    # The bot is in good shape. Try to grab a task.
    try:
      # This is a fairly complex function call, exceptions are expected.
      request, run_result = task_scheduler.bot_reap_task(
          dimensions, bot_id, version, state.get('lease_expiration_ts'))
      if not request:
        # No task found, tell it to sleep a bit.
        bot_event('request_sleep')
        self._cmd_sleep(sleep_streak, quarantined)
        return

      try:
        # This part is tricky since it intentionally runs a transaction after
        # another one.
        if request.properties.is_terminate:
          bot_event('bot_terminate', task_id=run_result.task_id)
          self._cmd_terminate(run_result.task_id)
        else:
          bot_event(
              'request_task', task_id=run_result.task_id,
              task_name=request.name)
          self._cmd_run(request, run_result.key, bot_id)
      except:
        logging.exception('Dang, exception after reaping')
        raise
    except runtime.DeadlineExceededError:
      # If the timeout happened before a task was assigned there is no problems.
      # If the timeout occurred after a task was assigned, that task will
      # timeout (BOT_DIED) since the bot didn't get the details required to
      # run it) and it will automatically get retried (TODO) when the task times
      # out.
      # TODO(maruel): Note the task if possible and hand it out on next poll.
      # https://code.google.com/p/swarming/issues/detail?id=130
      self.abort(500, 'Deadline')

  def _cmd_run(self, request, run_result_key, bot_id):
    cmd = None
    if request.properties.commands:
      cmd = request.properties.commands[0]
    elif request.properties.command:
      cmd = request.properties.command
    out = {
      'cmd': 'run',
      'manifest': {
        'bot_id': bot_id,
        'command': cmd,
        'dimensions': request.properties.dimensions,
        'env': request.properties.env,
        'extra_args': request.properties.extra_args,
        'grace_period': request.properties.grace_period_secs,
        'hard_timeout': request.properties.execution_timeout_secs,
        'host': utils.get_versioned_hosturl(),
        'io_timeout': request.properties.io_timeout_secs,
        'inputs_ref': request.properties.inputs_ref,
        'task_id': task_pack.pack_run_result_key(run_result_key),
      },
    }
    self.send_response(utils.to_json_encodable(out))

  def _cmd_sleep(self, sleep_streak, quarantined):
    out = {
      'cmd': 'sleep',
      'duration': task_scheduler.exponential_backoff(sleep_streak),
      'quarantined': quarantined,
    }
    self.send_response(out)

  def _cmd_terminate(self, task_id):
    out = {
      'cmd': 'terminate',
      'task_id': task_id,
    }
    self.send_response(out)

  def _cmd_update(self, expected_version):
    out = {
      'cmd': 'update',
      'version': expected_version,
    }
    self.send_response(out)

  def _cmd_restart(self, message):
    logging.info('Rebooting bot: %s', message)
    out = {
      'cmd': 'restart',
      'message': message,
    }
    self.send_response(out)


class BotEventHandler(_BotBaseHandler):
  """On signal that a bot had an event worth logging."""

  EXPECTED_KEYS = _BotBaseHandler.EXPECTED_KEYS | {u'event', u'message'}

  @auth.require(acl.is_bot)
  def post(self):
    (request, bot_id, version, state,
        dimensions, quarantined_msg) = self._process()
    event = request.get('event')
    if event not in ('bot_error', 'bot_rebooting', 'bot_shutdown'):
      self.abort_with_error(400, error='Unsupported event type')
    message = request.get('message')
    bot_management.bot_event(
        event_type=event, bot_id=bot_id, external_ip=self.request.remote_addr,
        dimensions=dimensions, state=state, version=version,
        quarantined=bool(quarantined_msg), task_id=None, task_name=None,
        message=message)

    if event == 'bot_error':
      line = (
          'Bot: https://%s/restricted/bot/%s\n'
          'Bot error:\n'
          '%s') % (
          app_identity.get_default_version_hostname(), bot_id, message)
      ereporter2.log_request(self.request, source='bot', message=line)
    self.send_response({})


class BotTaskUpdateHandler(auth.ApiHandler):
  """Receives updates from a Bot for a task.

  The handler verifies packets are processed in order and will refuse
  out-of-order packets.
  """
  ACCEPTED_KEYS = {
    u'bot_overhead', u'cost_usd', u'duration', u'exit_code',
    u'hard_timeout', u'id', u'io_timeout', u'isolated_stats', u'output',
    u'output_chunk_start', u'outputs_ref', u'task_id',
  }
  REQUIRED_KEYS = {u'id', u'task_id'}

  # TODO(vadimsh): Remove once bots use X-Whitelisted-Bot-Id or OAuth.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot)
  def post(self, task_id=None):
    # Unlike handshake and poll, we do not accept invalid keys here. This code
    # path is much more strict.
    request = self.parse_body()
    msg = log_unexpected_subset_keys(
        self.ACCEPTED_KEYS, self.REQUIRED_KEYS, request, self.request, 'bot',
        'keys')
    if msg:
      self.abort_with_error(400, error=msg)

    bot_id = request['id']
    cost_usd = request['cost_usd']
    task_id = request['task_id']

    bot_overhead = request.get('bot_overhead')
    duration = request.get('duration')
    exit_code = request.get('exit_code')
    hard_timeout = request.get('hard_timeout')
    io_timeout = request.get('io_timeout')
    isolated_stats = request.get('isolated_stats')
    output = request.get('output')
    output_chunk_start = request.get('output_chunk_start')
    outputs_ref = request.get('outputs_ref')

    if bool(isolated_stats) != (bot_overhead is not None):
      ereporter2.log_request(
          request=self.request,
          source='server',
          category='task_failure',
          message='Failed to update task: %s' % task_id)
      self.abort_with_error(
          400,
          error='Both bot_overhead and isolated_stats must be set '
                'simultaneously\nbot_overhead: %s\nisolated_stats: %s' %
                (bot_overhead, isolated_stats))

    run_result_key = task_pack.unpack_run_result_key(task_id)
    performance_stats = None
    if isolated_stats:
      download = isolated_stats['download']
      upload = isolated_stats['upload']
      performance_stats = task_result.PerformanceStats(
          bot_overhead=bot_overhead,
          isolated_download=task_result.IsolatedOperation(
              duration=download['duration'],
              initial_number_items=download['initial_number_items'],
              initial_size=download['initial_size'],
              items_cold=base64.b64decode(download['items_cold']),
              items_hot=base64.b64decode(download['items_hot'])),
          isolated_upload=task_result.IsolatedOperation(
              duration=upload['duration'],
              items_cold=base64.b64decode(upload['items_cold']),
              items_hot=base64.b64decode(upload['items_hot'])))

    if output is not None:
      try:
        output = base64.b64decode(output)
      except UnicodeEncodeError as e:
        logging.error('Failed to decode output\n%s\n%r', e, output)
        output = output.encode('ascii', 'replace')
      except TypeError as e:
        # Save the output as-is instead. The error will be logged in ereporter2
        # and returning a HTTP 500 would only force the bot to stay in a retry
        # loop.
        logging.error('Failed to decode output\n%s\n%r', e, output)
    if outputs_ref:
      outputs_ref = task_request.FilesRef(**outputs_ref)

    try:
      state = task_scheduler.bot_update_task(
          run_result_key=run_result_key,
          bot_id=bot_id,
          output=output,
          output_chunk_start=output_chunk_start,
          exit_code=exit_code,
          duration=duration,
          hard_timeout=hard_timeout,
          io_timeout=io_timeout,
          cost_usd=cost_usd,
          outputs_ref=outputs_ref,
          performance_stats=performance_stats)
      if not state:
        logging.info('Failed to update, please retry')
        self.abort_with_error(500, error='Failed to update, please retry')

      if state in (task_result.State.COMPLETED, task_result.State.TIMED_OUT):
        action = 'task_completed'
      else:
        assert state == task_result.State.RUNNING, state
        action = 'task_update'
      bot_management.bot_event(
          event_type=action, bot_id=bot_id,
          external_ip=self.request.remote_addr, dimensions=None, state=None,
          version=None, quarantined=None, task_id=task_id, task_name=None)
    except ValueError as e:
      ereporter2.log_request(
          request=self.request,
          source='server',
          category='task_failure',
          message='Failed to update task: %s' % e)
      self.abort_with_error(400, error=str(e))
    except webob.exc.HTTPException:
      raise
    except Exception as e:
      logging.exception('Internal error: %s', e)
      self.abort_with_error(500, error=str(e))

    # TODO(maruel): When a task is canceled, reply with 'DIE' so that the bot
    # reboots itself to abort the task abruptly. It is useful when a task hangs
    # and the timeout was set too long or the task was superseded by a newer
    # task with more recent executable (e.g. a new Try Server job on a newer
    # patchset on Rietveld).
    self.send_response({'ok': True})


class BotTaskErrorHandler(auth.ApiHandler):
  """It is a specialized version of ereporter2's /ereporter2/api/v1/on_error
  that also attaches a task id to it.

  This formally kills the task, marking it as an internal failure. This can be
  used by bot_main.py to kill the task when task_runner misbehaved.
  """

  EXPECTED_KEYS = {u'id', u'message', u'task_id'}

  # TODO(vadimsh): Remove once bots use X-Whitelisted-Bot-Id or OAuth.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot)
  def post(self, task_id=None):
    request = self.parse_body()
    bot_id = request.get('id')
    task_id = request.get('task_id', '')
    message = request.get('message', 'unknown')

    bot_management.bot_event(
        event_type='task_error', bot_id=bot_id,
        external_ip=self.request.remote_addr, dimensions=None, state=None,
        version=None, quarantined=None, task_id=task_id, task_name=None,
        message=message)
    line = (
        'Bot: https://%s/restricted/bot/%s\n'
        'Task failed: https://%s/user/task/%s\n'
        '%s') % (
        app_identity.get_default_version_hostname(), bot_id,
        app_identity.get_default_version_hostname(), task_id,
        message)
    ereporter2.log_request(self.request, source='bot', message=line)

    msg = log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'bot', 'keys')
    if msg:
      self.abort_with_error(400, error=msg)

    msg = task_scheduler.bot_kill_task(
        task_pack.unpack_run_result_key(task_id), bot_id)
    if msg:
      logging.error(msg)
      self.abort_with_error(400, error=msg)
    self.send_response({})


class ServerPingHandler(webapp2.RequestHandler):
  """Handler to ping when checking if the server is up.

  This handler should be extremely lightweight. It shouldn't do any
  computations, it should just state that the server is up. It's open to
  everyone for simplicity and performance.
  """

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Server up')


def get_routes():
  routes = [
      ('/bootstrap', BootstrapHandler),
      ('/bot_code', BotCodeHandler),
      ('/swarming/api/v1/bot/bot_code/<version:[0-9a-f]{40}>', BotCodeHandler),
      ('/swarming/api/v1/bot/event', BotEventHandler),
      ('/swarming/api/v1/bot/handshake', BotHandshakeHandler),
      ('/swarming/api/v1/bot/poll', BotPollHandler),
      ('/swarming/api/v1/bot/server_ping', ServerPingHandler),
      ('/swarming/api/v1/bot/task_update', BotTaskUpdateHandler),
      ('/swarming/api/v1/bot/task_update/<task_id:[a-f0-9]+>',
          BotTaskUpdateHandler),
      ('/swarming/api/v1/bot/task_error', BotTaskErrorHandler),
      ('/swarming/api/v1/bot/task_error/<task_id:[a-f0-9]+>',
          BotTaskErrorHandler),
  ]
  return [webapp2.Route(*i) for i in routes]
