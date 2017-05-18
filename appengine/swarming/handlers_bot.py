# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Internal bot API handlers."""

import base64
import json
import logging
import re

import webob
import webapp2

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine import runtime

from components import auth
from components import ereporter2
from components import utils
from server import acl
from server import bot_auth
from server import bot_code
from server import bot_management
from server import config
from server import task_pack
from server import task_queues
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


def get_bot_contact_server(request):
  """Gets the server contacted by the bot.

  Usually, this is the URL of the Swarming server itself, but if the bot
  is communicating to the server by a gRPC intermediary, this will be the
  IP address of the gRPC endpoint. The Native API will have an http or
  https protocol, while gRPC endpoints will have a fake "grpc://" protocol.
  This is to help consumers of this information (mainly the bot code
  generators) distinguish between native and gRPC bots.
  """
  server = request.host_url
  if 'luci-grpc' in request.headers:
    server = 'grpc://%s' % request.headers['luci-grpc']
  return server


class _BotApiHandler(auth.ApiHandler):
  """Like ApiHandler, but also implements machine authentication."""

  # Bots are passing credentials through special headers (not cookies), no need
  # for XSRF tokens.
  xsrf_token_enforce_on = ()

  @classmethod
  def get_auth_methods(cls, conf):
    return [auth.machine_authentication, auth.oauth_authentication]


class _BotAuthenticatingHandler(auth.AuthenticatingHandler):
  """Like AuthenticatingHandler, but also implements machine authentication.

  Handlers inheriting this class are used during bot bootstrap and self-update.

  Unlike _BotApiHandler handlers, _BotAuthenticatingHandler handlers don't check
  dimensions or bot_id, since they are not yet known when these handlers are
  called. They merely check that the bot credentials are known to the server, or
  the endpoint is being used by an account authorized to do bot bootstrap.
  """

  # Bots are passing credentials through special headers (not cookies), no need
  # for XSRF tokens.
  xsrf_token_enforce_on = ()

  @classmethod
  def get_auth_methods(cls, conf):
    return [auth.machine_authentication, auth.oauth_authentication]

  def check_bot_code_access(self, bot_id, generate_token):
    """Raises AuthorizationError if caller is not authorized to access bot code.

    Four variants here:
      1. A valid bootstrap token is passed as '?tok=...' parameter.
      2. An user, allowed to do a bootstrap, is using their credentials.
      3. An IP whitelisted machine is making this call.
      4. A bot (with given bot_id) is using it's own machine credentials.

    In later three cases we optionally generate and return a new bootstrap
    token, that can be used to authorize /bot_code calls.
    """
    existing_token = self.request.get('tok')
    if existing_token:
      payload = bot_code.validate_bootstrap_token(existing_token)
      if payload is None:
        raise auth.AuthorizationError('Invalid bootstrap token')
      logging.info('Using bootstrap token %r', payload)
      return existing_token

    machine_type = None
    if bot_id:
      bot_info = bot_management.get_info_key(bot_id).get()
      if bot_info:
        machine_type = bot_info.machine_type

    # TODO(vadimsh): Remove is_ip_whitelisted_machine check once all bots are
    # using auth for bootstrap and updating.
    if (not acl.is_bootstrapper() and
        not acl.is_ip_whitelisted_machine() and
        not (bot_id and bot_auth.is_authenticated_bot(bot_id, machine_type))):
      raise auth.AuthorizationError('Not allowed to access the bot code')

    return bot_code.generate_bootstrap_token() if generate_token else None

  def get_bot_contact_server(self):
    """Gets the server contacted by the bot."""
    return get_bot_contact_server(self.request)


class BootstrapHandler(_BotAuthenticatingHandler):
  """Returns python code to run to bootstrap a swarming bot."""

  @auth.public  # auth inside check_bot_code_access()
  def get(self):
    # We must pass a bootstrap token (generating it, if necessary) to
    # get_bootstrap(...), since bootstrap.py uses tokens exclusively (it can't
    # transparently pass OAuth headers to /bot_code).
    bootstrap_token = self.check_bot_code_access(
        bot_id=None, generate_token=True)
    self.response.headers['Content-Type'] = 'text/x-python'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot_bootstrap.py"')
    self.response.out.write(
        bot_code.get_bootstrap(self.request.host_url, bootstrap_token).content)


class BotCodeHandler(_BotAuthenticatingHandler):
  """Returns a zip file with all the files required by a bot.

  Optionally specify the hash version to download. If so, the returned data is
  cacheable.
  """

  @auth.public  # auth inside check_bot_code_access()
  def get(self, version=None):
    server = self.get_bot_contact_server()
    self.check_bot_code_access(
        bot_id=self.request.get('bot_id'), generate_token=False)
    if version:
      expected, _ = bot_code.get_bot_version(server)
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
        bot_code.get_swarming_bot_zip(server))


class _ProcessResult(object):
  """Returned by _BotBaseHandler._process."""

  # A dict with parsed JSON request body, as it was received.
  request = None
  # Bot identifier, extracted from 'id' dimension.
  bot_id = None
  # Version of the bot code, as reported by the bot itself.
  version = None
  # Dict with bot state (as reported by the bot).
  state = None
  # Dict with bot dimensions (union of bot-reported and server-side ones).
  dimensions = None
  # Instance of BotGroupConfig with server-side bot config (from bots.cfg).
  bot_group_cfg = None
  # Bot quarantine message (or None if the bot is not in a quarantine).
  quarantined_msg = None
  # DateTime indicating UTC time when bot will be reclaimed by Machine Provider,
  # or None if this is not a Machine Provider bot.
  lease_expiration_ts = None

  def __init__(self, **kwargs):
    for k, v in kwargs.iteritems():
      # Typo catching assert, ensure _ProcessResult class has the attribute.
      assert hasattr(self, k), k
      setattr(self, k, v)


class _BotBaseHandler(_BotApiHandler):
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

  def _process(self):
    """Fetches bot info and settings, does authorization and quarantine checks.

    Returns:
      _ProcessResult instance, see its fields for more info.

    Raises:
      auth.AuthorizationError if bot's credentials are invalid.
    """
    request = self.parse_body()
    version = request.get('version', None)

    dimensions = request.get('dimensions') or {}
    state = request.get('state') or {}
    bot_id = None
    if dimensions.get('id'):
      dimension_id = dimensions['id']
      if (isinstance(dimension_id, list) and len(dimension_id) == 1
          and isinstance(dimension_id[0], unicode)):
        bot_id = dimensions['id'][0]

    lease_expiration_ts = None
    machine_type = None
    if bot_id:
      logging.debug('Fetching bot info and settings')
      bot_info, bot_settings = ndb.get_multi([
          bot_management.get_info_key(bot_id),
          bot_management.get_settings_key(bot_id)])
      if bot_info:
        lease_expiration_ts = bot_info.lease_expiration_ts
        machine_type = bot_info.machine_type

    # Make sure bot self-reported ID matches the authentication token. Raises
    # auth.AuthorizationError if not.
    logging.debug('Fetching bot group config')
    bot_group_cfg = bot_auth.validate_bot_id_and_fetch_config(
        bot_id, machine_type)

    # The server side dimensions from bot_group_cfg override bot-provided ones.
    # If both server side config and bot report some dimension, server side
    # config wins. We still emit an error if bot tries to supply the dimension
    # and it disagrees with the server defined one. Don't report ['default'] as
    # an error, bot sends it in the handshake before it knows anything at all.
    for dim_key, from_cfg in bot_group_cfg.dimensions.iteritems():
      from_bot = sorted(dimensions.get(dim_key) or [])
      from_cfg = sorted(from_cfg)
      if from_bot and from_bot != ['default'] and from_bot != from_cfg:
        logging.error(
            'Dimensions in bots.cfg doesn\'t match ones provided by the bot\n'
            'bot_id: "%s", key: "%s", from_bot: %s, from_cfg: %s',
            bot_id, dim_key, from_bot, from_cfg)
      dimensions[dim_key] = from_cfg

    # Fill in all result fields except 'quarantined_msg'.
    result = _ProcessResult(
        request=request,
        bot_id=bot_id,
        version=version,
        state=state,
        dimensions=dimensions,
        bot_group_cfg=bot_group_cfg,
        lease_expiration_ts=lease_expiration_ts)

    # The bot may decide to "self-quarantine" itself. Accept both via
    # dimensions or via state. See bot_management._BotCommon.quarantined for
    # more details.
    if (bool(dimensions.get('quarantined')) or
        bool(state.get('quarantined'))):
      result.quarantined_msg = 'Bot self-quarantined'
      return result

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
      if not dimensions.get('pool'):
        quarantined_msg = 'Missing \'pool\' dimension'
        break

      if not all(
          config.validate_dimension_key(key) and
          isinstance(values, list) and
          all(config.validate_dimension_value(value) for value in values)
          for key, values in dimensions.iteritems()):
        quarantined_msg = (
            'Invalid dimensions type:\n%s' % json.dumps(dimensions,
              sort_keys=True, indent=2, separators=(',', ': ')))
        break

      dimensions_count = task_to_run.dimensions_powerset_count(dimensions)
      if dimensions_count > task_to_run.MAX_DIMENSIONS:
        quarantined_msg = 'Dimensions product %d is too high' % dimensions_count
        break

    if quarantined_msg:
      line = 'Quarantined Bot\nhttps://%s/restricted/bot/%s\n%s' % (
          app_identity.get_default_version_hostname(), bot_id,
          quarantined_msg)
      ereporter2.log_request(self.request, source='bot', message=line)
      result.quarantined_msg = quarantined_msg
      return result

    # Look for admin enforced quarantine.
    if bool(bot_settings and bot_settings.quarantined):
      result.quarantined_msg = 'Quarantined by admin'
      return result

    task_queues.assert_bot(dimensions)
    return result

  def get_bot_contact_server(self):
    """Gets the server contacted by the bot."""
    return get_bot_contact_server(self.request)


class BotHandshakeHandler(_BotBaseHandler):
  """First request to be called to get initial data like bot code version.

  The bot is server-controlled so the server doesn't have to support multiple
  API version. When running a task, the bot sync the the version specific URL.
  Once a bot finishes its currently running task, it'll be immediately upgraded
  on its next poll.

  This endpoint does not return commands to the bot, for example to upgrade
  itself. It'll be told so when it does its first poll.

  Response body is a JSON dict:
    {
      "bot_version": <sha-1 of swarming_bot.zip uncompressed content>,
      "server_version": "138-193f1f3",
      "bot_group_cfg_version": "0123abcdef",
      "bot_group_cfg": {
        "dimensions": { <server-defined dimensions> },
      }
    }
  """

  @auth.public  # auth happens in self._process()
  def post(self):
    res = self._process()
    bot_management.bot_event(
        event_type='bot_connected', bot_id=res.bot_id,
        external_ip=self.request.remote_addr,
        authenticated_as=auth.get_peer_identity().to_bytes(),
        dimensions=res.dimensions, state=res.state,
        version=res.version, quarantined=bool(res.quarantined_msg),
        task_id='', task_name=None, message=res.quarantined_msg)

    data = {
      'bot_version': bot_code.get_bot_version(self.get_bot_contact_server())[0],
      'server_version': utils.get_app_version(),
      'bot_group_cfg_version': res.bot_group_cfg.version,
      'bot_group_cfg': {
        # Let the bot know its server-side dimensions (from bots.cfg file).
        'dimensions': res.bot_group_cfg.dimensions,
      },
    }
    if res.bot_group_cfg.bot_config_script_content:
      logging.info(
          'Injecting %s: %d bytes',
          res.bot_group_cfg.bot_config_script,
          len(res.bot_group_cfg.bot_config_script_content))
      data['bot_config'] = res.bot_group_cfg.bot_config_script_content
    self.send_response(data)


class BotPollHandler(_BotBaseHandler):
  """The bot polls for a task; returns either a task, update command or sleep.

  In case of exception on the bot, this is enough to get it just far enough to
  eventually self-update to a working version. This is to ensure that coding
  errors in bot code doesn't kill all the fleet at once, they should still be up
  just enough to be able to self-update again even if they don't get task
  assigned anymore.
  """

  @auth.public  # auth happens in self._process()
  def post(self):
    """Handles a polling request.

    Be very permissive on missing values. This can happen because of errors
    on the bot, *we don't want to deny them the capacity to update*, so that the
    bot code is eventually fixed and the bot self-update to this working code.

    It makes recovery of the fleet in case of catastrophic failure much easier.
    """
    logging.debug('Request started')
    if config.settings().force_bots_to_sleep_and_not_run_task:
      # Ignore everything, just sleep. Tell the bot it is quarantined to inform
      # it that it won't be running anything anyway. Use a large streak so it
      # will sleep for 60s.
      self._cmd_sleep(1000, True)
      return

    res = self._process()
    sleep_streak = res.state.get('sleep_streak', 0)
    quarantined = bool(res.quarantined_msg)

    # Note bot existence at two places, one for stats at 1 minute resolution,
    # the other for the list of known bots.

    def bot_event(event_type, task_id=None, task_name=None):
      bot_management.bot_event(
          event_type=event_type, bot_id=res.bot_id,
          external_ip=self.request.remote_addr,
          authenticated_as=auth.get_peer_identity().to_bytes(),
          dimensions=res.dimensions, state=res.state,
          version=res.version, quarantined=quarantined,
          task_id=task_id, task_name=task_name, message=res.quarantined_msg)

    # Bot version is host-specific because the host URL is embedded in
    # swarming_bot.zip
    logging.debug('Fetching bot code version')
    expected_version, _ = bot_code.get_bot_version(
        self.get_bot_contact_server())
    if res.version != expected_version:
      bot_event('request_update')
      self._cmd_update(expected_version)
      return
    if quarantined:
      bot_event('request_sleep')
      self._cmd_sleep(sleep_streak, quarantined)
      return

    # If the server-side per-bot config for the bot has changed, we need
    # to restart this particular bot, so it picks up new config in /handshake.
    # Do this check only for bots that know about server-side per-bot configs
    # already (such bots send 'bot_group_cfg_version' state attribute).
    cur_bot_cfg_ver = res.state.get('bot_group_cfg_version')
    if cur_bot_cfg_ver and cur_bot_cfg_ver != res.bot_group_cfg.version:
      bot_event('request_restart')
      self._cmd_bot_restart('Restarting to pick up new bots.cfg config')
      return

    #
    # At that point, the bot should be in relatively good shape since it's
    # running the right version. It is still possible that invalid code was
    # pushed to the server, so be diligent about it.
    #

    # TODO(maruel): Remove this and migrate all use cases in bot_config.py
    # on_bot_idle().
    # Bot may need a reboot if it is running for too long. We do not reboot
    # quarantined bots.
    needs_restart, restart_message = bot_management.should_restart_bot(
        res.bot_id, res.state)
    if needs_restart:
      bot_event('request_restart')
      self._cmd_host_reboot(restart_message)
      return

    # The bot is in good shape. Try to grab a task.
    try:
      # This is a fairly complex function call, exceptions are expected.
      logging.debug('Reaping task')
      request, secret_bytes, run_result = task_scheduler.bot_reap_task(
          res.dimensions, res.version, res.lease_expiration_ts)
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
          self._cmd_run(request, secret_bytes, run_result.key, res.bot_id)
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

  def _cmd_run(self, request, secret_bytes, run_result_key, bot_id):
    cmd = None
    if request.properties.commands:
      cmd = request.properties.commands[0]
    elif request.properties.command:
      cmd = request.properties.command
    out = {
      'cmd': 'run',
      'manifest': {
        'bot_id': bot_id,
        'caches': [
          c.to_dict() for c in request.properties.caches
        ],
        'cipd_input': {
          'client_package': (
              request.properties.cipd_input.client_package.to_dict()),
          'packages': [
            p.to_dict() for p in request.properties.cipd_input.packages
          ],
          'server': request.properties.cipd_input.server,
        } if request.properties.cipd_input else None,
        'command': cmd,
        'dimensions': request.properties.dimensions,
        'env': request.properties.env,
        'extra_args': request.properties.extra_args,
        'grace_period': request.properties.grace_period_secs,
        'hard_timeout': request.properties.execution_timeout_secs,
        'host': utils.get_versioned_hosturl(),
        'io_timeout': request.properties.io_timeout_secs,
        'secret_bytes': (secret_bytes.secret_bytes.encode('base64')
                         if secret_bytes else None),
        'isolated': {
          'input': request.properties.inputs_ref.isolated,
          'namespace': request.properties.inputs_ref.namespace,
          'server': request.properties.inputs_ref.isolatedserver,
        } if request.properties.inputs_ref else None,
        'outputs': request.properties.outputs,
        'service_account': request.service_account,
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

  def _cmd_host_reboot(self, message):
    logging.info('Rebooting host: %s', message)
    out = {
      'cmd': 'host_reboot',
      'message': message,
    }
    self.send_response(out)

  def _cmd_bot_restart(self, message):
    logging.info('Restarting bot: %s', message)
    out = {
      'cmd': 'bot_restart',
      'message': message,
    }
    self.send_response(out)


class BotEventHandler(_BotBaseHandler):
  """On signal that a bot had an event worth logging."""

  EXPECTED_KEYS = _BotBaseHandler.EXPECTED_KEYS | {u'event', u'message'}

  ALLOWED_EVENTS = ('bot_error', 'bot_log', 'bot_rebooting', 'bot_shutdown')

  @auth.public  # auth happens in self._process()
  def post(self):
    res = self._process()
    event = res.request.get('event')
    if event not in self.ALLOWED_EVENTS:
      logging.error('Unexpected event type')
      self.abort_with_error(400, error='Unsupported event type')
    message = res.request.get('message')
    # Record the event in a BotEvent entity so it can be listed on the bot's
    # page.
    bot_management.bot_event(
        event_type=event, bot_id=res.bot_id,
        external_ip=self.request.remote_addr,
        authenticated_as=auth.get_peer_identity().to_bytes(),
        dimensions=res.dimensions, state=res.state,
        version=res.version, quarantined=bool(res.quarantined_msg),
        task_id=None, task_name=None,
        message=message)

    if event == 'bot_error':
      # Also logs this to ereporter2, so it will be listed in the server's
      # hourly ereporter2 report. THIS IS NOISY so it should only be done with
      # issues requiring action. In this case, include again the bot's URL since
      # there's no context in the report. Redundantly include the bot id so
      # messages are bucketted by bot.
      line = (
          '%s\n'
          '\nhttps://%s/restricted/bot/%s') % (
          message, app_identity.get_default_version_hostname(), res.bot_id)
      ereporter2.log_request(self.request, source='bot', message=line)
    self.send_response({})


class BotTaskUpdateHandler(_BotApiHandler):
  """Receives updates from a Bot for a task.

  The handler verifies packets are processed in order and will refuse
  out-of-order packets.
  """
  ACCEPTED_KEYS = {
    u'bot_overhead', u'cipd_pins', u'cipd_stats', u'cost_usd', u'duration',
    u'exit_code', u'hard_timeout', u'id', u'io_timeout', u'isolated_stats',
    u'output', u'output_chunk_start', u'outputs_ref', u'task_id',
  }
  REQUIRED_KEYS = {u'id', u'task_id'}

  @auth.public  # auth happens in bot_auth.validate_bot_id_and_fetch_config()
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
    task_id = request['task_id']

    machine_type = None
    bot_info = bot_management.get_info_key(bot_id).get()
    if bot_info:
      machine_type = bot_info.machine_type

    # Make sure bot self-reported ID matches the authentication token. Raises
    # auth.AuthorizationError if not.
    bot_auth.validate_bot_id_and_fetch_config(bot_id, machine_type)

    bot_overhead = request.get('bot_overhead')
    cipd_pins = request.get('cipd_pins')
    cipd_stats = request.get('cipd_stats')
    cost_usd = request.get('cost_usd', 0)
    duration = request.get('duration')
    exit_code = request.get('exit_code')
    hard_timeout = request.get('hard_timeout')
    io_timeout = request.get('io_timeout')
    isolated_stats = request.get('isolated_stats')
    output = request.get('output')
    output_chunk_start = request.get('output_chunk_start')
    outputs_ref = request.get('outputs_ref')

    if (isolated_stats or cipd_stats) and bot_overhead is None:
      ereporter2.log_request(
          request=self.request,
          source='server',
          category='task_failure',
          message='Failed to update task: %s' % task_id)
      self.abort_with_error(
          400,
          error='isolated_stats and cipd_stats require bot_overhead to be set'
                '\nbot_overhead: %s\nisolate_stats: %s' %
                (bot_overhead, isolated_stats))

    run_result_key = task_pack.unpack_run_result_key(task_id)
    performance_stats = None
    if bot_overhead is not None:
      performance_stats = task_result.PerformanceStats(
          bot_overhead=bot_overhead)
      if isolated_stats:
        download = isolated_stats.get('download') or {}
        upload = isolated_stats.get('upload') or {}
        def unpack_base64(d, k):
          x = d.get(k)
          if x:
            return base64.b64decode(x)
        performance_stats.isolated_download = task_result.OperationStats(
            duration=download.get('duration'),
            initial_number_items=download.get('initial_number_items'),
            initial_size=download.get('initial_size'),
            items_cold=unpack_base64(download, 'items_cold'),
            items_hot=unpack_base64(download, 'items_hot'))
        performance_stats.isolated_upload = task_result.OperationStats(
            duration=upload.get('duration'),
            items_cold=unpack_base64(upload, 'items_cold'),
            items_hot=unpack_base64(upload, 'items_hot'))
      if cipd_stats:
        performance_stats.package_installation = task_result.OperationStats(
            duration=cipd_stats.get('duration'))

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

    if cipd_pins:
      cipd_pins = task_result.CipdPins(
        client_package=task_request.CipdPackage(
            **cipd_pins['client_package']),
        packages=[
            task_request.CipdPackage(**args) for args in cipd_pins['packages']]
      )

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
          cipd_pins=cipd_pins,
          performance_stats=performance_stats)
      if not state:
        logging.info('Failed to update, please retry')
        self.abort_with_error(500, error='Failed to update, please retry')

      if state in (task_result.State.COMPLETED, task_result.State.TIMED_OUT):
        action = 'task_completed'
      elif state == task_result.State.CANCELED:
        action = 'task_canceled'
      else:
        assert state in (
            task_result.State.BOT_DIED, task_result.State.RUNNING), state
        action = 'task_update'
      bot_management.bot_event(
          event_type=action, bot_id=bot_id,
          external_ip=self.request.remote_addr,
          authenticated_as=auth.get_peer_identity().to_bytes(),
          dimensions=None, state=None,
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
    self.send_response(
        {'must_stop': state == task_result.State.CANCELED, 'ok': True})


class BotTaskErrorHandler(_BotApiHandler):
  """It is a specialized version of ereporter2's /ereporter2/api/v1/on_error
  that also attaches a task id to it.

  This formally kills the task, marking it as an internal failure. This can be
  used by bot_main.py to kill the task when task_runner misbehaved.
  """

  EXPECTED_KEYS = {u'id', u'message', u'task_id'}

  @auth.public  # auth happens in bot_auth.validate_bot_id_and_fetch_config
  def post(self, task_id=None):
    request = self.parse_body()
    bot_id = request.get('id')
    task_id = request.get('task_id', '')
    message = request.get('message', 'unknown')

    machine_type = None
    bot_info = bot_management.get_info_key(bot_id).get()
    if bot_info:
      machine_type = bot_info.machine_type

    # Make sure bot self-reported ID matches the authentication token. Raises
    # auth.AuthorizationError if not.
    bot_auth.validate_bot_id_and_fetch_config(bot_id, machine_type)

    bot_management.bot_event(
        event_type='task_error', bot_id=bot_id,
        external_ip=self.request.remote_addr,
        authenticated_as=auth.get_peer_identity().to_bytes(),
        dimensions=None, state=None,
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
      # 40 for old sha1 digest so old bot can still update, 64 for current
      # sha256 digest.
      ('/swarming/api/v1/bot/bot_code/<version:[0-9a-f]{40,64}>',
          BotCodeHandler),
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
