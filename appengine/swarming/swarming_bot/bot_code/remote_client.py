# coding: utf-8
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import collections
import copy
import datetime
import enum
import hashlib
import json
import logging
import os
import threading
import time
import traceback
import uuid

from utils import net

from bot_code.remote_client_errors import BotCodeError
from bot_code.remote_client_errors import ClaimError
from bot_code.remote_client_errors import InitializationError
from bot_code.remote_client_errors import InternalError
from bot_code.remote_client_errors import MintTokenError
from bot_code.remote_client_errors import PollError
from bot_code.remote_client_errors import RBEServerError


# RemoteClient will attempt to refresh the authentication headers once they are
# this close to the expiration.
#
# The total possible delay between the headers are checked and used is the sum:
#  1) FileRefresherThread update interval (15 sec).
#  2) FileReaderThread update interval (15 sec).
#  3) NET_CONNECTION_TIMEOUT_SEC, when resending requests on errors (3 min).
#  4) Various random delays if Swarming bot process is preempted by task
#     processes (e.g. heavy tests) that consume 100% of CPU.
#
# AUTH_HEADERS_EXPIRATION_SEC must be larger than this sum.
#
# Additionally, there's an upper limit: AUTH_HEADERS_EXPIRATION_SEC must be less
# than the minimum expiration time of headers produced by bot_config's
# get_authentication_headers hook (otherwise we'll be calling this hook all the
# time). On GCE machines it is usually 10 min.
AUTH_HEADERS_EXPIRATION_SEC = 9*60+30


# How long to wait for a response from the server. Must not be greater than
# AUTH_HEADERS_EXPIRATION_SEC, since otherwise there's a chance auth headers
# will expire while we wait for connection.
NET_CONNECTION_TIMEOUT_SEC = 4 * 60

# How many attempts to make when sending a request (1 == no retries).
NET_MAX_ATTEMPTS = net.URL_OPEN_MAX_ATTEMPTS


def utcnow():
  return datetime.datetime.utcnow()


def make_appengine_id(bot_id, work_dir):
  """Generate a value to use in the GOOGAPPUID cookie for AppEngine.

  AppEngine looks for this cookie: if it contains a value in the range 0-999,
  it is used to split traffic. For more details, see:
  https://cloud.google.com/appengine/docs/flexible/python/splitting-traffic

  The bot code will send requests with a value generated locally:
    GOOGAPPUID = sha1('YYYY-MM-DD-bot_id:work_dir') % 1000
  (from go/swarming-release-canaries)

  This scheme should result in the values being roughly uniformly distributed.
  The date is included in the hash to ensure that across different rollouts,
  it's not the same set of bots being used as the canary (otherwise we might
  be unlucky and get a unrepresentative sample).

  Args:
    bot_id: The bot ID.
    work_dir: The working directory used by the bot.

  Returns:
    An integer in the range [0, 999].
  """
  s = '%s-%s:%s' % (utcnow().strftime('%Y-%m-%d'), bot_id, work_dir)
  googappuid = int(hashlib.sha1(s.encode('utf-8')).hexdigest(), 16) % 1000
  logging.debug('GOOGAPPUID = sha1(%s) %% 1000 = %d', s, googappuid)
  return googappuid


class RemoteClientNative(object):
  """RemoteClientNative knows how to make authenticated calls to the backend.

  It also holds in-memory cache of authentication headers and periodically
  refreshes them (by calling supplied callback, that usually is implemented in
  terms of bot_config.get_authentication_headers() function).

  If the callback is None, skips authentication (this is used during initial
  stages of the bot bootstrap).

  If the callback returns (*, None), disables authentication. This allows
  bot_config.py to disable strong authentication on machines that don't have any
  credentials (the server uses only IP allowlist check in this case).

  If the callback returns (*, 0), effectively disables the caching of headers:
  the callback will be called for each request.
  """

  def __init__(self, server, auth_headers_callback, bot_id, work_dir):
    self._server = server
    self._session_token = None
    self._auth_headers_callback = auth_headers_callback
    self._lock = threading.Lock()
    self._headers = None
    self._exp_ts = None
    self._disabled = not auth_headers_callback
    self._bot_work_dir = work_dir
    self._bot_id = bot_id
    self._poll_request_uuid = None

  @property
  def server(self):
    return self._server

  @property
  def bot_id(self):
    return self._bot_id

  @property
  def session_token(self):
    return self._session_token

  @session_token.setter
  def session_token(self, tok):
    self._session_token = tok

  def initialize(self, quit_bit=None):
    """Grabs initial auth headers, retrying on errors a bunch of times.

    Disabled authentication (when auth_headers_callback returns None) is not
    an error. Retries only real exceptions raised by the callback.

    Raises InitializationError if all attempts fail. Aborts attempts and returns
    if quit_bit is signaled. If quit_bit is None, retries until success or until
    all attempts fail.
    """
    attempts = 30
    while not quit_bit or not quit_bit.is_set():
      try:
        logging.info('Fetching initial auth headers')
        headers = self._get_headers_or_throw()
        logging.info('Got auth headers: %s', headers.keys() or 'none')
        return
      except Exception as e:
        last_error = '%s\n%s' % (e, traceback.format_exc()[-2048:])
        logging.exception('Failed to grab initial auth headers')
      attempts -= 1
      if not attempts:
        raise InitializationError(last_error)
      time.sleep(2)

  @property
  def uses_auth(self):
    """Returns True if get_authentication_headers() returns some headers.

    If bot_config.get_authentication_headers() is not implement it will return
    False.
    """
    return bool(self.get_authentication_headers())

  def get_headers(self, include_auth=False):
    """Returns the headers to use to send a request.

    Args:
      include_auth: Whether or not to include authentication headers.

    Returns:
      A dict of HTTP headers.
    """
    headers = {
        'Cookie':
        'GOOGAPPUID=%d' % make_appengine_id(self._bot_id, self._bot_work_dir),
        'X-Luci-Swarming-Bot-ID':
        self._bot_id,
    }
    if include_auth:
      headers.update(self.get_authentication_headers())
    return headers

  def get_authentication_headers(self):
    """Returns a dict with the headers, refreshing them if necessary.

    Will always return a dict (perhaps empty if no auth headers are provided by
    the callback or it has failed).
    """
    try:
      return self._get_headers_or_throw()
    except Exception:
      logging.exception('Failed to refresh auth headers, using cached ones')
      return self._headers or {}

  @property
  def authentication_headers_expiration(self):
    """Returns int unix timestamp of when current cached auth headers expire.

    Returns 0 if unknown or None if not using auth at all.
    """
    return int(self._exp_ts) if not self._disabled else None

  def _get_headers_or_throw(self):
    if self._disabled:
      return {}
    with self._lock:
      if (not self._exp_ts or
          self._exp_ts - time.time() < AUTH_HEADERS_EXPIRATION_SEC):
        self._headers, self._exp_ts = self._auth_headers_callback()
        if self._exp_ts is None:
          logging.info('Headers callback returned None, disabling auth')
          self._disabled = True
          self._headers = {}
        elif self._exp_ts:
          next_check = max(
              0, self._exp_ts - AUTH_HEADERS_EXPIRATION_SEC - time.time())
          if self._headers:
            logging.info(
                'Fetched auth headers (%s), they expire in %d sec. '
                'Next check in %d sec.', self._headers.keys(),
                self._exp_ts - time.time(), next_check)
          else:
            logging.info(
                'No headers available yet, next check in %d sec.', next_check)
        else:
          logging.info('Using auth headers (%s).', self._headers.keys())
      return self._headers or {}

  def _url_read_json(self,
                     url_path,
                     data=None,
                     expected_error_codes=None,
                     retry_transient=True):
    """Does POST (if data is not None) or GET request to a JSON endpoint."""
    logging.info('Calling %s', url_path)
    return net.url_read_json(
        self._server + url_path,
        data=data,
        headers=self.get_headers(include_auth=True),
        timeout=NET_CONNECTION_TIMEOUT_SEC,
        follow_redirects=False,
        expected_error_codes=expected_error_codes,
        max_attempts=NET_MAX_ATTEMPTS if retry_transient else 1)

  def _url_retrieve(self, filepath, url_path):
    """Fetches the file from the given URL path on the server."""
    return net.url_retrieve(
        filepath,
        self._server + url_path,
        headers=self.get_headers(include_auth=True),
        timeout=NET_CONNECTION_TIMEOUT_SEC)

  def _maybe_update_session_token(self, resp):
    """Extracts a session token from the response if present.

    Replaces it with "<redacted>" to avoid logging the token.
    """
    if resp and isinstance(resp, dict):
      fresher = resp.get('session', None)
      if fresher:
        self._session_token = fresher
        resp['session'] = '<redacted>'

  def post_bot_event(self, event_type, message, attributes):
    """Logs bot-specific info to the server."""
    data = attributes.copy()
    data['event'] = event_type
    data['message'] = message
    data['session'] = self._session_token
    resp = self._url_read_json('/swarming/api/v1/bot/event', data=data)
    self._maybe_update_session_token(resp)

  def post_task_update(self,
                       task_id,
                       params,
                       stdout_and_chunk=None,
                       exit_code=None):
    """Posts task update to task_update.

    Arguments:
      stdout: Incremental output since last call, if any.
      stdout_chunk_start: Total number of stdout previously sent, for coherency
          with the server.
      params: Default JSON parameters for the POST.
      exit_code: if None, this is an intermediate update. If non-None, this is
          the final update.

    Returns:
      False if the task should stop.

    Raises:
      InternalError if can't contact the server after many attempts or the
      server replies with an error.
    """
    data = {
        'id': self._bot_id,
        'task_id': task_id,
    }
    data.update(params)
    # Preserving prior behaviour: empty stdout is not transmitted
    if stdout_and_chunk and stdout_and_chunk[0]:
      data['output'] = base64.b64encode(stdout_and_chunk[0]).decode()
      data['output_chunk_start'] = stdout_and_chunk[1]
    if exit_code != None:
      data['exit_code'] = exit_code
    data['session'] = self._session_token

    resp = self._url_read_json(
        '/swarming/api/v1/bot/task_update/%s' % task_id, data)
    self._maybe_update_session_token(resp)
    logging.debug('post_task_update() = %s', resp)
    if not resp or resp.get('error'):
      raise InternalError(
          resp.get('error') if resp else 'Failed to contact server')
    if resp.get('must_stop', False) and resp.get('stop_reason', '') != '':
      logging.warning('Server induced stop; reason: %s', resp['stop_reason'])
    return not resp.get('must_stop', False)

  def post_task_error(self,
                      task_id,
                      message,
                      missing_cas=None,
                      missing_cipd=None):
    """Logs task-specific info to the server"""
    data = {
        'id': self._bot_id,
        'session': self._session_token,
        'message': message,
        'task_id': task_id,
        'client_error': {
            'missing_cas': missing_cas or [],
            'missing_cipd': missing_cipd or [],
        },
    }

    resp = self._url_read_json(
        '/swarming/api/v1/bot/task_error/%s' % task_id,
        data=data)
    self._maybe_update_session_token(resp)
    return resp and resp['resp'] == 1

  def do_handshake(self, attributes, session_id):
    """Performs the initial handshake, initializes the session token."""
    data = attributes.copy()
    data['session_id'] = session_id
    resp = self._url_read_json('/swarming/api/v1/bot/handshake', data=data)
    self._maybe_update_session_token(resp)
    return resp

  def poll(self, attributes, force=False):
    """Polls Swarming server for commands; returns a (cmd, value) pair.

    Unlike other methods, this method doesn't retry on transient errors
    internally (it raises PollError instead). This allows the outer poll loop
    to do stuff (like ping RBE session) between `/bot/poll` attempts.

    During the RBE migration, the caller should always expect *any* allowed
    command to be returned, regardless if the bot is in RBE mode or not: the
    server may decide to end the RBE mode based on its config and do a full
    poll.

    Arguments:
      attributes: a dict with state and dimensions.
      force: if True and the bot has an RBE instance assigned, do a full
          Swarming poll that can pick up tasks (instead of a poll that just
          picks up lifecycle commands). Used by bots in the hybrid mode when
          they poll from both RBE and Swarming schedulers. Makes no effect on
          bots that do not have RBE configured.

    Raises:
      PollError if can't contact the server, the server replies with an error or
      the returned dict does not have the correct values set.
    """
    data = attributes.copy()
    data['session'] = self._session_token
    if force:
      data['force'] = True

    # This makes retry requests idempotent. See also crbug.com/1214700. Reuse
    # the UUID until we get a successful response.
    if not self._poll_request_uuid:
      self._poll_request_uuid = str(uuid.uuid4())
    data['request_uuid'] = self._poll_request_uuid

    resp = self._url_read_json('/swarming/api/v1/bot/poll',
                               data=data,
                               retry_transient=False)
    self._maybe_update_session_token(resp)
    if not resp or resp.get('error'):
      raise PollError(
          resp.get('error') if resp else 'Failed to contact server')

    # Successfully polled. Use a new UUID next time.
    self._poll_request_uuid = None

    cmd = '<unknown>'
    try:
      cmd = resp['cmd']
      if cmd == 'sleep':
        return (cmd, resp['duration'])
      if cmd == 'rbe':
        return (cmd, resp['rbe'])
      if cmd == 'terminate':
        return (cmd, resp['task_id'])
      if cmd == 'run':
        return (cmd, (resp['manifest'], resp.get('rbe')))
      if cmd == 'update':
        return (cmd, resp['version'])
      if cmd in ('restart', 'host_reboot'):
        return (cmd, resp['message'])
      if cmd == 'bot_restart':
        return (cmd, resp['message'])
      raise PollError('Unexpected command: %s\n%s' % (cmd, resp))
    except KeyError as e:
      raise PollError(
          'Unexpected response format for command %s: missing key %s' %
          (cmd, e))

  def claim(self, attributes, claim_id, task_id, task_to_run_shard,
            task_to_run_id):
    """Attempts to mark a pending task slice as being worked on by this bot.

    This is used by bots in RBE mode to transactionally claim tasks they receive
    via RBE. This call can be retried safely as long as all parameters (in
    particular `claim_id`) are the same in every call.

    Arguments:
      claim_id: an opaque string used to make the request idempotent.
      task_id: a TaskResultSummary packed ID identifying a task to claim.
      task_to_run_shard: integer with TaskToRun shard index.
      task_to_run_id: integer ID of the TaskToRun entity to claim.

    Returns one of:
      ('skip', 'Textual reason why') if the slice is no longer pending.
      ('terminate', '<task-id>') if picked up the special termination task.
      ('run', <manifest dict>) if successfully claimed the slice.

    Raises:
      ClaimError if can't contact the server, the server replies with an error
      or the returned dict does not have the correct values set.
    """
    data = attributes.copy()
    data['session'] = self._session_token
    data['claim_id'] = claim_id
    data['task_id'] = task_id
    data['task_to_run_shard'] = task_to_run_shard
    data['task_to_run_id'] = task_to_run_id

    resp = self._url_read_json('/swarming/api/v1/bot/claim', data=data)
    self._maybe_update_session_token(resp)
    if not resp or resp.get('error'):
      raise ClaimError(
          resp.get('error') if resp else 'Failed to contact server')

    cmd = '<unknown>'
    try:
      cmd = resp['cmd']
      if cmd == 'skip':
        return (cmd, resp['reason'])
      if cmd == 'terminate':
        return (cmd, resp['task_id'])
      if cmd == 'run':
        return (cmd, resp['manifest'])
      raise ClaimError('Unexpected outcome: %s\n%s' % (cmd, resp))
    except KeyError as e:
      raise ClaimError(
          'Unexpected response format for outcome %s: missing key %s' %
          (cmd, e))

  def get_bot_code(self, new_zip_path, bot_version):
    """Downloads code into the file specified by new_zip_fn (a string).

    Throws BotCodeError on error.
    """
    url_path = '/swarming/api/v1/bot/bot_code/%s' % bot_version
    if not self._url_retrieve(new_zip_path, url_path):
      raise BotCodeError(new_zip_path, self._server + url_path, bot_version)

  def mint_oauth_token(self, task_id, account_id, scopes):
    """Asks the server to generate an access token for a service account.

    Each task has two service accounts associated with it: 'system' and 'task'.
    Swarming server is capable of generating oauth tokens for them (if the bot
    is currently authorized to have access to them).

    Args:
      task_id: identifier of currently executing task.
      account_id: logical identifier of the account (e.g 'system' or 'task').
      scopes: list of OAuth scopes the new token should have.

    Returns:
      {
        'service_account': <str>,      # account email or 'bot', or 'none'
        'access_token': <str> or None, # actual token, if using real account
        'expiry': <int>,               # unix timestamp in seconds
      }

    Raises:
      InternalError if can't contact the server after many attempts or the
      server consistently replies with HTTP 5** errors.

      MintTokenError on fatal errors.
    """
    resp = self._url_read_json('/swarming/api/v1/bot/oauth_token',
                               data={
                                   'account_id': account_id,
                                   'id': self._bot_id,
                                   'scopes': scopes,
                                   'task_id': task_id,
                                   'session': self._session_token,
                               },
                               expected_error_codes=(400, ))
    self._maybe_update_session_token(resp)
    if not resp:
      raise InternalError(
          'Error when minting access token for account_id: %s' % account_id)
    if resp.get('error'):
      raise MintTokenError(resp['error'])
    return resp

  def mint_id_token(self, task_id, account_id, audience):
    """Asks the server to generate an ID token for a service account.

    Like mint_oauth_token, but returns ID tokens instead of OAuth access tokens.

    Args:
      task_id: identifier of currently executing task.
      account_id: logical identifier of the account (e.g 'system' or 'task').
      audience: an audience string to put into the token.

    Returns:
      {
        'service_account': <str>,  # account email or 'bot', or 'none'
        'id_token': <str> or None, # actual token, if using real account
        'expiry': <int>,           # unix timestamp in seconds
      }

    Raises:
      InternalError if can't contact the server after many attempts or the
      server consistently replies with HTTP 5** errors.

      MintTokenError on fatal errors.
    """
    resp = self._url_read_json('/swarming/api/v1/bot/id_token',
                               data={
                                   'account_id': account_id,
                                   'id': self._bot_id,
                                   'audience': audience,
                                   'task_id': task_id,
                                   'session': self._session_token,
                               },
                               expected_error_codes=(400, ))
    self._maybe_update_session_token(resp)
    if not resp:
      raise InternalError(
          'Error when minting ID token for account_id: %s' % account_id)
    if resp.get('error'):
      raise MintTokenError(resp['error'])
    return resp

  def rbe_create_session(self,
                         bot_version,
                         worker_properties,
                         retry_transient=False):
    """Creates a new RBE session via Swarming RBE backend.

    The RBE session will use the same dimensions and RBE instance as associated
    with the bot on the backend via the last /poll call. The only way to change
    dimensions of the RBE session is to call /poll.

    Arguments:
      bot_version: a string with bot version for monitoring and logs.
      worker_properties: a WorkerProperties instance or None.
      retry_transient: True to retry many times on transient errors. This is
          a very crude retry mechanism intended to be used only if there's no
          better retry loop already.

    Returns:
      RBECreateSessionResponse tuple.

    Raises:
      RBEServerError if the RPC fails for whatever reason.
    """
    data = {'session': self._session_token}
    if bot_version:
      data['bot_version'] = bot_version
    if worker_properties:
      assert isinstance(worker_properties, WorkerProperties), worker_properties
      data['worker_properties'] = worker_properties.to_dict()
    resp = self._url_read_json('/swarming/api/v1/bot/rbe/session/create',
                               data=data,
                               retry_transient=retry_transient)
    self._maybe_update_session_token(resp)
    if not resp:
      raise RBEServerError('Failed to create RBE session, see bot logs')
    if not isinstance(resp, dict):
      raise RBEServerError('Unexpected response: %s' % (resp, ))

    def get_str(key):
      val = resp.get(key)
      if not isinstance(val, str) or not val:
        raise RBEServerError('Missing or incorrect `%s` in %s' % (key, resp))
      return val

    return RBECreateSessionResponse(session_id=get_str('session_id'))

  def rbe_update_session(self,
                         status,
                         bot_version,
                         worker_properties,
                         lease=None,
                         blocking=True,
                         retry_transient=False):
    """Updates the state of an RBE session.

    Arguments:
      status: the desired bot session status as RBESessionStatus enum.
      bot_version: a string with bot version for monitoring and logs.
      worker_properties: a WorkerProperties instance or None.
      lease: an optional RBELease the bot is or was working on.
      blocking: if True, allow waiting for a bit for new leases to appear.
      retry_transient: True to retry many times on transient errors. This is
          a very crude retry mechanism intended to be used only if there's no
          better retry loop already.

    Returns:
      RBEUpdateSessionResponse tuple.

    Raises:
      RBEServerError if the RPC fails for whatever reason.
    """
    assert status in RBESessionStatus, status
    data = {
        'session': self._session_token,
        'status': status.name,
    }
    if bot_version:
      data['bot_version'] = bot_version
    if worker_properties:
      assert isinstance(worker_properties, WorkerProperties), worker_properties
      data['worker_properties'] = worker_properties.to_dict()
    if lease:
      assert isinstance(lease, RBELease), lease
      data['lease'] = lease.to_dict(omit_payload=True)
    if not blocking:
      data['nonblocking'] = True

    resp = self._url_read_json('/swarming/api/v1/bot/rbe/session/update',
                               data=data,
                               retry_transient=retry_transient)
    self._maybe_update_session_token(resp)
    if not resp:
      raise RBEServerError('Failed to update RBE session, see bot logs')
    if not isinstance(resp, dict):
      raise RBEServerError('Unexpected response: %s' % (resp, ))

    def get_str(key, optional=False):
      val = resp.get(key)
      if val is None and optional:
        return None
      if not isinstance(val, str) or not val:
        raise RBEServerError('Missing or incorrect `%s` in %s' % (key, resp))
      return val

    try:
      status = RBESessionStatus[get_str('status')]
    except KeyError as e:
      raise RBEServerError('Unrecognized status in response: %s' % e)

    lease = None
    if 'lease' in resp:
      try:
        lease = RBELease.from_dict(resp['lease'])
      except (ValueError, TypeError):
        raise RBEServerError('Invalid `lease` in %s' % (resp, ))

    return RBEUpdateSessionResponse(status=status, lease=lease)


################################################################################
## RBE wrappers.


class RBESessionException(Exception):
  """Raised on violation of RBESession protocol."""


class RBESessionStatus(enum.Enum):
  """RBE bot session statuses matching remoteworkers.BotStatus protobuf enum."""
  OK = 1
  UNHEALTHY = 2
  HOST_REBOOTING = 3
  BOT_TERMINATING = 4
  INITIALIZING = 5
  MAINTENANCE = 6


class RBELeaseState(enum.Enum):
  """RBE lease state matching remoteworkers.LeaseState protobuf enum."""
  PENDING = 1
  ACTIVE = 2
  COMPLETED = 3
  CANCELLED = 4


# Returned by rbe_create_session(...)
RBECreateSessionResponse = collections.namedtuple(
    'RBECreateSessionResponse',
    [
        # An RBE bot session ID as encoded in the session token.
        #
        # Primarily for the bot debug log. It is not used directly by anything.
        'session_id',
    ])

# Returned by rbe_update_session(...).
RBEUpdateSessionResponse = collections.namedtuple(
    'RBEUpdateSessionResponse',
    [
        # The bot session status as the RBE backend sees it.
        #
        # It is one of RBESessionStatus enum variants. In particular, a non-OK
        # status means the session is no longer alive and the bot should stop
        # using it.
        'status',

        # An optional lease assigned to the bot session, as RBELease instance.
        'lease',
    ])


class RBELease:
  """Represents a work assigned to a bot."""

  def __init__(self, lease_id, state, payload=None, result=None):
    """Constructs a lease given its details.

    Arguments:
      lease_id: a string lease ID.
      state: a RBELeaseState enum.
      payload: a dict with lease payload, if available.
      result: a dict with lease result, if available.
    """
    assert state in RBELeaseState, state
    self.id = lease_id
    self.state = state
    self.payload = payload
    self.result = result

  def __eq__(self, other):
    return (self.id == other.id and self.state == other.state
            and self.payload == other.payload and self.result == other.result)

  def clone(self):
    """Returns a copy of this object."""
    return RBELease(self.id, self.state, copy.deepcopy(self.payload),
                    copy.deepcopy(self.result))

  @staticmethod
  def from_dict(d):
    """Constructs RBELease given its dict representation.

    Raises:
      ValueError if the format is wrong.
      TypeError if types are wrong.
    """
    if not isinstance(d, dict):
      raise TypeError('Not a dict')

    def get_str(key):
      val = d.get(key, '')
      if not isinstance(val, str):
        raise TypeError('Invalid %s' % key)
      if not val:
        raise ValueError('Missing %s' % key)
      return val

    def get_optional_dict(key):
      val = d.get(key)
      if val is None:
        return None
      if not isinstance(val, dict):
        raise TypeError('Invalid %s' % key)
      return val

    try:
      state = RBELeaseState[get_str('state')]
    except KeyError as e:
      raise ValueError('Invalid state %s' % e)

    return RBELease(get_str('id'), state, get_optional_dict('payload'),
                    get_optional_dict('result'))

  def to_dict(self, omit_payload=False):
    """Converts RBELease to a dict representation.

    Arguments:
      omit_payload: if True, omit `payload` key.
    """
    d = {'id': self.id, 'state': self.state.name}
    if not omit_payload and self.payload is not None:
      d['payload'] = self.payload
    if self.result is not None:
      d['result'] = self.result
    return d


class WorkerProperties:
  """Properties describing the RBE Worker, passed to RBE as is."""

  def __init__(self, pool_id, pool_version):
    self.pool_id = pool_id
    self.pool_version = pool_version

  def __eq__(self, other):
    return (self.pool_id == other.pool_id
            and self.pool_version == other.pool_version)

  @staticmethod
  def from_dict(d):
    """Constructs WorkerProperties given its dict representation.

    Raises:
      TypeError if types are wrong.
    """
    if not isinstance(d, dict):
      raise TypeError('Not a dict')

    def get_str(key):
      val = d.get(key, '')
      if not isinstance(val, str):
        raise TypeError('Invalid %s' % key)
      return val

    return WorkerProperties(get_str('pool_id'), get_str('pool_version'))

  def to_dict(self):
    """Converts WorkerProperties to a dict representation."""
    d = {}
    if self.pool_id:
      d['pool_id'] = self.pool_id
    if self.pool_version:
      d['pool_version'] = self.pool_version
    return d


class RBESession:
  """An RBE bot session.

  It is created in the constructor and, once dead, can be recreated in-place via
  recreate(). A recreated session has a different ID.
  """

  def __init__(self,
               remote,
               instance,
               bot_version,
               worker_properties,
               session_id=None):
    """Creates a new RBE session via Swarming RBE backend.

    Arguments:
      remote: an instance of RemoteClientNative to use to call Swarming RBE.
      instance: an RBE instance this session will be running on.
      bot_version: a string with bot version for monitoring and logs.
      worker_properties: a WorkerProperties instance or None.
      session_id: if set, do not call rbe_create_session, use this RBE session.

    Raises:
      RBEServerError if the RPC fails for whatever reason.
    """
    if not session_id:
      resp = remote.rbe_create_session(bot_version, worker_properties)
      session_id = resp.session_id
    self._remote = remote
    self._instance = instance
    self._bot_version = bot_version
    self._worker_properties = worker_properties
    self._session_id = session_id
    self._last_acked_status = RBESessionStatus.OK
    self._active_lease = None
    self._finished_lease = None
    self._terminated = False

  def to_dict(self):
    """Returns the state of the session as a dict."""
    return {
        'instance':
        self._instance,
        'bot_version':
        self._bot_version,
        'worker_properties':
        self._worker_properties.to_dict() if self._worker_properties else None,
        'session_id':
        self._session_id,
        'last_acked_status':
        self._last_acked_status.name,
        'active_lease':
        self._active_lease.to_dict() if self._active_lease else None,
        'finished_lease':
        self._finished_lease.to_dict() if self._finished_lease else None,
        'terminated':
        self._terminated,
    }

  @staticmethod
  def from_dict(remote, dump):
    """Constructs RBESession from a dict created by to_dict().

    Raises:
      ValueError if the dump doesn't appear to be valid.
    """
    worker_properties = None
    try:
      wp = dump.get('worker_properties')
      if wp:
        worker_properties = WorkerProperties.from_dict(wp)
    except TypeError as e:
      raise ValueError('Invalid worker_properties dict: %s' % e)

    try:
      session = RBESession(remote, dump['instance'], dump['bot_version'],
                           worker_properties, dump['session_id'])
      last_acked_status = dump['last_acked_status']
      active_lease = dump['active_lease']
      finished_lease = dump['finished_lease']
      session._terminated = dump['terminated']
    except KeyError as e:
      raise ValueError('Missing key %s' % e)

    try:
      session._last_acked_status = RBESessionStatus[last_acked_status]
    except KeyError as e:
      raise ValueError('Invalid RBESessionStatus: %s' % e)

    try:
      if active_lease:
        session._active_lease = RBELease.from_dict(active_lease)
      if finished_lease:
        session._finished_lease = RBELease.from_dict(finished_lease)
    except TypeError:
      raise ValueError('Invalid lease dict')

    return session

  def restore(self, dump):
    """Update this session (in-place) with the state stored by to_dict().

    Raises:
      ValueError if the dump doesn't appear to be valid.
    """
    loaded = RBESession.from_dict(self._remote, dump)
    self._instance = loaded._instance
    self._session_id = loaded._session_id
    self._bot_version = loaded._bot_version
    self._worker_properties = loaded._worker_properties
    self._last_acked_status = loaded._last_acked_status
    self._active_lease = loaded._active_lease
    self._finished_lease = loaded._finished_lease
    self._terminated = loaded._terminated

  @property
  def instance(self):
    """The RBE instance this session is running on."""
    return self._instance

  @property
  def session_id(self):
    """The RBE session ID for logs."""
    return self._session_id

  @property
  def alive(self):
    """True if this session is open, but possibly terminating."""
    return not self._terminated and self._last_acked_status in (
        RBESessionStatus.OK,
        RBESessionStatus.MAINTENANCE,
        RBESessionStatus.HOST_REBOOTING,
        RBESessionStatus.BOT_TERMINATING,
    )

  @property
  def terminating(self):
    """True if this session is being closed (in particular by the server)."""
    return self._last_acked_status in (
        RBESessionStatus.HOST_REBOOTING,
        RBESessionStatus.BOT_TERMINATING,
    )

  @property
  def active_lease(self):
    """An RBELease the bot should be working on now."""
    return self._active_lease

  def update(self, status, blocking=True):
    """Updates the state of the session, picks up a new lease, if any.

    Should be called in the outer bot loop, when the bot is waiting for new
    tasks. This method reports the result of the last finished lease (if any)
    to the server and picks up a new lease (if any). It also recognizes when
    the session is closed by the server and updates `alive` property
    accordingly.

    When this method is called the session must be alive and must not have
    `active_lease` set, otherwise RBESessionException is raised.

    Calling this methods may update `alive` and `active_lease` properties
    as side effects:
      * A session may become dead if it is gone on the backend side.
      * There may be a new active lease assigned to the session after this call.

    Arguments:
      status: the new RBE session status to report as RBESessionStatus enum.
      blocking: if True, allow waiting for a bit for new leases to appear.

    Returns:
      A new active RBELease, if any. Also available via `active_lease` property.

    Raises:
      RBESessionException if the local session is in a wrong state.
      RBEServerError if the RPC fails for whatever reason.
    """
    if not self.alive:
      raise RBESessionException('Calling update(...) with dead session')
    if self.active_lease:
      raise RBESessionException('Calling update(...) with an active lease')

    # Report the result of the finished lease (if any), and get a new lease.
    assert (not self._finished_lease
            or self._finished_lease.state == RBELeaseState.COMPLETED
            ), self._finished_lease
    lease = self._update(status=status,
                         lease=self._finished_lease,
                         blocking=blocking)
    self._finished_lease = None  # flushed the result successfully

    # A dead session should not be producing new leases.
    if not self.alive:
      if lease:
        logging.error('Ignoring a lease from dead session: %s', lease.id)
      return None

    # A new lease should be in PENDING state and have a payload.
    if lease:
      if lease.state != RBELeaseState.PENDING:
        logging.error('Got a non-PENDING lease: %s', lease.id)
      if lease.payload is None:
        logging.error('Got a lease without payload: %s', lease.id)

    self._active_lease = lease
    return lease

  def ping_active_lease(self):
    """Notifies the backend the bot is still working on the active lease.

    This method "pings" the lease (making the RBE server know the bot is not
    dead yet) and polls its cancellation status. Must be called only if
    `active_lease` is set, otherwise RBESessionException is raised.

    Returns True if the lease is still alive and the bot should keep working on.
    Once the bot completes the lease, it should call finish_active_lease(...).

    Returns False, but doesn't unset `active_lease`, if the lease was canceled
    remotely. The bot should start graceful cancellation process in that case,
    eventually calling finish_active_lease(...) to mark the lease as completed.

    Returns False and unsets `active_lease` if the lease was "lost". This
    indicates the server already gave up on this lease and the bot is free to
    just drop it whenever it wants: no result reporting is necessary, no need
    to call finish_active_lease(...).

    Additionally calling this method may update `alive` property as a side
    effect: a session may become dead if it is gone on the backend side. The
    active lease is considered lost in that case as well. If the local session
    was already dead when the method was called, this function does nothing and
    just returns False.

    Returns:
      True to keep working on the active lease, False if it was canceled or
      lost (use `active_lease` value to decide).

    Raises:
      RBESessionException if the local session is in a wrong state.
      RBEServerError if the RPC fails for whatever reason.
    """
    if not self.active_lease:
      raise RBESessionException('ping_active_lease(...) without a lease')
    if not self.alive:
      logging.warning('The session is already gone')
      return False

    # Report the lease as ACTIVE. Do not use a poll token, it might have expired
    # already (also we are not polling for new tasks anyway). The session token
    # must still be good, since it is refreshed by _update. Report the latest
    # snapshot of the dimensions though, since the API always wants dimensions.
    self._active_lease.state = RBELeaseState.ACTIVE
    lease = self._update(status=RBESessionStatus.OK, lease=self._active_lease)

    # If the session is gone, the lease is lost.
    if not self.alive:
      logging.error('The session was lost')
      self.abandon()
      return False

    # No lease in the response means the active lease was lost.
    if not lease:
      logging.error('Lost active lease %s', self._active_lease.id)
      self._active_lease = None
      return False

    # This must not be happening either, but also treat it as a lost lease.
    if lease.id != self._active_lease.id:
      logging.error('Got unexpected lease ID: want %s, got %s',
                    self._active_lease.id, lease.id)
      self._active_lease = None
      return False

    # Keep working on the lease if the server tells it is still ACTIVE.
    logging.info('The lease %s is %s', lease.id, lease.state)
    return lease.state == RBELeaseState.ACTIVE

  def finish_active_lease(self, result, flush=False):
    """Marks the current active lease as done.

    Must be called only if `active_lease` is set. This method unsets it, thus
    signifying the session is ready to pick up a new lease in update(...). Must
    be called even if the lease was canceled by the server.

    If `flush` is False, the result of the finished lease will be reported to
    the backend with the next update(...) or terminate(...) calls, whenever they
    happen. In this case this is a purely local state change, it doesn't do any
    RPCs.

    If `flush` is True, the result will be reported to the backend right here
    via a call to update(...) with MAINTENANCE status and the latest used
    dimensions. Note that this may flip the session into the dead state if the
    session expired on the backend side.

    If the session is already dead, the result will be lost.

    Arguments:
      result: a dict with task execution results or None if not available.

    Raises:
      RBESessionException if the local session is in a wrong state.
      RBEServerError if `flush` is True and the RPC fails for whatever reason.
    """
    if not self.active_lease:
      raise RBESessionException('finish_active_lease(...) without a lease')

    lease, self._active_lease = self._active_lease, None
    lease.state = RBELeaseState.COMPLETED
    lease.result = copy.deepcopy(result)

    assert not self._finished_lease
    self._finished_lease = lease

    if not flush:
      return

    if not self.alive:
      logging.error('Losing results of %s', self._finished_lease.id)
      self._finished_lease = None
      return

    lease = self._update(status=RBESessionStatus.MAINTENANCE,
                         lease=self._finished_lease,
                         blocking=False,
                         retry_transient=True)
    self._finished_lease = None  # flushed the result successfully
    if lease:
      logging.error('Ignoring unexpected lease in MAINTENANCE: %s', lease.id)

  def terminate(self, status=RBESessionStatus.BOT_TERMINATING):
    """Terminates this RBE session.

    Does nothing if the session is dead (in particular was already terminated).
    Ignores `active_lease`.

    This is the same as passing BOT_TERMINATING or HOST_REBOOTING status to
    update(...), except it retries the call a bunch of times on transient RPC
    errors to increase chances of successfully reporting results of the last
    finished lease. If errors are still happening, eventually just gives up.
    Session termination usually happens when the process is exiting, there's no
    time left to retry forever.
    """
    assert status in (
        RBESessionStatus.BOT_TERMINATING,
        RBESessionStatus.HOST_REBOOTING,
    ), status

    if self._active_lease:
      logging.error('Ignoring active lease %s', self._active_lease.id)

    if not self.alive:
      if self._finished_lease:
        logging.error('Losing results of %s', self._finished_lease.id)
      return

    try:
      lease = self._update(status=status,
                           lease=self._finished_lease,
                           retry_transient=True)
      if lease:
        logging.error('Ignoring a lease from terminated session: %s', lease.id)
      self._finished_lease = None  # flushed the result
    except RBEServerError as e:
      logging.error('Error terminating RBE session: %s', e)

  def recreate(self):
    """Opens a new session that replaces the current one.

    Should be called only for dead sessions (`alive` is False). Raises
    RBESessionException otherwise. Uses dimensions last passed to update(...).

    On success updates `session_id` and `alive`.

    Raises:
      RBESessionException if the local session is in a wrong state.
      RBEServerError if the RPC fails for whatever reason.
    """
    if self.alive:
      raise RBESessionException('recreate(...) with a living session')

    # The previous session is gone. Log any abandoned leases.
    self.abandon()

    # Try to create a replacement session using the same parameters. We need to
    # pass the previous session token to grab server-signed parameters from it
    # in case the poll token is already stale.
    resp = self._remote.rbe_create_session(self._bot_version,
                                           self._worker_properties)
    self._session_id = resp.session_id
    self._last_acked_status = RBESessionStatus.OK
    self._terminated = False

  def abandon(self):
    """Abandons this session, logging any pending state that is lost now.

    Should be called only for dead or terminating sessions. Raises
    RBESessionException otherwise.

    Doesn't do any RPCs.

    Raises:
      RBESessionException if the local session is in a wrong state.
    """
    if self.alive and not self.terminating:
      raise RBESessionException('abandon() with a living session')
    if self._active_lease:
      logging.error('Lost active lease %s', self._active_lease.id)
      self._active_lease = None
    if self._finished_lease:
      logging.error('Lost results of %s', self._finished_lease.id)
      self._finished_lease = None

  def _update(self, status, lease=None, blocking=True, retry_transient=False):
    """Used internally by other methods.

    Updates `alive` and `terminating` properties based on the server response.
    Doesn't touch the state related to leases.

    Arguments:
      status: the desired bot session status as RBESessionStatus enum.
      lease: an optional RBELease the bot is or was working on.
      blocking: if True, allow waiting for a bit for new leases to appear.
      retry_transient: True to retry many times on transient errors.

    Returns:
      RBELease returned by the backend, if any.

    Raises:
      RBEServerError if the RPC fails for whatever reason.
    """
    assert status in RBESessionStatus, status

    # Update the session on the backend side, flush the finished lease result,
    # pick up a new lease.
    resp = self._remote.rbe_update_session(
        status,
        self._bot_version,
        self._worker_properties,
        lease,
        blocking,
        retry_transient,
    )

    logging.debug('RBE %s: %s => %s', self._session_id, status, resp.status)
    if resp.status != RBESessionStatus.OK:
      # The server told us the session is pending termination or already gone.
      self._last_acked_status = resp.status
    else:
      # Use whatever we told the server. The server accepted this status.
      self._last_acked_status = status

    # If we asked the server to terminate the session and the server ACKed this,
    # the session is considered gracefully terminated.
    self._terminated = (status in (
        RBESessionStatus.HOST_REBOOTING,
        RBESessionStatus.BOT_TERMINATING,
    ) and self._last_acked_status in (
        RBESessionStatus.HOST_REBOOTING,
        RBESessionStatus.BOT_TERMINATING,
    ))

    return resp.lease


################################################################################
## Session state serialization.


class SessionState:
  """Passed between the main bot process and the task runner."""

  def __init__(self, session_id, session_token, rbe_session):
    """Constructs a new SessionState.

    Args:
      session_id: Swarming bot session ID, just for logs.
      session_token the current bot session token.
      rbe_session: an RBESession.to_dict() value if the bot is in RBE mode.
    """
    self.session_id = session_id
    self.session_token = session_token
    self.rbe_session = rbe_session

  def dump(self, path):
    """Dumps the state of the session to a JSON file on disk.

    Raises:
      OSError if can't open or write the file.
    """
    with open(path, 'w') as f:
      json.dump(
          {
              'session_id': self.session_id,
              'session_token': self.session_token,
              'rbe_session': self.rbe_session,
          }, f)

  @staticmethod
  def load(path):
    """Constructs SessionState from a dump created by dump().

    Raises:
      OSError if can't open or read the file.
      ValueError if the dump doesn't appear to be valid.
    """
    with open(path, 'r') as f:
      try:
        dump = json.load(f)
      except ValueError as e:
        raise ValueError('Not a valid JSON: %s' % e)
    try:
      return SessionState(dump['session_id'], dump['session_token'],
                          dump['rbe_session'])
    except KeyError as e:
      raise ValueError('Missing required key %s' % e)
