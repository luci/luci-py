# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Functions to generate OAuth access tokens to use by bots or inside tasks."""

import collections
import datetime
import logging
import os
import random

from google.appengine.api import app_identity
from google.appengine.api import memcache

from components import auth
from components import net
from components import utils

from server import pools_config
from server import realms
from server import service_accounts_utils
from server import task_pack


# Matches tokenserver.minter.SERVICE_ACCOUNT_TOKEN_ACCESS_TOKEN
TOKEN_KIND_ACCESS_TOKEN = 1
# Matches tokenserver.minter.SERVICE_ACCOUNT_TOKEN_ID_TOKEN
TOKEN_KIND_ID_TOKEN = 2

# Brackets for possible lifetimes of OAuth tokens produced by this module.
MIN_TOKEN_LIFETIME_SEC = 5 * 60
MAX_TOKEN_LIFETIME_SEC = 3600 * 60  # this is just hardcoded by Google APIs

AccessToken = collections.namedtuple(
    'AccessToken',
    [
        'access_token',  # actual access or ID token
        'expiry',  # unix timestamp (in seconds, int) when it expires
    ])


class PermissionError(Exception):
  """The service account is not allowed to be used by the caller."""


class MisconfigurationError(Exception):
  """Something is not correctly configured.

  The difference from PermissionError is that generally the account is allowed
  to be used, but some misconfiguration (e.g. IAM permissions) preclude the
  token server or Swarming from using it.

  Can be transformed into HTTP 400 error and returned to the caller.
  """


class InternalError(Exception):
  """Something unexpectedly misbehaves.

  This generally should not happen. It's fine to return HTTP 500 if it does. The
  content of the exception is scrubbed of private information that should not be
  visible to the caller.
  """


def has_token_server():
  """Returns True if Token Server URL is configured.

  Token Server is required to use task-associated service accounts.
  """
  return bool(auth.get_request_auth_db().token_server_url)


def get_task_account_token(task_id, bot_id, kind, scopes=None, audience=None):
  """Returns an access or ID token for a service account associated with a task.

  Assumes authorization checks have been made already. If the task is not
  configured to use service account returns ('none', None). If the task is
  configured to use whatever bot is using when calling Swarming, returns
  ('bot', None).

  Otherwise returns (<email>, AccessToken with valid token for <email>).

  Args:
    task_id: ID of the task.
    bot_id: ID of the bot that executes the task, for logs.
    kind: either TOKEN_KIND_ACCESS_TOKEN or TOKEN_KIND_ID_TOKEN.
    scopes: a list of requested OAuth scopes for TOKEN_KIND_ACCESS_TOKEN.
    audience: a requested audience for TOKEN_KIND_ID_TOKEN.

  Returns:
    (<service account email> or 'bot' or 'none', AccessToken or None).

  Raises:
    PermissionError if the token server forbids the usage.
    MisconfigurationError if the service account is misconfigured.
    InternalError if the RPC fails unexpectedly.
  """
  # Asserts 'scopes' are not empty when using TOKEN_KIND_ACCESS_TOKEN, etc.
  _check_token_args(kind, scopes, audience)

  # Grab corresponding TaskRequest.
  try:
    result_summary_key = task_pack.run_result_key_to_result_summary_key(
        task_pack.unpack_run_result_key(task_id))
    task_request_key = task_pack.result_summary_key_to_request_key(
        result_summary_key)
  except ValueError as exc:
    logging.error('Unexpectedly bad task_id: %s', exc)
    raise MisconfigurationError('Bad task_id: %s' % task_id)

  task_request = task_request_key.get()
  if not task_request:
    raise MisconfigurationError('No such task request: %s' % task_id)

  # 'none' or 'bot' cases are handled by the bot locally, no token for them.
  if task_request.service_account in ('none', 'bot'):
    return task_request.service_account, None

  # The only possible case is a service account email. Double check this.
  if not service_accounts_utils.is_service_account(
      task_request.service_account):
    raise MisconfigurationError(
        'Not a service account email: %s' % task_request.service_account)

  # The code below works only with realms-aware tasks. All tasks should have
  # the realm already.
  if not task_request.realm:
    raise MisconfigurationError('The task is not using LUCI Realms')

  # Re-check if the service account is still allowed to run in the realm,
  # because it may have changed since the last check.
  pool_cfg = pools_config.get_pool_config(task_request.pool)
  realms.check_tasks_act_as(task_request, pool_cfg, enforce=True)

  # Ask the token server to generate the token.
  audit_tags = [
      'swarming:bot_id:%s' % bot_id,
      'swarming:task_id:%s' % task_id,
      'swarming:task_name:%s' % task_request.name,
  ]
  access_token, expiry = _mint_service_account_token(
      task_request.service_account, task_request.realm, audit_tags,
      kind, scopes, audience)

  # Log and return the token.
  token = AccessToken(
      access_token, int(utils.datetime_to_timestamp(expiry) / 1e6))
  _check_and_log_token(
      'task associated', kind, task_request.service_account, token)
  return task_request.service_account, token


def get_system_account_token(system_service_account, kind,
                             scopes=None, audience=None):
  """Returns an access or ID token to use on bots to call internal services.

  "Internal services" are (loosely) everything that is needed for correct
  functioning of the internal guts of the bot (at least Isolate and CIPD).

  Assumes authorization checks have been made already. If the bot is not
  configured to use service account returns ('none', None). If the bot is
  configured to use whatever it is using when calling Swarming itself, returns
  ('bot', None).

  Otherwise returns (<email>, AccessToken with valid token for <email>).

  Args:
    system_service_account: whatever is specified in bots.cfg for the bot.
    kind: either TOKEN_KIND_ACCESS_TOKEN or TOKEN_KIND_ID_TOKEN.
    scopes: a list of requested OAuth scopes for TOKEN_KIND_ACCESS_TOKEN.
    audience: a requested audience for TOKEN_KIND_ID_TOKEN.

  Returns:
    (<service account email> or 'bot' or 'none', AccessToken or None).

  Raises:
    PermissionError if the usage of the account is forbidden.
    MisconfigurationError if the service account is misconfigured.
    InternalError if the RPC fails unexpectedly.
  """
  # Asserts 'scopes' are not empty when using TOKEN_KIND_ACCESS_TOKEN, etc.
  _check_token_args(kind, scopes, audience)

  if not system_service_account:
    return 'none', None  # no auth is configured

  if system_service_account == 'bot':
    return 'bot', None  # the bot should use tokens provided by local hooks

  # TODO(crbug.com/1184230): ID tokens for system account are not implemented.
  # It is possible to implement them using generateIdToken Cloud IAM call, but
  # they aren't needed yet.
  if kind == TOKEN_KIND_ID_TOKEN:
    raise MisconfigurationError(
        'ID tokens for system account are not implemented')

  # Attempt to mint a token (or grab an existing one from cache).
  try:
    blob, expiry = auth.get_access_token(
        scopes=scopes,
        act_as=system_service_account,
        min_lifetime_sec=MIN_TOKEN_LIFETIME_SEC)
  except auth.AccessTokenError as exc:
    if exc.transient:
      raise InternalError(exc.message)
    raise MisconfigurationError(exc.message)

  assert isinstance(blob, basestring)
  assert isinstance(expiry, (int, long, float))
  token = AccessToken(blob, int(expiry))
  _check_and_log_token('bot associated', kind, system_service_account, token)
  return system_service_account, token


### Private code


def _check_token_args(kind, scopes, audience):
  """Asserts args match the token kind."""
  if kind == TOKEN_KIND_ACCESS_TOKEN:
    assert scopes is not None
    assert audience is None
  elif kind == TOKEN_KIND_ID_TOKEN:
    assert scopes is None
    assert audience is not None
  else:
    raise AssertionError('Unknown token kind %s' % (kind,))


def _mint_service_account_token(service_account, realm, audit_tags,
                                kind, scopes, audience):
  """Does the RPC to the token server to get an access or ID token using realm.

  Args:
    service_account: a service account email to use.
    realm: a realm name to use.
    audit_tags: a list with information tags to send with the RPC, for logging.
    kind: either TOKEN_KIND_ACCESS_TOKEN or TOKEN_KIND_ID_TOKEN.
    scopes: a list of requested OAuth scopes for TOKEN_KIND_ACCESS_TOKEN.
    audience: a requested audience for TOKEN_KIND_ID_TOKEN.

  Raises:
    PermissionError if the token server forbids the usage.
    MisconfigurationError if the service account is misconfigured.
    InternalError if the RPC fails unexpectedly.
  """
  # extract LUCI project from realm '<project>:<realm>'.
  luci_project = realm.split(':')[0]
  assert luci_project
  resp = _call_token_server(
      'MintServiceAccountToken',
      {
          'tokenKind': kind,
          'serviceAccount': service_account,
          'realm': realm,
          'oauthScope': scopes,
          'idTokenAudience': audience,
          'minValidityDuration': MIN_TOKEN_LIFETIME_SEC,
          'auditTags': _common_audit_tags() + audit_tags,
      },
      luci_project)
  try:
    token = str(resp['token'])
    service_version = str(resp['serviceVersion'])
    expiry = utils.parse_rfc3339_datetime(resp['expiry'])
  except (KeyError, ValueError) as exc:
    logging.error('Bad response from the token server (%s):\n%r', exc, resp)
    raise InternalError('Bad response from the token server, see server logs')
  logging.info('The token server replied, its version: %s', service_version)
  return token, expiry


def _call_token_server(method, request, project_id=None):
  """Sends an RPC to tokenserver.minter.TokenMinter service.

  Args:
    method: name of the method to call.
    request: dict with request fields.
    project_id: if set, act with the authority of this LUCI project.

  Returns:
    Dict with response fields.

  Raises:
    PermissionError on HTTP 403 reply.
    MisconfigurationError if the service account is misconfigured.
    InternalError if the RPC fails unexpectedly.
  """
  # Double check token server URL looks sane ('https://....'). This is checked
  # when it's imported from the config. This check should never fail.
  ts_url = auth.get_request_auth_db().token_server_url
  try:
    utils.validate_root_service_url(ts_url)
  except ValueError as exc:
    raise MisconfigurationError(
        'Invalid token server URL %s: %s' % (ts_url, exc))

  # See TokenMinter in
  # https://chromium.googlesource.com/infra/luci/luci-go/+/master/tokenserver/api/minter/v1/token_minter.proto
  # But beware that proto JSON serialization uses camelCase, not snake_case.
  try:
    return net.json_request(
        url='%s/prpc/tokenserver.minter.TokenMinter/%s' % (ts_url, method),
        method='POST',
        payload=request,
        project_id=project_id,
        scopes=[net.EMAIL_SCOPE])
  except net.Error as exc:
    logging.error('Error calling %s (HTTP %s: %s):\n%s', method,
                  exc.status_code, exc.message, exc.response)
    if exc.status_code == 403:
      raise PermissionError(
          'HTTP 403 from the token server:\n%s' % exc.response)
    if exc.status_code == 400:
      raise MisconfigurationError(
          'HTTP 400 from the token server:\n%s' % exc.response)
    # Don't put the response body into the error message, it may contain
    # internal details (that are public to Swarming server, but may not be
    # public to whoever is calling the Swarming server now).
    raise InternalError('Failed to call %s, see server logs' % method)


def _common_audit_tags():
  """Returns a list of tags that describe circumstances of the RPC call.

  They end up in Token Server's logs and can be used to correlate token server
  requests to Swarming requests.
  """
  # Note: particular names and format of tags is chosen to be consistent with
  # Token Server's logging.
  return [
      'swarming:gae_request_id:%s' % os.getenv('REQUEST_LOG_ID', '?'),
      'swarming:service_version:%s/%s' % (app_identity.get_application_id(),
                                          utils.get_app_version()),
  ]


def _check_and_log_token(flavor, kind, account_email, token):
  """Checks the lifetime and logs details about the generated access token."""
  if kind == TOKEN_KIND_ACCESS_TOKEN:
    kind_str = 'access'
  elif kind == TOKEN_KIND_ID_TOKEN:
    kind_str = 'ID'
  else:
    raise AssertionError('Impossible %s' % (kind,))
  expires_in = token.expiry - utils.time_time()
  logging.info(
      'Got %s %s token: email=%s, fingerprint=%s, expiry=%d, expiry_in=%d',
      flavor, kind_str, account_email,
      utils.get_token_fingerprint(token.access_token),
      token.expiry, expires_in)
  # Give 2 min of wiggle room to account for various effects related to
  # relativity of clocks (us vs Google backends that produce the token) and
  # indeterminism of network propagation delays. 2 min should be more than
  # enough to account for them. These asserts should never be hit.
  assert expires_in < MAX_TOKEN_LIFETIME_SEC + 60
  assert expires_in > MIN_TOKEN_LIFETIME_SEC - 60
