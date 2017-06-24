# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Functions to generate OAuth access tokens to use by bots or inside tasks."""

import collections
import logging

from components import auth
from components import utils


# Brackets for possible lifetimes of tokens produced by this module.
MIN_TOKEN_LIFETIME_SEC = 5 * 60
MAX_TOKEN_LIFETIME_SEC = 3600 * 60  # this is just hardcoded by Google APIs


AccessToken = collections.namedtuple('AccessToken', [
  'access_token',  # actual access token
  'expiry',        # unix timestamp (in seconds) when it expires
])


def get_task_account_token(task_id, scopes):  # pylint: disable=unused-argument
  """Returns an access token for a service account associated with a task.

  Assumes authorization checks have been made already. If the task is not
  configured to use service account returns ('none', None). If the task is
  configured to use whatever bot is using when calling Swarming, returns
  ('bot', None).

  Otherwise returns (<email>, AccessToken with valid token for <email>).

  Args:
    task_id: ID of the task.
    scopes: list of requested OAuth scopes.

  Returns:
    (<service account email> or 'bot' or 'none', AccessToken or None).

  Raises:
    auth.AccessTokenError if the token can't be generated.
  """
  # TODO(vadimsh): Grab TaskRequest entity based on 'task_id' and use data
  # there to generate new access token for task-associated service account.
  raise NotImplementedError('"task" service accounts are not implemented yet')


def get_system_account_token(system_service_account, scopes):
  """Returns an access token to use on bots when calling internal services.

  "Internal services" are (loosely) everything that is needed for correct
  functioning of the internal guts of the bot (at least Isolate and CIPD).

  Assumes authorization checks have been made already. If the bot is not
  configured to use service account returns ('none', None). If the bot is
  configured to use whatever it is using when calling Swarming itself, returns
  ('bot', None).

  Otherwise returns (<email>, AccessToken with valid token for <email>).

  Args:
    system_service_account: whatever is specified in bots.cfg for the bot.
    scopes: list of requested OAuth scopes.

  Returns:
    (<service account email> or 'bot' or 'none', AccessToken or None).

  Raises:
    auth.AccessTokenError if the token can't be generated.
  """
  if not system_service_account:
    return 'none', None  # no auth is configured

  if system_service_account == 'bot':
    return 'bot', None  # the bot should use tokens provided by local hooks

  # Attempt to mint a token (or grab an existing one from cache).
  blob, expiry = auth.get_access_token(
      scopes=scopes,
      act_as=system_service_account,
      min_lifetime_sec=MIN_TOKEN_LIFETIME_SEC)
  assert isinstance(blob, basestring)
  assert isinstance(expiry, (int, long, float))
  token = AccessToken(blob, int(expiry))
  _check_and_log_token(system_service_account, token)
  return system_service_account, token


### Private code


def _check_and_log_token(account_email, token):
  """Checks the lifetime and logs details about the generated access token."""
  expires_in = token.expiry - utils.time_time()
  logging.info(
      'Got access token: email=%s, fingerprint=%s, expiry=%d, expiry_in=%d',
      account_email,
      utils.get_token_fingerprint(token.access_token),
      token.expiry,
      expires_in)
  # Give 2 min of wiggle room to account for various effects related to
  # relativity of clocks (us vs Google backends that produce the token) and
  # indeterminism of network propagation delays. 2 min should be more than
  # enough to account for them. These asserts should never be hit.
  assert expires_in < MAX_TOKEN_LIFETIME_SEC + 60
  assert expires_in > MIN_TOKEN_LIFETIME_SEC - 60
