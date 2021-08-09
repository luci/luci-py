# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Project token implementation."""

import datetime
import logging

from google.appengine.ext import ndb

from components import utils

from . import exceptions
from . import api

@ndb.tasklet
def project_token_async(
    project_id,
    oauth_scopes,
    auth_request_func,
    min_validity_duration_sec=5*60,
    tags=None,
    token_server_url=None,
    ):
  """Creates a project token by contacting the token server.

  Args:
    project_id (str): For which project to obtain a token.
      This is the LUCI project for which the call is expected to issue
      an identity token.
      Example: 'chromium'
    oauth_scopes (str): List of OAuth scopes for the project token.
      Example: 'https://www.googleapis.com/auth/cloud-platform.read-only'
    auth_request_func: Function to perform an authenticated request.
      Required to hit token creation APIs which require authentication.
    min_validity_duration_sec (int): defines requested lifetime of a new token.
      It will bet set as tokens' TTL if there's no existing cached tokens with
      sufficiently long lifetime. Default is 5 minutes.
    tags (list of str): optional list of key:value pairs to use as audit tags
      along with the request to obtain a token.
    token_server_url (str): the URL for the token service that will mint the
      token. Defaults to the URL provided by the primary auth service.

  Returns:
    token as ndb.Future.

  Raises:
    ValueError if args are invalid.
    TokenCreationError if could not create a token.
    TokenAuthorizationError on HTTP 403 response from auth service.
  """

  # Validate tags.
  tags = sorted(tags or [])
  for tag in tags:
    parts = tag.split(':', 1)
    if len(parts) != 2 or parts[0] == '' or parts[1] == '':
      raise ValueError('Bad project token tag: %r' % tag)

  # Grab the token service URL.
  if not token_server_url:
    token_server_url = api.get_request_auth_db().token_server_url
    if not token_server_url:
      raise exceptions.TokenCreationError('Token server URL is not configured')

  if isinstance(oauth_scopes, str):
    oauth_scopes = [oauth_scopes]

  # End of validation.

  # See MintProjectTokenRequest in
  # https://github.com/luci/luci-go/blob/main/tokenserver/api/minter/v1/token_minter.proto.
  req = {
    'luci_project': project_id,
    'oauth_scope': oauth_scopes,
    'min_validity_duration': min_validity_duration_sec,
    'audit_tags': tags,
  }

  # Request a new one.
  logging.info(
      'Minting a project token for %r',
      {k: v for k, v in sorted(req.items()) if v},
  )
  res = yield auth_request_func(
      '%s/prpc/tokenserver.minter.TokenMinter/MintProjectToken' %
          token_server_url,
      method='POST',
      payload=req)

  token = res.get('accessToken')
  if not token or not isinstance(token, basestring):
    logging.error('Bad MintProjectToken response: %s', res)
    raise exceptions.TokenCreationError('Bad response, no token')

  service_account_email = res.get('serviceAccountEmail')
  if not service_account_email or (
      not isinstance(service_account_email, basestring)):
    logging.error('Bad MintProjectToken response: %s', res)
    raise exceptions.TokenCreationError(
        'Bad response, no service account email')

  expiry = utils.parse_rfc3339_datetime(res.get('expiry'))
  if not isinstance(expiry, datetime.datetime):
    logging.error('Bad MintProjectToken response: %s', res)
    raise exceptions.TokenCreationError(
        'Unexpected response, expiry is absent or not a number')

  token = {
      'access_token': str(token),
      'exp_ts': utils.datetime_to_timestamp(expiry) / 10**6
  }

  logging.info(
      'Token server "%s" generated project token for %s, fingerprint=%s)',
      res.get('serviceVersion'),
      service_account_email,
      utils.get_token_fingerprint(str(token))
  )

  raise ndb.Return(token)


def project_token(**kwargs):
  """Blocking version of project_token_async."""
  return project_token_async(**kwargs).get_result()
