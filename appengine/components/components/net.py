# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Wrapper around urlfetch to call REST API with service account credentials."""

import json
import logging
import urllib

from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import auth
from components import utils


class Error(Exception):
  """Raised on non-transient errors."""
  def __init__(self, msg, status_code, response):
    super(Error, self).__init__(msg)
    self.status_code = status_code
    self.response = response


class NotFoundError(Error):
  """Raised if endpoint returns 404."""


class AuthError(Error):
  """Raised if endpoint returns 401 or 403."""


# Do not log Error exception raised from a tasklet, it is expected to happen.
ndb.add_flow_exception(Error)


def urlfetch_async(**kwargs):
  """To be mocked in tests."""
  return ndb.get_context().urlfetch(**kwargs)


@ndb.tasklet
def request_async(
    url,
    method='GET',
    payload=None,
    params=None,
    headers=None,
    scopes=None,
    service_account_key=None,
    deadline=None,
    max_attempts=None):
  """Sends a REST API request, returns raw unparsed response.

  Retries the request on transient errors for up to |max_attempts| times.

  Args:
    url: url to send the request to.
    method: HTTP method to use, e.g. GET, POST, PUT.
    payload: raw data to put in the request body.
    params: dict with query GET parameters (i.e. ?key=value&key=value).
    headers: additional request headers.
    scopes: OAuth2 scopes for the access token (ok skip auth if None).
    service_account_key: auth.ServiceAccountKey with credentials.
    deadline: deadline for a single attempt (10 sec by default).
    max_attempts: how many times to retry on errors (4 times by default).

  Returns:
    Buffer with raw response.

  Raises:
    NotFoundError on 404 response.
    AuthError on 401 or 403 response.
    Error on any other non-transient error.
  """
  deadline = 10 if deadline is None else deadline
  max_attempts = 4 if max_attempts is None else max_attempts

  if utils.is_local_dev_server():
    protocols = ('http://', 'https://')
  else:
    protocols = ('https://',)
  assert url.startswith(protocols) and '?' not in url, url
  if params:
    url += '?' + urllib.urlencode(params)

  if scopes:
    access_token = auth.get_access_token(scopes, service_account_key)[0]
    headers = (headers or {}).copy()
    headers['Authorization'] = 'Bearer %s' % access_token

  if payload is not None:
    assert isinstance(payload, str), type(payload)
    assert method in ('CREATE', 'POST', 'PUT'), method

  attempt = 0
  while attempt < max_attempts:
    if attempt:
      logging.info('Retrying...')
    attempt += 1
    logging.info('%s %s', method, url)
    try:
      response = yield urlfetch_async(
          url=url,
          payload=payload,
          method=method,
          headers=headers,
          follow_redirects=False,
          deadline=deadline,
          validate_certificate=True)
    except (apiproxy_errors.DeadlineExceededError, urlfetch.Error) as e:
      # Transient network error or URL fetch service RPC deadline.
      logging.warning('%s %s failed: %s', method, url, e)
      continue

    # Transient error on the other side.
    if response.status_code >= 500 or response.status_code == 408:
      logging.warning(
          '%s %s failed with HTTP %d: %r',
          method, url, response.status_code, response.content)
      continue

    # Non-transient error.
    if 300 <= response.status_code < 500:
      logging.warning(
          '%s %s failed with HTTP %d: %r',
          method, url, response.status_code, response.content)
      cls = Error
      if response.status_code == 404:
        cls = NotFoundError
      elif response.status_code in (401, 403):
        cls = AuthError
      raise cls(
          'Failed to call %s: HTTP %d' % (url, response.status_code),
          response.status_code, response.content)

    # Success. Beware of large responses.
    if len(response.content) > 1024 * 1024:
      logging.warning('Response size: %.1f KiB', len(response.content) / 1024.0)
    raise ndb.Return(response.content)

  raise Error(
      'Failed to call %s after %d attempts' % (url, max_attempts),
      response.status_code, response.content)


def request(*args, **kwargs):
  """Blocking version of request_async."""
  return request_async(*args, **kwargs).get_result()


@ndb.tasklet
def json_request_async(
    url,
    method='GET',
    payload=None,
    params=None,
    headers=None,
    scopes=None,
    service_account_key=None,
    deadline=None,
    max_attempts=None):
  """Sends a JSON REST API request, returns deserialized response.

  Retries the request on transient errors for up to |max_attempts| times.

  Args:
    url: url to send the request to.
    method: HTTP method to use, e.g. GET, POST, PUT.
    payload: object to serialized to JSON and put in the request body.
    params: dict with query GET parameters (i.e. ?key=value&key=value).
    headers: additional request headers.
    scopes: OAuth2 scopes for the access token (ok skip auth if None).
    service_account_key: auth.ServiceAccountKey with credentials.
    deadline: deadline for a single attempt.
    max_attempts: how many times to retry on errors.

  Returns:
    Deserialized JSON response.

  Raises:
    NotFoundError on 404 response.
    AuthError on 401 or 403 response.
    Error on any other non-transient error.
  """
  if payload is not None:
    headers = (headers or {}).copy()
    headers['Content-Type'] = 'application/json; charset=utf-8'
    payload = utils.encode_to_json(payload)
  response = yield request_async(
      url=url,
      method=method,
      payload=payload,
      params=params,
      headers=headers,
      scopes=scopes,
      service_account_key=service_account_key,
      deadline=deadline,
      max_attempts=max_attempts)
  try:
    response = json.loads(response)
  except ValueError as e:
    raise Error('Bad JSON response: %s' % e, None, response)
  raise ndb.Return(response)


def json_request(*args, **kwargs):
  """Blocking version of json_request_async."""
  return json_request_async(*args, **kwargs).get_result()
