# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Wrapper around urlfetch to call REST API with service account credentials."""

import json
import logging
import time
import urllib
import urlparse

from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import auth
from components import utils
from components.auth import delegation


EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
TS_MON_CLIENT = "luci-py-server"


# Maybe import gae_ts_mon
# Not all users of this library will necessarily have gae_ts_mon,
# So we need to tiptoe around them
try:
  from gae_ts_mon.common import http_metrics
except ImportError:
  http_metrics = None

class Error(Exception):
  """Raised on non-transient errors.

  Attribute response is response body.
  """
  def __init__(self, msg, status_code, response, headers=None):
    super(Error, self).__init__(msg)
    self.status_code = status_code
    self.headers = headers
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


def is_transient_error(response, url):
  """Returns True to retry the request."""
  if response.status_code >= 500 or response.status_code == 408:
    return True
  # Retry 404 iff it is a Cloud Endpoints API call *and* the
  # result is not JSON. This assumes that we only use JSON encoding.
  if response.status_code == 404:
    content_type = response.headers.get('Content-Type', '')
    return (
        urlparse.urlparse(url).path.startswith('/_ah/api/') and
        not content_type.startswith('application/json'))
  return False


def _error_class_for_status(status_code):
  if status_code == 404:
    return NotFoundError
  if status_code in (401, 403):
    return AuthError
  return Error


def _update_http_metrics(metrics_name, response, elapsed_ms, payload):
  # <response> is possibly None
  # set status_code to STATUS_EXCEPTION and response_size to None
  # then update to real values if there actually is a <response>
  status_code = http_metrics.STATUS_EXCEPTION
  response_size = None
  if response is not None:
    status_code = response.status_code
    response_size = len(response.content)
  # The following code can be replaced with
  # http_metrics.update_http_metrics() once it is rolled in from upstream
  # infra repo
  fields = {'client':TS_MON_CLIENT, 'status': status_code,
            'name': metrics_name}
  http_metrics.response_status.increment(fields=fields)
  fields = {'client':TS_MON_CLIENT, 'name':metrics_name}
  http_metrics.durations.add(elapsed_ms, fields=fields)
  if payload is not None:
    http_metrics.request_bytes.add(len(payload), fields=fields)
  if response_size is not None:
    http_metrics.response_bytes.add(response_size, fields=fields)


@ndb.tasklet
def request_async(
    url,
    method='GET',
    payload=None,
    params=None,
    headers=None,
    scopes=None,
    service_account_key=None,
    delegation_token=None,
    deadline=None,
    max_attempts=None,
    metrics_name=None):
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
    delegation_token: delegation token returned by auth.delegate.
    deadline: deadline for a single attempt (10 sec by default).
    max_attempts: how many times to retry on errors (4 times by default).
    metrics_name: the name of the resource for the purpose of metrics collection
      ie it will be assigned to the "name" field of various http metrics
      recorded against the request. If None, then the domain name from <url>
      will be used.

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

  headers = (headers or {}).copy()

  if scopes:
    tok, _ = yield auth.get_access_token_async(scopes, service_account_key)
    headers['Authorization'] = 'Bearer %s' % tok

  if delegation_token:
    if isinstance(delegation_token, auth.DelegationToken):
      delegation_token = delegation_token.token
    assert isinstance(delegation_token, basestring)
    headers[delegation.HTTP_HEADER] = delegation_token

  if payload is not None:
    assert isinstance(payload, str), type(payload)
    assert method in ('CREATE', 'POST', 'PUT'), method

  attempt = 0
  response = None
  last_status_code = None
  if metrics_name is None:
      parsed_url = urlparse.urlparse(url)
      metrics_name = parsed_url.netloc
  while attempt < max_attempts:
    if attempt:
      logging.info('Retrying...')
    attempt += 1
    logging.info('%s %s', method, url)
    start_timestamp = time.time()
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
    finally:
      if http_metrics is not None:
        elapsed_ms = (time.time() - start_timestamp) * 1000
        _update_http_metrics(metrics_name, response, elapsed_ms, payload)

    last_status_code = response.status_code

    # Transient error on the other side.
    if is_transient_error(response, url):
      logging.warning(
          '%s %s failed with HTTP %d\nHeaders: %r\nBody: %r',
          method, url, response.status_code, response.headers, response.content)
      continue

    # Non-transient error.
    if 300 <= response.status_code < 500:
      logging.warning(
          '%s %s failed with HTTP %d\nHeaders: %r\nBody: %r',
          method, url, response.status_code, response.headers, response.content)
      raise _error_class_for_status(response.status_code)(
          'Failed to call %s: HTTP %d' % (url, response.status_code),
          response.status_code, response.content, headers=response.headers)

    # Success. Beware of large responses.
    if len(response.content) > 1024 * 1024:
      logging.warning('Response size: %.1f KiB', len(response.content) / 1024.0)
    raise ndb.Return(response.content)

  raise _error_class_for_status(last_status_code)(
      'Failed to call %s after %d attempts' % (url, max_attempts),
      response.status_code if response else None,
      response.content if response else None,
      headers=response.headers if response else None)


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
    delegation_token=None,
    deadline=None,
    max_attempts=None,
    metrics_name=None):
  """Sends a JSON REST API request, returns deserialized response.

  Automatically strips prefixes formed from characters in the set ")]}'\n"
  before deserializing JSON. Such prefixes sometimes used in REST APIs as
  a precaution against XSSI attacks.

  Retries the request on transient errors for up to |max_attempts| times.

  Args:
    url: url to send the request to.
    method: HTTP method to use, e.g. GET, POST, PUT.
    payload: object to be serialized to JSON and put in the request body.
    params: dict with query GET parameters (i.e. ?key=value&key=value).
    headers: additional request headers.
    scopes: OAuth2 scopes for the access token (or skip auth if None).
    service_account_key: auth.ServiceAccountKey with credentials.
    delegation_token: delegation token returned by auth.delegate.
    deadline: deadline for a single attempt.
    max_attempts: how many times to retry on errors.
    metrics_name: the name of the resource for the purpose of metrics collection
      ie it will be assigned to the "name" field of various http metrics
      recorded against the request. If None, then the domain name from <url>
      will be used.

  Returns:
    Deserialized JSON response.

  Raises:
    NotFoundError on 404 response.
    AuthError on 401 or 403 response.
    Error on any other non-transient error.
  """
  if payload is not None:
    headers = (headers or {}).copy()
    headers['Accept'] = 'application/json; charset=utf-8'
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
      delegation_token=delegation_token,
      deadline=deadline,
      max_attempts=max_attempts,
      metrics_name=metrics_name)
  try:
    response = json.loads(response.lstrip(")]}'\n"))
  except ValueError as e:
    raise Error('Bad JSON response: %s' % e, None, response)
  raise ndb.Return(response)


def json_request(*args, **kwargs):
  """Blocking version of json_request_async."""
  return json_request_async(*args, **kwargs).get_result()
