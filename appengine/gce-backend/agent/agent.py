#!/usr/bin/python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Machine Provider agent for GCE instances."""

import datetime
import httplib2
import json
import socket
import sys


METADATA_BASE_URL = 'http://metadata/computeMetadata/v1'
PUBSUB_BASE_URL = 'https://pubsub.googleapis.com/v1/projects'


class Error(Exception):
  pass


class RequestError(Error):
  def __init__(self, response, content):
    self.response = response
    self.content = content


# 404
class NotFoundError(RequestError):
  pass


# 409
class ConflictError(RequestError):
  pass


def get_metadata(key=''):
  """Retrieves the specified metadata from the metadata server.

  Args:
    key: The key whose value should be returned from the metadata server.

  Returns:
    The value associated with the metadata key.

  Raises:
    httplib2.ServerNotFoundError: If the metadata server cannot be found.
      Probably indicates that this script is not running on a GCE instance.
    NotFoundError: If the metadata key could not be found.
  """
  response, content = httplib2.Http().request(
      '%s/%s' % (METADATA_BASE_URL, key),
      headers={'Metadata-Flavor': 'Google'},
      method='GET',
  )
  if response['status'] == '404':
    raise NotFoundError(response, content)
  return content


class AuthorizedHTTPRequest(httplib2.Http):
  """Subclass for HTTP requests authorized with service account credentials."""

  def __init__(self, service_account='default'):
    super(AuthorizedHTTPRequest, self).__init__()
    self._expiration_time = datetime.datetime.now()
    self._service_account = service_account
    self._token = None
    self.refresh_token()

  @property
  def service_account(self):
    return self._service_account

  @property
  def token(self):
    return self._token

  def refresh_token(self):
    """Fetches and caches the token from the metadata server."""
    token = json.loads(get_metadata(
        'instance/service-accounts/%s/token' % self.service_account,
    ))
    seconds = token['expires_in'] - 60
    self._expiration_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=seconds)
    )
    self._token = token['access_token']

  def request(self, *args, **kwargs):
    # Tokens fetched from the metadata server are valid for at least
    # 10 more minutes, so the token will still be valid for the request
    # (i.e. no risk of refreshing to a token that is about to expire).
    if self._expiration_time < datetime.datetime.now():
      self.refresh_token()
    kwargs['headers'] = kwargs.get('headers', {}).copy()
    kwargs['headers']['Authorization'] = 'Bearer %s' % self.token
    return super(AuthorizedHTTPRequest, self).request(*args, **kwargs)


class PubSub(object):
  """Class for interacting with Cloud Pub/Sub."""

  def __init__(self, service_account='default'):
    self._http = AuthorizedHTTPRequest(service_account=service_account)

  def pull(self, subscription, project):
    """Polls for new messages on a Cloud Pub/Sub pull subscription.

    Args:
      subscription: Name of the pull subscription.
      project: Project the pull subscription exists in.

    Returns:
      A JSON response from the Pub/Sub service.

    Raises:
      NotFoundError: If the subscription and/or topic can't be found.
    """
    response, content = self._http.request(
        '%s/%s/subscriptions/%s:pull' % (
            PUBSUB_BASE_URL, project, subscription),
        body=json.dumps({'maxMessages': 1, 'returnImmediately': False}),
        method='POST',
    )
    if response['status'] == '404':
      raise NotFoundError(response, json.loads(content))
    return json.loads(content)


def main():
  """Listens for Cloud Pub/Sub communication."""
  # Attributes tell us what subscription has been created for us to listen to.
  project = get_metadata('instance/attributes/pubsub_subscription_project')
  subscription = get_metadata('instance/attributes/pubsub_subscription')
  pubsub = PubSub()

  response = pubsub.pull(subscription, project)
  print response
  # TODO(smut): Process messages.


if __name__ == '__main__':
  sys.exit(main())
