# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A helper script for wrapping url calls."""

import json
import logging
import os
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, os.path.join(THIS_DIR, 'third_party'))

from utils import net

# The index of the query elements from urlparse.
QUERY_INDEX = 4

# The timeout to apply whenever opening a url.
URL_OPEN_TIMEOUT = 5 * 60

# Query parameter key used when doing requests by the bot.
COUNT_KEY = 'UrlOpenAttempt'


# TODO(maruel): Remove it once switch over is complete. Note that it's actually
# reading, not opening.
UrlOpen = net.url_read


class Error(Exception):
  pass


class XsrfRemote(object):
  """Transparently adds XSRF token to requests."""
  TOKEN_RESOURCE = '/auth/api/v1/accounts/self/xsrf_token'

  def __init__(self, url, token_resource=None):
    self.url = url.rstrip('/')
    self.token = None
    self.token_resource = token_resource or self.TOKEN_RESOURCE
    self.xsrf_request_params = {}

  def url_read(self, resource, **kwargs):
    url = self.url + resource
    if kwargs.get('data') == None:
      # No XSRF token for GET.
      return UrlOpen(url, **kwargs)

    if not self.token:
      self.token = self.refresh_token()
    headers = {'X-XSRF-Token': self.token}
    resp = UrlOpen(url, headers=headers, **kwargs)
    if not resp:
      # This includes 403 because the XSRF token expired. Renew the token.
      # TODO(maruel): It'd be great if it were transparent.
      headers = {'X-XSRF-Token': self.refresh_token()}
      resp = UrlOpen(url, headers=headers, **kwargs)
    if not resp:
      raise Error('Failed to connect to %s' % url)
    return resp

  def url_read_json(self, resource, **kwargs):
    if kwargs.get('data') is not None:
      kwargs['data'] = json.dumps(
          kwargs['data'], sort_keys=True, separators=(',', ':'))
      kwargs['content_type'] = 'application/json; charset=utf-8'
    return self.url_read(resource, **kwargs)

  def refresh_token(self):
    """Returns a fresh token. Necessary as the token may expire after an hour.
    """
    url = self.url + self.token_resource
    reply = UrlOpen(
        url,
        content_type='application/json; charset=utf-8',
        headers={'X-XSRF-Token-Request': '1'},
        data=json.dumps(
            self.xsrf_request_params, sort_keys=True, separators=(',', ':')))
    if not reply:
      raise Error('Failed to connect to %s' % url)
    self.token = json.loads(reply)['xsrf_token']
    return self.token



def DownloadFile(local_file, url):
  """Downloads the data from the given url and saves it in the local_file.

  Args:
    local_file: Where to save the data downloaded from the url.
    url: Where to fetch the data from.

  Returns:
    True if the file is successfully downloaded.
  """
  local_file = os.path.abspath(local_file)

  url_data = UrlOpen(url)

  if url_data is None:
    return False

  try:
    with open(local_file, 'wb') as f:
      f.write(url_data)
  except IOError as e:
    logging.error('Failed to write to %s\n%s', local_file, e)
    return False

  return True
