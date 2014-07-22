# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A helper script for wrapping url calls."""

import httplib
import json
import logging
import math
import os
import random
import socket
import time
import urllib
import urllib2
import urlparse


# The index of the query elements from urlparse.
QUERY_INDEX = 4

# The timeout to apply whenever opening a url.
URL_OPEN_TIMEOUT = 5 * 60

# Query parameter key used when doing requests by the bot.
COUNT_KEY = 'UrlOpenAttempt'


class Error(Exception):
  pass


class XsrfRemote(object):
  """Transparently adds XSRF token to requests."""

  def __init__(self, url):
    self.url = url.rstrip('/')
    self.token = None
    self.max_tries = 40

  def url_read(self, resource, **kwargs):
    url = self.url + resource
    if kwargs.get('data') == None:
      # No XSRF token for GET.
      return UrlOpen(url, max_tries=self.max_tries, **kwargs)

    if not self.token:
      self.token = self.refresh_token()
    headers = {'X-XSRF-Token': self.token}
    resp = UrlOpen(url, headers=headers, max_tries=self.max_tries, **kwargs)
    if not resp:
      # This includes 403 because the XSRF token expired. Renew the token.
      # TODO(maruel): It'd be great if it were transparent.
      headers = {'X-XSRF-Token': self.refresh_token()}
      resp = UrlOpen(url, headers=headers, max_tries=self.max_tries, **kwargs)
    if not resp:
      raise Error('Failed to connect to %s' % url)
    return resp

  def url_read_json(self, resource, **kwargs):
    if kwargs.get('data') is not None:
      kwargs['data'] = json.dumps(
          kwargs['data'], sort_keys=True, separators=(',', ':'))
      kwargs['content_type'] = 'application/json'
    return self.url_read(resource, **kwargs)

  def refresh_token(self):
    """Returns a fresh token. Necessary as the token may expire after an hour.
    """
    url = self.url + '/auth/api/v1/accounts/self/xsrf_token'
    self.token = UrlOpen(url, max_tries=self.max_tries)
    if not self.token:
      raise Error('Failed to connect to %s' % url)
    return self.token


def UrlOpen(url, data=None, max_tries=5, wait_duration=None, headers=None):
  """Attempts to open the given url multiple times.

  UrlOpen will attempt to open the the given url several times, stopping
  if it succeeds at reaching the url. It also includes an additional data pair
  in the data that is sent to indicate how many times it has attempted to
  connect so far.

  Args:
    url: The url to open.
    data: The unencoded data to send to the url. This must be a mapping object.
    max_tries: The maximum number of times to try sending this data. Must be
        greater than 0.
    wait_duration: The number of seconds to wait between successive attempts.
        This must be greater than or equal to 0. If no value is given then a
        random value between 0.1 and 10 will be chosen each time (with
        exponential back off to give later retries a longer wait).
    headers: List of HTTP headers to add.

  Returns:
    The reponse from the url contacted. If it failed to connect or is given
    invalid arguments, then it returns None.
  """
  if max_tries <= 0:
    logging.error('UrlOpen(%s): Invalid number of tries: %d', url, max_tries)
    return None

  if wait_duration and wait_duration < 0:
    logging.error('UrlOpen(%s): Invalid wait duration: %d', url, wait_duration)
    return None

  method = 'POST' if data is not None else 'GET'
  data = data or {}

  if COUNT_KEY in data:
    logging.error(
        'UrlOpen(%s): key \'%s\' is duplicate.', url, COUNT_KEY)
    return None

  url_response = None
  for attempt in range(max_tries):
    data[COUNT_KEY] = attempt
    try:
      # urlencode requires that all strings be in ASCII form.
      for key, value in data.iteritems():
        if isinstance(value, basestring):
          data[key] = value.encode('utf-8', 'xmlcharrefreplace')

      encoded_data = urllib.urlencode(data)

      if method == 'POST':
        # Simply specifying data to urlopen makes it a POST.
        request = urllib2.Request(url, encoded_data)
      elif method == 'GET':
        url_parts = list(urlparse.urlparse(url))
        url_parts[QUERY_INDEX] = encoded_data
        request = urllib2.Request(urlparse.urlunparse(url_parts))
      else:
        raise AssertionError('Unknown method: %s' % method)

      for header, value in (headers or {}).iteritems():
        request.add_header(header, value)
      url_response = urllib2.urlopen(request, timeout=URL_OPEN_TIMEOUT).read()
    except urllib2.HTTPError as e:
      if e.code >= 500:
        # The HTTPError was due to a server error, so retry the attempt.
        logging.warning('UrlOpen(%s): attempt %d: %s ', url, attempt, e)
      else:
        # This HTTPError means we reached the server and there was a problem
        # with the request, so don't retry.
        logging.exception('UrlOpen(%s): %s', url, e)
        return None
    except (httplib.HTTPException, socket.error, urllib2.URLError) as e:
      logging.warning('UrlOpen(%s): attempt %d: %s', url, attempt, e)

    if url_response is not None:
      logging.info('UrlOpen(%s) got %d bytes.', url, len(url_response))
      return url_response
    elif attempt != max_tries - 1:
      # Only sleep if we are going to try and connect again.
      if wait_duration is None:
        duration = random.random() * 3 + math.pow(1.5, (attempt + 1))
        duration = min(10, max(0.1, duration))
      else:
        duration = wait_duration

      time.sleep(duration)

  logging.error('UrlOpen(%s): Unable to open after %d attempts', url, max_tries)
  return None


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
