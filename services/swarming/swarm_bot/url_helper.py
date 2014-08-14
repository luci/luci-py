# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A helper script for wrapping url calls."""

import os
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, os.path.join(THIS_DIR, 'third_party'))

from utils import net


# TODO(maruel): Remove it once switch over is complete. Note that it's actually
# reading, not opening.
UrlOpen = net.url_read
# TODO(maruel): Rename callers.
DownloadFile = net.url_retrieve


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
      return net.url_read(url, **kwargs)

    if not self.token:
      self.token = self.refresh_token()
    resp = self._url_read_post(url, **kwargs)
    if resp is None:
      # This includes 403 because the XSRF token expired. Renew the token.
      # TODO(maruel): It'd be great if it were transparent.
      self.refresh_token()
      resp = self._url_read_post(url, **kwargs)
    if resp is None:
      raise Error('Failed to connect to %s' % url)
    return resp

  def url_read_json(self, resource, **kwargs):
    url = self.url + resource
    if kwargs.get('data') == None:
      # No XSRF token required for GET.
      return net.url_read_json(url, **kwargs)

    if not self.token:
      self.token = self.refresh_token()
    resp = self._url_read_json_post(url, **kwargs)
    if resp is None:
      # This includes 403 because the XSRF token expired. Renew the token.
      # TODO(maruel): It'd be great if it were transparent.
      self.refresh_token()
      resp = self._url_read_json_post(url, **kwargs)
    if resp is None:
      raise Error('Failed to connect to %s' % url)
    return resp

  def refresh_token(self):
    """Returns a fresh token. Necessary as the token may expire after an hour.
    """
    url = self.url + self.token_resource
    resp = net.url_read_json(
        url,
        headers={'X-XSRF-Token-Request': '1'},
        data=self.xsrf_request_params)
    if resp is None:
      raise Error('Failed to connect to %s' % url)
    self.token = resp['xsrf_token']
    return self.token

  def _url_read_post(self, url, **kwargs):
    headers = (kwargs.pop('headers', None) or {}).copy()
    headers['X-XSRF-Token'] = self.token
    return net.url_read(url, headers=headers, **kwargs)

  def _url_read_json_post(self, url, **kwargs):
    headers = (kwargs.pop('headers', None) or {}).copy()
    headers['X-XSRF-Token'] = self.token
    return net.url_read_json(url, headers=headers, **kwargs)
