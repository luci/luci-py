# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import threading
import time
import traceback

from utils import net


# RemoteClient will attempt to refresh the authentication headers once they are
# this close to the expiration.
AUTH_HEADERS_EXPIRATION_SEC = 6*60


# How long to wait for a response from the server. Must not be greater than
# AUTH_HEADERS_EXPIRATION_SEC, since otherwise there's a chance auth headers
# will expire while we wait for connection.
NET_CONNECTION_TIMEOUT_SEC = 5*60


class InitializationError(Exception):
  """Raised by RemoteClient.initialize on fatal errors."""
  def __init__(self, last_error):
    super(InitializationError, self).__init__('Failed to grab auth headers')
    self.last_error = last_error


class RemoteClient(object):
  """RemoteClient knows how to make authenticated calls to the backend.

  It also holds in-memory cache of authentication headers and periodically
  refreshes them (by calling supplied callback, that usually is implemented in
  terms of bot_config.get_authentication_headers() function).

  If the callback is None, skips authentication (this is used during initial
  stages of the bot bootstrap).
  """

  def __init__(self, server, auth_headers_callback):
    self._server = server
    self._auth_headers_callback = auth_headers_callback
    self._lock = threading.Lock()
    self._headers = None
    self._exp_ts = None
    self._disabled = not auth_headers_callback

  def initialize(self, quit_bit):
    """Grabs initial auth headers, retrying on errors a bunch of times.

    Raises InitializationError if all attempts fail. Aborts attempts and returns
    if quit_bit is signaled.
    """
    attempts = 30
    while not quit_bit.is_set():
      try:
        self._get_headers_or_throw()
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

  def _get_headers_or_throw(self):
    if self._disabled:
      return {}
    with self._lock:
      if (self._exp_ts is None or
          self._exp_ts - time.time() < AUTH_HEADERS_EXPIRATION_SEC):
        self._headers, self._exp_ts = self._auth_headers_callback()
        if self._exp_ts is None:
          logging.info('Not using auth headers')
          self._disabled = True
          self._headers = {}
        else:
          logging.info(
              'Refreshed auth headers, they expire in %d sec',
              self._exp_ts - time.time())
      return self._headers or {}

  def url_read_json(self, url_path, data=None):
    """Does POST (if data is not None) or GET request to a JSON endpoint."""
    return net.url_read_json(
        self._server + url_path,
        data=data,
        headers=self.get_authentication_headers(),
        timeout=NET_CONNECTION_TIMEOUT_SEC,
        follow_redirects=False)

  def url_retrieve(self, filepath, url_path):
    """Fetches the file from the given URL path on the server."""
    return net.url_retrieve(
        filepath,
        self._server + url_path,
        headers=self.get_authentication_headers(),
        timeout=NET_CONNECTION_TIMEOUT_SEC)
