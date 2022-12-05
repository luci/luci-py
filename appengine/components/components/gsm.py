# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Helper functions for working with Google Secret Manager."""

import base64
import collections
import logging
import random
import threading
import time

from components import net


class Secret(object):
  """Representation of a secret version in Google Secret Manager.

  Can be declared as a global variable and shared across threads. The secret
  value will be fetched lazily and then periodically refetched.
  """

  _FetchOutcome = collections.namedtuple(
      '_FetchOutcome',
      [
          'value',  # bytes with the actual secret value
          'exception',  # an Exception if the fetch failed
          'expiry',  # time.time() when to refetch
          'errors',  # incremented on fetch errors
      ])

  def __init__(self, project, secret, version):
    self._project = project
    self._secret = secret
    self._version = version

    # Mocked in tests.
    self._time = time.time
    self._random = random.random
    self._json_request = net.json_request

    # Protects fields below.
    self._lock = threading.Lock()
    # Outcome of the latest fetch operation or None if haven't fetched yet.
    self._latest = None
    # The thread that fetches the value now or None.
    self._fetching_thread = None
    # When the fetch started or None.
    self._fetching_time = None

  def access(self):
    """Returns the current value of the secret, refetching it if necessary."""
    with self._lock:
      # If we have no value at all, fetch it in a blocking way. This will block
      # all other accessors too (they'll sit waiting for the lock to be
      # released). We want that for the initial fetch.
      self._latest = self._latest or self._fetch(0)

      # If the latest fetched value is fresh enough, use it.
      staleness = self._time() - self._latest.expiry
      if staleness < 0:
        return self._latest_or_raise

      # Check if some thread is already refetching the value.
      if self._fetching_thread is not None:
        # Log error if it got stuck. This should not be happening normally.
        if self._time() - self._fetching_time > 120:
          logging.error(
              'Secret %s staleness is %.1f sec, it is updated by thread %s',
              self._secret_id, staleness, self._fetching_thread)
        return self._latest_or_raise

      # If no one is fetching, we'll have to do it (outside the lock to allow
      # other threads reuse the known value while we fetch).
      self._fetching_thread = threading.current_thread()
      self._fetching_time = self._time()
      # This is used to do exponential back-off on errors.
      prev_errors = self._latest.errors

    fetched = None
    try:
      fetched = self._fetch(prev_errors)
    finally:
      with self._lock:
        # Notify other threads we are done fetching (successfully or not).
        assert self._latest
        assert self._fetching_thread == threading.current_thread()
        self._fetching_thread = None
        self._fetching_time = None
        # Don't interrupt the exception flow if `fetched` is None (i.e. some
        # completely unexpected exception happened in _fetch).
        if fetched:
          if not fetched.exception:
            # On success, switch to use the new value (with its new expiration).
            self._latest = fetched
          else:
            # On a fetch error, keep using the old value (if any), but adjust
            # its expiry to request a refetch per the retry policy. This is
            # a throttling mechanism to avoid busy retry loops in case of
            # persistent fetch errors.
            self._latest = self._FetchOutcome(value=self._latest.value,
                                              exception=self._latest.exception,
                                              expiry=fetched.expiry,
                                              errors=fetched.errors)
          # If we are here, there's no active exception and it is OK to return,
          # but pylint is still confused, so disable the check.
          # pylint: disable=lost-exception
          return self._latest_or_raise

  @property
  def _secret_id(self):
    return '%s/%s/%s' % (self._project, self._secret, self._version)

  @property
  def _latest_or_raise(self):
    if self._latest.exception:
      raise self._latest.exception
    return self._latest.value

  def _fetch(self, prev_errors):
    logging.info('Fetching secret %s...', self._secret_id)
    try:
      resp = self._json_request(
          url=('https://secretmanager.googleapis.com/v1/'
               'projects/%s/secrets/%s/versions/%s:access' %
               (self._project, self._secret, self._version)),
          method='GET',
          scopes=['https://www.googleapis.com/auth/cloud-platform'])
      exp_sec = 2 * 3600.0 + self._random() * 3600.0
      logging.info('Fetched secret %s (refetch in %.1f sec): %s',
                   self._secret_id, exp_sec, resp['name'])
      return self._FetchOutcome(value=base64.b64decode(resp['payload']['data']),
                                exception=None,
                                expiry=self._time() + exp_sec,
                                errors=0)
    except net.Error as exception:
      exp_sec = 2**min(9, prev_errors) + self._random() * 15.0
      logging.exception(
          'Failed to fetch secret %s on attempt %d, refetch in %.1f sec',
          self._secret_id, prev_errors + 1, exp_sec)
      return self._FetchOutcome(value=None,
                                exception=exception,
                                expiry=self._time() + exp_sec,
                                errors=prev_errors + 1)
