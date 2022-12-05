#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import sys
import threading
import unittest

try:
  import Queue as queue
except ImportError:
  import queue

from test_support import test_env
test_env.setup_test_env()

from components import gsm
from components import net
from test_support import test_case


class SecretTest(test_case.TestCase):
  def setUp(self):
    super(SecretTest, self).setUp()
    self.now = 12345678.0
    self.lock = threading.Lock()
    self.error = None
    self.fetches = 0
    self.threads = []

  def secret(self, fetch_queue=None, blocked=None):
    s = gsm.Secret('some-project', 'some-secret', 'some-version')
    s._time = lambda: self.now
    s._random = lambda: 0.0

    def mock_json_request(url, method, scopes):
      self.assertEqual(
          url, 'https://secretmanager.googleapis.com/v1/'
          'projects/some-project/secrets/some-secret/versions/some-version'
          ':access')
      self.assertEqual(method, 'GET')
      self.assertEqual(scopes,
                       ['https://www.googleapis.com/auth/cloud-platform'])

      # Multi-threaded tests use a queue.
      if fetch_queue:
        with self.lock:
          self.fetches += 1
        if blocked:
          blocked.set()
        item = fetch_queue.get(block=True)
        if isinstance(item, Exception):
          raise item
        return {
            'name': 'doesnt-really-matter',
            'payload': {
                'data': base64.b64encode(item)
            },
        }

      # Single-threaded tests just use generated values.
      with self.lock:
        self.fetches += 1
        if self.error:
          raise net.Error(self.error, 500, None)
        return {
            'name': 'doesnt-really-matter',
            'payload': {
                'data': base64.b64encode('secret-value-%d' % self.fetches)
            },
        }

    s._json_request = mock_json_request
    return s

  def launch(self, cb):
    th = threading.Thread(target=cb)
    th.start()
    self.threads.append(th)

  def join(self):
    for th in self.threads:
      th.join()
    self.threads = []

  def test_happy_path_single_thread(self):
    s = self.secret()
    self.assertEqual(s.access(), 'secret-value-1')

    # Still using the cached value a bit later.
    self.now += 600.0
    self.assertEqual(s.access(), 'secret-value-1')

    # Refetched when accessed much later.
    self.now += 3 * 3600.0
    self.assertEqual(s.access(), 'secret-value-2')

  def test_completely_broken(self):
    self.error = 'Broken'

    s = self.secret()

    with self.assertRaises(net.Error):
      s.access()
    self.assertEqual(self.fetches, 1)

    # Tick second by second and notice when actual fetches happen.
    fetched_at = []
    for t in range(100):
      self.now += 1
      fetches = self.fetches
      with self.assertRaises(net.Error):
        s.access()
      if self.fetches != fetches:
        self.assertEqual(self.fetches, fetches + 1)
        fetched_at.append(t)

    # Should see exponential backoff pattern.
    self.assertEqual(fetched_at, [0, 2, 6, 14, 30, 62])

  def test_many_threads(self):
    fetch_q = queue.Queue()
    blocked = threading.Event()
    secret = self.secret(fetch_q, blocked)

    lock = threading.Lock()
    results = []

    def access_thread():
      res = secret.access()
      with lock:
        results.append(res)

    for _ in range(4):
      self.launch(access_thread)

    # Unblocks all fetching threads.
    fetch_q.put('val')
    # Waits for them to be done.
    self.join()

    # Only one fetch actually happened.
    self.assertEqual(self.fetches, 1)
    # All threads saw the same value.
    self.assertEqual(results, ['val'] * 4)
    del results[:]

    # Advance time to trigger expiry.
    self.now += 3 * 3600

    # This thread will attempt to refetch and would block there.
    blocked.clear()
    self.launch(access_thread)
    blocked.wait()

    # Meanwhile try to access the value from the main thread. It should
    # immediately return the cached value.
    self.assertEqual(secret.access(), 'val')

    # Let the blocked thread finish and store the new value.
    fetch_q.put('new-val')
    self.join()

    # Verify it saw the new value.
    self.assertEqual(self.fetches, 2)
    self.assertEqual(results, ['new-val'])

    # It is now used by all threads.
    self.assertEqual(secret.access(), 'new-val')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
