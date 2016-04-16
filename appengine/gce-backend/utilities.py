# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for GCE Backend."""

from google.appengine.ext import ndb


def batch_process_async(items, f, max_concurrent=50):
  """Processes asynchronous calls in parallel, but batched.

  Args:
    items: List of items to process.
    f: f(item) -> ndb.Future. Asynchronous function to apply to each item.
    max_concurrent: Maximum number of futures to have pending concurrently.
  """
  futures = []
  while items:
    num_futures = len(futures)
    if num_futures < max_concurrent:
      futures.extend([f(item) for item in items[:max_concurrent - num_futures]])
      items = items[max_concurrent - num_futures:]
    ndb.Future.wait_any(futures)
    futures = [future for future in futures if not future.done()]
  if futures:
    ndb.Future.wait_all(futures)
