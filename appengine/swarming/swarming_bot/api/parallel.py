# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Implements parallel map (pmap)."""

import Queue
import sys
import threading


# Current thread pools. This is effectively a leak.
_POOL = []
_POOL_LOCK = threading.Lock()
_QUEUE_IN = Queue.Queue()
_QUEUE_OUT = Queue.Queue()


def pmap(fn, items):
  """Runs map() in parallel.

  Rethrows any exception caught.

  Returns:
  - list(fn() result's) in order.
  """
  if not items:
    return []
  if len(items) == 1:
    return [fn(items[0])]

  # Try to reuse the common pool. Can only be used by one mapper at a time.
  locked = _POOL_LOCK.acquire(False)
  if locked:
    # _QUEUE_OUT may not be empty if the previous run threw.
    while not _QUEUE_OUT.empty():
      _QUEUE_OUT.get()
    try:
      return _pmap(_POOL, _QUEUE_IN, _QUEUE_OUT, fn, items)
    finally:
      _POOL_LOCK.release()

  # A pmap() is currently running, create a temporary pool.
  return _pmap([], Queue.Queue(), Queue.Queue(), fn, items)


def _pmap(pool, queue_in, queue_out, fn, items):
  while len(pool) < len(items) and len(pool) < 64:
    t = threading.Thread(
        target=_run, name='parallel%d' % len(pool), args=(queue_in, queue_out))
    t.daemon = True
    t.start()
    pool.append(t)

  for index, item in enumerate(items):
    queue_in.put((index, fn, item))
  out = [None] * len(items)
  for _ in xrange(len(items)):
    index, result = queue_out.get()
    if index < 0:
      # This is an exception.
      raise result[0], result[1], result[2]
    out[index] = result
  return out


def _run(queue_in, queue_out):
  while True:
    index, fn, item = queue_in.get()
    try:
      result = fn(item)
    except:  # pylint: disable=bare-except
      # Starts at -1 otherwise -0 == 0.
      index = -index - 1
      result = sys.exc_info()
    finally:
      queue_out.put((index, result))
