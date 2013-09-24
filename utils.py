# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Miscellaneous functions and classes."""

import functools
import threading
import time


class CacheWithExpiration(object):
  """Holds state of a cache for cache_with_expiration decorator."""

  def __init__(self, func, expiration_sec):
    self.func = func
    self.expiration_sec = expiration_sec
    self.lock = threading.Lock()
    self.value = None
    self.expires = None

  def get_value(self):
    """Returns a cached value refreshing it if it has expired."""
    now = time.time()
    with self.lock:
      if self.expires is None or now > self.expires:
        self.value = self.func()
        self.expires = now + self.expiration_sec
      return self.value


def cache_with_expiration(expiration_sec):
  """Decorator that implements in-memory cache for a zero-parameter function."""
  def decorator(func):
    cache = CacheWithExpiration(func, expiration_sec)
    # functools.wraps doesn't like 'instancemethod', use lambda as a proxy.
    # pylint: disable=W0108
    return functools.wraps(func)(lambda: cache.get_value())
  return decorator


def constant_time_equals(a, b):
  """Compares two strings in constant time regardless of theirs content."""
  if len(a) != len(b):
    return False
  result = 0
  for x, y in zip(a, b):
    result |= ord(x) ^ ord(y)
  return result == 0
