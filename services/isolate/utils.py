# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Miscellaneous functions and classes."""

import functools
import threading
import time


class _Cache(object):
  """Holds state of a cache for cache_with_expiration and cache decorators."""

  def __init__(self, func, expiration_sec):
    self.func = func
    self.expiration_sec = expiration_sec
    self.lock = threading.Lock()
    self.value = None
    self.value_is_set = False
    self.expires = None

  def get_value(self):
    """Returns a cached value refreshing it if it has expired."""
    with self.lock:
      if not self.value_is_set or (self.expires and time.time() > self.expires):
        self.value = self.func()
        self.value_is_set = True
        if self.expiration_sec:
          self.expires = time.time() + self.expiration_sec
      return self.value

  def get_wrapper(self):
    """Returns a callable object that can be used in place of |func|.

    It's basically self.get_value, updated by functools.wraps to look more like
    original function.
    """
    # functools.wraps doesn't like 'instancemethod', use lambda as a proxy.
    # pylint: disable=W0108
    return functools.wraps(self.func)(lambda: self.get_value())


def cache(func):
  """Decorator that implements permanent cache of a zero-parameter function."""
  return _Cache(func, None).get_wrapper()


def cache_with_expiration(expiration_sec):
  """Decorator that implements in-memory cache for a zero-parameter function."""
  def decorator(func):
    return _Cache(func, expiration_sec).get_wrapper()
  return decorator


def constant_time_equals(a, b):
  """Compares two strings in constant time regardless of theirs content."""
  if len(a) != len(b):
    return False
  result = 0
  for x, y in zip(a, b):
    result |= ord(x) ^ ord(y)
  return result == 0
