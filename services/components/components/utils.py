# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Mixed bag of NDB utilities."""

# Disable 'Access to a protected member ...'. NDB uses '_' for other purposes.
# pylint: disable=W0212

# Disable: 'Method could be a function'. It can't: NDB expects a method.
# pylint: disable=R0201

import datetime
import functools
import inspect
import json
import os
import threading
import time

from email import utils as email_utils

from google.appengine.api import memcache
from google.appengine.api import modules


DATETIME_FORMAT = u'%Y-%m-%d %H:%M:%S'
DATE_FORMAT = u'%Y-%m-%d'
VALID_DATETIME_FORMATS = ('%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S')


# UTC datetime corresponding to zero Unix timestamp.
_EPOCH = datetime.datetime.utcfromtimestamp(0)


def is_local_dev_server():
  """Returns True if running on local development server.

  This function is safe to run outside the scope of a HTTP request.
  """
  return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')


def is_canary():
  """Returns True if the server is running a canary instance.

  We define a 'canary instance' any instance that has the suffix '-dev' in its
  instance name.

  This function is safe to run outside the scope of a HTTP request.
  """
  return os.environ['APPLICATION_ID'].endswith('-dev')


def get_module_version_list(module_list, tainted):
  """Returns a list of pairs (module name, version name) to fetch logs for.

  Arguments:
    module_list: list of modules to list, defaults to all modules.
    tainted: if False, excludes versions with '-tainted' in their name.
  """
  result = []
  if not module_list:
    # If the function it called too often, it'll raise a OverQuotaError. So
    # cache it for 10 minutes.
    module_list = memcache.get('modules_list')
    if not module_list:
      module_list = modules.get_modules()
      memcache.set('modules_list', module_list, time=10*60)

  for module in module_list:
    # If the function it called too often, it'll raise a OverQuotaError.
    # Versions is a bit more tricky since we'll loose data, since versions are
    # changed much more often than modules. So cache it for 1 minute.
    key = 'modules_list-' + module
    version_list = memcache.get(key)
    if not version_list:
      version_list = modules.get_versions(module)
      memcache.set(key, version_list, time=60)
    result.extend(
        (module, v) for v in version_list if tainted or '-tainted' not in v)
  return result


def get_request_as_int(request, key, default, min_value, max_value):
  """Returns a request value as int."""
  value = request.params.get(key, '')
  try:
    value = int(value)
  except ValueError:
    return default
  return min(max_value, max(min_value, value))


def get_request_as_datetime(request, key):
  value_text = request.params.get(key)
  if value_text:
    for f in VALID_DATETIME_FORMATS:
      try:
        return datetime.datetime.strptime(value_text, f)
      except ValueError:
        continue
  return None


def constant_time_equals(a, b):
  """Compares two strings in constant time regardless of theirs content."""
  if len(a) != len(b):
    return False
  result = 0
  for x, y in zip(a, b):
    result |= ord(x) ^ ord(y)
  return result == 0


def to_units(number):
  """Convert a string to numbers."""
  UNITS = ('', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y')
  unit = 0
  while number >= 1024.:
    unit += 1
    number = number / 1024.
    if unit == len(UNITS) - 1:
      break
  if unit:
    return '%.2f%s' % (number, UNITS[unit])
  return '%d' % number


def datetime_to_rfc2822(dt):
  """datetime -> string value for Last-Modified header as defined by RFC2822."""
  assert dt.tzinfo is None, 'Expecting UTC timestamp: %s' % dt
  return email_utils.formatdate(datetime_to_timestamp(dt) / 1000000.0)


def datetime_to_timestamp(value):
  """Converts UTC datetime to integer timestamp in microseconds since epoch."""
  if not isinstance(value, datetime.datetime):
    raise ValueError(
        'Expecting datetime object, got %s instead' % type(value).__name__)
  if value.tzinfo is not None:
    raise ValueError('Only UTC datetime is supported')
  dt = value - _EPOCH
  return dt.microseconds + 1000 * 1000 * (dt.seconds + 24 * 3600 * dt.days)


def timestamp_to_datetime(value):
  """Converts integer timestamp in microseconds since epoch to UTC datetime."""
  if not isinstance(value, (int, long, float)):
    raise ValueError(
        'Expecting a number, got %s instead' % type(value).__name__)
  return _EPOCH + datetime.timedelta(microseconds=value)


### Cache


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


## JSON


def to_json_encodable(data):
  """Converts data into json-compatible data."""
  if isinstance(data, unicode) or data is None:
    return data
  if isinstance(data, str):
    return data.decode('utf-8')
  if isinstance(data, (int, float, long)):
    # Note: overflowing is an issue with int and long.
    return data
  if isinstance(data, (list, set, tuple)):
    return [to_json_encodable(i) for i in data]
  if isinstance(data, dict):
    assert all(isinstance(k, basestring) for k in data), data
    return {
      to_json_encodable(k): to_json_encodable(v) for k, v in data.iteritems()
    }

  if isinstance(data, datetime.datetime):
    # Convert datetime objects into a string, stripping off milliseconds. Only
    # accept naive objects.
    if data.tzinfo is not None:
      raise ValueError('Can only serialize naive datetime instance')
    return data.strftime(DATETIME_FORMAT)
  if isinstance(data, datetime.date):
    return data.strftime(DATE_FORMAT)
  if isinstance(data, datetime.timedelta):
    # Convert timedelta into seconds, stripping off milliseconds.
    return int(data.total_seconds())

  if hasattr(data, 'to_dict') and callable(data.to_dict):
    # This takes care of ndb.Model.
    return to_json_encodable(data.to_dict())

  if hasattr(data, 'urlsafe') and callable(data.urlsafe):
    # This takes care of ndb.Key.
    return to_json_encodable(data.urlsafe())

  if inspect.isgenerator(data) or isinstance(data, xrange):
    # Handle it like a list. Sadly, xrange is not a proper generator so it has
    # to be checked manually.
    return [to_json_encodable(i) for i in data]

  assert False, 'Don\'t know how to handle %r' % data


def encode_to_json(data):
  """Converts any data as a json string."""
  return json.dumps(
      to_json_encodable(data),
      sort_keys=True,
      separators=(',', ':'),
      encoding='utf-8')
