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
import logging
import os
import re
import threading
import time

from email import utils as email_utils

from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import memcache as gae_memcache
from google.appengine.api import modules
from google.appengine.api import taskqueue

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

DATETIME_FORMAT = u'%Y-%m-%d %H:%M:%S'
DATE_FORMAT = u'%Y-%m-%d'
VALID_DATETIME_FORMATS = ('%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S')


# UTC datetime corresponding to zero Unix timestamp.
EPOCH = datetime.datetime.utcfromtimestamp(0)

# Module to run task queue tasks on by default. Used by get_task_queue_host
# function. Can be changed by 'set_task_queue_module' function.
_task_queue_module = 'backend'


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
    module_list = gae_memcache.get('modules_list')
    if not module_list:
      module_list = modules.get_modules()
      gae_memcache.set('modules_list', module_list, time=10*60)

  for module in module_list:
    # If the function it called too often, it'll raise a OverQuotaError.
    # Versions is a bit more tricky since we'll loose data, since versions are
    # changed much more often than modules. So cache it for 1 minute.
    key = 'modules_list-' + module
    version_list = gae_memcache.get(key)
    if not version_list:
      version_list = modules.get_versions(module)
      gae_memcache.set(key, version_list, time=60)
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


### Time


def utcnow():
  """Returns datetime.utcnow(), used for testing.

  Use this function so it can be mocked everywhere.
  """
  return datetime.datetime.utcnow()


def time_time():
  """Returns the equivalent of time.time() as mocked if applicable."""
  return (utcnow() - EPOCH).total_seconds()


def milliseconds_since_epoch(now):
  """Returns the number of milliseconds since unix epoch as an int."""
  now = now or utcnow()
  return int(round((now - EPOCH).total_seconds() * 1000.))


def datetime_to_rfc2822(dt):
  """datetime -> string value for Last-Modified header as defined by RFC2822."""
  if not isinstance(dt, datetime.datetime):
    raise TypeError(
        'Expecting datetime object, got %s instead' % type(dt).__name__)
  assert dt.tzinfo is None, 'Expecting UTC timestamp: %s' % dt
  return email_utils.formatdate(datetime_to_timestamp(dt) / 1000000.0)


def datetime_to_timestamp(value):
  """Converts UTC datetime to integer timestamp in microseconds since epoch."""
  if not isinstance(value, datetime.datetime):
    raise ValueError(
        'Expecting datetime object, got %s instead' % type(value).__name__)
  if value.tzinfo is not None:
    raise ValueError('Only UTC datetime is supported')
  dt = value - EPOCH
  return dt.microseconds + 1000 * 1000 * (dt.seconds + 24 * 3600 * dt.days)


def timestamp_to_datetime(value):
  """Converts integer timestamp in microseconds since epoch to UTC datetime."""
  if not isinstance(value, (int, long, float)):
    raise ValueError(
        'Expecting a number, got %s instead' % type(value).__name__)
  return EPOCH + datetime.timedelta(microseconds=value)


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
      if not self.value_is_set or (self.expires and time_time() > self.expires):
        self.value = self.func()
        self.value_is_set = True
        if self.expiration_sec:
          self.expires = time_time() + self.expiration_sec
      return self.value

  def clear(self):
    """Clears stored cached value."""
    with self.lock:
      self.value = None
      self.value_is_set = False
      self.expires = None

  def get_wrapper(self):
    """Returns a callable object that can be used in place of |func|.

    It's basically self.get_value, updated by functools.wraps to look more like
    original function.
    """
    # functools.wraps doesn't like 'instancemethod', use lambda as a proxy.
    # pylint: disable=W0108
    wrapper = functools.wraps(self.func)(lambda: self.get_value())
    wrapper.__parent_cache__ = self
    return wrapper


def cache(func):
  """Decorator that implements permanent cache of a zero-parameter function."""
  return _Cache(func, None).get_wrapper()


def cache_with_expiration(expiration_sec):
  """Decorator that implements in-memory cache for a zero-parameter function."""
  def decorator(func):
    return _Cache(func, expiration_sec).get_wrapper()
  return decorator


def clear_cache(func):
  """Given a function decorated with @cache, resets cached value."""
  func.__parent_cache__.clear()


def memcache(key, timeout=None):
  """Decorator that implements memcache-based cache for a function.

  The generated cache key contains current application version and values of all
  function arguments converted to string using `repr`.

  Args:
    key (str): unique string that will be used as a part of cache key.
    timeout (int): cache timeout in seconds.

  Example:
    @memcache('f')
    def f(a, b=2):
      # Heavy computation
      return 42

  Decorator raises:
    NotImplementedError if function uses varargs or kwargs.
  """
  memcache_set_kwargs = {}
  if timeout is not None:
    memcache_set_kwargs['timeout'] = timeout

  def decorator(func):
    argspec = inspect.getargspec(func)
    if argspec.varargs:
      raise NotImplementedError(
          'varargs in memcached functions are not supported')
    if argspec.keywords:
      raise NotImplementedError(
          'kwargs in memcached functions are not supported')

    @functools.wraps(func)
    def decorated(*args, **kwargs):
      key_args = []
      for i, name in enumerate(argspec.args):
        if name in ('self', 'cls') and i == 0:
          # Assume nobody will use self/cls argument for something different
          # that current instance/class.
          continue
        if i < len(args):
          arg_value = args[i]
        elif name in kwargs:
          arg_value = kwargs[name]
        else:
          # argspec.defaults contains _last_ default values, so we need to shift
          # |i| left.
          default_value_index = i - (len(argspec.args) - len(argspec.defaults))
          if default_value_index < 0:
            # Parameter not provided. Call function to cause TypeError
            func(*args, **kwargs)
            assert False, 'Function call did not fail'
          arg_value = argspec.defaults[default_value_index]
        key_args.append(arg_value)

      cache_key = 'utils.memcache/%s/%s%s' % (
          get_app_version(), key, repr(key_args))

      result = gae_memcache.get(cache_key)
      if isinstance(result, tuple):
        return result[0]

      result = func(*args, **kwargs)
      gae_memcache.set(cache_key, (result,), **memcache_set_kwargs)
      return result

    return decorated
  return decorator


@cache
def get_app_version():
  """Returns currently running version (not necessary a default one)."""
  # Sadly, this causes an RPC and when called too frequently, throws quota
  # errors.
  return modules.get_current_version_name()


@cache
def get_versioned_hosturl():
  """Returns the url hostname of this instance locked to the currently running
  version.

  This function hides the fact that app_identity.get_default_version_hostname()
  returns None on the dev server and modules.get_hostname() returns incorrectly
  qualified hostname for HTTPS usage on the prod server. <3
  """
  if is_local_dev_server():
    # TODO(maruel): It'd be nice if it were easier to use a ephemeral SSL
    # certificate here and not assume unsecured connection.
    return 'http://' + modules.get_hostname()

  return 'https://%s-dot-%s' % (
      get_app_version(), app_identity.get_default_version_hostname())


@cache
def get_urlfetch_service_id():
  """Returns a value for X-URLFetch-Service-Id header for GAE <-> GAE calls.

  Usually it can be omitted. It is required in certain environments.
  """
  if is_local_dev_server():
    return 'LOCAL'
  hostname = app_identity.get_default_version_hostname().split('.')
  return hostname[-2].upper() if len(hostname) >= 3 else 'APPSPOT'


@cache
def get_app_revision_url():
  """Returns URL of a git revision page for currently running app version.

  Works only for non-tainted versions uploaded with tools/update.py: app version
  should look like '162-efaec47'. Assumes all services that use 'components'
  live in a single repository.

  Returns None if a version is tainted or has unexpected name.
  """
  rev = re.match(r'\d+-([a-f0-9]+)$', get_app_version())
  template = 'https://code.google.com/p/swarming/source/detail?r=%s'
  return template % rev.group(1) if rev else None


### Task queue


@cache
def get_task_queue_host():
  """Returns domain name of app engine instance to run a task queue task on.

  By default will use 'backend' module. Can be changed by calling
  set_task_queue_module during application startup.

  This domain name points to a matching version of appropriate app engine
  module - <version>.<module>.<app-id>.appspot.com where:
    version: version of the module that is calling this function.
    module: app engine module to execute task on.

  That way a task enqueued from version 'A' of default module would be executed
  on same version 'A' of backend module.
  """
  # modules.get_hostname sometimes fails with unknown internal error.
  # Cache its result in a memcache to avoid calling it too often.
  cache_key = 'task_queue_host:%s:%s' % (_task_queue_module, get_app_version())
  value = gae_memcache.get(cache_key)
  if not value:
    value = modules.get_hostname(module=_task_queue_module)
    gae_memcache.set(cache_key, value)
  return value


def set_task_queue_module(module):
  """Changes a module used by get_task_queue_host() function.

  Should be called during application initialization if default 'backend' module
  is not appropriate.
  """
  global _task_queue_module
  _task_queue_module = module
  clear_cache(get_task_queue_host)


def enqueue_task(url, queue_name, payload=None, name=None,
                 use_dedicated_module=True, transactional=False):
  """Adds a task to a task queue.

  If |use_dedicated_module| is True (default) a task will be executed by
  a separate backend module instance that runs same version as currently
  executing instance. Otherwise it will run on a current version of default
  module.

  Returns True if a task was successfully added, logs error and returns False
  if task queue is acting up.
  """
  try:
    headers = None
    if use_dedicated_module:
      headers = {'Host': get_task_queue_host()}
    # Note that just using 'target=module' here would redirect task request to
    # a default version of a module, not the currently executing one.
    taskqueue.add(
        url=url,
        queue_name=queue_name,
        payload=payload,
        name=name,
        headers=headers,
        transactional=transactional)
    return True
  except (
      taskqueue.Error,
      runtime.DeadlineExceededError,
      runtime.apiproxy_errors.CancelledError,
      runtime.apiproxy_errors.DeadlineExceededError,
      runtime.apiproxy_errors.OverQuotaError) as e:
    logging.warning(
        'Problem adding task \'%s\' to task queue \'%s\' (%s): %s',
        url, queue_name, e.__class__.__name__, e)
    return False


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


## Hacks


def fix_protobuf_package():
  """Modifies 'google' package to include path to 'google.protobuf' package."""
  import google
  protobuf_pkg = os.path.join(THIS_DIR, 'third_party', 'protobuf', 'google')
  if protobuf_pkg not in google.__path__:
    google.__path__.append(protobuf_pkg)
