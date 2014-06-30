# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Backend functions to gather error reports and save them.

Also adds logging formatException() filtering to reduce the file paths length in
error logged.
"""

import collections
import datetime
import hashlib
import logging
import os
import re

import webapp2
import webob

from google.appengine import runtime
from google.appengine.api import logservice
from google.appengine.ext import ndb


# Paths that can be stripped from the stack traces by relative_path().
PATHS_TO_STRIP = (
  # On AppEngine, cwd is always the application's root directory.
  os.getcwd() + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(runtime.__file__))) + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(webapp2.__file__))) + '/',
  # Fallback to stripping at appid.
  os.path.dirname(os.getcwd()) + '/',
  './',
)


# Handle this error message specifically.
SOFT_MEMORY = 'Exceeded soft private memory limit'


# Markers to read back a stack trace.
_STACK_TRACE_MARKER = 'Traceback (most recent call last):'
_RE_STACK_TRACE_FILE = (
    r'^(?P<prefix>  File \")(?P<file>[^\"]+)(?P<suffix>\"\, line )'
    r'(?P<line_no>\d+)(?P<rest>|\, in .+)$')


# Number of first error records to show in the category error list.
_ERROR_LIST_HEAD_SIZE = 10
# Number of last error records to show in the category error list.
_ERROR_LIST_TAIL_SIZE = 10


### Models.


class ErrorReportingInfo(ndb.Model):
  """Notes the last timestamp to be used to resume collecting errors."""
  KEY_ID = 'root'

  timestamp = ndb.FloatProperty()


### Private suff.


class _CappedList(object):
  """List of objects with only several first ones and several last ones
  actually stored.

  Basically a structure for:
  0 1 2 3 ..... N-3 N-2 N-1 N
  """

  def __init__(self, head_size, tail_size, items=None):
    assert head_size > 0, head_size
    assert tail_size > 0, tail_size
    self.head_size = head_size
    self.tail_size = tail_size
    self.head = []
    self.tail = collections.deque()
    self.total_count = 0
    for item in (items or []):
      self.append(item)

  def append(self, item):
    """Adds item to the list.

    If list is small enough, will add it to the head, else to the tail
    (evicting oldest stored tail item).
    """
    # Keep count of all elements ever added (even though they may not be
    # actually stored).
    self.total_count += 1
    # List is still short, grow head.
    if len(self.head) < self.head_size:
      self.head.append(item)
    else:
      # List is long enough to start using tail. Grow tail, but keep only
      # end of it.
      if len(self.tail) == self.tail_size:
        self.tail.popleft()
      self.tail.append(item)

  @property
  def has_gap(self):
    """True if this list contains skipped middle section."""
    return len(self.head) + len(self.tail) < self.total_count


class _ErrorCategory(object):
  """Describes a 'class' of error messages' according to an unique signature."""
  def __init__(self, signature, version, module, message, resource):
    self.signature = signature
    self.version = version
    self.module = module
    self.message = message
    self.resource = resource
    self.events = _CappedList(_ERROR_LIST_HEAD_SIZE, _ERROR_LIST_TAIL_SIZE)


def _utcnow():
  return datetime.datetime.utcnow()


def _signature_from_message(message):
  """Calculates a signature and extract the exception if any.

  Arguments:
    message: a complete log entry potentially containing a stack trace.

  Returns:
    tuple of a signature and the exception type, if any.
  """
  lines = message.splitlines()
  if not lines:
    return '', None

  if _STACK_TRACE_MARKER not in lines:
    # Not an exception. Use the first line as the 'signature'.

    # Look for special messages to reduce.
    if lines[0].startswith(SOFT_MEMORY):
      # Consider SOFT_MEMORY an 'exception'.
      return SOFT_MEMORY, SOFT_MEMORY
    return lines[0].strip(), None

  # It is a stack trace.
  stacktrace = []
  index = lines.index(_STACK_TRACE_MARKER) + 1
  while index < len(lines):
    if not re.match(_RE_STACK_TRACE_FILE, lines[index]):
      break
    if (len(lines) > index + 1 and
        re.match(_RE_STACK_TRACE_FILE, lines[index+1])):
      # It happens occasionally with jinja2 templates.
      stacktrace.append(lines[index])
      index += 1
    else:
      stacktrace.extend(lines[index:index+2])
      index += 2

  if index >= len(lines):
    # Failed at grabbing the exception.
    return lines[0].strip(), None

  # SyntaxError produces this.
  if lines[index].strip() == '^':
    index += 1

  assert index > 0
  while True:
    ex_type = lines[index].split(':', 1)[0].strip()
    if ex_type:
      break
    if not index:
      logging.error('Failed to process message.\n%s', message)
      # Fall back to returning the first line.
      return lines[0].strip(), None
    index -= 1

  path = None
  line_no = -1
  for l in reversed(stacktrace):
    m = re.match(_RE_STACK_TRACE_FILE, l)
    if m:
      path = os.path.basename(m.group('file'))
      line_no = int(m.group('line_no'))
      break
  signature = '%s@%s:%d' % (ex_type, path, line_no)
  if len(signature) > 256:
    signature = 'hash:%s' % hashlib.sha1(signature).hexdigest()
  return signature, ex_type


def _extract_exceptions_from_logs(start_time, end_time, module_versions):
  """Yields ErrorRecord objects from the logs.

  Arguments:
    start_time: epoch time to start searching. If 0 or None, defaults to
                1970-01-01.
    end_time: epoch time to stop searching. If 0 or None, defaults to
              time.time().
    module_versions: list of tuple of module-version to gather info about.
  """
  if start_time and end_time and start_time >= end_time:
    raise webob.exc.HTTPBadRequest(
        'Invalid range, start_time must be before end_time.')
  for entry in logservice.fetch(
      start_time=start_time or None,
      end_time=end_time or None,
      minimum_log_level=logservice.LOG_LEVEL_ERROR,
      include_incomplete=True,
      include_app_logs=True,
      module_versions=module_versions):
    # Merge all error messages. The main reason to do this is that sometimes
    # a single logging.error() 'Traceback' is split on each line as an
    # individual log_line entry.
    msgs = []
    log_time = None
    for log_line in entry.app_logs:
      # TODO(maruel): Specifically handle:
      # 'Request was aborted after waiting too long to attempt to service your
      # request.'
      # For an unknown reason, it is logged at level info (!?)
      if log_line.level < logservice.LOG_LEVEL_ERROR:
        continue
      msg = log_line.message.strip('\n')
      if not msg.strip():
        continue
      # The message here is assumed to be utf-8 encoded but that is not
      # guaranteed. The dashboard does prints out utf-8 log entries properly.
      try:
        msg = msg.decode('utf-8')
      except UnicodeDecodeError:
        msg = msg.decode('ascii', 'replace')
      msgs.append(msg)
      log_time = log_time or log_line.time

    yield ErrorRecord(
        entry.request_id,
        entry.start_time, log_time, entry.latency, entry.mcycles,
        entry.ip, entry.nickname, entry.referrer, entry.user_agent,
        entry.host, entry.resource, entry.method, entry.task_queue_name,
        entry.was_loading_request, entry.version_id, entry.module_id,
        entry.url_map_entry, entry.app_engine_release, entry.instance_key,
        entry.status, '\n'.join(msgs))


### Public API.


class ErrorRecord(object):
  """Describes the context in which an error was logged."""

  # Use slots to reduce memory footprint of ErrorRecord object.
  __slots__ = (
      'request_id', 'start_time', 'exception_time', 'latency', 'mcycles', 'ip',
      'nickname', 'referrer', 'user_agent', 'host', 'resource', 'method',
      'task_queue_name', 'was_loading_request', 'version', 'module',
      'handler_module', 'gae_version', 'instance', 'status', 'message',
      'short_signature', 'exception_type', 'signature')

  def __init__(
      self, request_id, start_time, exception_time, latency, mcycles,
      ip, nickname, referrer, user_agent,
      host, resource, method, task_queue_name,
      was_loading_request, version, module, handler_module, gae_version,
      instance,
      status, message):
    # Unique identifier.
    self.request_id = request_id
    # Initial time the request was handled.
    self.start_time = start_time
    # Time of the exception.
    self.exception_time = exception_time
    # Wall-clock time duration of the request.
    self.latency = latency
    # CPU usage in mega-cyclers.
    self.mcycles = mcycles

    # Who called.
    self.ip = ip
    self.nickname = nickname
    self.referrer = referrer
    self.user_agent = user_agent

    # What was called.
    self.host = host
    self.resource = resource
    self.method = method
    self.task_queue_name = task_queue_name

    # What handled the call.
    self.was_loading_request = was_loading_request
    self.version = version
    self.module = module
    self.handler_module = handler_module
    self.gae_version = gae_version
    self.instance = instance

    # What happened.
    self.status = status
    self.message = message

    # Creates an unique signature string based on the message.
    self.short_signature, self.exception_type = _signature_from_message(message)
    self.signature = self.short_signature + '@' + version


def should_ignore_error_record(ignored_lines, ignored_exceptions, error_record):
  """Returns True if an ErrorRecord should be ignored."""
  if any(l in ignored_lines for l in error_record.message.splitlines()):
    return True
  return error_record.exception_type in ignored_exceptions


def get_default_start_time():
  """Calculates default value for start_time."""
  info = ErrorReportingInfo.get_by_id(id=ErrorReportingInfo.KEY_ID)
  return info.timestamp if info else None


def generate_report(start_time, end_time, module_versions, ignorer):
  """Returns a list of _ErrorCategory to generate a report.

  Arguments:
    start_time: time to look for report, defaults to last email sent.
    end_time: time to end the search for error, defaults to now.
    module_versions: list of tuple of module-version to gather info about.
    ignorer: function that returns True if an ErrorRecord should be ignored.

  Returns:
    tuple of two list of _ErrorCategory, the ones that should be reported and
    the ones that should be ignored.
  """
  ignored = {}
  categories = {}
  for i in _extract_exceptions_from_logs(start_time, end_time, module_versions):
    bucket = ignored if ignorer and ignorer(i) else categories
    if i.signature not in bucket:
      bucket[i.signature] = _ErrorCategory(
          i.signature, i.version, i.module, i.message, i.resource)
    bucket[i.signature].events.append(i)
  return categories.values(), ignored.values()


def serialize(obj):
  """Serializes an object to make it acceptable for json serialization."""
  if obj is None or isinstance(obj, (basestring, int, float, long)):
    return obj
  if isinstance(obj, (list, tuple)):
    return [serialize(i) for i in obj]
  if isinstance(obj, dict):
    return dict((k, serialize(v)) for k, v in obj.iteritems())
  # Serialize complex object as a dict.
  members = [
      i for i in dir(obj)
      if not i[0].startswith(('_', 'has_')) and not callable(getattr(obj, i))
  ]
  return serialize(dict((k, getattr(obj, k)) for k in members))


def log_request_id(request_id):
  """Returns a logservice.RequestLog for a request id or None if not found."""
  request = list(logservice.fetch(
      include_incomplete=True, include_app_logs=True, request_ids=[request_id]))
  if not request:
    logging.info('Dang, didn\'t find the request_id %s', request_id)
    return None
  assert len(request) == 1, request
  return request[0]


### Formatter


def relative_path(path):
  """Strips the current working directory or common library prefix.

  Used by Formatter.
  """
  for i in PATHS_TO_STRIP:
    if path.startswith(i):
      return path[len(i):]
  return path


def reformat_stack(stack):
  """Post processes the stack trace through relative_path()."""
  out = stack.splitlines(True)
  def replace(l):
    m = re.match(_RE_STACK_TRACE_FILE, l, re.DOTALL)
    if m:
      groups = list(m.groups())
      groups[1] = relative_path(groups[1])
      return ''.join(groups)
    return l
  return ''.join(map(replace, out))


class Formatter(object):
  """Formats exceptions nicely.

  Is is very important that this class does not throw exceptions.
  """
  def __init__(self, original):
    self._original = original

  def formatTime(self, record, datefmt=None):
    return self._original.formatTime(record, datefmt)

  def format(self, record):
    """Overrides formatException()."""
    # Cache the traceback text to avoid having the wrong formatException() be
    # called, e.g. we don't want the one in self._original.formatException() to
    # be called.
    if record.exc_info and not record.exc_text:
      record.exc_text = self.formatException(record.exc_info)
    return self._original.format(record)

  def formatException(self, exc_info):
    return reformat_stack(self._original.formatException(exc_info))


def register_formatter():
  """Registers a nicer logging Formatter to all currently registered handlers.

  This is optional but helps reduce spam significantly so it is highly
  encouraged.
  """
  for handler in logging.getLogger().handlers:
    # pylint: disable=W0212
    formatter = handler.formatter or logging._defaultFormatter
    if not isinstance(formatter, Formatter):
      # Prevent immediate recursion.
      handler.setFormatter(Formatter(formatter))
