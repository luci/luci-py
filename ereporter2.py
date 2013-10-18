# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates and emails exception reports.

Inspired by google/appengine/ext/ereporter but crawls logservice instead of
using the DB.

Also adds logging formatException() filtering to reduce the file paths length.
"""

import hashlib
import logging
import os
import re
import time
from xml.sax import saxutils

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
import jinja2
import webapp2
from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import logservice
from google.appengine.api import mail
from google.appengine.api import mail_errors
from google.appengine.ext import ndb
# pylint: enable=E0611,F0401


# Paths that can be stripped from the stack traces by relative_path().
PATHS_TO_STRIP = (
  os.getcwd() + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(runtime.__file__))) + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(webapp2.__file__))) + '/',
  # Fallback to stripping at appid.
  os.path.dirname(os.getcwd()) + '/',
)


# Content of the exception report.
# Unusual layout is to ensure template is useful with tags stripped for the bare
# text format.
REPORT_HTML = (
  '<html><head><title>Exceptions on "{{app_id}}".</title></head>\n'
  '<body><h3>{{occurrence_count}} occurrences of {{error_count}} '
      'errors across {{version_count}} versions.'
      '{% if ignored_count %}<br>\nIgnored {{ignored_count}} errors.{% endif %}'
      '</h3>\n'
  '{% for category in errors %}\n'
  '<span style="font-size:130%">{{category.signature}}</span><br>\n'
  '{{category.events.0.handler_module}}<br>\n'
  '{{category.events.0.method}} {{category.events.0.host}}'
      '{{category.events.0.resource}} (HTTP {{category.events.0.status}})<br>\n'
  '<pre>{{category.events.0.message}}</pre>\n'
  '{{category.count}} occurrences: '
      '{% for event in category.events %}<a '
      'href="{{request_id_url}}{{event.request_id}}">Entry</a> {% endfor %}'
  '<p>\n'
  '<br>\n'
  '{% endfor %}</body>\n'
)


# Cache the precompiled template.
REPORT_HTML_TEMPLATE = jinja2.Template(REPORT_HTML)


# Markers to read back a stack trace.
STACK_TRACE_MARKER = 'Traceback (most recent call last):'
RE_STACK_TRACE_FILE = (
    r'^(?P<prefix>  File \")(?P<file>[^\"]+)(?P<suffix>\"\, line )'
    r'(?P<line_no>\d+)(?P<rest>|\, in .+)$')

# Handle this error message specifically.
SOFT_MEMORY = 'Exceeded soft private memory limit'


class ErrorReportingInfo(ndb.Model):
  """Notes the last timestamp to be used to resume collecting errors."""
  KEY_ID = 'root'

  timestamp = ndb.FloatProperty()


class ErrorCategory(object):
  """Describes a 'class' of error messages' according to an unique signature."""
  def __init__(self, signature, version, module, message, resource):
    self.signature = signature
    self.version = version
    self.module = module
    self.message = message
    self.resource = resource
    self.events = []

  @property
  def count(self):
    return len(self.events)

  @property
  def request_ids(self):
    return [i.request_id for i in self.events]


class ErrorRecord(object):
  """Describes the context in which an error was logged."""
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
    self.short_signature, self.exception_type = signature_from_message(message)
    self.signature = self.short_signature + '@' + version


def signature_from_message(message):
  """Calculates a signature and extract the exception if any.

  Arguments:
    message: a complete log entry potentially containing a stack trace.

  Returns:
    tuple of a signature and the exception type, if any.
  """
  lines = message.splitlines()
  if STACK_TRACE_MARKER not in lines:
    # Not an exception. Use the first line as the 'signature'.

    # Look for special messages to reduce.
    if lines[0].startswith(SOFT_MEMORY):
      # Consider SOFT_MEMORY an 'exception'.
      return SOFT_MEMORY, SOFT_MEMORY
    return lines[0].strip(), None

  # It is a stack trace.
  stacktrace = []
  index = lines.index(STACK_TRACE_MARKER) + 1
  while index < len(lines):
    if not re.match(RE_STACK_TRACE_FILE, lines[index]):
      break
    if (len(lines) > index + 1 and
        re.match(RE_STACK_TRACE_FILE, lines[index+1])):
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
    m = re.match(RE_STACK_TRACE_FILE, l)
    if m:
      path = os.path.basename(m.group('file'))
      line_no = int(m.group('line_no'))
      break
  signature = '%s@%s:%d' % (ex_type, path, line_no)
  if len(signature) > 256:
    signature = 'hash:%s' % hashlib.sha1(signature).hexdigest()
  return signature, ex_type


def should_ignore_error_record(error_record):
  """Returns True if an ErrorRecord should be ignored."""
  # Ignore these failures, there's nothing to do.
  IGNORED_LINES = (
    # And..?
    '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
        'appengine/_internal/django/template/__init__.py:729: UserWarning: '
        'api_milliseconds does not return a meaningful value',
    # Thanks AppEngine.
    'Process terminated because the request deadline was exceeded during a '
        'loading request.',
  )
  # Ignore these exceptions.
  IGNORED_EXCEPTIONS = (
    'CancelledError',
    'DeadlineExceededError',
    SOFT_MEMORY,
  )

  if any(l in IGNORED_LINES for l in error_record.message.splitlines()):
    return True
  return error_record.exception_type in IGNORED_EXCEPTIONS


def records_to_html(error_categories, ignored_count, request_id_url):
  """Generates and returns an HTML error report.

  Arguments:
    error_categories: List of ErrorCategory reports.
  """
  error_categories.sort(key=lambda e: (e.version, -e.count))
  template_values = {
    'app_id': app_identity.get_application_id(),
    'error_count': len(error_categories),
    'errors': error_categories,
    'ignored_count': ignored_count,
    'occurrence_count': sum(e.count for e in error_categories),
    'request_id_url': request_id_url,
    'version_count': len(set(e.version for e in error_categories)),
  }
  return REPORT_HTML_TEMPLATE.render(template_values)


def email_html(to, subject, body):
  """Sends an email including a textual representation of the HTML body."""
  mail_args = {
    'body': saxutils.unescape(re.sub(r'<[^>]+>', r'', body)),
    'html': body,
    'sender': 'no_reply@%s.appspotmail.com' % app_identity.get_application_id(),
    'subject': subject,
  }
  try:
    if to:
      mail_args['to'] = to
      mail.send_mail(**mail_args)
    else:
      mail.send_mail_to_admins(**mail_args)
    return True
  except mail_errors.BadRequestError:
    return False


def _extract_exceptions_from_logs(start_time, end_time, module_versions):
  """Yields ErrorRecord objects from the logs."""
  for entry in logservice.fetch(
      start_time=start_time,
      end_time=end_time,
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
      if log_line.level < logservice.LOG_LEVEL_ERROR:
        continue
      msg = log_line.message.strip('\n')
      if not msg.strip():
        continue
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


def _email_batch(recipients, batch, ignored_count, request_id_url):
  """Emails a batch of ErrorCategories."""
  batch_size = len(batch)
  while batch:
    body = records_to_html(batch[:batch_size], ignored_count, request_id_url)
    subject = 'Exceptions on "%s"' % app_identity.get_application_id()
    if email_html(recipients, subject, body):
      # Success.
      batch = batch[batch_size:]
      continue
    if batch_size == 1:
      msg = 'Can\'t email exceptions to %s' % recipients
      logging.error(msg)
      break
    # Try again with a smaller batch.
    batch_size = max(batch_size / 2, 1)


def generate_html_report(module_versions, request_id_url):
  """Returns an HTML report since the last email report and return it."""
  info = ErrorReportingInfo.get_by_id(ErrorReportingInfo.KEY_ID)
  start_time = info.timestamp if info else None
  ignored = {}
  categories = {}
  for i in _extract_exceptions_from_logs(start_time, None, module_versions):
    if should_ignore_error_record(i):
      category = ignored.setdefault(
          i.signature,
          ErrorCategory(
              i.signature, i.version, i.module, i.message, i.resource))
    else:
      category = categories.setdefault(
          i.signature,
          ErrorCategory(
              i.signature, i.version, i.module, i.message, i.resource))
    category.events.append(i)
  ignored_count = sum(len(c.events) for c in ignored.itervalues())
  return records_to_html(categories.values(), ignored_count, request_id_url)


def generate_and_email_report(recipients, module_versions, request_id_url):
  """Generates and emails an exception report.

  To be called from a cron_job.

  Arguments:
    recipients: str containing comma separated email addresses
    module_versions: list of tuple of module-version combinations to listen to
    request_id_url: base url to use to link to a specific request_id
  """
  # Never read anything more recent than 5 minutes to cope with mild level of
  # logservice inconsistency. High levels of logservice inconsistencies will
  # result in lost messages.
  end_time = time.time() - 5*60
  logging.info(
      'generate_and_email_report(%s, %s), %s',
      recipients, module_versions, end_time)
  ignored = 0
  total = 0
  signatures_seen = set()
  try:
    info = ErrorReportingInfo.get_by_id(id=ErrorReportingInfo.KEY_ID)
    start_time = info.timestamp if info else None

    categories = {}
    batch_max_size = 100
    for i in _extract_exceptions_from_logs(
        start_time, end_time, module_versions):
      if should_ignore_error_record(i):
        ignored += 1
        continue
      category = categories.setdefault(
          i.signature,
          ErrorCategory(
              i.signature, i.version, i.module, i.message, i.resource))
      category.events.append(i)
      total += 1
      signatures_seen.add(i.signature)

      if total == batch_max_size or len(categories) > 50:
        _email_batch(recipients, categories.values(), ignored, request_id_url)
        logging.info('New timestamp %s', i.start_time)
        ErrorReportingInfo(
            id=ErrorReportingInfo.KEY_ID, timestamp=i.start_time).put()
        categories = {}

    # Iteration is completed.
    if categories:
      _email_batch(recipients, categories.values(), ignored, request_id_url)
    logging.info('New timestamp %s', end_time)
    ErrorReportingInfo(id=ErrorReportingInfo.KEY_ID, timestamp=end_time).put()
  except runtime.DeadlineExceededError:
    # Hitting the deadline here isn't a big deal if it is rare.
    logging.warning('Got a DeadlineExceededError')
  return 'Processed %d items, ignored %d, sent to %s:\n%s' % (
      total, ignored, recipients, '\n'.join(sorted(signatures_seen)))


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


def log_request_id_to_dict(request_id):
  """Returns the json representation of a request log entry.

  Returns None if no request is found.
  """
  logging.info('log_request_id_to_dict(%r)', request_id)
  request = list(logservice.fetch(
      include_incomplete=True, include_app_logs=True, request_ids=[request_id]))
  if not request:
    logging.info('Dang, didn\'t find the request_id %s', request_id)
    return None
  assert len(request) == 1
  return serialize(request[0])


### Formatter


def relative_path(path):
  """Strips the current working directory or common library prefix.

  Used by Formatter.
  """
  for i in PATHS_TO_STRIP:
    if path.startswith(i):
      return path[len(i):]
  return path


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
    """Post processes the stack trace through relative_path()."""
    out = self._original.formatException(exc_info).splitlines(True)
    def replace(l):
      m = re.match(RE_STACK_TRACE_FILE, l, re.DOTALL)
      if m:
        groups = list(m.groups())
        groups[1] = relative_path(groups[1])
        return ''.join(groups)
      return l
    return ''.join(map(replace, out))


def register_formatter():
  """Registers a nicer logging Formatter to all currently registered handlers.

  This is optional but helps reduce spam significantly so it is highly
  encouraged.
  """
  for handler in logging.getLogger().handlers:
    # pylint: disable=W0212
    formatter = handler.formatter or logging._defaultFormatter
    handler.setFormatter(Formatter(formatter))
