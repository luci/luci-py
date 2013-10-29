# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates and emails exception reports.

Inspired by google/appengine/ext/ereporter but crawls logservice instead of
using the DB.

Also adds logging formatException() filtering to reduce the file paths length.
"""

import datetime
import hashlib
import logging
import os
import re
import time
from xml.sax import saxutils

import jinja2
import webob
import webapp2
from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import logservice
from google.appengine.api import mail
from google.appengine.api import mail_errors
from google.appengine.ext import ndb


# Paths that can be stripped from the stack traces by relative_path().
PATHS_TO_STRIP = (
  # On AppEngine, cwd is always the application's root directory.
  os.getcwd() + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(runtime.__file__))) + '/',
  os.path.dirname(os.path.dirname(os.path.dirname(webapp2.__file__))) + '/',
  # Fallback to stripping at appid.
  os.path.dirname(os.getcwd()) + '/',
)


# These are the default templates but any template can be used.
REPORT_HEADER = (
  '<h2>Report for {{start_str}} ({{start}}) to {{end_str}} ({{end}})</h2>\n'
  'Modules-Versions:\n'
  '<ul>{% for i in module_versions %}<li>{{i.0}} - {{i.1}}</li>\n{% endfor %}'
  '</ul>\n')
REPORT_HEADER_TEMPLATE = jinja2.Template(REPORT_HEADER)

# Content of the exception report.
# Unusual layout is to ensure template is useful with tags stripped for the bare
# text format.
REPORT_CONTENT = (
  '<h3>{% if report_url %}<a href="{{report_url}}?start={{start}}&end={{end}}">'
      '{% endif %}'
      '{{occurrence_count}} occurrences of {{error_count}} errors across '
      '{{version_count}} versions.'
      '{% if ignored_count %}<br>\nIgnored {{ignored_count}} errors.{% endif %}'
      '{% if report_url %}</a>{% endif %}</h3>\n'
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
  '{% endfor %}'
)
REPORT_CONTENT_TEMPLATE = jinja2.Template(REPORT_CONTENT)


REPORT_TITLE = 'Exceptions on "{{app_id}}"'
REPORT_TITLE_TEMPLATE = jinja2.Template(REPORT_TITLE)

ERROR_TITLE = 'Failed to email exceptions on "{{app_id}}"'
ERROR_CONTENT = (
    '<strong>Failed to email the report due to {{exception}}</strong>.<br>\n'
    'Visit it online at <a href="{{report_url}}?start={{start}}&end={{end}}">'
      '{{report_url}}?start={{start}}&end={{end}}</a>.\n')


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


def should_ignore_error_record(ignored_lines, ignored_exceptions, error_record):
  """Returns True if an ErrorRecord should be ignored."""
  if any(l in ignored_lines for l in error_record.message.splitlines()):
    return True
  return error_record.exception_type in ignored_exceptions


def time2str(t):
  """Converts a time since epoch into human readable datetime str."""
  return str(datetime.datetime(*time.gmtime(t)[:7])).rsplit('.', 1)[0]


def get_template_env(start_time, end_time, module_versions):
  """Generates commonly used jinja2 template variables."""
  return {
    'app_id': app_identity.get_application_id(),
    'end': end_time,
    'end_str': time2str(end_time),
    'module_versions': module_versions,
    'start': start_time or 0,
    'start_str': time2str(start_time or 0),
  }


def records_to_html(
    error_categories, ignored_count, request_id_url, report_url, template,
    extras):
  """Generates and returns an HTML error report.

  Arguments:
    error_categories: list of ErrorCategory reports.
    ignored_count: number of errors not reported because ignored.
    request_id_url: base url to link to a specific request_id.
    report_url: base url to use to recreate this report.
    template: precompiled jinja2 template. REPORT_CONTENT_TEMPLATE is
              recommended.
    extras: extra dict to use to render the template.
  """
  error_categories.sort(key=lambda e: (e.version, -e.count))
  template_values = {
    'error_count': len(error_categories),
    'errors': error_categories,
    'ignored_count': ignored_count,
    'occurrence_count': sum(e.count for e in error_categories),
    'report_url': report_url,
    'request_id_url': request_id_url,
    'version_count': len(set(e.version for e in error_categories)),
  }
  template_values.update(extras)
  return template.render(template_values)


def report_to_html(
    report, ignored, title_template, header_template, content_template,
    request_id_url, env):
  """Helper function to generate a full HTML report."""
  title = title_template.render(env)
  header = header_template.render(env)
  # Create two reports, the one for the actual error messages and another one
  # for the ignored messages.
  report_out = records_to_html(
      report, 0, request_id_url, None, content_template, env)
  ignored_out = records_to_html(
      ignored, 0, request_id_url, None, content_template, env)
  return ''.join((
      '<html>\n<head><title>',
      title,
      '</title></head>\n<body>',
      header,
      report_out,
      '<hr>\n<h2>Ignored reports</h2>\n',
      ignored_out,
      '</body></html>'))


def email_html(to, subject, body):
  """Sends an email including a textual representation of the HTML body.

  The body must not contain <html> or <body> tags.
  """
  mail_args = {
    'body': saxutils.unescape(re.sub(r'<[^>]+>', r'', body)),
    'html': '<html><body>%s</body></html>' % body,
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


def get_default_start_time():
  """Calculates default value for start_time."""
  info = ErrorReportingInfo.get_by_id(id=ErrorReportingInfo.KEY_ID)
  return info.timestamp if info else None


def generate_report(start_time, end_time, module_versions, ignorer):
  """Returns a list of ErrorCategory to generate a report.

  Arguments:
    start_time: time to look for report, defaults to last email sent.
    end_time: time to end the search for error, defaults to now.
    module_versions: list of tuple of module-version to gather info about.
    ignorer: function that returns True if an ErrorRecord should be ignored.

  Returns:
    tuple of two list of ErrorCategory, the ones that should be reported and the
    ones that should be ignored.
  """
  ignored = {}
  categories = {}
  for i in _extract_exceptions_from_logs(start_time, end_time, module_versions):
    bucket = ignored if ignorer and ignorer(i) else categories
    category = bucket.setdefault(
        i.signature,
        ErrorCategory(i.signature, i.version, i.module, i.message, i.resource))
    category.events.append(i)
  return categories.values(), ignored.values()


def _get_end_time_for_email():
  """Exists so it can be mocked when testing generate_and_email_report().

  Do not read by default anything more recent than 5 minutes to cope with mild
  level of logservice inconsistency. High levels of logservice inconsistencies
  will result in lost messages.
  """
  return int(time.time() - 5*60)


def generate_and_email_report(
    module_versions, ignorer, recipients, request_id_url,
    report_url, title_template, content_template, extras):
  """Generates and emails an exception report.

  To be called from a cron_job.

  Arguments:
    module_versions: list of tuple of module-version to gather info about.
    ignorer: function that returns True if an ErrorRecord should be ignored.
    recipients: str containing comma separated email addresses.
    request_id_url: base url to use to link to a specific request_id.
    report_url: base url to use to recreate this report.
    title_template: precompiled jinja2 template to render the email subject.
                      REPORT_TITLE_TEMPLATE is recommended.
    content_template: precompiled jinja2 template to render the email content.
                      REPORT_CONTENT_TEMPLATE is recommended.+
    extras: extra dict to use to render the template.

  Returns:
    True if the email was sent successfully.
  """
  start_time = get_default_start_time()
  end_time = _get_end_time_for_email()
  logging.info(
      'generate_and_email_report(%s, %s, %s, ..., %s)',
      start_time, end_time, module_versions, recipients)
  result = False
  try:
    categories, ignored = generate_report(
        start_time, end_time, module_versions, ignorer)
    more_extras = get_template_env(start_time, end_time, module_versions)
    more_extras.update(extras or {})
    body = records_to_html(
        categories, sum(len(c.events) for c in ignored), request_id_url,
        report_url, content_template, more_extras)
    if categories:
      subject_line = title_template.render(more_extras)
      if not email_html(recipients, subject_line, body):
        # Send an email to alert.
        more_extras['report_url'] = report_url
        subject_line = jinja2.Template(ERROR_TITLE).render(more_extras)
        body = jinja2.Template(ERROR_CONTENT).render(more_extras)
        email_html(recipients, subject_line, body)
    logging.info('New timestamp %s', end_time)
    ErrorReportingInfo(id=ErrorReportingInfo.KEY_ID, timestamp=end_time).put()
    result = True
  except runtime.DeadlineExceededError:
    # Hitting the deadline here isn't a big deal if it is rare.
    logging.warning('Got a DeadlineExceededError')
  logging.info(
      'Processed %d items, ignored %d, reduced to %d categories, sent to %s.',
      sum(len(c.events) for c in categories),
      sum(len(c.events) for c in ignored),
      len(categories),
      recipients)
  return result


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
    if not isinstance(formatter, Formatter):
      # Prevent immediate recursion.
      handler.setFormatter(Formatter(formatter))
