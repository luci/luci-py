# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""UI code to generate email and HTML reports."""

import datetime
import logging
import re
import time
from xml.sax import saxutils

import jinja2

from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import mail
from google.appengine.api import mail_errors

from . import api


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
  '{{category.events.head.0.handler_module}}<br>\n'
  '{{category.events.head.0.method}} {{category.events.head.0.host}}'
      '{{category.events.head.0.resource}} '
      '(HTTP {{category.events.head.0.status}})<br>\n'
  '<pre>{{category.events.head.0.message}}</pre>\n'
  '{{category.events.total_count}} occurrences: '
      '{% for event in category.events.head %}<a '
      'href="{{request_id_url}}{{event.request_id}}">Entry</a> {% endfor %}'
      '{% if category.events.has_gap %}&hellip;{% endif %}'
      '{% for event in category.events.tail %}<a '
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


### Private stuff.


def _get_end_time_for_email():
  """Exists so it can be mocked when testing generate_and_email_report().

  Do not read by default anything more recent than 5 minutes to cope with mild
  level of logservice inconsistency. High levels of logservice inconsistencies
  will result in lost messages.
  """
  # TODO(maruel): Use datetime.datetime.utcnow() for consistency.
  return int(time.time() - 5*60)


def _time2str(t):
  """Converts a time since epoch into human readable datetime str."""
  return str(datetime.datetime(*time.gmtime(t)[:7])).rsplit('.', 1)[0]


def _records_to_html(
    error_categories, ignored_count, request_id_url, report_url, template,
    extras):
  """Generates and returns an HTML error report.

  Arguments:
    error_categories: list of _ErrorCategory reports.
    ignored_count: number of errors not reported because ignored.
    request_id_url: base url to link to a specific request_id.
    report_url: base url to use to recreate this report.
    template: precompiled jinja2 template. REPORT_CONTENT_TEMPLATE is
              recommended.
    extras: extra dict to use to render the template.
  """
  error_categories.sort(key=lambda e: (e.version, -e.events.total_count))
  template_values = {
    'error_count': len(error_categories),
    'errors': error_categories,
    'ignored_count': ignored_count,
    'occurrence_count': sum(e.events.total_count for e in error_categories),
    'report_url': report_url,
    'request_id_url': request_id_url,
    'version_count': len(set(e.version for e in error_categories)),
  }
  template_values.update(extras)
  return template.render(template_values)


def _email_html(to, subject, body):
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


### Public API.


def get_template_env(start_time, end_time, module_versions):
  """Generates commonly used jinja2 template variables."""
  return {
    'app_id': app_identity.get_application_id(),
    'end': end_time,
    'end_str': _time2str(end_time),
    'module_versions': module_versions or [],
    'start': start_time or 0,
    'start_str': _time2str(start_time or 0),
  }


def report_to_html(
    report, ignored, header_template, content_template,
    request_id_url, env):
  """Helper function to generate a full HTML report."""
  header = header_template.render(env)
  # Create two reports, the one for the actual error messages and another one
  # for the ignored messages.
  report_out = _records_to_html(
      report, 0, request_id_url, None, content_template, env)
  ignored_out = _records_to_html(
      ignored, 0, request_id_url, None, content_template, env)
  return ''.join((
      header,
      report_out,
      '<hr>\n<h2>Ignored reports</h2>\n',
      ignored_out))


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
  start_time = api.get_default_start_time()
  end_time = _get_end_time_for_email()
  logging.info(
      'generate_and_email_report(%s, %s, %s, ..., %s)',
      start_time, end_time, module_versions, recipients)
  result = False
  try:
    categories, ignored = api.generate_report(
        start_time, end_time, module_versions, ignorer)
    more_extras = get_template_env(start_time, end_time, module_versions)
    more_extras.update(extras or {})
    body = _records_to_html(
        categories, sum(c.events.total_count for c in ignored), request_id_url,
        report_url, content_template, more_extras)
    if categories:
      subject_line = title_template.render(more_extras)
      if not _email_html(recipients, subject_line, body):
        # Send an email to alert.
        more_extras['report_url'] = report_url
        subject_line = jinja2.Template(ERROR_TITLE).render(more_extras)
        body = jinja2.Template(ERROR_CONTENT).render(more_extras)
        _email_html(recipients, subject_line, body)
    logging.info('New timestamp %s', end_time)
    api.ErrorReportingInfo(
        id=api.ErrorReportingInfo.KEY_ID, timestamp=end_time).put()
    result = True
  except runtime.DeadlineExceededError:
    # Hitting the deadline here isn't a big deal if it is rare.
    logging.warning('Got a DeadlineExceededError')
  logging.info(
      'Processed %d items, ignored %d, reduced to %d categories, sent to %s.',
      sum(c.events.total_count for c in categories),
      sum(c.events.total_count for c in ignored),
      len(categories),
      recipients)
  return result
