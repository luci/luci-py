# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""UI code to generate email and HTML reports."""

import datetime
import logging
import os
import re
import time
from xml.sax import saxutils

from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import mail
from google.appengine.api import mail_errors

from . import api
from components import template

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


### Private stuff.


# Function to be used to filter the log entries to only keep the interesting
# ones.
_LOG_FILTER = None

# List of email addresses to use to email reports. It's a function so it doesn't
# need to be loaded at process startup.
_GET_ADMINS = None


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
    error_categories, ignored_count, request_id_url, report_url, template_name,
    extras):
  """Generates and returns an HTML error report.

  Arguments:
    error_categories: list of _ErrorCategory reports.
    ignored_count: number of errors not reported because ignored.
    request_id_url: base url to link to a specific request_id.
    report_url: base url to use to recreate this report.
    template_name: jinja2 template. ereporter2_content_template.html is
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
  return template.render(template_name, template_values)


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
    report, ignored, header_template_name, content_template_name,
    request_id_url, env):
  """Helper function to generate a full HTML report."""
  header = template.render(header_template_name, env)
  # Create two reports, the one for the actual error messages and another one
  # for the ignored messages.
  report_out = _records_to_html(
      report, 0, request_id_url, None, content_template_name, env)
  ignored_out = _records_to_html(
      ignored, 0, request_id_url, None, content_template_name, env)
  return ''.join((
      header,
      report_out,
      '<hr>\n<h2>Ignored reports</h2>\n',
      ignored_out))


def generate_and_email_report(
    module_versions, ignorer, recipients, request_id_url,
    report_url, title_template_name, content_template_name, extras):
  """Generates and emails an exception report.

  To be called from a cron_job.

  Arguments:
    module_versions: list of tuple of module-version to gather info about.
    ignorer: function that returns True if an ErrorRecord should be ignored.
    recipients: str containing comma separated email addresses.
    request_id_url: base url to use to link to a specific request_id.
    report_url: base url to use to recreate this report.
    title_template_name: jinja2 template to render the email subject.
        ereporter2_title.html is recommended.
    content_template_name: jinja2 template to render the email content.
        ereporter2_content.html is recommended.
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
        report_url, content_template_name, more_extras)
    if categories:
      subject_line = template.render(title_template_name, more_extras)
      if not _email_html(recipients, subject_line, body):
        # Send an email to alert.
        more_extras['report_url'] = report_url
        subject_line = template.render(
            'ereporter2/error_title.html', more_extras)
        body = template.render('ereporter2/error_content.html', more_extras)
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


def configure(get_admins, log_filter):
  global _GET_ADMINS
  global _LOG_FILTER

  _GET_ADMINS = get_admins
  _LOG_FILTER = log_filter

  template.bootstrap({'ereporter2': os.path.join(ROOT_DIR, 'templates')})
