# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""HTTP Handlers."""

import time

import webapp2

from google.appengine.api import app_identity

from . import api
from . import ui
from components import auth
from components import decorators
from components import template
from components import utils


# Access to a protected member XXX of a client class - pylint: disable=W0212


class RestrictedEreporter2Report(auth.AuthenticatingHandler):
  """Returns all the recent errors as a web page."""

  @auth.require(auth.is_admin)
  def get(self):
    """Reports the errors logged and ignored.

    Arguments:
      start: epoch time to start looking at. Defaults to the messages since the
             last email.
      end: epoch time to stop looking at. Defaults to now.
      modules: comma separated modules to look at.
      tainted: 0 or 1, specifying if desiring tainted versions. Defaults to 1.
    """
    request_id_url = '/restricted/ereporter2/request/'
    end = int(float(self.request.get('end', 0)) or time.time())
    start = int(
        float(self.request.get('start', 0)) or
        api.get_default_start_time() or 0)
    modules = self.request.get('modules')
    if modules:
      modules = modules.split(',')
    tainted = bool(int(self.request.get('tainted', '1')))
    module_versions = utils.get_module_version_list(modules, tainted)
    report, ignored = api.generate_report(
        start, end, module_versions, ui._LOG_FILTER)
    env = ui.get_template_env(start, end, module_versions)
    content = ui.report_to_html(
        report, ignored,
        'ereporter2_report_header.html',
        'ereporter2_report_content.html',
        request_id_url, env)
    self.response.write(
        template.render('ereporter2_report.html', {'content': content}))


class RestrictedEreporter2Request(auth.AuthenticatingHandler):
  """Dumps information about single logged request."""

  @auth.require(auth.is_admin)
  def get(self, request_id):
    # TODO(maruel): Add UI.
    data = api.log_request_id_to_dict(request_id)
    if not data:
      self.abort(404, detail='Request id was not found.')
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.write(utils.encode_to_json(data))


class InternalEreporter2Mail(webapp2.RequestHandler):
  """Handler class to generate and email an exception report."""
  @decorators.require_cronjob
  def get(self):
    """Sends email(s) containing the errors logged."""
    # Do not use self.request.host_url because it will be http:// and will point
    # to the backend, with an host format that breaks the SSL certificate.
    # TODO(maruel): On the other hand, Google Apps instances are not hosted on
    # appspot.com.
    host_url = 'https://%s.appspot.com' % app_identity.get_application_id()
    request_id_url = host_url + '/restricted/ereporter2/request/'
    report_url = host_url + '/restricted/ereporter2/report'
    recipients = self.request.get('recipients', ui._GET_ADMINS())
    result = ui.generate_and_email_report(
        utils.get_module_version_list(None, False),
        ui._LOG_FILTER,
        recipients,
        request_id_url,
        report_url,
        'ereporter2_report_title.html',
        'ereporter2_report_content.html',
        {})
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if result:
      self.response.write('Success.')
    else:
      # Do not HTTP 500 since we do not want it to be retried.
      self.response.write('Failed.')


def get_frontend_routes():
  return [
     webapp2.Route(
          r'/restricted/ereporter2/report',
          RestrictedEreporter2Report),
     webapp2.Route(
          r'/restricted/ereporter2/request/<request_id:[0-9a-fA-F]+>',
          RestrictedEreporter2Request),
  ]


def get_backend_routes():
  # This requires a cron job to this URL.
  return [
    webapp2.Route(
        r'/internal/cron/ereporter2/mail', InternalEreporter2Mail),
  ]
