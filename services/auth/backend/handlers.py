# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Auth Server backend url handlers."""

import webapp2

from google.appengine.api import app_identity

from components import decorators
from components import ereporter2
from components import utils

from common import ereporter2_config


class InternalEreporter2Mail(webapp2.RequestHandler):
  """Handler class to generate and email an exception report."""
  @decorators.require_cronjob
  def get(self):
    """Sends email(s) containing the errors logged."""
    # Do not use self.request.host_url because it will be http:// and will point
    # to the backend, with an host format that breaks the SSL certificate.
    host_url = 'https://%s.appspot.com' % app_identity.get_application_id()

    # TODO(vadimsh): Implement ereporter2/* endpoints once auth is added.
    request_id_url = host_url + '/restricted/ereporter2/request/'
    report_url = host_url + '/restricted/ereporter2/report'

    # TODO(vadimsh): Fetch recipients from an auth group once auth is added.
    recipients = []

    result = ereporter2.generate_and_email_report(
        utils.get_module_version_list(None, False),
        ereporter2_config.should_ignore_error_record,
        recipients,
        request_id_url,
        report_url,
        ereporter2.REPORT_TITLE_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        {})

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if result:
      self.response.write('Success.')
    else:
      # Do not HTTP 500 since we do not want it to be retried.
      self.response.write('Failed.')


def get_routes():
  return [
    webapp2.Route(r'/internal/cron/ereporter2/mail', InternalEreporter2Mail),
  ]


def create_application(debug=False):
  return webapp2.WSGIApplication(get_routes(), debug=debug)
