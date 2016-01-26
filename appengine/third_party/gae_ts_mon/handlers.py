# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import logging

import webapp2

from infra_libs.ts_mon import config
from infra_libs.ts_mon.common import interface


class SendHandler(webapp2.RequestHandler):
  def get(self):
    if self.request.headers.get('X-Appengine-Cron') != 'true':
      self.abort(403)

    interface.flush()


app = webapp2.WSGIApplication([
    (r'/internal/cron/ts_mon/send', SendHandler),
], debug=True)
config.initialize(app)
