# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import webapp2

from components import auth
from components import utils


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


assert utils.is_local_dev_server()
auth.disable_process_cache()

# Add a fake admin for local dev server.
if not auth.is_replica():
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')

# /_ah/warmup is used by the smoke test to detect that app is alive.
app = webapp2.WSGIApplication(
    [webapp2.Route(r'/_ah/warmup', WarmupHandler)], debug=True)
