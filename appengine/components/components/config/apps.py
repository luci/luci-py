# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""WSGI app with a cron job endpoint.

Used when config component is included via app.yaml includes. If app has
a backend module, it must be included there.
"""

import webapp2

from components import utils

from . import handlers


def create_backend_application():
  return webapp2.WSGIApplication(
      handlers.get_backend_routes(),
      debug=utils.is_local_dev_server())


backend = create_backend_application()
