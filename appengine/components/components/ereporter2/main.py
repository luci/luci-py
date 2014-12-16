# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""WSGI app with API, UI and task queue endpoints.

Used when 'ereporter2' component is included via app.yaml includes. If app has
a backend module, it must be included there too.
"""

import webapp2

from components import utils

from . import handlers
from . import ui


def create_wsgi_application():
  ui.configure()
  routes = []
  routes.extend(handlers.get_frontend_routes())
  routes.extend(handlers.get_backend_routes())
  return webapp2.WSGIApplication(routes, debug=utils.is_local_dev_server())


APP = create_wsgi_application()
