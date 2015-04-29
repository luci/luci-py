# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""WSGI app with task queue endpoints.

Used when 'metrics' component is included via app.yaml includes. If app has
a backend module, it must be included there too.
"""

import webapp2

from components import utils

from . import metrics


def create_wsgi_application():
  return webapp2.WSGIApplication(
      metrics.get_backend_routes(),
      debug=utils.is_local_dev_server())


APP = create_wsgi_application()
