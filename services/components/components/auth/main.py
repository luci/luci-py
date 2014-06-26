# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""WSGI app with Auth REST API and UI endpoints.

Used when 'auth' component is included via app.yaml includes.
"""

from .ui import app
APP = app.create_wsgi_application()
