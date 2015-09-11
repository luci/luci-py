# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Configures includes (components.auth).

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

from components import utils

# OpenID requires real accounts, it's not convenient on dev server, especially
# for smoke tests.
components_auth_USE_OPENID = not utils.is_local_dev_server()

# Auth component UI is tweaked manually, see handlers_frontend.py.
components_auth_UI_CUSTOM_CONFIG = True

# Use backend module and dedicated task queue for change log generation.
components_auth_BACKEND_MODULE = 'backend'
components_auth_PROCESS_CHANGE_TASK_QUEUE = 'process-auth-db-change'
