# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Configures includes (components.auth).

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

from components import utils

utils.import_third_party()
utils.fix_protobuf_package()

import change_log

# Auth component UI is tweaked manually, see handlers_frontend.py.
components_auth_UI_CUSTOM_CONFIG = True

# Use a custom change task enqueuer so changelog generation is handled by
# Auth Service v2.
components_auth_CUSTOM_PROCESS_CHANGE_TASK_ENQUEUER = \
    change_log.enqueue_v2_changelog_task

# Use backend module and dedicated task queue for change log generation.
components_auth_BACKEND_MODULE = 'backend'
components_auth_PROCESS_CHANGE_TASK_QUEUE = 'process-auth-db-change'
