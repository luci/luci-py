# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Handlers for HTTP requests."""

from components import utils

import handlers_cron
import handlers_endpoints
import handlers_queues


utils.set_task_queue_module('default')

endpoints_app = handlers_endpoints.create_endpoints_app()
cron_app = handlers_cron.create_cron_app()
queue_app = handlers_queues.create_queues_app()
