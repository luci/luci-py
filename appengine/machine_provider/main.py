# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Handlers for HTTP requests."""

from components import utils

import gae_ts_mon

import handlers_cron
import handlers_endpoints
import handlers_frontend
import handlers_queues


utils.set_task_queue_module('default')

cron_app = handlers_cron.create_cron_app()
endpoints_app = handlers_endpoints.create_endpoints_app()
frontend_app = handlers_frontend.create_frontend_app()
queue_app = handlers_queues.create_queues_app()

gae_ts_mon.initialize(app=cron_app)
gae_ts_mon.initialize(app=frontend_app)
gae_ts_mon.initialize(app=queue_app)
