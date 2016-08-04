# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Handlers for HTTP requests."""

from components import utils

import gae_ts_mon

import handlers_cron
import handlers_queues


utils.set_task_queue_module('default')

cron_app = handlers_cron.create_cron_app()
queues_app = handlers_queues.create_queues_app()

gae_ts_mon.initialize(app=cron_app)
gae_ts_mon.initialize(app=queues_app)
