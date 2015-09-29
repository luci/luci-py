# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Handlers for HTTP requests."""

from components import utils

import handlers_cron
import handlers_pubsub
import handlers_queues


utils.set_task_queue_module('default')

cron_app = handlers_cron.create_cron_app()
pubsub_app = handlers_pubsub.create_pubsub_app()
queues_app = handlers_queues.create_queues_app()
