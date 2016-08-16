# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Handlers for HTTP requests."""

from components import utils

import gae_ts_mon

import config
import handlers_cron
import handlers_endpoints
import handlers_queues


def is_enabled_callback():
  return config.settings().enable_ts_monitoring


def main():
  utils.set_task_queue_module('default')
  apps = (
    handlers_endpoints.create_endpoints_app(),
    handlers_cron.create_cron_app(),
    handlers_queues.create_queues_app(),
  )
  for app in apps[1:]:
    # Not callable on endpoints app
    gae_ts_mon.initialize(app=app, is_enabled_fn=is_enabled_callback)
  return apps


endpoints_app, cron_app, queues_app = main()
