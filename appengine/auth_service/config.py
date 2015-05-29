# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Adapter between luci-config client and the rest of auth_service."""

import logging

from components import config


def is_remote_configured():
  """True if luci-config backend URL is defined.

  If luci-config backend URL is not set auth_service will use datastore
  as source of truth for configuration (with some simple web UI to change it).

  If luci-config backend URL is set, UI for config management will be read only
  and all config changes must be performed through luci-config.
  """
  return bool(get_remote_url())


def get_remote_url():
  """Returns URL of luci-config service if configured, to display in UI."""
  settings = config.ConfigSettings.cached()
  if settings and settings.service_hostname:
    return 'https://%s' % settings.service_hostname
  return None


def refetch_config():
  """Refetches all configs from luci-config (if enabled).

  Called as a cron job.
  """
  if not is_remote_configured():
    logging.info('Remote is not configured')
    return
  # TODO(vadimsh): Implement, add to cron.yaml when implemented.
