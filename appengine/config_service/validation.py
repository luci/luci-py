# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

import common


def is_valid_service_id(service_id):
  return bool(common.SERVICE_ID_RGX.match(service_id))


def validate_config(config_set, path, content, log_errors=False):
  """Validate config content."""
  # TODO(nodir): implement custom validation.
  # TODO(nodir): validate service/luci-config:projects.cfg.
  # TODO(nodir): validate service/luci-config:import.cfg.
  return True
