# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

import common


def validate_config(config_set, path, content, log_errors=False):
  """Validate config content."""
  # TODO(nodir): implement custom validation.
  # TODO(nodir): validate service/luci-config:projects.cfg.
  # TODO(nodir): validate service/luci-config:import.cfg.
  # TODO(nodir): validate service/luci-config:schemas.cfg.
  # TODO(nodir): validate service/<project_id>:project.cfg.
  # TODO(nodir): validate service/<project_id>:refs.cfg. Ref names must start
  # with 'refs/'.
  return True
