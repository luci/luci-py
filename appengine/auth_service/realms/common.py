# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Shared constants and internal utilities."""

import re

from components import utils


# Used in place of a project in full names of internal realms.
INTERNAL_PROJECT = '@internal'

# Root realm is included in all other realms, see realms_config.proto.
ROOT_REALM = '@root'
# Legacy realm is used for older realm-less resources, see realms_config.proto.
LEGACY_REALM = '@legacy'
# Project realm is used as a place to put whole-project resources, see
# realms_config.proto.
PROJECT_REALM = '@project'

# Allowed non-special (not "@...") realm names in realms.cfg.
REALM_NAME_RE = re.compile(r'^[a-z0-9_\.\-/]{1,400}$')


@utils.cache
def cfg_path():
  """Returns a path of the project config file to read."""
  if utils.is_local_dev_server() or utils.is_dev():
    return 'realms-dev.cfg'
  return 'realms.cfg'
