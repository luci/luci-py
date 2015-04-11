# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

################################################################################
## Config set patterns.

SERVICE_ID_PATTERN = '[a-z0-9\-]+'
SERVICE_ID_RGX = re.compile('^%s$' % SERVICE_ID_PATTERN)
SERVICE_CONFIG_SET_RGX = re.compile('^services/(%s)$' % SERVICE_ID_PATTERN)

PROJECT_ID_PATTERN = SERVICE_ID_PATTERN
PROJECT_ID_RGX = re.compile('^%s$' % PROJECT_ID_PATTERN)
PROJECT_CONFIG_SET_RGX = re.compile('^projects/(%s)$' % PROJECT_ID_PATTERN)

BRANCH_CONFIG_SET_RGX = re.compile(
    '^projects/(%s)/branches/.+$' % PROJECT_ID_PATTERN)


################################################################################
## Known config file names.

# luci-config configs.
PROJECT_REGISTRY_FILENAME = 'projects.cfg'
ACL_FILENAME = 'acl.cfg'

# Project configs.
PROJECT_METADATA_FILENAME = 'project.cfg'
BRANCHES_FILENAME = 'branches.cfg'
