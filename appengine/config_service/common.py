# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

SERVICE_ID_PATTERN = '[a-z0-9\-]+'
SERVICE_ID_RGX = re.compile('^%s$' % SERVICE_ID_PATTERN)
SERVICE_CONFIG_SET_RGX = re.compile('^services/(%s)$' % SERVICE_ID_PATTERN)
