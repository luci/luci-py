# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import re

# Matches service account email (or rather close enough superset of it).
_SERVICE_ACCOUNT_RE = re.compile(r'^[0-9a-zA-Z_\-\.\+\%]+@[0-9a-zA-Z_\-\.]+$')


def is_service_account(value):
  """Returns True if given value looks like a service account email."""
  return bool(_SERVICE_ACCOUNT_RE.match(value))
