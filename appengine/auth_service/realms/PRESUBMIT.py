# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
import re

USE_PYTHON3 = True
PRESUBMIT_VERSION = '2.0.0'

_IGNORE_FREEZE_FOOTER = 'Ignore-Freeze'
_INFRA_DATA = r"chrome-internal-review.googlesource.com/c/infradata/config/"


def CheckPermissionsFooter(input_api, output_api):
  footers = input_api.change.GitFootersFromDescription()

  for file in input_api.change.AffectedFiles():
    if 'permissions.py' in str(file) and _IGNORE_FREEZE_FOOTER not in footers:
      return [
          output_api.PresubmitError('''
        There is a freeze in effect on permissions.py.

        If changing the PermissionsDB, please go make the same change at
        configs/chrome-infra-auth-dev/permissions.cfg and include a git
        footer that looks like:

        Ignore-Freeze: {_INFRA_DATA}
        ''')
      ]
  return []
