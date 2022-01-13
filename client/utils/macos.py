# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""This is simple wrapper of objc or Carbon.File/MacOS for macos."""

import sys
import unicodedata


if sys.platform == 'darwin':
  import re

  import objc

  # Extract 43 from error like 'Mac Error -43'
  _mac_error_re = re.compile('^MAC Error -(\d+)$')

  Error = OSError

  def get_errno(e):
    m = _mac_error_re.match(e.args[0])
    if not m:
      return None
    return -int(m.groups()[0])

  def native_case(p):
    path = objc.FSRef.from_pathname(p.encode('utf-8').decode())
    return unicodedata.normalize('NFC', path.as_pathname())
