# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""OS abstraction OS specific utility functions."""

# pylint: disable=unnecessary-lambda

import sys
import os

if os.name == 'posix':
  from api.platforms import posix

if os.name != 'darwin':  # As of writing, macOS can't run on GCE.
  is_gce = lambda: gce.is_gce()  # to reuse gce.is_gce mock, if any
  from api.platforms import gce

if sys.platform == 'aix':
  from api.platforms import aix

if sys.platform == 'cygwin':
  from api.platforms import win

if sys.platform == 'darwin':
  from api.platforms import osx
  is_gce = lambda: False


if sys.platform == 'win32':
  from api.platforms import win


if sys.platform == 'linux':
  try:
    from api.platforms import android
  except OSError:
    logging.warning('failed to import android', exc_info=True)
    android = None
  from api.platforms import linux
