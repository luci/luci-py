# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""OS abstraction OS specific utility functions."""

# pylint: disable=unnecessary-lambda

import sys

import six

if sys.platform == 'cygwin':
  import gce
  import posix
  import win
  is_gce = lambda: gce.is_gce() # to reuse gce.is_gce mock, if any

if sys.platform == 'darwin':
  import osx
  import posix
  is_gce = lambda: False


if sys.platform == 'win32':
  import gce
  import win
  is_gce = lambda: gce.is_gce() # to reuse gce.is_gce mock, if any


if sys.platform.startswith('linux'):
  try:
    if six.PY2:
      import android
  except OSError:
    android = None
  from api.platforms import gce
  from api.platforms import linux
  from api.platforms import posix
  is_gce = lambda: gce.is_gce() # to reuse gce.is_gce mock, if any
