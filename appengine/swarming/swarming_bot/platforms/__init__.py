# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OS abstraction OS specific utility functions."""

import sys


if sys.platform == 'cygwin':
  import gce
  import posix
  import win
  from gce import is_gce

if sys.platform == 'darwin':
  import osx
  import posix
  is_gce = lambda: False


if sys.platform == 'win32':
  import gce
  import win
  from gce import is_gce


if sys.platform == 'linux2':
  import gce
  import linux
  import posix
  from gce import is_gce
