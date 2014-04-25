#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs swarming server locally via dev_appserver.py."""

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components'))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components', 'third_party'))

from tools import run_dev_appserver


if __name__ == '__main__':
  sys.exit(run_dev_appserver.main(sys.argv[1:], APP_DIR))
