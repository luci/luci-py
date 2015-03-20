#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components'))

from tools import run_coverage


if __name__ == '__main__':
  sys.exit(run_coverage.main(
      APP_DIR,
      ('tools',),
      'PRESUBMIT.py,components,*test*,tool*'))
