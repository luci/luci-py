#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates Swarming server with the version derived from the current git
checkout state.
"""

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components', 'tools'))

import update_instance


def main():
  return update_instance.main(
      sys.argv[1:], APP_DIR, [os.path.join(APP_DIR, 'app.yaml')])


if __name__ == '__main__':
  sys.exit(main())
