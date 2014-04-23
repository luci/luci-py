#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates Isolate server with the version derived from the current git checkout
state.
"""

import os
import sys

import app_config

ROOT_DIR = os.path.dirname(app_config.APP_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'components', 'tools'))

import update_instance


def main():
  return update_instance.main(
      sys.argv[1:], app_config.APP_DIR, app_config.MODULES)


if __name__ == '__main__':
  sys.exit(main())
