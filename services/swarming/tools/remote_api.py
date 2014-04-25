#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an swarming server instance via remote_api."""

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components'))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components', 'third_party'))

from tools import remote_api


def setup_context():
  """Symbols to import into interactive console."""
  sys.path.insert(0, APP_DIR)
  sys.path.insert(0, os.path.join(APP_DIR, 'third_party'))
  sys.path.insert(0, os.path.join(APP_DIR, 'components', 'third_party'))

  # Unused variable 'XXX'; they are accessed via locals().
  # pylint: disable=W0612
  from common import swarm_constants
  from components import auth
  from server import admin_user
  from server import dimension_mapping
  from server import dimensions_utils
  from server import test_management
  from server import test_request
  from server import test_runner
  from server import user_manager
  from stats import daily_stats
  from stats import machine_stats
  from stats import runner_stats

  import main

  return locals().copy()


if __name__ == '__main__':
  sys.exit(remote_api.main(sys.argv[1:], APP_DIR, setup_context))
