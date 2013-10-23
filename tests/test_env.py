# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(TESTS_DIR)


def find_gae_sdk():
  base_dir = BASE_DIR
  while True:
    gae_dir = os.path.join(base_dir, 'google_appengine')
    if os.path.isfile(os.path.join(gae_dir, 'dev_appserver.py')):
      return gae_dir
    root_base_dir = os.path.dirname(base_dir)
    if root_base_dir == base_dir:
      return None
    base_dir = root_base_dir


def setup_test_env():
  """Sets up App Engine/Django test environment."""
  gae_dir = find_gae_sdk()
  sys.path.insert(0, gae_dir)

  import dev_appserver  # pylint: disable=F0401
  dev_appserver.fix_sys_path()

  # TODO(maruel): Load it from app.yaml.
  os.environ['APPLICATION_ID'] = 'isolateserver-dev'

  sys.path.insert(0, BASE_DIR)
  sys.path.insert(0, os.path.join(TESTS_DIR, 'third_party'))
  sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))
