#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses an isolateserver instance via remote_api."""

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
  import acl
  import config
  import gcs
  import handlers_frontend
  import handlers_common
  import model
  import stats

  from components import auth
  from model import ContentEntry

  return locals().copy()


if __name__ == '__main__':
  sys.exit(remote_api.main(sys.argv[1:], APP_DIR, setup_context))
