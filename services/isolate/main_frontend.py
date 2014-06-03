# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module is the entry point to load the AppEngine instance."""

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))

import acl
from components import ereporter2
import handlers_frontend


def create_application():
  """Bootstraps the app and creates the url router."""
  ereporter2.register_formatter()
  # This call does a DB operation, which is normally banned outside of an HTTP
  # handler context, but only happens when running on the dev_appserver so it is
  # fine.
  acl.bootstrap()

  return handlers_frontend.create_application()


app = create_application()
