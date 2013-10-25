# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module is the entry point to load the AppEngine instance."""

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))

import acl
from components import ereporter2
import handlers


def CreateApplication():
  """Bootstraps the app and creates the url router."""
  ereporter2.register_formatter()
  acl.bootstrap()
  return handlers.CreateApplication()


app = CreateApplication()
