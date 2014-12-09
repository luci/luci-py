#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# from support import test_case
import test_env

# import endpoint_handlers_api


class HandlerTest(unittest.TestCase):
  """Test the handlers."""
  APP_DIR = ROOT_DIR

  def setUp(self):
    super(HandlerTest, self).setUp()
    self.testbed.init_user_stub()

  def test_pre_upload_ok(self):
    """Ascertain that PreUploadHandler correctly posts the JSON request."""
    pass
