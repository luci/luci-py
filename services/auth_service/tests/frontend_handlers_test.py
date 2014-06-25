#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env
test_env.setup_test_env()

# From components/third_party/
import webtest

from support import test_case
from frontend import handlers


class FrontendHandlersTest(test_case.TestCase):
  """Tests the frontend handlers."""

  def setUp(self):
    super(FrontendHandlersTest, self).setUp()
    self.app = webtest.TestApp(handlers.create_application(debug=True))

  def test_warmup(self):
    response = self.app.get('/_ah/warmup')
    self.assertEqual(200, response.status_code)
    self.assertEqual('ok', response.body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
