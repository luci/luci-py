#!/usr/bin/env vpython
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from test_support import test_case

from components.prpc import codes


class PRPCCodeTestCase(test_case.TestCase):
  def test_all_statusCode_valid(self):
    for k in dir(codes.StatusCode):
      if isinstance(getattr(codes.StatusCode, k), codes.StatusCodeBase):
        httpCode = codes.StatusCode.to_http_code(getattr(codes.StatusCode, k))
        self.assertNotEqual(httpCode, None)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
