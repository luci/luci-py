#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

import test_env
test_env.setup_test_env()

from support import test_case

from components.ereporter2 import formatter


# Access to a protected member XXX of a client class - pylint: disable=W0212


class Ereporter2FormatterTest(test_case.TestCase):
  def test_relative_path(self):
    data = [
      os.getcwd(),
      os.path.dirname(os.path.dirname(os.path.dirname(
          formatter.runtime.__file__))),
      os.path.dirname(os.path.dirname(os.path.dirname(
          formatter.webapp2.__file__))),
      os.path.dirname(os.getcwd()),
      '.',
    ]
    for value in data:
      i = os.path.join(value, 'foo')
      self.assertEqual('foo', formatter._relative_path(i))

    self.assertEqual('bar/foo', formatter._relative_path('bar/foo'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
