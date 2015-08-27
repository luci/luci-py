#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import win


class TestWin(unittest.TestCase):
  def test_from_cygwin_path(self):
    data = [
      ('foo', None),
      ('x:\\foo$', None),
      ('X:\\foo$', None),
      ('/cygdrive/x/foo$', 'x:\\foo$'),
    ]
    for i, (inputs, expected) in enumerate(data):
      actual = win.from_cygwin_path(inputs)
      self.assertEqual(expected, actual, (inputs, expected, actual, i))

  def test_to_cygwin_path(self):
    data = [
      ('foo', None),
      ('x:\\foo$', '/cygdrive/x/foo$'),
      ('X:\\foo$', '/cygdrive/x/foo$'),
      ('/cygdrive/x/foo$', None),
    ]
    for i, (inputs, expected) in enumerate(data):
      actual = win.to_cygwin_path(inputs)
      self.assertEqual(expected, actual, (inputs, expected, actual, i))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
