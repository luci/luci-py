#!/usr/bin/env python
# coding: utf-8
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import collections
import logging
import re
import sys
import unittest

# Setups environment.
import test_env_handlers

import cipd
import swarming_rpcs


class TestPinChecker(unittest.TestCase):
  def setUp(self):
    super(TestPinChecker, self).setUp()
    self.cp = collections.namedtuple('CipdPackage', 'path package_name version')

  def test_correct_pins(self):
    a = self.cp('path', 'package_name/${platform}-${os_ver}', 'latest')
    b = self.cp('path', 'package_name/windows-amd64-something_10', 'deadbeef'*5)

    with cipd.pin_check_fn(None, None) as check:
      # will not raise
      check(a, b)

      a = self.cp('path', 'other/${platform}-${os_ver}', 'latest')
      b = self.cp('path', 'other/windows-amd64-something_10', 'deadbeef'*5)

      # will not raise
      check(a, b)

  def test_mismatched_pins(self):
    # if a is already a pin, b must match its version exactly
    a = self.cp('path', 'package_name/${platform}-${os_ver}', 'deadbeef'*5)
    b = self.cp('path', 'package_name/windows-amd64-something_10', 'badc0ffe'*5)

    with cipd.pin_check_fn(None, None) as check:
      with self.assertRaisesRegexp(ValueError, 'Mismatched pins'):
        check(a, b)

  def test_mismatched_paths(self):
    a = self.cp('path', 'package_name/${platform}-${os_ver}', 'latest')
    b = self.cp('else', 'package_name/windows-amd64-something_10', 'deadbeef'*5)

    with cipd.pin_check_fn(None, None) as check:
      with self.assertRaisesRegexp(ValueError, 'Mismatched path'):
        check(a, b)

  def test_mismatched_names(self):
    a = self.cp('', 'package_name/${platform}-${os_ver}', 'latest')
    b = self.cp('', 'else/windows-amd64-something_10', 'deadbeef'*5)

    with cipd.pin_check_fn(None, None) as check:
      with self.assertRaisesRegexp(ValueError, 'Mismatched package_name'):
        check(a, b)

    a = self.cp('', 'package_name/${platform}-${os_ver}', 'latest')
    b = self.cp('', 'package_name/windows-amd64-something_10', 'deadbeef'*5)
    # will not raise
    check(a, b)

    # This doesn't match the previous knowledge of platform or os_ver, so it
    # will not match.
    a = self.cp('', 'package_name/${platform}-${os_ver}', 'latest')
    b = self.cp('', 'package_name/linux-32-nerds', 'deadbeef'*5)

    with self.assertRaisesRegexp(ValueError, 'Mismatched package_name'):
      check(a, b)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
