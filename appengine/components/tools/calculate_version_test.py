#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

import calculate_version


class CalculateVersionTest(unittest.TestCase):
  def test_pristine_version(self):
    actual = calculate_version.get_limited_version(
      "1234", "123abcd", True, "auser", "", 63)
    expected = "1234-123abcd"
    self.assertEqual(expected, actual)

  def test_pristine_version_with_tag(self):
    actual = calculate_version.get_limited_version(
      "1234", "123abcd", True, "auser", "atag", 63)
    expected = "1234-123abcd-atag"
    self.assertEqual(expected, actual)

  def test_non_pristine_version(self):
    actual = calculate_version.get_limited_version(
      "1234", "123abcd", False, "auser", "", 63)
    expected = "1234-123abcd-tainted-auser"
    self.assertEqual(expected, actual)

  def test_remove_tainted(self):
    actual = calculate_version.get_limited_version(
      "1234", "123abcd", False, "auser", "", 20)
    expected = "1234-123abcd-auser"
    self.assertEqual(expected, actual)

  def test_truncate_long_username(self):
    actual = calculate_version.get_limited_version(
      "12345", "123abcd", False, "alongusername", "", 20)
    expected = "12345-123abcd-alongu"
    self.assertEqual(expected, actual)
    self.assertEqual(20, len(actual))

  def test_truncate_long_username_with_tag(self):
    actual = calculate_version.get_limited_version(
      "12345", "123abcd", False, "alongusername", "atag", 25)
    expected = "12345-123abcd-alongu-atag"
    self.assertEqual(expected, actual)
    self.assertEqual(25, len(actual))

  def test_raise_on_truncate_pristine_version(self):
    with self.assertRaises(calculate_version.VersionError):
      calculate_version.get_limited_version(
        "1234", "123abcd", True, "auser", "atag", 16)

  def test_raise_on_truncate_username_to_less_than_six_chars(self):
    with self.assertRaises(calculate_version.VersionError):
      calculate_version.get_limited_version(
        "123456", "123abcd", False, "alongusername", "", 20)

  def test_short_username_not_needing_truncation_ok(self):
    actual = calculate_version.get_limited_version(
      "12345", "123abcd", False, "auser", "atag", 24)
    expected = "12345-123abcd-auser-atag"
    self.assertEqual(expected, actual)
    self.assertEqual(24, len(actual))

  def test_raise_on_short_username_needing_truncation(self):
    with self.assertRaises(calculate_version.VersionError):
      calculate_version.get_limited_version(
        "12345", "123abcd", False, "auser", "atag", 23)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
