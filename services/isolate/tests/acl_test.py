#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import acl


class IpUtilsTest(unittest.TestCase):
  def test_parse_ip(self):
    data = [
      # (input, expected)
      ('allo', (None, None)),
      ('0.0.0.0', ('v4', 0L)),
      ('255.255.255.255', ('v4', 4294967295L)),
      ('255.256.255.255', (None, None)),
      ('0:0:0:0:0:0:0:0', ('v6', 0L)),
      (
        'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
        ('v6', 340282366920938463463374607431768211455L)),
    ]
    actual = [(v, acl.parse_ip(v)) for v, _ in data]
    self.assertEqual(data, actual)

  def test_ip_to_str(self):
    data = [
      # (input, expected)
      (('v4', 0L), 'v4-0'),
      (('v4', 4294967295L), 'v4-4294967295'),
      (('v6', 0L), 'v6-0'),
      (
        ('v6', 340282366920938463463374607431768211455L),
        'v6-340282366920938463463374607431768211455'),
    ]
    actual = [(v, acl.ip_to_str(*v)) for v, _ in data]
    self.assertEqual(data, actual)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
