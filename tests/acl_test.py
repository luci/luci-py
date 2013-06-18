#!/usr/bin/env python
# Copyright (c) 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

import test_env

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

test_env.setup_test_env()

import acl


class AclTest(unittest.TestCase):
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
