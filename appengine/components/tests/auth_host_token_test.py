#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import sys
import unittest

import test_env
test_env.setup_test_env()

from components.auth import host_token
from test_support import test_case


class HostTokenTest(test_case.TestCase):
  def setUp(self):
    super(HostTokenTest, self).setUp()
    self.mock(host_token.logging, 'error', lambda *_: None)

  def test_is_valid_host(self):
    self.assertTrue(host_token.is_valid_host('abcd-c4'))
    self.assertTrue(host_token.is_valid_host('abcd-c4.domain.com'))
    self.assertTrue(host_token.is_valid_host('ABCD-C4.domain.com'))
    self.assertFalse(host_token.is_valid_host(None))
    self.assertFalse(host_token.is_valid_host(''))
    self.assertFalse(host_token.is_valid_host('.'))
    self.assertFalse(host_token.is_valid_host('abcd..ef'))
    self.assertFalse(host_token.is_valid_host('abcd-c4<'))
    self.assertFalse(host_token.is_valid_host('abcd-c4:egf'))

  def test_create_host_token_fail(self):
    with self.assertRaises(ValueError):
      host_token.create_host_token('.')

  def test_validate_host_token_ok(self):
    tok = host_token.create_host_token('abcd-c4')
    self.assertEqual('abcd-c4', host_token.validate_host_token(tok))

  def test_validate_host_token_wrong_data(self):
    bad_tok = host_token.HostToken.generate(embedded={'not_h': '1'})
    self.assertIsNone(host_token.validate_host_token(bad_tok))

  def test_validate_host_token_bad_hostname_in_token(self):
    bad_tok = host_token.HostToken.generate(embedded={'h': '...'})
    self.assertIsNone(host_token.validate_host_token(bad_tok))

  def test_validate_host_token_bad_token(self):
    self.assertIsNone(host_token.validate_host_token('I am not a token'))

  def test_validate_host_token_expiration(self):
    origin = datetime.datetime(2014, 1, 1, 1, 1, 1)
    self.mock_now(origin)
    token = host_token.create_host_token('abcd-c4', expiration_sec=60)

    self.mock_now(origin, 59)
    self.assertEqual('abcd-c4', host_token.validate_host_token(token))

    self.mock_now(origin, 61)
    self.assertIsNone(host_token.validate_host_token(token))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
