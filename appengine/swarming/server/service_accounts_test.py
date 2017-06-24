#!/usr/bin/env python
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from test_support import test_case

from components import auth
from components import utils

from server import service_accounts


class SystemAccountTokenTest(test_case.TestCase):
  def setUp(self):
    super(SystemAccountTokenTest, self).setUp()
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))

  def test_none(self):
    self.assertEqual(
        ('none', None),
        service_accounts.get_system_account_token(None, ['scope']))

  def test_bot(self):
    self.assertEqual(
        ('bot', None),
        service_accounts.get_system_account_token('bot', ['scope']))

  def test_token(self):
    calls = []
    def mocked(**kwargs):
      calls.append(kwargs)
      return 'fake-token', utils.time_time() + 3600
    self.mock(auth, 'get_access_token', mocked)

    tok = service_accounts.AccessToken('fake-token', utils.time_time() + 3600)
    self.assertEqual(
        ('bot@example.com', tok),
        service_accounts.get_system_account_token('bot@example.com', ['scope']))

    self.assertEqual([{
        'act_as': 'bot@example.com',
        'min_lifetime_sec': service_accounts.MIN_TOKEN_LIFETIME_SEC,
        'scopes': ['scope'],
    }], calls)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
