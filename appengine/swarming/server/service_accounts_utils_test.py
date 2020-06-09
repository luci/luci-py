#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import test_env
test_env.setup_test_env()

from test_support import test_case

from server import service_accounts_utils


class ServiceAccountUtilsTest(test_case.TestCase):

  def test_is_service_account(self):
    self.assertTrue(
        service_accounts_utils.is_service_account(
            'a@proj.iam.gserviceaccount.com'))
    self.assertFalse(service_accounts_utils.is_service_account('bot:something'))
    self.assertFalse(
        service_accounts_utils.is_service_account('user:something@something'))
    self.assertFalse(service_accounts_utils.is_service_account(''))
