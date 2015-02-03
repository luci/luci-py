#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env_handlers
from support import test_case

import dev_appserver
dev_appserver.fix_sys_path()

import webapp2
import webtest

from components import auth_testing

import handlers_endpoints


class ClientAPITest(test_env_handlers.AppTestBase, test_case.EndpointsTestCase):

  api_service_cls = handlers_endpoints.SwarmingService

  def setUp(self):
    super(ClientAPITest, self).setUp()
    test_case.EndpointsTestCase.setUp(self)
    self.app = self._api_app
    # TODO(cmassaro): the above will change as endpoints are defined

  def test_client_task_result_ok(self):
    """Assert that client_task_result produces a result entity."""
    self.client_create_task()

    self.set_as_bot()
    task_id = self.bot_run_task()
    request = ClientResult(task_id=task_id)

    response = self.call_api(
        'client_task_result', self.message_to_dict(request), 200)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
