#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import sys
import unittest

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env
test_env.setup_test_env()

# From components/third_party/
import webtest

from components import auth_testing
from components import template

from support import test_case

from common import importer
from common import replication

from frontend import handlers


GOOD_IMPORTER_CONFIG = [
  {
    'domain': 'example.com',
    'systems': ['ldap'],
    'url': 'http://example.com/stuff.tar.gz',
  },
  {
    'format': 'plainlist',
    'group': 'chromium-committers',
    'url': 'http://chromium-committers.appspot.com/chromium',
  },
]

BAD_IMPORTER_CONFIG = [
  # Missing 'url'.
  {
    'domain': 'example.com',
    'systems': ['ldap'],
  },
]


class FrontendHandlersTest(test_case.TestCase):
  """Tests the frontend handlers."""

  def setUp(self):
    super(FrontendHandlersTest, self).setUp()
    self.mock(replication, 'trigger_replication', lambda *_args, **_kws: None)
    self.app = webtest.TestApp(
        handlers.create_application(debug=True),
        extra_environ={'REMOTE_ADDR': '127.0.0.1'})
    auth_testing.mock_is_admin(self, True)
    auth_testing.mock_get_current_identity(self)

  def tearDown(self):
    try:
      template.reset()
    finally:
      super(FrontendHandlersTest, self).tearDown()

  def test_warmup(self):
    response = self.app.get('/_ah/warmup')
    self.assertEqual(200, response.status_code)
    self.assertEqual('ok', response.body)

  def test_importer_config_get_default(self):
    response = self.app.get('/auth_service/api/v1/importer/config', status=200)
    self.assertEqual({'config': []}, response.json)

  def test_importer_config_get(self):
    importer.write_config(GOOD_IMPORTER_CONFIG)
    response = self.app.get('/auth_service/api/v1/importer/config', status=200)
    self.assertEqual({'config': GOOD_IMPORTER_CONFIG}, response.json)

  def test_importer_config_post_ok(self):
    response = self.app.post_json(
        '/auth_service/api/v1/importer/config',
        {'config': GOOD_IMPORTER_CONFIG},
        headers={'X-XSRF-Token': auth_testing.generate_xsrf_token_for_test()},
        status=200)
    self.assertEqual({'ok': True}, response.json)
    self.assertEqual(GOOD_IMPORTER_CONFIG, importer.read_config())

  def test_importer_config_post_bad(self):
    response = self.app.post_json(
        '/auth_service/api/v1/importer/config',
        {'config': BAD_IMPORTER_CONFIG},
        headers={'X-XSRF-Token': auth_testing.generate_xsrf_token_for_test()},
        status=400)
    self.assertEqual({'text': 'Invalid config format.'}, response.json)
    self.assertEqual([], importer.read_config())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
