#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import webtest

from components import auth_testing
from components import template
from test_support import test_case

import handlers_frontend
import importer
import replication


GOOD_IMPORTER_CONFIG = """
# Comment.
tarball {
  domain: "example.com"
  systems: "ldap"
  url: "http://example.com/stuff.tar.gz"
}
plainlist {
  group: "chromium-committers"
  url: "http://chromium-committers.appspot.com/chromium"
}
"""

BAD_IMPORTER_CONFIG = """
# Missing 'url'.
tarball {
  domain: "example.com"
  systems: "ldap"
}
"""


class FrontendHandlersTest(test_case.TestCase):
  """Tests the frontend handlers."""

  def setUp(self):
    super(FrontendHandlersTest, self).setUp()
    self.mock(replication, 'trigger_replication', lambda *_args, **_kws: None)
    self.app = webtest.TestApp(
        handlers_frontend.create_application(debug=True),
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
    self.assertEqual({'config': ''}, response.json)

  def test_importer_config_get(self):
    importer.write_config_text(GOOD_IMPORTER_CONFIG)
    response = self.app.get('/auth_service/api/v1/importer/config', status=200)
    self.assertEqual({'config': GOOD_IMPORTER_CONFIG}, response.json)

  def test_importer_config_post_ok(self):
    response = self.app.post_json(
        '/auth_service/api/v1/importer/config',
        {'config': GOOD_IMPORTER_CONFIG},
        headers={'X-XSRF-Token': auth_testing.generate_xsrf_token_for_test()},
        status=200)
    self.assertEqual({'ok': True}, response.json)
    self.assertEqual(GOOD_IMPORTER_CONFIG, importer.read_config_text())

  def test_importer_config_post_bad(self):
    response = self.app.post_json(
        '/auth_service/api/v1/importer/config',
        {'config': BAD_IMPORTER_CONFIG},
        headers={'X-XSRF-Token': auth_testing.generate_xsrf_token_for_test()},
        status=400)
    self.assertEqual(
        {'text': '"url" field is required in TarballEntry'}, response.json)
    self.assertEqual('', importer.read_config_text())

  def test_importer_config_post_locked(self):
    self.mock(handlers_frontend.config, 'is_remote_configured', lambda: True)
    response = self.app.post_json(
        '/auth_service/api/v1/importer/config',
        {'config': GOOD_IMPORTER_CONFIG},
        headers={'X-XSRF-Token': auth_testing.generate_xsrf_token_for_test()},
        status=409)
    self.assertEqual(
        {'text': 'The configuration is managed elsewhere'}, response.json)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
