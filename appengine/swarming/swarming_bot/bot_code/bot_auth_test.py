#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import json
import logging
import os
import sys
import tempfile
import time
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

from depot_tools import auto_stub
from depot_tools import fix_encoding
from third_party import requests
from utils import file_path

import bot_auth


def call_rpc(ctx, scopes):
  r = requests.post(
      url='http://127.0.0.1:%d/rpc/LuciLocalAuthService.GetOAuthToken' %
          ctx['rpc_port'],
      data=json.dumps({
        'scopes': scopes,
        'secret': ctx['secret'],
      }),
      headers={'Content-Type': 'application/json'})
  return r.json()


class AuthSystemTest(auto_stub.TestCase):
  def setUp(self):
    super(AuthSystemTest, self).setUp()
    self.tmp_dir = tempfile.mkdtemp(prefix='bot_main')
    self.counter = 0

  def tearDown(self):
    file_path.rmtree(self.tmp_dir)
    super(AuthSystemTest, self).tearDown()

  def write_auth_params(self, auth_params):
    self.counter += 1
    path = os.path.join(self.tmp_dir, 'auth_params_%d.json' % self.counter)
    with open(path, 'w') as f:
      json.dump(auth_params, f)
    return path

  def test_works(self):
    """High level test of the overall functionality."""
    auth_params_path = self.write_auth_params({
      'swarming_http_headers': {
        'Authorization': 'Bearer bot-own-token',
      },
    })

    auth_sys = bot_auth.AuthSystem()
    try:
      auth_sys.start(auth_params_path)

      self.assertEqual(
          {'Authorization': 'Bearer bot-own-token'},
          auth_sys.bot_headers)
      self.assertEqual(
          ['rpc_port', 'secret'],
          sorted(auth_sys.local_auth_context))

      # Try to actually use the local RPC service to grab a token.
      resp = call_rpc(auth_sys.local_auth_context, ['A', 'B', 'C'])
      self.assertEqual([u'access_token', u'expiry'], sorted(resp))
      self.assertEqual(u'bot-own-token', resp['access_token'])
      self.assertGreater(resp['expiry'], time.time())

    finally:
      auth_sys.stop()


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
