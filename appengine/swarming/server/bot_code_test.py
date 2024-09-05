#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import auth
from test_support import test_case

from components import config
from proto.config import config_pb2
from server import bot_code

CLIENT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(test_env.APP_DIR)), 'client')
sys.path.insert(0, CLIENT_DIR)
sys.path.insert(0, os.path.join(CLIENT_DIR, 'third_party', 'httplib2',
                                'python3'))
sys.path.insert(0, os.path.join(CLIENT_DIR, 'third_party'))
from depot_tools import fix_encoding
sys.path.pop(0)
sys.path.pop(0)


class BotManagementTest(test_case.TestCase):
  def setUp(self):
    super(BotManagementTest, self).setUp()
    self.testbed.init_user_stub()

    self.mock(
        auth, 'get_current_identity',
        lambda: auth.Identity(auth.IDENTITY_USER, 'joe@localhost'))

  def test_get_bot_channel(self):
    cfg = config_pb2.SettingsCfg(bot_deployment={'canary_percent': 20})
    stable, canary = 0, 0
    for i in range(0, 1000):
      channel = bot_code.get_bot_channel('bot-%d' % i, cfg)
      if channel == bot_code.STABLE_BOT:
        stable += 1
      elif channel == bot_code.CANARY_BOT:
        canary += 1
      else:
        raise AssertionError('Unexpected channel')
    self.assertEqual(stable, 802)
    self.assertEqual(canary, 198)  # roughly 20%

  def test_get_bot_version(self):
    bot_code.ConfigBundleRev(
        key=bot_code.config_bundle_rev_key(),
        stable_bot=bot_code.BotArchiveInfo(
            digest='stable-digest',
            bot_config_rev='stable-rev',
        ),
        canary_bot=bot_code.BotArchiveInfo(
            digest='canary-digest',
            bot_config_rev='canary-rev',
        ),
    ).put()
    self.assertEqual(
        bot_code.get_bot_version(bot_code.STABLE_BOT),
        ('stable-digest', 'stable-rev'),
    )
    self.assertEqual(
        bot_code.get_bot_version(bot_code.CANARY_BOT),
        ('canary-digest', 'canary-rev'),
    )

  def test_get_bootstrap(self):
    def get_self_config_mock(path, revision=None, store_last_good=False):
      self.assertEqual('scripts/bootstrap.py', path)
      self.assertEqual(None, revision)
      self.assertEqual(True, store_last_good)
      return 'rev1', 'foo bar'

    self.mock(config, 'get_self_config', get_self_config_mock)
    f = bot_code.get_bootstrap('localhost', 'token')
    expected = ('#!/usr/bin/env python\n'
                '# coding: utf-8\n'
                'host_url = \'localhost\'\n'
                'bootstrap_token = \'token\'\n'
                'foo bar')
    self.assertEqual(expected, f.content)

  def test_get_bot_config(self):
    def get_self_config_mock(path, revision=None, store_last_good=False):
      self.assertEqual('scripts/bot_config.py', path)
      self.assertEqual(None, revision)
      self.assertEqual(True, store_last_good)
      return 'rev1', 'foo bar'

    self.mock(config, 'get_self_config', get_self_config_mock)
    f, rev = bot_code.get_bot_config()
    self.assertEqual('foo bar', f.content)
    self.assertEqual('rev1', rev)

  def test_bootstrap_token(self):
    tok = bot_code.generate_bootstrap_token()
    self.assertEqual(
        {'for': 'user:joe@localhost'}, bot_code.validate_bootstrap_token(tok))


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
