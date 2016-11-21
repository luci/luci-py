#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import auth
from components import auth_testing
from components import config
from components import utils
from components.auth import ipaddr
from test_support import test_case

from proto import bots_pb2
from server import bot_auth
from server import bot_groups_config


TEST_CONFIG = bots_pb2.BotsCfg(
  trusted_dimensions=['pool'],
  bot_group=[
    bots_pb2.BotGroup(
      bot_id=['bot_with_token'],
      auth=bots_pb2.BotAuth(require_luci_machine_token=True),
      dimensions=['pool:with_token']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_service_account'],
      auth=bots_pb2.BotAuth(require_service_account='a@example.com'),
      dimensions=['pool:with_service_account']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_ip_whitelist'],
      auth=bots_pb2.BotAuth(ip_whitelist='ip_whitelist'),
      dimensions=['pool:with_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_service_account_and_ip_whitelist'],
      auth=bots_pb2.BotAuth(
          require_service_account='a@example.com', ip_whitelist='ip_whitelist'),
      dimensions=['pool:with_service_account_and_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_token_and_ip_whitelist'],
      auth=bots_pb2.BotAuth(
          require_luci_machine_token=True, ip_whitelist='ip_whitelist'),
      dimensions=['pool:with_token_and_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['broken_config'],
      dimensions=['pool:broken_config']),
  ],
)


class BotAuthTest(test_case.TestCase):
  def setUp(self):
    super(BotAuthTest, self).setUp()

    self.logs = []
    self.mock(logging, 'error', lambda l, *_args: self.logs.append(l % _args))

    auth.bootstrap_ip_whitelist(auth.BOTS_IP_WHITELIST, ['1.2.3.4', '1.2.3.5'])
    auth.bootstrap_ip_whitelist('ip_whitelist', ['1.1.1.1', '1.2.3.4'])

    auth_testing.reset_local_state()

  def mock_config(self, cfg):
    def get_self_config_mock(path, cls, **_kwargs):
      self.assertEquals('bots.cfg', path)
      self.assertEquals(cls, bots_pb2.BotsCfg)
      return None, cfg
    self.mock(config, 'get_self_config', get_self_config_mock)
    utils.clear_cache(bot_groups_config._fetch_bot_groups)

  def mock_caller(self, ident, ip):
    self.mock(
        auth, 'get_peer_identity', lambda: auth.Identity.from_bytes(ident))
    self.mock(auth, 'get_peer_ip', lambda: ipaddr.ip_from_string(ip))

  def assert_error_log(self, msg):
    if not any(msg in m for m in self.logs):
      self.fail(
          'expected to see %r in the following log:\n%s' %
          (msg, '\n'.join(self.logs)))

  def test_default_config_not_ip_whitelisted(self):
    # With default config, callers not in 'bots' IP whitelist are rejected.
    self.mock_config(None)
    self.mock_caller('anonymous:anonymous', '1.1.1.1')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('some-bot')

  def test_default_config_ip_whitelisted(self):
    # With default config, callers in 'bots' IP whitelist are allowed.
    self.mock_config(None)
    self.mock_caller('anonymous:anonymous', '1.2.3.5')
    bot_auth.validate_bot_id_and_fetch_config('some-bot')

  def test_ip_whitelist_based_auth_ok(self):
    # Caller passes 'bots' IP whitelist and belongs to 'ip_whitelist'.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('anonymous:anonymous', '1.2.3.4')
    cfg = bot_auth.validate_bot_id_and_fetch_config('bot_with_ip_whitelist')
    self.assertEquals({u'pool': [u'with_ip_whitelist']}, cfg.dimensions)

  def test_unknown_bot_id(self):
    # Caller supplies bot_id not in the config.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('anonymous:anonymous', '1.2.3.4')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('unknown_bot_id')
    self.assert_error_log('unknown bot_id, not in the config')

  def test_machine_token_ok(self):
    # Caller is using valid machine token.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:bot_with_token.domain', '1.2.3.5')
    cfg = bot_auth.validate_bot_id_and_fetch_config('bot_with_token')
    self.assertEquals({u'pool': [u'with_token']}, cfg.dimensions)

  def test_machine_token_bad_id(self):
    # Caller is using machine token that doesn't match bot_id.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:some-other-bot.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('bot_with_token')
    self.assert_error_log('bot ID doesn\'t match the machine token used')
    self.assert_error_log('bot_id: "bot_with_token"')
    self.assert_error_log('original bot_id: "bot_with_token"')

  def test_machine_token_ip_whitelist_ok(self):
    # Caller is using valid machine token and belongs to the IP whitelist.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:bot_with_token_and_ip_whitelist.domain', '1.2.3.4')
    cfg = bot_auth.validate_bot_id_and_fetch_config(
        'bot_with_token_and_ip_whitelist')
    self.assertEquals(
        {u'pool': [u'with_token_and_ip_whitelist']}, cfg.dimensions)

  def test_machine_token_ip_whitelist_not_ok(self):
    # Caller is using valid machine token but doesn't belongs to the IP
    # whitelist.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:bot_with_token_and_ip_whitelist.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config(
          'bot_with_token_and_ip_whitelist')
    self.assert_error_log('bot IP is not whitelisted')

  def test_service_account_ok(self):
    # Caller is using valid service account.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('user:a@example.com', '1.2.3.5')
    cfg = bot_auth.validate_bot_id_and_fetch_config('bot_with_service_account')
    self.assertEquals({u'pool': [u'with_service_account']}, cfg.dimensions)

  def test_service_account_not_ok(self):
    # Caller is using wrong service account.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('user:b@example.com', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('bot_with_service_account')
    self.assert_error_log('bot is not using expected service account')

  def test_service_account_ip_whitelist_ok(self):
    # Caller is using valid service account and belongs to the IP whitelist.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('user:a@example.com', '1.2.3.4')
    cfg = bot_auth.validate_bot_id_and_fetch_config(
        'bot_with_service_account_and_ip_whitelist')
    self.assertEquals(
        {u'pool': [u'with_service_account_and_ip_whitelist']}, cfg.dimensions)

  def test_service_account_ip_whitelist_not_ok(self):
    # Caller is using valid service account and doesn't belong to the IP
    # whitelist.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('user:a@example.com', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config(
          'bot_with_service_account_and_ip_whitelist')
    self.assert_error_log('bot IP is not whitelisted')

  def test_broken_config_section(self):
    # This should not happen in practice, but test in case it somewhat happens.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('anonymous:anonymous', '1.2.3.4')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('broken_config')
    self.assert_error_log('invalid bot group config, no auth method defined')

  def test_composite_machine_token_ok(self):
    # Caller is using valid machine token.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:bot_with_token.domain', '1.2.3.5')
    cfg = bot_auth.validate_bot_id_and_fetch_config('bot_with_token--vm123')
    self.assertEquals({u'pool': [u'with_token']}, cfg.dimensions)

  def test_composite_machine_token_bad_id(self):
    # Caller is using machine token that doesn't match bot_id.
    self.mock_config(TEST_CONFIG)
    self.mock_caller('bot:some-other-bot.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.validate_bot_id_and_fetch_config('bot_with_token--vm123')
    self.assert_error_log('bot ID doesn\'t match the machine token used')
    self.assert_error_log('bot_id: "bot_with_token"')
    self.assert_error_log('original bot_id: "bot_with_token--vm123"')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
