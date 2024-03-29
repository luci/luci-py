#!/usr/bin/env vpython
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import copy
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import auth
from components import auth_testing
from components import config
from components.auth import ipaddr
from test_support import test_case

from proto.config import bots_pb2
from server import bot_auth
from server import bot_groups_config
from server import config as srv_cfg


TEST_CONFIG = bots_pb2.BotsCfg(
  trusted_dimensions=['pool'],
  bot_group=[
    bots_pb2.BotGroup(
      bot_id=['bot_with_token'],
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:with_token']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_service_account'],
      auth=[
        bots_pb2.BotAuth(require_service_account=[
          'a@example.com',
          'x@example.com',
        ]),
      ],
      dimensions=['pool:with_service_account']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_ip_whitelist'],
      auth=[bots_pb2.BotAuth(ip_whitelist='ip_whitelist')],
      dimensions=['pool:with_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_gce_token'],
      auth=[
        bots_pb2.BotAuth(
          require_gce_vm_token=bots_pb2.BotAuth.GCE(project='expected_proj'),
        ),
      ],
      dimensions=['pool:bot_with_gce_token']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_service_account_and_ip_whitelist'],
      auth=[
        bots_pb2.BotAuth(
          require_service_account=['a@example.com'],
          ip_whitelist='ip_whitelist',
        ),
      ],
      dimensions=['pool:with_service_account_and_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_token_and_ip_whitelist'],
      auth=[
        bots_pb2.BotAuth(
          require_luci_machine_token=True,
          ip_whitelist='ip_whitelist'),
      ],
      dimensions=['pool:with_token_and_ip_whitelist']),
    bots_pb2.BotGroup(
      bot_id=['bot_with_fallback_to_ip_wl'],
      auth=[
        bots_pb2.BotAuth(require_luci_machine_token=True, log_if_failed=True),
        bots_pb2.BotAuth(ip_whitelist='ip_whitelist'),
      ],
      dimensions=['pool:with_fallback_to_ip_wl']),
    bots_pb2.BotGroup(
      bot_id=['bot_host--container1'],
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:container1']),
    bots_pb2.BotGroup(
      bot_id=['bot_host--container2'],
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:container2']),
    bots_pb2.BotGroup(
      bot_id=['bot_host--container{3..4}'],
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:container_range']),
    bots_pb2.BotGroup(
      bot_id=['bot_host'],
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:bot_host']),
    bots_pb2.BotGroup(
      auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
      dimensions=['pool:unassigned']),
  ],
)


class BotAuthTest(test_case.TestCase):
  def setUp(self):
    super(BotAuthTest, self).setUp()

    # Mock this out, otherwise it tries to fetch settings.cfg.
    self.mock(srv_cfg, 'get_ui_client_id', lambda: None)

    self.logs = []
    self.mock(logging, 'error', lambda l, *_args: self.logs.append(l % _args))

    auth.bootstrap_ip_whitelist(
        auth.bots_ip_whitelist(), ['1.2.3.4', '1.2.3.5'])
    auth.bootstrap_ip_whitelist('ip_whitelist', ['1.1.1.1', '1.2.3.4'])

    auth_testing.reset_local_state()
    self.mock_config(TEST_CONFIG)

  def mock_config(self, cfg):
    def get_self_config_mock(path, cls, **_kwargs):
      self.assertEquals('bots.cfg', path)
      self.assertEquals(cls, bots_pb2.BotsCfg)
      return 'rev', cfg
    self.mock(config, 'get_self_config', get_self_config_mock)
    bot_groups_config.clear_cache()

  def mock_caller(self, ident, ip, gce_instance=None, gce_project=None):
    self.mock(
        auth, 'get_peer_identity', lambda: auth.Identity.from_bytes(ident))
    self.mock(auth, 'get_peer_ip', lambda: ipaddr.ip_from_string(ip))
    self.mock(
        auth, 'get_auth_details',
        lambda: auth.new_auth_details(
            gce_instance=gce_instance, gce_project=gce_project))

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
      bot_auth.authenticate_bot('some-bot')

  def test_default_config_ip_whitelisted(self):
    # With default config, callers in 'bots' IP whitelist are allowed.
    self.mock_config(None)
    self.mock_caller('anonymous:anonymous', '1.2.3.5')
    bot_auth.authenticate_bot('some-bot')

  def test_ip_whitelist_based_auth_ok(self):
    # Caller passes 'bots' IP whitelist and belongs to 'ip_whitelist'.
    self.mock_caller('anonymous:anonymous', '1.2.3.4')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_ip_whitelist')
    self.assertEquals({u'pool': [u'with_ip_whitelist']}, group_cfg.dimensions)
    self.assertEquals('ip_whitelist', auth_cfg.ip_whitelist)

  def test_unknown_bot_id(self):
    # Prepare bots.cfg with no default group
    cfg_without_default = copy.copy(TEST_CONFIG)
    cfg_without_default.bot_group.pop() # last one is default

    # Caller supplies bot_id not in the config.
    self.mock_config(cfg_without_default)
    self.mock_caller('anonymous:anonymous', '1.2.3.4')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('unknown_bot_id')
    self.assert_error_log('unknown bot_id, not in the config')

  def test_machine_token_ok(self):
    # Caller is using valid machine token.
    self.mock_caller('bot:bot_with_token.domain', '1.2.3.5')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_token')
    self.assertEquals({u'pool': [u'with_token']}, group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_luci_machine_token)

  def test_machine_token_bad_id(self):
    # Caller is using machine token that doesn't match bot_id.
    self.mock_caller('bot:some-other-bot.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_token')
    self.assert_error_log('bot ID doesn\'t match the machine token used')
    self.assert_error_log('bot_id: "bot_with_token"')

  def test_machine_token_ip_whitelist_ok(self):
    # Caller is using valid machine token and belongs to the IP whitelist.
    self.mock_caller('bot:bot_with_token_and_ip_whitelist.domain', '1.2.3.4')
    group_cfg, auth_cfg = bot_auth.authenticate_bot(
        'bot_with_token_and_ip_whitelist')
    self.assertEquals({u'pool': [u'with_token_and_ip_whitelist']},
                      group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_luci_machine_token)
    self.assertEquals('ip_whitelist', auth_cfg.ip_whitelist)

  def test_machine_token_ip_whitelist_not_ok(self):
    # Caller is using valid machine token but doesn't belongs to the IP
    # whitelist.
    self.mock_caller('bot:bot_with_token_and_ip_whitelist.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_token_and_ip_whitelist')
    self.assert_error_log('IP not allowed')

  def test_service_account_ok(self):
    # Caller is using valid service account.
    self.mock_caller('user:a@example.com', '1.2.3.5')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_service_account')
    self.assertEquals({u'pool': [u'with_service_account']},
                      group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_service_account)

  def test_alternative_service_account_ok(self):
    # Caller is using the second service account.
    self.mock_caller('user:x@example.com', '1.2.3.5')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_service_account')
    self.assertEquals({u'pool': [u'with_service_account']},
                      group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_service_account)

  def test_service_account_not_ok(self):
    # Caller is using wrong service account.
    self.mock_caller('user:b@example.com', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_service_account')
    self.assert_error_log('bot is not using expected service account')

  def test_service_account_ip_whitelist_ok(self):
    # Caller is using valid service account and belongs to the IP whitelist.
    self.mock_caller('user:a@example.com', '1.2.3.4')
    group_cfg, auth_cfg = bot_auth.authenticate_bot(
        'bot_with_service_account_and_ip_whitelist')
    self.assertEquals({u'pool': [u'with_service_account_and_ip_whitelist']},
                      group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_service_account)
    self.assertEquals('ip_whitelist', auth_cfg.ip_whitelist)

  def test_service_account_ip_whitelist_not_ok(self):
    # Caller is using valid service account and doesn't belong to the IP
    # whitelist.
    self.mock_caller('user:a@example.com', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_service_account_and_ip_whitelist')
    self.assert_error_log('IP not allowed')

  def test_gce_token_ok(self):
    self.mock_caller(
        'bot:irrelevant', '1.2.3.4',
        gce_instance='bot_with_gce_token',
        gce_project='expected_proj')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_gce_token')
    self.assertEquals({u'pool': [u'bot_with_gce_token']}, group_cfg.dimensions)
    self.assertEquals(bot_groups_config.BotAuthGCE(project=u'expected_proj'),
                      auth_cfg.require_gce_vm_token)

  def test_gce_token_not_present(self):
    self.mock_caller(
        'bot:irrelevant', '1.2.3.4',
        gce_instance=None,
        gce_project=None)
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_gce_token')
    self.assert_error_log('bot is not using X-Luci-Gce-Vm-Token')

  def test_gce_token_wrong_project(self):
    self.mock_caller(
        'bot:irrelevant', '1.2.3.4',
        gce_instance='bot_with_gce_token',
        gce_project='not_expected')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_gce_token')
    self.assert_error_log('got GCE VM token from unexpected project')

  def test_gce_token_wrong_instance(self):
    self.mock_caller(
        'bot:irrelevant', '1.2.3.4',
        gce_instance='wrong_instance',
        gce_project='expected_proj')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_gce_token')
    self.assert_error_log('bot ID and GCE instance name do not match')

  def test_composite_machine_token_ok(self):
    # Caller is using valid machine token.
    self.mock_caller('bot:bot_with_token.domain', '1.2.3.5')
    group_cfg, auth_cfg = bot_auth.authenticate_bot('bot_with_token--vm123')
    self.assertEquals({u'pool': [u'with_token']}, group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_luci_machine_token)

  def test_composite_machine_token_bad_id(self):
    # Caller is using machine token that doesn't match bot_id.
    self.mock_caller('bot:some-other-bot.domain', '1.2.3.5')
    with self.assertRaises(auth.AuthorizationError):
      bot_auth.authenticate_bot('bot_with_token--vm123')
    self.assert_error_log('bot ID doesn\'t match the machine token used')
    self.assert_error_log('bot_id: "bot_with_token"')

  def test_containerized_bot_id(self):
    # Caller is using machine token that matches hostname
    self.mock_caller('bot:bot_host.domain', '1.2.3.5')
    group_cfg, _ = bot_auth.authenticate_bot('bot_host--container1')
    self.assertEquals({u'pool': [u'container1']}, group_cfg.dimensions)
    group_cfg, _ = bot_auth.authenticate_bot('bot_host--container2')
    self.assertEquals({u'pool': [u'container2']}, group_cfg.dimensions)
    group_cfg, _ = bot_auth.authenticate_bot('bot_host--container3')
    self.assertEquals({u'pool': [u'container_range']}, group_cfg.dimensions)
    group_cfg, _ = bot_auth.authenticate_bot('bot_host--container99')
    self.assertEquals({u'pool': [u'bot_host']}, group_cfg.dimensions)

  def test_first_method_is_used(self):
    self.mock_caller('bot:bot_with_fallback_to_ip_wl.domain', '2.2.2.2')
    group_cfg, auth_cfg = bot_auth.authenticate_bot(
        'bot_with_fallback_to_ip_wl')
    self.assertEquals({u'pool': [u'with_fallback_to_ip_wl']},
                      group_cfg.dimensions)
    self.assertTrue(auth_cfg.require_luci_machine_token)
    self.assertFalse(auth_cfg.ip_whitelist)

  def test_fallback_to_another_method(self):
    self.mock_caller('anonymous:anonymous', '1.1.1.1')
    group_cfg, auth_cfg = bot_auth.authenticate_bot(
        'bot_with_fallback_to_ip_wl')
    self.assertEquals({u'pool': [u'with_fallback_to_ip_wl']},
                      group_cfg.dimensions)
    self.assertFalse(auth_cfg.require_luci_machine_token)
    self.assertTrue(auth_cfg.ip_whitelist)

  def test_multiple_methods_fail(self):
    self.mock_caller('anonymous:anonymous', '2.2.2.2')
    with self.assertRaises(auth.AuthorizationError) as err:
      bot_auth.authenticate_bot('bot_with_fallback_to_ip_wl')
    self.assertEqual(
        "All auth methods failed: Bot ID doesn't match the token used; "
        "IP not allowed", err.exception.message)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
