#!/usr/bin/env vpython
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import config
from components.config import validation
from test_support import test_case

from proto.config import bots_pb2
from server import bot_groups_config


TEST_CONFIG = bots_pb2.BotsCfg(
    trusted_dimensions=['pool'],
    bot_group=[
        bots_pb2.BotGroup(
            bot_id=['bot1', 'bot{2..3}'],
            auth=[
                bots_pb2.BotAuth(require_luci_machine_token=True),
                bots_pb2.BotAuth(require_service_account=['z@example.com']),
                bots_pb2.BotAuth(require_gce_vm_token=bots_pb2.BotAuth.GCE(
                    project='proj'), ),
            ],
            owners=['owner@example.com'],
            dimensions=['pool:A', 'pool:B', 'other:D'],
            logs_cloud_project='google.com:chromecompute',
        ),
        # This group includes an injected bot_config and system_service_account.
        bots_pb2.BotGroup(
            bot_id=['other_bot', 'bot1--xxx'],
            bot_id_prefix=['bot'],
            auth=[bots_pb2.BotAuth(require_service_account=['a@example.com'])],
            bot_config_script='foo.py',
            system_service_account='bot',
            logs_cloud_project='chrome-infra-logs'),
        bots_pb2.BotGroup(auth=[bots_pb2.BotAuth(ip_whitelist='bots')],
                          dimensions=['pool:default']),
    ],
)

EXPECTED_GROUP_1 = bot_groups_config._make_bot_group_config(
    owners=(u'owner@example.com', ),
    auth=(
        bot_groups_config.BotAuth(
            log_if_failed=False,
            require_luci_machine_token=True,
            require_service_account=(),
            require_gce_vm_token=None,
            ip_whitelist=u'',
        ),
        bot_groups_config.BotAuth(
            log_if_failed=False,
            require_luci_machine_token=False,
            require_service_account=('z@example.com', ),
            require_gce_vm_token=None,
            ip_whitelist=u'',
        ),
        bot_groups_config.BotAuth(
            log_if_failed=False,
            require_luci_machine_token=False,
            require_service_account=(),
            require_gce_vm_token=bot_groups_config.BotAuthGCE('proj'),
            ip_whitelist=u'',
        ),
    ),
    dimensions={
        u'pool': [u'A', u'B'],
        u'other': [u'D']
    },
    bot_config_script='',
    bot_config_script_rev='',
    bot_config_script_content='',
    bot_config_script_sha256='',
    system_service_account='',
    logs_cloud_project='google.com:chromecompute',
    is_default=False)

EXPECTED_GROUP_2 = bot_groups_config._make_bot_group_config(
    owners=(),
    auth=(bot_groups_config.BotAuth(
        log_if_failed=False,
        require_luci_machine_token=False,
        require_service_account=(u'a@example.com', ),
        require_gce_vm_token=None,
        ip_whitelist=u'',
    ), ),
    dimensions={u'pool': []},
    bot_config_script='foo.py',
    bot_config_script_rev='',
    bot_config_script_content='print("Hi")',
    bot_config_script_sha256=
    '566238a1eb9839809ff20c120387d91042c3efce7d7f30d16470caec93740e1b',
    system_service_account='bot',
    logs_cloud_project='chrome-infra-logs',
    is_default=False)

EXPECTED_GROUP_3 = bot_groups_config._make_bot_group_config(
    owners=(),
    auth=(bot_groups_config.BotAuth(
        log_if_failed=False,
        require_luci_machine_token=False,
        require_service_account=(),
        require_gce_vm_token=None,
        ip_whitelist=u'bots',
    ), ),
    dimensions={u'pool': [u'default']},
    bot_config_script='',
    bot_config_script_rev='',
    bot_config_script_content='',
    bot_config_script_sha256='',
    system_service_account='',
    logs_cloud_project=None,
    is_default=True)


DEFAULT_AUTH_CFG = [bots_pb2.BotAuth(ip_whitelist='bots')]


class ValidationCtx(validation.Context):

  def assert_errors(self, test, messages):
    test.assertEquals(
        self.result().messages,
        [validation.Message(severity=logging.ERROR, text=m) for m in messages])


class BotGroupsConfigTest(test_case.TestCase):
  def validator_test(self, cfg, messages):
    ctx = ValidationCtx()
    bot_groups_config._validate_bots_cfg(cfg, ctx)
    ctx.assert_errors(self, messages)

  def mock_config(self, cfg):
    def get_self_config_mock(path, cls=None, **kwargs):
      self.assertEqual({'store_last_good': True}, kwargs)
      if path == 'bots.cfg':
        self.assertEqual(cls, bots_pb2.BotsCfg)
        return '123', cfg
      self.assertEqual('scripts/foo.py', path)
      return '123', 'print("Hi")'

    self.mock(config, 'get_self_config', get_self_config_mock)
    bot_groups_config.clear_cache()

  def test_version(self):
    self.assertEqual('hash:7d27288175e223', EXPECTED_GROUP_1.version)
    self.assertEqual('hash:2208a8f7c5f4aa', EXPECTED_GROUP_2.version)
    self.assertEqual('hash:035053f35deb41', EXPECTED_GROUP_3.version)

  def test_expand_bot_id_expr_success(self):

    def check(expected, expr):
      self.assertEquals(expected,
                        list(bot_groups_config._expand_bot_id_expr(expr)))

    check(['abc'], 'abc')
    check(['abc1def', 'abc2def'], 'abc{1,2}def')
    check(['abc1def', 'abc2def', 'abc3def'], 'abc{1..3}def')

  def test_expand_bot_id_expr_fail(self):
    def check_fail(expr):
      with self.assertRaises(ValueError):
        list(bot_groups_config._expand_bot_id_expr(expr))
    check_fail('')
    check_fail('abc{ab}def')
    check_fail('abc{..}def')
    check_fail('abc{a..b}def')
    check_fail('abc{1,2,3..10}def')
    check_fail('abc{')
    check_fail('abc{1')
    check_fail('}def')
    check_fail('1}def')
    check_fail('abc{1..}')
    check_fail('abc{..2}')

  def test_fetch_bot_groups(self):
    self.mock_config(TEST_CONFIG)
    cfg = bot_groups_config._fetch_bot_groups()

    self.assertEquals(
        {
            u'bot1': EXPECTED_GROUP_1,
            u'bot1--xxx': EXPECTED_GROUP_2,
            u'bot2': EXPECTED_GROUP_1,
            u'bot3': EXPECTED_GROUP_1,
            u'other_bot': EXPECTED_GROUP_2,
        }, cfg.direct_matches)
    self.assertEquals([('bot', EXPECTED_GROUP_2)], cfg.prefix_matches)
    self.assertEquals(EXPECTED_GROUP_3, cfg.default_group)

  def test_get_bot_group_config(self):
    self.mock_config(TEST_CONFIG)
    self.assertEquals(
        EXPECTED_GROUP_1, bot_groups_config.get_bot_group_config('bot1'))
    self.assertEquals(EXPECTED_GROUP_1,
                      bot_groups_config.get_bot_group_config('bot1--zzz'))
    self.assertEquals(EXPECTED_GROUP_2,
                      bot_groups_config.get_bot_group_config('bot1--xxx'))
    self.assertEquals(EXPECTED_GROUP_3,
                      bot_groups_config.get_bot_group_config('?'))

  def test_empty_config_is_valid(self):
    self.validator_test(bots_pb2.BotsCfg(), [])

  def test_good_config_is_valid(self):
    self.validator_test(TEST_CONFIG, [])

  def test_trusted_dimensions_valid(self):
    cfg = bots_pb2.BotsCfg(trusted_dimensions=['pool', 'project'])
    self.validator_test(cfg, [])

  def test_trusted_dimensions_invalid(self):
    cfg = bots_pb2.BotsCfg(trusted_dimensions=['pool:blah'])
    self.validator_test(cfg, [
      u'trusted_dimensions: invalid dimension key u\'pool:blah\''
    ])

  def test_bad_bot_id(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(bot_id=['blah{}'], auth=DEFAULT_AUTH_CFG),
    ])
    self.validator_test(cfg, [
        'bot_group #0: bad bot_id expression "blah{}" - Invalid set "", '
        'not a list and not a range'
    ])

  def test_bot_id_duplication(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(bot_id=['b{0..5}'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id=['b5'], auth=DEFAULT_AUTH_CFG),
    ])
    self.validator_test(
        cfg, ['bot_group #1: bot_id "b5" was already mentioned in group #0'])

  def test_empty_prefix(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=[''], auth=DEFAULT_AUTH_CFG)
    ])
    self.validator_test(cfg,
                        ['bot_group #0: empty bot_id_prefix is not allowed'])

  def test_duplicate_prefixes(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
    ])
    self.validator_test(
        cfg,
        ['bot_group #1: bot_id_prefix "abc-" is already specified in group #0'])

  def test_duplicate_bot_and_prefix_ids(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(
            bot_id=['abc', 'ok'],
            bot_id_prefix=['xyz'],
            auth=DEFAULT_AUTH_CFG,
            dimensions=['g:first']),
        bots_pb2.BotGroup(
            bot_id=['xyz'],
            bot_id_prefix=['abc', 'ok-'],
            auth=DEFAULT_AUTH_CFG,
            dimensions=['g:second']),
        bots_pb2.BotGroup(
            bot_id=['foo'],
            bot_id_prefix=['foo'],
            auth=DEFAULT_AUTH_CFG,
            dimensions=['g:third']),
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, dimensions=['g:default']),
    ])
    self.validator_test(cfg, [
        (u'bot_group #1: bot_id "xyz" was already mentioned as bot_id_prefix '
         'in group #0'),
        (u'bot_group #1: bot_id_prefix "abc" is already specified as bot_id '
         'in group #0'),
        (u'bot_group #2: bot_id_prefix "foo" is already specified as bot_id '
         'in group #2'),
    ])

  def test_bad_auth_completely_missing(self):
    cfg = bots_pb2.BotsCfg(bot_group=[bots_pb2.BotGroup()])
    self.validator_test(cfg, ['bot_group #0: an "auth" entry is required'])

  def test_bad_auth_cfg_two_methods(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=[
            bots_pb2.BotAuth(
                require_luci_machine_token=True,
                require_service_account=['abc@example.com']),
        ])
    ])
    self.validator_test(cfg, [
        'bot_group #0: require_luci_machine_token and require_service_account '
        'can\'t be used at the same time'
    ])

  def test_bad_auth_cfg_three_methods(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=[
            bots_pb2.BotAuth(
                require_luci_machine_token=True,
                require_service_account=['abc@example.com'],
                require_gce_vm_token=bots_pb2.BotAuth.GCE(project='zzz')),
        ])
    ])
    self.validator_test(cfg, [
        'bot_group #0: require_luci_machine_token and require_service_account '
        'and require_gce_vm_token can\'t be used at the same time'
    ])

  def test_bad_auth_cfg_no_ip_whitelist(self):
    cfg = bots_pb2.BotsCfg(
        bot_group=[bots_pb2.BotGroup(auth=[bots_pb2.BotAuth()])])
    self.validator_test(cfg, [
        'bot_group #0: if all auth requirements are unset, '
        'ip_whitelist must be set'
    ])

  def test_bad_required_service_account(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=[
            bots_pb2.BotAuth(require_service_account=['not-an-email']),
        ])
    ])
    self.validator_test(
        cfg, ['bot_group #0: invalid service account email "not-an-email"'])

  def test_bad_require_gce_vm_token_no_proj(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=[
            bots_pb2.BotAuth(require_gce_vm_token=bots_pb2.BotAuth.GCE()),
        ])
    ])
    self.validator_test(
        cfg, ['bot_group #0: missing project in require_gce_vm_token'])

  def test_bad_ip_whitelist_name(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=[
            bots_pb2.BotAuth(ip_whitelist='bad ## name'),
        ])
    ])
    self.validator_test(
        cfg, ['bot_group #0: invalid ip_whitelist name "bad ## name"'])

  def test_bad_owners(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(
            bot_id=['blah'], auth=DEFAULT_AUTH_CFG, owners=['bad email']),
    ])
    self.validator_test(cfg, ['bot_group #0: invalid owner email "bad email"'])

  def test_bad_dimension_not_kv(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(
            bot_id=['blah'], auth=DEFAULT_AUTH_CFG, dimensions=['not_kv_pair']),
    ])
    self.validator_test(cfg, [u'bot_group #0: bad dimension u\'not_kv_pair\''])

  def test_bad_dimension_bad_dim_key(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(
            bot_id=['blah'],
            auth=DEFAULT_AUTH_CFG,
            dimensions=['blah####key:value:value']),
    ])
    self.validator_test(cfg, [
        u'bot_group #0: bad dimension u\'blah####key:value:value\'',
    ])

  def test_intersecting_prefixes(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['abc-def-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['xyz-def-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['xyz-'], auth=DEFAULT_AUTH_CFG),
    ])
    self.validator_test(cfg, [
        (u'bot_group #1: bot_id_prefix "abc-def-" contains prefix "abc-", '
         'defined in group #0, making group assigned for bots with prefix '
         '"abc-" ambigious'),
        (u'bot_group #3: bot_id_prefix "xyz-" is subprefix of "xyz-def-", '
         'defined in group #2, making group assigned for bots with prefix '
         '"xyz-" ambigious'),
    ])

  def test_two_default_groups(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG),
    ])
    self.validator_test(cfg,
                        [u'bot_group #1: group #0 is already set as default'])

  def test_system_service_account_bad_email(self):
    cfg = bots_pb2.BotsCfg(bot_group=[
        bots_pb2.BotGroup(
            bot_id=['blah'],
            auth=DEFAULT_AUTH_CFG,
            system_service_account='bad email'),
    ])
    self.validator_test(cfg, [
      'bot_group #0: invalid system service account email "bad email"'
    ])

  def test_system_service_account_bot_on_non_oauth_machine(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(
          bot_id=['blah'],
          auth=[bots_pb2.BotAuth(ip_whitelist='bots')],
          system_service_account='bot'),
      ])
    self.validator_test(cfg, [
      'bot_group #0: system_service_account "bot" requires '
      'auth.require_service_account to be used'
    ])

  def test_system_service_account_bot_on_oauth_machine(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(
          bot_id=['blah'],
          auth=[bots_pb2.BotAuth(require_service_account=['blah@example.com'])],
          system_service_account='bot'),
      ])
    self.validator_test(cfg, [])


class CacheTest(test_case.TestCase):
  # This test needs be run independently
  # run by sequential_test_runner.py
  no_run = 1

  def setUp(self):
    super(CacheTest, self).setUp()
    bot_groups_config.clear_cache()
    self.epoch = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(self.epoch)

  def mock_config(self, cfg):
    calls = {}
    def get_self_config_mock(path, cls=None, **kwargs):
      calls[path] = calls.get(path, 0) + 1
      self.assertEqual({'store_last_good': True}, kwargs)
      if path not in cfg:
        return None, None
      rev, value = cfg[path]
      if cls:
        self.assertIsInstance(value, cls)
      return rev, value
    self.mock(config, 'get_self_config', get_self_config_mock)
    return calls

  def cached_config_entity(self):
    head = bot_groups_config._bots_cfg_head_key().get()
    body = bot_groups_config._bots_cfg_body_key().get()
    if not head:
      self.assertIsNone(body)
      return None
    self.assertIsNotNone(body)
    # Body's fields must match corresponding head's fields.
    for k in head._properties:
      self.assertEqual(getattr(head, k), getattr(body, k))
    return head

  def test_fetch_bot_groups_cache(self):
    # Empty config initially. Gets cached in memory.
    cfg = bot_groups_config._fetch_bot_groups()
    self.assertEqual('none',  cfg.rev)

    # New config becomes available.
    self.mock_config({'bots.cfg': ('rev1', TEST_CONFIG)})
    bot_groups_config.refetch_from_config_service(None)

    # Still using the empty config from the cache.
    cfg = bot_groups_config._fetch_bot_groups()
    self.assertEqual('none',  cfg.rev)

    # Until the cache expires.
    self.mock_now(self.epoch + datetime.timedelta(hours=1))
    cfg = bot_groups_config._fetch_bot_groups()
    self.assertEqual('rev1',  cfg.rev)

  def test_refetch_from_config_service_empty(self):
    self.mock_config({})
    ctx = validation.Context.raise_on_error()

    # Importing empty config.
    cfg = bot_groups_config.refetch_from_config_service(ctx)
    self.assertIsNone(cfg)

    cached = self.cached_config_entity()
    self.assertTrue(cached.empty)
    self.assertEqual(self.epoch, cached.last_update_ts)

    # Reimporting empty config.
    self.mock_now(self.epoch + datetime.timedelta(hours=12))
    cfg = bot_groups_config.refetch_from_config_service(ctx)
    self.assertIsNone(cfg)

    # No changes in the datastore (same last_update_ts).
    cached = self.cached_config_entity()
    self.assertTrue(cached.empty)
    self.assertEqual(self.epoch, cached.last_update_ts)

  def test_refetch_from_config_service_non_empty(self):
    self.mock_config({'bots.cfg': ('rev1', TEST_CONFIG)})
    ctx = validation.Context.raise_on_error()

    # Importing non-empty config.
    cfg = bot_groups_config.refetch_from_config_service(ctx)
    self.assertEqual(TEST_CONFIG, cfg.bots)
    self.assertEqual('rev1', cfg.rev)
    self.assertTrue(cfg.digest)

    cached = self.cached_config_entity()
    self.assertFalse(cached.empty)
    self.assertEqual('rev1', cached.bots_cfg_rev)
    self.assertEqual(cfg.digest, cached.digest)
    self.assertEqual(self.epoch, cached.last_update_ts)

    # Some time later reimporting exact same config => no changes.
    self.mock_now(self.epoch + datetime.timedelta(hours=12))
    cfg = bot_groups_config.refetch_from_config_service(ctx)
    self.assertEqual('rev1', cfg.rev)

    # Same last_update_ts, indicating datastore wasn't touched.
    cached = self.cached_config_entity()
    self.assertEqual('rev1', cached.bots_cfg_rev)
    self.assertEqual(self.epoch, cached.last_update_ts)

    # Now the config changes, it gets reimported.
    self.mock_config({'bots.cfg': ('rev2', TEST_CONFIG)})

    prev_digest = cfg.digest
    cfg = bot_groups_config.refetch_from_config_service(ctx)
    self.assertEqual('rev2', cfg.rev)
    self.assertTrue(cfg.digest != prev_digest)

    # New config is stored.
    cached = self.cached_config_entity()
    self.assertEqual('rev2', cached.bots_cfg_rev)
    self.assertEqual(
        self.epoch + datetime.timedelta(hours=12), cached.last_update_ts)

  def test_get_expanded_bots_cfg_empty(self):
    self.mock_config({})
    cfg = bot_groups_config.refetch_from_config_service()
    self.assertIsNone(cfg)

    # Read from the cache.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg()
    self.assertTrue(fetched)
    self.assertIsNone(cfg)

  def test_get_expanded_bots_cfg_non_empty(self):
    self.mock_config({'bots.cfg': ('rev1', TEST_CONFIG)})
    cfg = bot_groups_config.refetch_from_config_service()
    self.assertIsNotNone(cfg)

    # Read from the cache.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg()
    self.assertTrue(fetched)
    self.assertEqual(TEST_CONFIG, cfg.bots)

    # Rereading, but providing known_digest.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg(cfg.digest)
    self.assertFalse(fetched)

  def test_get_expanded_bots_cfg_changing(self):
    self.mock_config({'bots.cfg': ('rev1', TEST_CONFIG)})
    bot_groups_config.refetch_from_config_service()

    # Read from the cache.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg()
    self.assertTrue(fetched)
    self.assertEqual('rev1', cfg.rev)

    # Now the cache is changing.
    self.mock_config({'bots.cfg': ('rev2', TEST_CONFIG)})
    bot_groups_config.refetch_from_config_service()

    # Reading new value.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg(cfg.digest)
    self.assertTrue(fetched)
    self.assertEqual('rev2', cfg.rev)

  def test_get_expanded_bots_cfg_bootstrap_empty(self):
    self.mock_config({})

    # No cache.
    self.assertIsNone(self.cached_config_entity())

    # Bootstraps empty cache value.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg()
    self.assertTrue(fetched)
    self.assertIsNone(cfg)

    cached = self.cached_config_entity()
    self.assertTrue(cached.empty)

  def test_get_expanded_bots_cfg_bootstrap_non_empty(self):
    self.mock_config({'bots.cfg': ('rev1', TEST_CONFIG)})

    # No cache.
    self.assertIsNone(self.cached_config_entity())

    # Bootstraps cache value.
    fetched, cfg = bot_groups_config._get_expanded_bots_cfg()
    self.assertTrue(fetched)
    self.assertEqual(TEST_CONFIG, cfg.bots)

    cached = self.cached_config_entity()
    self.assertFalse(cached.empty)
    self.assertEqual('rev1', cached.bots_cfg_rev)

  def test_expands_bot_config_scripts_ok(self):
    good_script = "# coding=utf-8\nprint('Hello')\n"

    calls = self.mock_config({
        'bots.cfg': (
            'rev1',
            bots_pb2.BotsCfg(
                bot_group=[
                    bots_pb2.BotGroup(
                        bot_id=['bot1'],
                        auth=[
                            bots_pb2.BotAuth(require_luci_machine_token=True)
                        ],
                        bot_config_script='script.py',
                    ),
                    bots_pb2.BotGroup(
                        bot_id=['bot2'],
                        auth=[
                            bots_pb2.BotAuth(require_luci_machine_token=True)
                        ],
                        bot_config_script='script.py',
                    ),
                ],)),
        'scripts/script.py': ('rev2', good_script),
    })

    # Has 'bot_config_script_content' populated.
    cfg = bot_groups_config.refetch_from_config_service()
    self.assertEqual(
        bots_pb2.BotsCfg(
            bot_group=[
                bots_pb2.BotGroup(
                    bot_id=['bot1'],
                    auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
                    bot_config_script='script.py',
                    bot_config_script_rev='rev2',
                    bot_config_script_content=good_script,
                ),
                bots_pb2.BotGroup(
                    bot_id=['bot2'],
                    auth=[bots_pb2.BotAuth(require_luci_machine_token=True)],
                    bot_config_script='script.py',
                    bot_config_script_rev='rev2',
                    bot_config_script_content=good_script,
                ),
            ],), cfg.bots)

    # The script was fetched only once.
    self.assertEqual({'bots.cfg': 1, u'scripts/script.py': 1}, calls)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
