#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import config
from components import utils
from components.config import validation
from test_support import test_case

from proto import bots_pb2
from server import bot_groups_config


TEST_CONFIG = bots_pb2.BotsCfg(
  trusted_dimensions=['pool'],
  bot_group=[
    bots_pb2.BotGroup(
      bot_id=['bot1', 'bot{2..3}'],
      auth=bots_pb2.BotAuth(require_luci_machine_token=True),
      owners=['owner@example.com'],
      dimensions=['pool:A', 'pool:B', 'other:D'],
    ),
    bots_pb2.BotGroup(
      bot_id=['other_bot'],
      bot_id_prefix=['bot'],
      auth=bots_pb2.BotAuth(require_service_account='a@example.com')),
    bots_pb2.BotGroup(
      auth=bots_pb2.BotAuth(ip_whitelist='bots'),
      dimensions=['pool:default']),
  ],
)


EXPECTED_GROUP_1 = bot_groups_config._make_bot_group_config(
    require_luci_machine_token=True,
    require_service_account=u'',
    ip_whitelist=u'',
    owners=(u'owner@example.com',),
    dimensions={u'pool': [u'A', u'B'], u'other': [u'D']})

EXPECTED_GROUP_2 = bot_groups_config._make_bot_group_config(
    require_luci_machine_token=False,
    require_service_account=u'a@example.com',
    ip_whitelist=u'',
    owners=(),
    dimensions={u'pool': []})

EXPECTED_GROUP_3 = bot_groups_config._make_bot_group_config(
    require_luci_machine_token=False,
    require_service_account=u'',
    ip_whitelist=u'bots',
    owners=(),
    dimensions={u'pool': [u'default']})


DEFAULT_AUTH_CFG = bots_pb2.BotAuth(ip_whitelist='bots')


class BotGroupsConfigTest(test_case.TestCase):
  def validator_test(self, cfg, messages):
    ctx = validation.Context()
    bot_groups_config.validate_settings(cfg, ctx)
    self.assertEquals(ctx.result().messages, [
      validation.Message(severity=logging.ERROR, text=m)
      for m in messages
    ])

  def mock_config(self, cfg):
    def get_self_config_mock(path, cls, **_kwargs):
      self.assertEquals('bots.cfg', path)
      self.assertEquals(cls, bots_pb2.BotsCfg)
      return None, cfg
    self.mock(config, 'get_self_config', get_self_config_mock)
    utils.clear_cache(bot_groups_config._fetch_bot_groups)

  def test_version(self):
    self.assertEqual('hash:4564ed4cc34544', EXPECTED_GROUP_1.version)
    self.assertEqual('hash:18206c33fffaa8', EXPECTED_GROUP_2.version)

  def test_expand_bot_id_expr_success(self):
    def check(expected, expr):
      self.assertEquals(
        expected, list(bot_groups_config._expand_bot_id_expr(expr)))
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

    self.assertEquals({
      'bot1': EXPECTED_GROUP_1,
      'bot2': EXPECTED_GROUP_1,
      'bot3': EXPECTED_GROUP_1,
      'other_bot': EXPECTED_GROUP_2,
    }, cfg.direct_matches)
    self.assertEquals([('bot', EXPECTED_GROUP_2)], cfg.prefix_matches)
    self.assertEquals(EXPECTED_GROUP_3, cfg.default_group)

  def test_get_bot_group_config(self):
    self.mock_config(TEST_CONFIG)
    self.assertEquals(
        EXPECTED_GROUP_1, bot_groups_config.get_bot_group_config('bot1'))
    self.assertEquals(
        EXPECTED_GROUP_2, bot_groups_config.get_bot_group_config('botzzz'))
    self.assertEquals(
        EXPECTED_GROUP_3, bot_groups_config.get_bot_group_config('unknown'))

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
      'trusted_dimensions: invalid dimension key - "pool:blah", '
      'must match ^[a-zA-Z\\-\\_\\.]+$'
    ])

  def test_bad_bot_id(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(bot_id=['blah{}'], auth=DEFAULT_AUTH_CFG),
      ])
    self.validator_test(cfg, [
      'bot_group #0: bad bot_id expression "blah{}" - Invalid set "", '
      'not a list and not a range'
    ])

  def test_bot_id_duplication(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(bot_id=['b{0..5}'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id=['b5'], auth=DEFAULT_AUTH_CFG),
      ])
    self.validator_test(cfg, [
      'bot_group #1: bot_id "b5" was already mentioned in group #0'
    ])

  def test_empty_prefix(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=[''], auth=DEFAULT_AUTH_CFG)
      ])
    self.validator_test(cfg, [
      'bot_group #0: empty bot_id_prefix is not allowed'
    ])

  def test_duplicate_prefixes(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
      ])
    self.validator_test(cfg, [
      'bot_group #1: bot_id_prefix "abc-" is already specified in group #0'
    ])

  def test_bad_auth_cfg_two_methods(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=bots_pb2.BotAuth(
          require_luci_machine_token=True,
          require_service_account='abc@example.com',
        ))
      ])
    self.validator_test(cfg, [
      'bot_group #0: require_luci_machine_token and require_service_account '
      'can\'t both be used at the same time'
    ])

  def test_bad_auth_cfg_no_ip_whitelist(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=bots_pb2.BotAuth())
      ])
    self.validator_test(cfg, [
      'bot_group #0: if both require_luci_machine_token and '
      'require_service_account are unset, ip_whitelist is required'
    ])

  def test_bad_required_service_account(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=bots_pb2.BotAuth(
          require_service_account='not-an-email'
        ))
      ])
    self.validator_test(cfg, [
      'bot_group #0: invalid service account email "not-an-email"'
    ])

  def test_bad_ip_whitelist_name(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=bots_pb2.BotAuth(
          ip_whitelist='bad ## name'
        ))
      ])
    self.validator_test(cfg, [
      'bot_group #0: invalid ip_whitelist name "bad ## name"'
    ])

  def test_bad_owners(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(
          bot_id=['blah'],
          auth=DEFAULT_AUTH_CFG,
          owners=['bad email']),
      ])
    self.validator_test(cfg, [
      'bot_group #0: invalid owner email "bad email"'
    ])

  def test_bad_dimension_not_kv(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(
          bot_id=['blah'],
          auth=DEFAULT_AUTH_CFG,
          dimensions=['not_kv_pair']),
      ])
    self.validator_test(cfg, [
      'bot_group #0: bad dimension "not_kv_pair", not a key:value pair'
    ])

  def test_bad_dimension_bad_dim_key(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(
          bot_id=['blah'],
          auth=DEFAULT_AUTH_CFG,
          dimensions=['blah####key:value:value']),
      ])
    self.validator_test(cfg, [
      'bot_group #0: bad dimension key in "blah####key:value:value", '
      'should match ^[a-zA-Z\\-\\_\\.]+$'
    ])

  def test_intersecting_prefixes(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(bot_id_prefix=['abc-'], auth=DEFAULT_AUTH_CFG),
        bots_pb2.BotGroup(bot_id_prefix=['abc-def-'], auth=DEFAULT_AUTH_CFG),
      ])
    self.validator_test(cfg, [
      'bot_id_prefix "abc-", defined in group #0, is subprefix of "abc-def-", '
      'defined in group #1; it makes group assigned for bots with prefix '
      '"abc-def-" ambigious'
    ])

  def test_machine_types(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [])

  def test_machine_type_name_unspecified(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: name is required'
    ])

  def test_machine_type_name_reused_same_bot_group(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=1),
          bots_pb2.MachineType(name='abc', lease_duration_secs=456,
                               mp_dimensions=['key:value'], target_size=2),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #1: reusing name "abc"'
    ])

  def test_machine_type_name_reused_different_bot_group(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=1),
        ]),
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=456,
                               mp_dimensions=['key:value'], target_size=2),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #1: machine_type #0: reusing name "abc"'
    ])

  def test_machine_type_lease_duration_secs_unspecified(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc',
                               mp_dimensions=['key:value'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: lease_duration_secs is required'
    ])

  def test_machine_type_lease_duration_secs_zero(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=0,
                               mp_dimensions=['key:value'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: lease_duration_secs is required'
    ])

  def test_machine_type_lease_duration_secs_negative(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=-123,
                               mp_dimensions=['key:value'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: lease_duration_secs must be positive'
    ])

  def test_machine_type_mp_dimensions_unspecified(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=[], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: at least one dimension is required'
    ])

  def test_machine_type_mp_dimensions_wrong_format(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key'], target_size=1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: mp_dimensions #0:'
      ' bad dimension "key", not a key:value pair'
    ])

  def test_machine_type_target_size_unspecified(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value']),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: target_size is required'
    ])

  def test_machine_type_target_size_zero(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=0),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: target_size is required'
    ])

  def test_machine_type_target_size_negative(self):
    cfg = bots_pb2.BotsCfg(
      bot_group=[
        bots_pb2.BotGroup(auth=DEFAULT_AUTH_CFG, machine_type=[
          bots_pb2.MachineType(name='abc', lease_duration_secs=123,
                               mp_dimensions=['key:value'], target_size=-1),
        ]),
    ])
    self.validator_test(cfg, [
      'bot_group #0: machine_type #0: target_size must be positive'
    ])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
