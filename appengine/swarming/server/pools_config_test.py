#!/usr/bin/env python
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components import auth
from components import config
from components import utils
from components.config import validation
from test_support import test_case

from proto import pools_pb2
from server import pools_config


TEST_CONFIG = pools_pb2.PoolsCfg(pool=[
  pools_pb2.Pool(
    name=['pool_name', 'another_name'],
    schedulers=pools_pb2.Schedulers(
      user=['user:a@example.com', 'b@example.com'],
      group=['group1', 'group2'],
      trusted_delegation=[
        pools_pb2.TrustedDelegation(
          peer_id='delegatee@example.com',
          require_any_of=pools_pb2.TrustedDelegation.TagList(
            tag=['k:tag1', 'k:tag2'],
          ),
        ),
      ],
    ),
    allowed_service_account=[
      'a1@example.com',
      'a2@example.com',
    ],
    allowed_service_account_group=[
      'accounts_group1',
      'accounts_group2',
    ],
  ),
], forbid_unknown_pools=True)


class PoolsConfigTest(test_case.TestCase):
  def validator_test(self, cfg, messages):
    ctx = validation.Context()
    pools_config._validate_pools_cfg(cfg, ctx)
    self.assertEquals(ctx.result().messages, [
      validation.Message(severity=logging.ERROR, text=m)
      for m in messages
    ])

  def mock_config(self, cfg):
    def get_self_config_mock(path, cls=None, **kwargs):
      self.assertEqual({'store_last_good': True}, kwargs)
      self.assertEqual('pools.cfg', path)
      self.assertEqual(cls, pools_pb2.PoolsCfg)
      return 'rev', cfg
    self.mock(config, 'get_self_config', get_self_config_mock)
    utils.clear_cache(pools_config._fetch_pools_config)

  def test_get_pool_config(self):
    self.mock_config(TEST_CONFIG)
    self.assertTrue(pools_config.forbid_unknown_pools())
    self.assertEqual(None, pools_config.get_pool_config('unknown'))

    expected1 = pools_config.PoolConfig(
        name=u'pool_name',
        rev='rev',
        scheduling_users=frozenset([
          auth.Identity('user', 'b@example.com'),
          auth.Identity('user', 'a@example.com'),
        ]),
        scheduling_groups=frozenset([u'group2', u'group1']),
        trusted_delegatees={
          auth.Identity('user', 'delegatee@example.com'):
            pools_config.TrustedDelegatee(
              peer_id=auth.Identity('user', 'delegatee@example.com'),
              required_delegation_tags=frozenset([u'k:tag1', u'k:tag2']),
            ),
        },
        service_accounts=frozenset([u'a2@example.com', u'a1@example.com']),
        service_accounts_groups=(u'accounts_group1', u'accounts_group2'))
    expected2 = expected1._replace(name='another_name')

    self.assertEqual(expected1, pools_config.get_pool_config('pool_name'))
    self.assertEqual(expected2, pools_config.get_pool_config('another_name'))

  def test_empty_config_is_valid(self):
    self.validator_test(pools_pb2.PoolsCfg(), [])

  def test_good_config_is_valid(self):
    self.validator_test(TEST_CONFIG, [])

  def test_missing_pool_name(self):
    cfg = pools_pb2.PoolsCfg(pool=[pools_pb2.Pool()])
    self.validator_test(cfg, [
      'pool #0 (unnamed): at least one pool name must be given',
    ])

  def test_bad_pool_name(self):
    n = 'x'*300
    cfg = pools_pb2.PoolsCfg(pool=[pools_pb2.Pool(name=[n])])
    self.validator_test(cfg, [
      'pool #0 (%s): bad pool name "%s", not a valid dimension value' % (n, n),
    ])

  def test_duplicate_pool_name(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc']),
      pools_pb2.Pool(name=['abc']),
    ])
    self.validator_test(cfg, [
      'pool #1 (abc): pool "abc" was already declared',
    ])

  def test_bad_scheduling_user(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        user=['not valid email'],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): bad user value "not valid email" - '
      'Identity has invalid format: not valid email',
    ])

  def test_bad_scheduling_group(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        group=['!!!'],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): bad group name "!!!"',
    ])

  def test_no_delegatee_peer_id(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        trusted_delegation=[pools_pb2.TrustedDelegation()],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): trusted_delegation #0 (): "peer_id" is required',
    ])

  def test_bad_delegatee_peer_id(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        trusted_delegation=[pools_pb2.TrustedDelegation(
          peer_id='not valid email',
        )],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): trusted_delegation #0 (not valid email): bad peer_id '
      'value "not valid email" - Identity has invalid format: not valid email',
    ])

  def test_duplicate_delegatee_peer_id(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        trusted_delegation=[
          pools_pb2.TrustedDelegation(peer_id='a@example.com'),
          pools_pb2.TrustedDelegation(peer_id='a@example.com'),
        ],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): trusted_delegation #0 (a@example.com): peer '
      '"a@example.com" was specified twice',
    ])

  def test_bad_delegation_tag(self):
    cfg = pools_pb2.PoolsCfg(pool=[
      pools_pb2.Pool(name=['abc'], schedulers=pools_pb2.Schedulers(
        trusted_delegation=[pools_pb2.TrustedDelegation(
          peer_id='a@example.com',
          require_any_of=pools_pb2.TrustedDelegation.TagList(
            tag=['not kv'],
          ),
        )],
      )),
    ])
    self.validator_test(cfg, [
      'pool #0 (abc): trusted_delegation #0 (a@example.com): bad tag #0 '
      '"not kv" - must be <key>:<value>',
    ])

  def test_bad_service_account(self):
    cfg = pools_pb2.PoolsCfg(pool=[pools_pb2.Pool(
      name=['abc'],
      allowed_service_account=['not an email'],
    )])
    self.validator_test(cfg, [
      'pool #0 (abc): bad allowed_service_account #0 "not an email"',
    ])

  def test_bad_service_account_group(self):
    cfg = pools_pb2.PoolsCfg(pool=[pools_pb2.Pool(
      name=['abc'],
      allowed_service_account_group=['!!!'],
    )])
    self.validator_test(cfg, [
      'pool #0 (abc): bad allowed_service_account_group #0 "!!!"',
    ])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
