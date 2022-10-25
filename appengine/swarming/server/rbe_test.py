#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import mock

from test_support import test_case

from server import bot_groups_config
from server import pools_config
from server import rbe

from proto.config import bots_pb2
from proto.config import pools_pb2


class RBETest(test_case.TestCase):
  @staticmethod
  def bot_groups_config(rbe_mode_percent=0,
                        enable_rbe_on=None,
                        disable_rbe_on=None):
    kwargs = {f: None for f in bot_groups_config.BotGroupConfig._fields}
    kwargs['rbe_migration'] = bots_pb2.BotGroup.RBEMigration(
        rbe_mode_percent=rbe_mode_percent or 0,
        enable_rbe_on=enable_rbe_on or [],
        disable_rbe_on=disable_rbe_on or [],
    )
    return bot_groups_config.BotGroupConfig(**kwargs)

  @staticmethod
  def pool_config(rbe_instance):
    rbe_mgration = None
    if rbe_instance is not None:
      rbe_mgration = pools_pb2.Pool.RBEMigration(rbe_instance=rbe_instance)
    return pools_config.init_pool_config(rbe_migration=rbe_mgration)

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('server.rbe._quasi_random_100')
  def test_get_rbe_instance(self, quasi_random_100, pool_config):
    pools = {
        'pool-1-a': self.pool_config('instance-1'),
        'pool-1-b': self.pool_config('instance-1'),
        'pool-2': self.pool_config('instance-2'),
        'pool-no-rbe-1': self.pool_config(None),
        'pool-no-rbe-2': self.pool_config(''),
    }
    pool_config.side_effect = lambda pool: pools[pool]

    quasi_random_100.return_value = 30.0

    # Randomizer.
    instance = rbe.get_rbe_instance('bot-id', ['pool-1-a'],
                                    self.bot_groups_config(rbe_mode_percent=25))
    self.assertIsNone(instance)
    instance = rbe.get_rbe_instance('bot-id', ['pool-1-a'],
                                    self.bot_groups_config(rbe_mode_percent=30))
    self.assertEqual(instance, 'instance-1')

    # Explicitly enabled.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a'],
        self.bot_groups_config(rbe_mode_percent=25, enable_rbe_on=['bot-id']))
    self.assertEqual(instance, 'instance-1')

    # Explicitly disabled.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a'],
        self.bot_groups_config(rbe_mode_percent=30, disable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

    # The pool is not using RBE.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-no-rbe-1', 'pool-no-rbe-1'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

    # Pools agree on RBE instance.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a', 'pool-1-b'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertEqual(instance, 'instance-1')

    # Pools disagree on RBE instance.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a', 'pool-2'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

  def test_quasi_random_100(self):
    for i in range(1000):
      val = rbe._quasi_random_100(u'bot-%d' % i)
      self.assertGreaterEqual(val, 0.0)
      self.assertLessEqual(val, 100.0)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
