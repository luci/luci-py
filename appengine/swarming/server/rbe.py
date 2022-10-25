# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functionality related to RBE Scheduler."""

import hashlib
import logging

from server import pools_config


def get_rbe_instance(bot_id, pools, bot_group_cfg):
  """Returns an RBE instance to use for the given bot.

  If the bot should not be using RBE, returns None.

  Args:
    bot_id: ID of the bot as a string.
    pools: pool IDs the bot belongs to.
    bot_group_cfg: a BotGroupConfig tuple with bot's bot group config.

  Returns:
    A string with RBE instance name to use by the bot or None.
  """
  rbe_migration = bot_group_cfg.rbe_migration
  if not rbe_migration:
    return None

  if bot_id in rbe_migration.enable_rbe_on:
    use_rbe = True
  elif bot_id in rbe_migration.disable_rbe_on:
    use_rbe = False
  else:
    use_rbe = _quasi_random_100(bot_id) <= float(rbe_migration.rbe_mode_percent)

  if not use_rbe:
    logging.info('RBE: bot %s is not using RBE', bot_id)
    return None

  # Check all pools the bot belongs to (most commonly only one) have RBE
  # enabled and they all use the same RBE instance.
  assert isinstance(pools, list), pools
  rbe_instances = set()
  for pool in pools:
    cfg = pools_config.get_pool_config(pool)
    if cfg and cfg.rbe_migration and cfg.rbe_migration.rbe_instance:
      rbe_instances.add(cfg.rbe_migration.rbe_instance)
    else:
      rbe_instances.add(None)

  rbe_instances = sorted(rbe_instances)
  if len(rbe_instances) != 1:
    logging.warning(
        'RBE: disabling RBE for bot %s: bot pools disagree on RBE instance: %r',
        bot_id, rbe_instances)
    return None

  rbe_instance = rbe_instances[0]
  if not rbe_instance:
    logging.warning(
        'RBE: disabling RBE for bot %s: pools are not configure to use RBE',
        bot_id)
    return None

  logging.info('RBE: bot %s is using RBE instance %s', bot_id, rbe_instance)
  return rbe_instance


### Private stuff.


def _quasi_random_100(s):
  """Given a string, returns a quasi-random float in range [0; 100]."""
  digest = hashlib.sha256(s).digest()
  num = float(ord(digest[0]) + ord(digest[1]) * 256)
  return num * 100.0 / (256.0 + 256.0 * 256.0)
