# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Functions to fetch and parse pools.cfg file with list of pools.

See comments in proto/pools.proto for more context. Structures defined here are
used primarily by task_scheduler.check_schedule_request_acl.
"""

import collections

from components import auth
from components import config
from components import utils
from components.config import validation

from proto import pools_pb2
from server import config as local_config
from server import service_accounts


POOLS_CFG_FILENAME = 'pools.cfg'


# Validated read-only representation of one pool.
PoolConfig = collections.namedtuple('PoolConfig', [
  # Name of the pool.
  'name',
  # Revision of pools.cfg file this config came from.
  'rev',
  # Set of auth.Identity that can schedule jobs in the pool.
  'scheduling_users',
  # Set of group names with users that can schedule jobs in the pool.
  'scheduling_groups',
  # Map {auth.Identity of a delegatee => TrustedDelegatee tuple}.
  'trusted_delegatees',
  # Set of service account emails allowed in this pool, specified explicitly.
  'service_accounts',
  # Additional list of groups with allowed service accounts.
  'service_accounts_groups',
])


# Validated read-only fields of one trusted delegation scenario.
TrustedDelegatee = collections.namedtuple('TrustedDelegatee', [
  # auth.Identity of the delegatee (the one who's minting the delegation token).
  'peer_id',
  # A set of tags to look for in the delegation token to allow the delegation.
  'required_delegation_tags',
])


def get_pool_config(pool_name):
  """Returns PoolConfig for the given pool or None if not defined."""
  return _fetch_pools_config().get(pool_name)


### Private stuff.


def _to_ident(s):
  if ':' not in s:
    s = 'user:' + s
  return auth.Identity.from_bytes(s)


def _validate_ident(ctx, title, s):
  try:
    return _to_ident(s)
  except ValueError as exc:
    ctx.error('bad %s value "%s" - %s', title, s, exc)
    return None


@utils.cache_with_expiration(60)
def _fetch_pools_config():
  """Loads pools.cfg and parses it into a map of PoolConfig tuples."""
  # store_last_good=True tells config components to update the config file
  # in a cron job. Here we just read from the datastore. In case it's the first
  # call ever, or config doesn't exist, it returns (None, None).
  rev, cfg = config.get_self_config(
      POOLS_CFG_FILENAME, pools_pb2.PoolsCfg, store_last_good=True)
  if not cfg:
    return {}

  # The config is already validated at this point.

  pools = {}
  for msg in cfg.pool:
    pools[msg.name] = PoolConfig(
        name=msg.name,
        rev=rev,
        scheduling_users=frozenset(_to_ident(u) for u in msg.schedulers.user),
        scheduling_groups=frozenset(msg.schedulers.group),
        trusted_delegatees={
          _to_ident(d.peer_id): TrustedDelegatee(
              peer_id=_to_ident(d.peer_id),
              required_delegation_tags=frozenset(d.require_any_of.tag))
          for d in msg.schedulers.trusted_delegation
        },
        service_accounts=frozenset(msg.allowed_service_account),
        service_accounts_groups=tuple(msg.allowed_service_account_group))
  return pools


@validation.self_rule(POOLS_CFG_FILENAME, pools_pb2.PoolsCfg)
def _validate_pools_cfg(cfg, ctx):
  """Validates pools.cfg file."""
  pools = set()
  for i, msg in enumerate(cfg.pool):
    with ctx.prefix('pool #%d (%s): ', i, msg.name or 'unnamed'):
      # Validate name.
      if not msg.name:
        ctx.error('missing name')
      elif not local_config.validate_dimension_value(msg.name):
        ctx.error('bad name "%s", not a valid dimension value', msg.name)
      elif msg.name in pools:
        ctx.error('pool "%s" was already declared', msg.name)
      else:
        pools.add(msg.name)

      # Validate schedulers.user.
      for u in msg.schedulers.user:
        _validate_ident(ctx, 'user', u)

      # Validate schedulers.group.
      for g in msg.schedulers.group:
        if not auth.is_valid_group_name(g):
          ctx.error('bad group name "%s"', g)

      # Validate schedulers.trusted_delegation.
      seen_peers = set()
      for d in msg.schedulers.trusted_delegation:
        with ctx.prefix('trusted_delegation #%d (%s): ', i, d.peer_id):
          if not d.peer_id:
            ctx.error('"peer_id" is required')
          else:
            peer_id = _validate_ident(ctx, 'peer_id', d.peer_id)
            if peer_id in seen_peers:
              ctx.error('peer "%s" was specified twice', d.peer_id)
            elif peer_id:
              seen_peers.add(peer_id)
          for i, tag in enumerate(d.require_any_of.tag):
            if ':' not in tag:
              ctx.error('bad tag #%d "%s" - must be <key>:<value>', i, tag)

      # Validate service accounts.
      for i, account in enumerate(msg.allowed_service_account):
        if not service_accounts.is_service_account(account):
          ctx.error('bad allowed_service_account #%d "%s"', i, account)

      # Validate service account groups.
      for i, group in enumerate(msg.allowed_service_account_group):
        if not auth.is_valid_group_name(group):
          ctx.error('bad allowed_service_account_group #%d "%s"', i, group)
