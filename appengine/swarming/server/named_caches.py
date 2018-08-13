# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Swarming bot named cache management, i.e. list of known named cache and their
state on each bot.

    +--------------+
    |NamedCacheRoot|
    |id=<pool>     |
    +--------------+
        |
        +----------------+
        |                |
        |                v
    +----------+     +----------+
    |NamedCache|     |NamedCache|
    |id=<name> | ... |id=<name> |
    +----------+     +----------+


The cached size for each named cache is the P(95) for the caches found on the
fleet. It is updated every 24 hours, so that if a large cache is not re-observed
for 24h, it will be lowered. When an higher size is observed that is more than
10% of the previous one, the value is immediately updated.

Caches for named cache that haven't been updated for 8 days are deleted.

The caches will only be precalculated for the pools defined in pools.cfg.
"""

import datetime
import logging

from google.appengine.ext import ndb

from components import utils
from server import bot_management
from server import pools_config


### Models.


class NamedCacheRoot(ndb.Model):
  """There is one NamedCacheRoot entity per pool."""


class NamedCache(ndb.Model):
  """Represents the state of a single named cache.

  Parent is NamedCacheRoot for the pool.

  This entity is not stored in a transaction, as we want it to be hot in
  memcache, even if the hint is not exactly right.
  """
  ts = ndb.DateTimeProperty()
  max_size = ndb.IntegerProperty(indexed=False)


### Private APIs.


def _named_cache_key(pool, named_cache):
  """Returns the ndb.Key to a NamedCache."""
  return ndb.Key(NamedCacheRoot, pool, NamedCache, named_cache)


def _update_named_cache(pool, named_cache, size):
  """Opportunistically update a named cache if the hint was off by 10% or more.

  It will be updated when:
  - The NamedCache is older than 24 hours, where the new size is used and the
    old maximum ignored.
  - The new maximum is at least 10% higher than the previous one.
  """
  key = _named_cache_key(pool, named_cache)
  e = key.get()
  now = utils.utcnow()
  exp = now - datetime.timedelta(hours=24)
  if not e or e.max_size <= size*0.9 or e.ts < exp:
    e = NamedCache(key=key, ts=now, max_size=size)
    e.put()
    return True
  return False


### Public APIs.


def get_hints(pool, names):
  """Returns the hints for each named caches.

  Returns:
    list of hints in bytes for each named cache, or -1 when there's no hint
    available.
  """
  keys = [_named_cache_key(pool, n) for n in names]
  entities = ndb.get_multi(keys)
  # TODO(maruel): We could define default hints in the pool.
  # TODO(maruel): Look for closely named caches.
  return [e.max_size if e else -1 for e in entities]


def task_update_pool(pool):
  """Updates the NamedCache for a pool.

  This needs to be able to scale for several thousands bots and hundreds of
  different caches.

  - Query all the bots in a pool.
  - Calculate the named caches for the bots in this pool.
  - Update the entities.
  """
  q = bot_management.BotInfo.query()
  found = {}
  for bot in bot_management.filter_dimensions(q, [u'pool:'+pool]):
    state = bot.state
    if not state or not isinstance(state, dict):
      continue
    # TODO(maruel): Use structured data instead of adhoc json.
    c = state.get('named_cache')
    if not isinstance(c, dict):
      continue
    for key, value in sorted(c.iteritems()):
      if not value or not isinstance(value, list) or len(value) != 2:
        continue
      if not value[0] or not isinstance(value[0], list) or len(value[0]) != 2:
        continue
      s = value[0][1]
      found.setdefault(key, []).append(s)
  logging.info('Found %d caches in pool %r', len(found), pool)
  # TODO(maruel): Parallelise.
  for name, sizes in sorted(found.iteritems()):
    # Adhoc calculation to take the ~P(95).
    sizes.sort()
    size = sizes[int(float(len(sizes)) * 0.95)]
    if _update_named_cache(pool, name, size):
      logging.debug('Updated %s for %d', name, size)

  # Delete the old ones.
  exp = utils.utcnow() - datetime.timedelta(days=8)
  ancestor = ndb.Key(NamedCacheRoot, pool)
  q = NamedCache.query(NamedCache.ts < exp, ancestor=ancestor)
  keys = q.fetch(keys_only=True)
  if keys:
    logging.info('Deleting %d stale entities', len(keys))
    ndb.delete_multi(q)
  return True


def cron_update_named_caches():
  """Update NamedCache very old BotEvent entites."""
  for pool in pools_config.known():
    if not utils.enqueue_task(
        '/internal/taskqueue/update_named_cache',
        'named-cache-task',
        params={'pool': pool},
    ):
      logging.error('Failed to enqueue named cache task for pool %s', pool)
