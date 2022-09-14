# coding: utf-8
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Ambient task queues generated from the actual load.

This means that the task queues are deduced by the actual load, they are never
explicitly defined. They are eventually deleted by a cron job once no incoming
task with the exact set of dimensions is triggered anymore.

Used to optimize scheduling.

    +---------+
    |BotRoot  |                                                bot_management.py
    |id=bot_id|
    +---------+
        |
        +------+
        |      |
        |      v
        |  +-------------+
        |  |BotDimensions|
        |  |id=1         |
        |  +-------------+
        |
        +---------------- ... ----+
        |                         |
        v                         v
    +-------------------+     +-------------------+
    |BotTaskDimensions  | ... |BotTaskDimensions  |
    |id=<dimension_hash>| ... |id=<dimension_hash>|
    +-------------------+     +-------------------+

    +-------Root------------+
    |TaskDimensionsRoot     |  (not stored)
    |id=<pool:foo or id:foo>|
    +-----------------------+
        |
        +---------------- ... -------+
        |                            |
        v                            v
    +----------------------+     +----------------------+
    |TaskDimensions        | ... |TaskDimensions        |
    |  +-----------------+ | ... |  +-----------------+ |
    |  |TaskDimensionsSet| |     |  |TaskDimensionsSet| |
    |  +-----------------+ |     |  +-----------------+ |
    |id=<dimension_hash>   |     |id=<dimension_hash>   |
    +----------------------+     +----------------------+
"""

import collections
import datetime
import hashlib
import json
import logging
import random
import struct
import time
import urllib

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import datastore_utils
from components import utils
from server import config
from server.constants import OR_DIM_SEP


# Extends the validity of TaskDimensionsSet and BotTaskDimensions, added to the
# incoming TaskRequest expiration_secs.
#
# This determines the frequency at which these entities must be refreshed. This
# value is a trade off between constantly updating BotTaskDimensions and
# TaskDimensions vs keeping them alive for longer than necessary, causing
# unnecessary queries in get_queues() users.
#
# The 10 minutes delta is from assert_task_async() which advance the timer by a
# random value to up 10 minutes early.
#
# For 8000 task queues, that gives a rate of (8000/(4*60*60)) 0.5QPS.
_EXTEND_VALIDITY = datetime.timedelta(hours=4, minutes=10)


# Additional time where TaskDimensionsSet (and by extension TaskDimensions) are
# kept in the DB even if not applicable anymore. This is to reduce the
# thundering-herd problem when a lot of tasks are created for queues with no
# bot.
_KEEP_DEAD = datetime.timedelta(days=1, minutes=10)


# How long to keep BotTaskDimensions past their actual expiration time in
# cron_tidy_bots. This is needed to reduce contention between cron_tidy_bots
# and assert_bot_async. assert_bot_async will delete BotTaskDimensions as soon
# as their expire, and cron_tidy_bots will delete remaining ones (e.g. dead
# bot dimensions) a bit later.
_TIDY_BOT_DIMENSIONS_CRON_LAG = datetime.timedelta(minutes=5)


# crbug.com/1064848
# Expiration time for task queues in memcache
# It expires in 5 minutes for now, But it should be shortened further or
# removed according to load on Datastore.
_EXPIRATION_TIME_TASK_QUEUES = 60 * 5


class Error(Exception):
  pass


### Models.


class BotDimensions(ndb.Model):
  """Notes the current valid bot dimensions.

  Parent is BotRoot.
  Key id is 1.

  This duplicates information from BotEvent but it is leveraged to quickly
  assert if _rebuild_bot_cache_async() must be called or not.
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False

  # 'key:value' strings. This is stored to enable the removal of stale entities
  # when the bot changes its dimensions.
  dimensions_flat = ndb.StringProperty(repeated=True)

  # Validity time, at which this entity should be considered irrelevant.
  valid_until_ts = ndb.DateTimeProperty(indexed=False)

  def _pre_put_hook(self):
    super(BotDimensions, self)._pre_put_hook()
    if self.key.integer_id() != 1:
      raise datastore_errors.BadValueError(
          '%s.key.id must be 1' % self.__class__.__name__)
    _validate_dimensions_flat(self)


class BotTaskDimensions(ndb.Model):
  """Stores a single precalculated queue hash for this bot.

  Parent is BotRoot.
  Key id is <dimensions_hash>. It is guaranteed to fit 32 bits.

  TODO(vadimsh): Move this entity hierarchy under a different entity group.
  There are no transactions that touch both BotEvent (etc.) and
  BotTaskDimensions (etc.) at the same time. These sets of entities just
  cause bogus transaction collisions.

  This hash could be conflicting different properties set, but this doesn't
  matter at this level because disambiguation is done in TaskDimensions
  entities.

  The number of stored entities is:
    <number of bots> x <TaskDimensions each bot support>

  The actual number is a direct function of the variety of the TaskDimensions.
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False
  # Don't bother with memcache, this entity is read only through queries.
  _use_memcache = False

  # Validity time, at which this entity should be considered irrelevant.
  valid_until_ts = ndb.DateTimeProperty()
  # 'key:value' strings. This is stored to enable the removal of stale entities
  # when the bot changes its dimensions.
  dimensions_flat = ndb.StringProperty(repeated=True, indexed=False)

  def is_valid(self, bot_dimensions):
    """Returns true if this entity is still valid for the bot dimensions."""
    for i in self.dimensions_flat:
      k, v = i.split(':', 1)
      if v not in bot_dimensions.get(k, []):
        return False
    return True

  def _pre_put_hook(self):
    super(BotTaskDimensions, self)._pre_put_hook()
    if not self.valid_until_ts:
      raise datastore_errors.BadValueError(
          '%s.valid_until_ts is required' % self.__class__.__name__)
    _validate_dimensions_flat(self)


class TaskDimensionsRoot(ndb.Model):
  """Ghost root entity to group kinds of tasks to a common root.

  This root entity is not stored in the DB.

  id is either 'id:<value>' or 'pool:<value>'. For a request dimensions set that
  specifies both keys, TaskDimensions is listed under 'id:<value>'.
  """


class TaskDimensionsSet(ndb.Model):
  """Embedded struct to store a set of dimensions.

  This entity is not stored, it is contained inside TaskDimensions.sets.
  """
  # Validity time, at which this entity should be considered irrelevant.
  # Entities with valid_until_ts in the past are considered inactive and are not
  # used. valid_until_ts is set in assert_task_async() to
  # "TaskRequest.expiration_ts + _EXTEND_VALIDITY".
  #
  # It is updated when an assert_task_async() call sees that valid_until_ts
  # becomes lower than TaskRequest.expiration_ts for a later task. This enables
  # not updating the entity too frequently, at the cost of keeping a dead queue
  # "alive" for a bit longer than strictly necessary.
  #
  # In practice, do not remove this set until _KEEP_DEAD has expired, to
  # further reduce the workload when tasks keep on coming when there's no bot
  # available.
  valid_until_ts = ndb.DateTimeProperty()

  # A list 'key:value' strings in arbitrary order, but without duplicates.
  #
  # This is stored to enable match_bot(). Stores flattened dimension sets as
  # produced by expand_dimensions_to_flats, i.e. dimension values here never
  # have "|" in them. Such dimension sets can directly be matched with bot
  # dimensions (as happens in match_bot).
  dimensions_flat = ndb.StringProperty(repeated=True, indexed=False)

  def _equals(self, dimensions_flat_set):
    """True if the set of 'key:value' pairs equals stored dimensions_flat."""
    assert isinstance(dimensions_flat_set, set)
    if len(dimensions_flat_set) != len(self.dimensions_flat):
      return False
    return dimensions_flat_set.issuperset(self.dimensions_flat)

  def _pre_put_hook(self):
    super(TaskDimensionsSet, self)._pre_put_hook()
    if not self.valid_until_ts:
      raise datastore_errors.BadValueError(
          '%s.valid_until_ts is required' % self.__class__.__name__)
    _validate_dimensions_flat(self)


class TaskDimensions(ndb.Model):
  """List dimension sets that match some dimensions hash.

  Parent is TaskDimensionsRoot
  Key id is <dimensions_hash>. It is guaranteed to fit 32 bits.

  For tasks that use OR-ed dimensions (`{"k": "a|b"}`), the <dimensions_hash> is
  taken over the original dimensions (with "|" still inside), but the stored
  dimensions set are flat (i.e. `sets` stores `[{"k": "a"}, {"k": "b"}]`).

  If there's a collision on <dimensions_hash> (i.e. two different dimensions
  sets hash to the same value), `sets` stores dimensions sets from all colliding
  entries. Collisions are eventually resolved when bots poll for tasks: they
  get all tasks that match <dimensions_hash>, and then filter out ones that
  don't match bot's dimensions.
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False

  # Lowest value of TaskDimensionsSet.valid_until_ts where this entity must be
  # updated. See its documentation for more details.
  #
  # It may be in the past up to _KEEP_DEAD.
  valid_until_ts = ndb.ComputedProperty(
      lambda self: self._calc_valid_until_ts())

  # One or multiple sets of request dimensions this dimensions_hash represents.
  # A bot will consume tasks with this <dimensions_hash> if it matches any of
  # sets stored here. Stores flat dimension dicts.
  sets = ndb.LocalStructuredProperty(TaskDimensionsSet, repeated=True)

  def assert_request(self, now, valid_until_ts, task_dimensions_flat):
    """Updates this entity to assert this task_dimensions_flat is supported.

    `task_dimensions_flat` here don't have "|" in them anymore, i.e. they are
    already flattened via expand_dimensions_to_flats(...).

    Returns:
      True if the entity was updated; it can be that a TaskDimensionsSet was
      added, updated or a stale one was removed.
    """
    # Update the expiration timestamp or add a new entry.
    updated = False
    s = self.match_request_flat(task_dimensions_flat)
    if not s:
      self.sets.append(
          TaskDimensionsSet(
              valid_until_ts=valid_until_ts,
              dimensions_flat=task_dimensions_flat))
      updated = True
    elif s.valid_until_ts <= valid_until_ts:
      s.valid_until_ts = valid_until_ts
      updated = True

    # Always trim old entries.
    cutoff = now - _KEEP_DEAD
    old = len(self.sets)
    self.sets = [s for s in self.sets if s.valid_until_ts >= cutoff]

    # True if anything has changed.
    return updated or len(self.sets) != old

  def match_bot(self, bot_dimensions_set):
    """Returns the TaskDimensionsSet that matches bot_dimensions_set or None.

    Arguments:
      - bot_dimensions_set a set of "key:value" pairs with bot dimensions.
    """
    for s in self.sets:
      if bot_dimensions_set.issuperset(s.dimensions_flat):
        return s
    return None

  def match_request_flat(self, task_dimensions_flat):
    """Returns the stored TaskDimensionsSet that equals task_dimensions_flat.

    `task_dimensions_flat` here don't have "|" in them anymore, i.e. they are
    already flattened via expand_dimensions_to_flats(...).
    """
    d = set(task_dimensions_flat)
    for s in self.sets:
      if s._equals(d):
        return s
    return None

  def _calc_valid_until_ts(self):
    if not self.sets:
      raise datastore_errors.BadValueError(
          '%s.sets must be specified' % self.__class__.__name__)
    return min(s.valid_until_ts for s in self.sets)

  def _pre_put_hook(self):
    super(TaskDimensions, self)._pre_put_hook()
    sets = set()
    for s in self.sets:
      s._pre_put_hook()
      sets.add('\000'.join(s.dimensions_flat))
    if len(sets) != len(self.sets):
      # Make sure there's no duplicate TaskDimensionsSet.
      raise datastore_errors.BadValueError(
          '%s.sets must all be unique' % self.__class__.__name__)


### Private APIs.


# Limit in rebuild_task_cache_async. Meant to be overridden in unit test.
_CAP_FUTURES_LIMIT = 50


def _validate_dimensions_flat(obj):
  """Validates obj.dimensions_flat; throws BadValueError if invalid."""
  if not obj.dimensions_flat:
    raise datastore_errors.BadValueError(
        '%s.dimensions_flat is empty' % obj.__class__.__name__)
  if len(obj.dimensions_flat) != len(set(obj.dimensions_flat)):
    raise datastore_errors.BadValueError(
        '%s.dimensions_flat has duplicate entries' % obj.__class__.__name__)
  if sorted(obj.dimensions_flat) != obj.dimensions_flat:
    raise datastore_errors.BadValueError(
        '%s.dimensions_flat must be sorted' % obj.__class__.__name__)


def _flush_futures(futures):
  return [f.get_result() for f in futures]


@ndb.tasklet
def _stale_BotTaskDimensions_async(bot_dimensions, bot_root_key, now):
  """Finds BotTaskDimensions that expired or do not match the current bot
  dimensions.

  Returns:
    A list of ndb.Key of BotTaskDimensions entities.
  """
  qit = BotTaskDimensions.query(ancestor=bot_root_key).iter(batch_size=256,
                                                            deadline=60)
  scanned = 0
  stale = []
  try:
    while (yield qit.has_next_async()):
      scanned += 1
      ent = qit.next()
      if ent.valid_until_ts < now or not ent.is_valid(bot_dimensions):
        stale.append(ent.key)
    raise ndb.Return(stale)
  finally:
    logging.info(
        'crbug.com/1064848: call from _stale_BotTaskDimensions_async: '
        'scanned %d, stale %d', scanned, len(stale))


@ndb.tasklet
def _fresh_BotTaskDimensions_slice_async(bot_dimensions_set, bot_root_key, now,
                                         tasks_root_key):
  """Generates fresh BotTaskDimensions for this bot with matching task queues
  from the given TaskDimensionsRoot entity group.

  The expected total number of TaskDimensions is in the tens or few hundreds, as
  it depends on all the kinds of different task dimensions that this bot could
  run that are ACTIVE queues, e.g. TaskDimensions.valid_until_ts is in the
  future.

  Returns:
    List of BotTaskDimensions entities ready to be put into datastore.
  """
  qit = TaskDimensions.query(ancestor=tasks_root_key).iter(batch_size=256,
                                                           deadline=60)
  scanned = 0
  fresh = []
  try:
    while (yield qit.has_next_async()):
      scanned += 1
      task_dimensions = qit.next()
      # match_bot() returns a TaskDimensionsSet if there's a match. It may still
      # be expired.
      s = task_dimensions.match_bot(bot_dimensions_set)
      dimensions_hash = task_dimensions.key.integer_id()
      if s and s.valid_until_ts >= now:
        # Reuse TaskDimensionsSet.valid_until_ts.
        fresh.append(
            BotTaskDimensions(id=dimensions_hash,
                              parent=bot_root_key,
                              valid_until_ts=s.valid_until_ts,
                              dimensions_flat=s.dimensions_flat))
      elif s:
        logging.debug(
            'crbug.com/1064848: found a matching TaskDimensionsSet(%s), '
            'but it was stale.', dimensions_hash)
    raise ndb.Return(fresh)
  finally:
    logging.debug(
        'crbug.com/1064848: call from _fresh_BotTaskDimensions_slice_async: '
        'scanned %d, fresh %d', scanned, len(fresh))


@ndb.tasklet
def _fresh_BotTaskDimensions_async(bot_dimensions, bot_root_key, now):
  """Generates fresh BotTaskDimensions for this bot.

  Returns:
    List of BotTaskDimensions entities ready to be put into datastore.
  """
  # All TaskDimensionsRoot with tasks that can potentially hit this bot.
  # There's one per pool plus one for the bot id. The old entries are removed by
  # cron job /internal/cron/task_queues_tidy triggered every N minutes
  # (see cron.yaml).
  roots = [ndb.Key(TaskDimensionsRoot, u'id:' + bot_dimensions[u'id'][0])]
  for pool in bot_dimensions['pool']:
    roots.append(ndb.Key(TaskDimensionsRoot, u'pool:' + pool))
  # Run queries in parallel, then just merge the results.
  bot_dimensions_set = set(bot_dimensions_to_flat(bot_dimensions))
  fresh = yield [
      _fresh_BotTaskDimensions_slice_async(bot_dimensions_set, bot_root_key,
                                           now, r) for r in roots
  ]
  raise ndb.Return(sum(fresh, []))


@ndb.tasklet
def _assert_bot_old_async(bot_root_key, bot_dimensions):
  """Prepares BotTaskDimensions entities as needed.

  Coupled with assert_task_async(), enables get_queues() to work by by knowing
  which TaskDimensions applies to this bot.

  Arguments:
    bot_root_key: ndb.Key to bot_management.BotRoot
    bot_dimensions: dictionary of the bot dimensions

  Returns:
    Number of matches or None if hit the cache, thus nothing was updated.
  """
  # Check if the bot dimensions changed since last _rebuild_bot_cache_async()
  # call and still valid. It rebuilds cache every 20-40 minutes to avoid
  # missing tasks that can be assigned to the bot. Tasks can be missed because
  # _rebuild_bot_cache_async isn't an atomic operation, and also it takes time
  # for secondary indices to be ready since they are *eventually* consistent.
  now = utils.utcnow()
  dims = yield ndb.Key(BotDimensions, 1, parent=bot_root_key).get_async()
  if (dims and dims.dimensions_flat == bot_dimensions_to_flat(bot_dimensions)
      and dims.valid_until_ts > now):
    # Cache hit, no need to look further.
    logging.debug(
        'assert_bot_async: cache hit. bot_id: %s, valid_until: %s, '
        'bot_dimensions: %s', bot_root_key.string_id(), dims.valid_until_ts,
        bot_dimensions)
    raise ndb.Return(None)

  matches = yield _rebuild_bot_cache_async(bot_dimensions, bot_root_key)
  raise ndb.Return(matches)


@ndb.tasklet
def _rebuild_bot_cache_async(bot_dimensions, bot_root_key):
  """Rebuilds the BotTaskDimensions cache for a single bot.

  This is done by a linear scan for all the TaskDimensions under the
  TaskDimensionsRoot entities with key id 'id:<bot_id>' and 'pool:<pool>', for
  each pool exposed by the bot. Only the TaskDimensions with TaskDimensionsRoot
  id with bot's id or the bot's pool are queried, not *all* TaskDimensions.

  Normally bots are in one or an handful of pools so the number of queries
  should be relatively low. This is all ancestor queries, so they are
  consistent.

  Runtime expectation: the scale is low enough that it can be run inline with
  the bot's poll request.

  The update completes with BotDimensions being updated and memcache entry for
  get_queues() updated.

  Returns:
    Number of matches.
  """
  now = utils.utcnow()
  bot_id = bot_dimensions[u'id'][0]
  stale = []
  matches = set()
  try:
    # Find keys to delete and entities to add.
    stale, fresh = yield [
        _stale_BotTaskDimensions_async(bot_dimensions, bot_root_key, now),
        _fresh_BotTaskDimensions_async(bot_dimensions, bot_root_key, now),
    ]

    # A set of fresh (still matching) queue numbers.
    matches = set(ent.key.integer_id() for ent in fresh)
    # A fresh match is not actually stale.
    stale = [ent for ent in stale if ent.integer_id() not in matches]

    to_put = fresh

    # Seal the fact that it has been updated.
    # The expiration is randomized to make sure different bots refresh
    # BotTaskDimensions cache at different times to spread the load.
    valid_until_ts = now + datetime.timedelta(minutes=random.randint(20, 40))
    to_put.append(
        BotDimensions(id=1,
                      parent=bot_root_key,
                      dimensions_flat=bot_dimensions_to_flat(bot_dimensions),
                      valid_until_ts=valid_until_ts))

    # Land all changes in one big happy transaction.
    @ndb.tasklet
    def commit_async():
      yield [
          ndb.put_multi_async(to_put),
          ndb.delete_multi_async(stale),
      ]
    yield datastore_utils.transaction_async(commit_async)

    # Proactive refresh the cache with new values.
    yield ndb.get_context().memcache_set(
        bot_id,
        sorted(matches),
        namespace='task_queues',
        time=_EXPIRATION_TIME_TASK_QUEUES)

    raise ndb.Return(len(matches))

  finally:
    logging.debug(
        '_rebuild_bot_cache_async(%s) in %.3fs. Registered for queues %d; '
        'cleaned %d', bot_id, (utils.utcnow() - now).total_seconds(),
        len(matches), len(stale))


def _get_task_dimensions_key(dimensions_hash, dimensions):
  """Returns the ndb.Key for the task dimensions."""
  # Both 'id' and 'pool' are guaranteed to have at most 1 item for a single
  # TaskProperty.
  if u'id' in dimensions:
    return ndb.Key(
        TaskDimensionsRoot, u'id:%s' % dimensions[u'id'][0],
        TaskDimensions, dimensions_hash)
  return ndb.Key(
      TaskDimensionsRoot, u'pool:%s' % dimensions[u'pool'][0],
      TaskDimensions, dimensions_hash)


@ndb.tasklet
def _ensure_TaskDimensions_async(task_dimensions_key, now, valid_until_ts,
                                 task_dimensions_flats):
  """Adds, updates or deletes the TaskDimensions.

  Returns:
    What it did (created, updated, etc.) as a string, for logging.
  """
  outcome = None
  task_dimensions = yield task_dimensions_key.get_async()
  if not task_dimensions:
    task_dimensions = TaskDimensions(key=task_dimensions_key)
    outcome = 'created'
  # Force all elements in `task_dimensions_flats` to be evaluated
  updated = [
      task_dimensions.assert_request(now, valid_until_ts, df)
      for df in task_dimensions_flats
  ]
  if any(updated):
    if task_dimensions.sets:
      outcome = outcome or 'updated'
      yield task_dimensions.put_async()
    else:
      outcome = 'deleted'
      yield task_dimensions.key.delete_async()
  raise ndb.Return(outcome or 'up-to-date')


def _hash_data(data):
  """Returns a 32 bits non-zero hash."""
  assert isinstance(data, str), repr(data)
  digest = hashlib.md5(data).digest()
  # Note that 'L' means C++ unsigned long which is (usually) 32 bits and
  # python's int is 64 bits.
  return int(struct.unpack('<L', digest[:4])[0]) or 1


def _get_query_BotDimensions_keys(task_dimensions_flat):
  """Returns a BotDimensions ndb.Key ndb.QueryIterator for the bots that
  corresponds to these task request dimensions.
  """
  assert not ndb.in_transaction()
  q = BotDimensions.query()
  for d in task_dimensions_flat:
    q = q.filter(BotDimensions.dimensions_flat == d)
  return q.iter(batch_size=100, keys_only=True, deadline=15)


@ndb.tasklet
def _refresh_BotTaskDimensions_async(now, valid_until_ts, task_dimensions_flat,
                                     bot_task_key):
  """Creates or refreshes a BotTaskDimensions.

  Arguments:
  - now: datetime.datetime of 'now'
  - valid_until_ts: datetime.datetime determines until when this
    task_dimensions_flat should remain valid.
  - task_dimensions_flat: list of '<key>:<value>' for the task request
    dimensions.
  - bot_task_key: ndb.Key to a BotTaskDimensions

  Returns:
    True if the entity was updated, False if no-op.
  """
  bot_task = yield bot_task_key.get_async()
  bot_id = bot_task_key.parent().string_id()
  logging.debug('refreshing BotTaskDimensions. bot_id: %s', bot_id)
  # Play safe. If the BotTaskDimensions was close to be ignored, refresh the
  # memcache entry.
  cutoff = now - datetime.timedelta(minutes=1)
  need_memcache_clear = True
  need_db_store = True
  if bot_task and set(bot_task.dimensions_flat) == set(task_dimensions_flat):
    need_memcache_clear = bot_task.valid_until_ts < cutoff
    # Skip storing if the validity period was already updated.
    need_db_store = bot_task.valid_until_ts < valid_until_ts

  if need_db_store:
    logging.debug(
        'Storing new BotTaskDimensions to DB.'
        'bot_id:%s, valid_until_ts:%s, task_dimensions_flat:%s', bot_id,
        valid_until_ts, task_dimensions_flat)
    yield BotTaskDimensions(
        key=bot_task_key,
        valid_until_ts=valid_until_ts,
        dimensions_flat=task_dimensions_flat).put_async()
  else:
    logging.debug(
        'Keeping stored BotTaskDimensions.'
        'bot_id: %s, valid_until_ts: %s', bot_id, bot_task.valid_until_ts)
  if need_memcache_clear:
    logging.debug('Deleting cached task queues. bot_id: %s', bot_id)
    yield ndb.get_context().memcache_delete(bot_id, namespace='task_queues')
  else:
    logging.debug('Keeping cached task queues. bot_id: %s, valid_until_ts: %s',
                  bot_id, bot_task.valid_until_ts)
  raise ndb.Return(need_db_store)


@ndb.tasklet
def _tidy_stale_TaskDimensions():
  """Removes all stale TaskDimensions entities."""
  # Inactive TaskDimensions are kept up to _KEEP_DEAD (to reduce churn on
  # TaskDimensions for tasks that show up, disappear and then show up again, all
  # within _KEEP_DEAD time interval).
  cutoff = utils.utcnow() - _KEEP_DEAD
  qit = TaskDimensions.query(TaskDimensions.valid_until_ts < cutoff).iter(
      batch_size=512, keys_only=True, deadline=5 * 60)

  # Group stale TaskDimensions by the root entity key, to delete entities
  # from the same entity group via a single transaction.
  cleanup_by_root = {}
  fetched = 0
  try:
    while (yield qit.has_next_async()):
      key = qit.next()
      cleanup_by_root.setdefault(key.root(), []).append(key)
      fetched += 1
  except datastore_errors.Timeout:
    logging.error('Timeout after fetching %d entities, proceeding...', fetched)

  # Deletes stale TaskDimensions.
  @ndb.tasklet
  def cleanup(keys):
    assert keys
    root_id = keys[0].parent().string_id()

    @ndb.tasklet
    def txn():
      ents = yield ndb.get_multi_async(keys)
      stale = [ent.key for ent in ents if ent and ent.valid_until_ts < cutoff]
      yield ndb.delete_multi_async(stale)
      raise ndb.Return(stale)

    try:
      deleted = yield datastore_utils.transaction_async(txn)
      for key in deleted:
        logging.debug('- TD: %d', key.integer_id())
      raise ndb.Return(len(deleted))
    except datastore_utils.CommitError as exc:
      logging.warning('_tidy_stale_TaskDimensions: error cleaning %s: %s',
                      root_id, exc)
      raise ndb.Return(0)

  # Cleanup all entity groups in parallel.
  deleted = yield [cleanup(keys) for keys in cleanup_by_root.values()]
  logging.info('_tidy_stale_TaskDimensions: deleted %d', sum(deleted))


@ndb.tasklet
def _tidy_stale_BotTaskDimensions():
  """Removes all stale BotTaskDimensions entities.

  This also cleans up entities for bots that were deleted or that died, as the
  corresponding entities will become stale after _EXTEND_VALIDITY.

  There should normally not be any entities here that relate to active bots that
  periodically call assert_bot_async(...): the cleanup happens there instead.
  """
  # Delay cleanup for a bit to let active bots do their own cleanup in
  # assert_bot_async(...) to avoid colliding with them. There's no real harm
  # in having BotTaskDimensions for inactive bots in Datastore for a little
  # longer.
  now = utils.utcnow()
  cutoff = now - _TIDY_BOT_DIMENSIONS_CRON_LAG
  qit = BotTaskDimensions.query(BotTaskDimensions.valid_until_ts < cutoff).iter(
      batch_size=512, keys_only=True, deadline=5 * 60)

  # Group stale BotTaskDimensions by the root entity key, to delete entities
  # from the same entity group via a single transaction.
  cleanup_by_root = {}
  fetched = 0
  try:
    while (yield qit.has_next_async()):
      key = qit.next()
      cleanup_by_root.setdefault(key.root(), []).append(key)
      fetched += 1
  except datastore_errors.Timeout:
    logging.error('Timeout after fetching %d entities, proceeding...', fetched)

  # Deletes stale BotTaskDimensions and flushes memcache.
  @ndb.tasklet
  def cleanup(keys):
    assert keys
    bot_id = keys[0].parent().string_id()

    @ndb.tasklet
    def txn():
      ents = yield ndb.get_multi_async(keys)
      stale = [ent.key for ent in ents if ent and ent.valid_until_ts < now]
      yield ndb.delete_multi_async(stale)
      raise ndb.Return(stale)

    try:
      deleted = yield datastore_utils.transaction_async(txn)
      if deleted:
        yield ndb.get_context().memcache_delete(bot_id, namespace='task_queues')
      for key in deleted:
        logging.debug('- BTD: %d for bot %s', key.integer_id(), bot_id)
      raise ndb.Return(len(deleted))
    except datastore_utils.CommitError as exc:
      logging.warning('_tidy_stale_BotTaskDimensions: error cleaning %s: %s',
                      bot_id, exc)
      raise ndb.Return(0)

  # Cleanup all entity groups in parallel.
  deleted = yield [cleanup(keys) for keys in cleanup_by_root.values()]
  logging.info('_tidy_stale_BotTaskDimensions: deleted %d', sum(deleted))


@ndb.tasklet
def _assert_task_props_old_async(properties, expiration_ts):
  """Asserts a TaskDimensions for a specific TaskProperties.

  Implementation of assert_task_async().
  """
  dimensions_hash = hash_dimensions(properties.dimensions)
  data = {
    u'dimensions': properties.dimensions,
    u'dimensions_hash': str(dimensions_hash),
    # _EXTEND_VALIDITY here is a way to lower the QPS of the taskqueue
    # 'rebuild-cache'.
    u'valid_until_ts': expiration_ts + _EXTEND_VALIDITY,
  }
  payload = utils.encode_to_json(data)

  # If this task specifies an 'id' value, update the cache inline since we know
  # there's only one bot that can run it, so it won't take long. This permits
  # tasks for specific bots like 'terminate' tasks to execute faster.
  # Related bug: crbug.com/1062746
  if properties.dimensions.get(u'id'):
    yield rebuild_task_cache_async(payload)
    raise ndb.Return(None)

  task_dimensions_key = _get_task_dimensions_key(dimensions_hash,
                                                 properties.dimensions)
  task_dimensions = yield task_dimensions_key.get_async()
  if task_dimensions:
    # Reduce the check to be 5~10 minutes earlier to help reduce an attack on
    # task queues when there's a strong on-going load of tasks happening. This
    # jitter is essentially removed from _EXTEND_VALIDITY window.
    jitter = datetime.timedelta(seconds=random.randint(5*60, 10*60))
    valid_until_ts = expiration_ts - jitter

    # Need to ensure each OR variant is known and fresh.
    stale = False
    for task_dims in expand_dimensions_to_flats(properties.dimensions):
      s = task_dimensions.match_request_flat(task_dims)
      # TaskDimensions(dimensions_hash) exists, but doesn't contain our
      # dimensions. This may happen on hash collisions. The entity actually
      # contains a list of dimension sets, all matching `dimensions_hash`, even
      # if their original TaskProperties.dimensions just accidentally happen to
      # hash to the same `dimensions_hash` integer. We need to enqueue a task to
      # add new dimensions to this list.
      if not s:
        logging.info(
            'assert_task_async(%d): failed to match the dimensions; triggering '
            'rebuild-task-cache', dimensions_hash)
        stale = True
        break
      # The dimension set was known before, but it is close to expiration,
      # need to refresh it. This will also refresh all other dimension sets
      # that resulted from expand_dimensions_to_flats(...).
      if s.valid_until_ts < valid_until_ts:
        logging.info(
            'assert_task_async(%d): set.valid_until_ts(%s) < expected(%s); '
            'triggering rebuild-task-cache', dimensions_hash, s.valid_until_ts,
            valid_until_ts)
        stale = True
        break
      # The dimension set is present and fresh.
      logging.debug('assert_task_async(%d): hit. valid_until_ts(%s)',
                    dimensions_hash, s.valid_until_ts)

    # If all expand_dimensions_to_flats(...) are fresh, we are done.
    if not stale:
      raise ndb.Return(None)
  else:
    logging.info(
        'assert_task_async(%d): new request kind; triggering '
        'rebuild-task-cache', dimensions_hash)

  # This eventually calls rebuild_task_cache_async(...). We can't use the
  # request ID since the request was not stored yet, so embed all the necessary
  # information.
  res = yield utils.enqueue_task_async(
      '/internal/taskqueue/important/task_queues/rebuild-cache',
      'rebuild-task-cache',
      payload=payload)
  if not res:
    logging.error('Failed to enqueue TaskDimensions update %x', dimensions_hash)
    raise Error('Failed to trigger task queue; please try again')


@ndb.tasklet
def _refresh_all_BotTaskDimensions_async(
    now, valid_until_ts, task_dimensions_flat, task_dimensions_hash):
  """Updates BotTaskDimensions for task_dimensions_flat.

  `task_dimensions_flat` here is a dimensions dict with values that don't have
  "|" in them, they are already flattened via expand_dimensions_to_flats.

  `task_dimensions_hash` is hash of the original dimensions, pre-flattening,
  matching the key of the corresponding TaskDimensions entity.

  Used by rebuild_task_cache_async.
  """
  logging.debug(
      '_refresh_all_BotTaskDimensions_async: refreshing bots with '
      'task_dimension_hash: %s, task_dimensions: %s', task_dimensions_hash,
      task_dimensions_flat)
  # Number of BotTaskDimensions entities that were created/updated in the DB.
  updated = 0
  # Number of BotTaskDimensions entities that matched this task queue.
  viable = 0
  try:
    # TODO(vadimsh): Group mutations by BotRoot entity group key.
    pending = []
    qit = _get_query_BotDimensions_keys(task_dimensions_flat)
    while (yield qit.has_next_async()):
      bot_dims_key = qit.next()
      bot_task_key = ndb.Key(BotTaskDimensions,
                             task_dimensions_hash,
                             parent=bot_dims_key.parent())
      viable += 1
      future = _refresh_BotTaskDimensions_async(
          now, valid_until_ts, task_dimensions_flat, bot_task_key)
      pending.append(future)

      while len(pending) > _CAP_FUTURES_LIMIT:
        ndb.Future.wait_any(pending)
        tmp = []
        for f in pending:
          if f.done():
            if (yield f):
              updated += 1
          else:
            tmp.append(f)
        pending = tmp

    for f in pending:
      if (yield f):
        updated += 1
    raise ndb.Return(None)
  finally:
    # The main reason for this log entry is to confirm the timing of the first
    # part (updating BotTaskDimensions) versus the second part (updating
    # TaskDimensions in rebuild_task_cache_async).
    # Only log at info level when something was done. This helps scanning
    # quickly the logs.
    fn = logging.info if updated else logging.debug
    fn(
        '_refresh_all_BotTaskDimensions_async: viable bots: %d; bots updated: '
        '%d', viable, updated)


@ndb.tasklet
def _refresh_TaskDimensions_async(now, valid_until_ts, task_dimensions_flats,
                                  task_dimensions_key):
  """Updates TaskDimensions for task_dimensions_flat.

  Used by rebuild_task_cache_async.

  `task_dimensions_flats` is a list of dimension dicts that don't have "|" in
  them, i.e. they are already flattened via expand_dimensions_to_flats.

  Returns:
    True if was updated or already up-to-date, or False if should retry.
  """
  # First do a dry run. If the dry run passes, skip the transaction.
  #
  # The rationale is that there can be concurrent triggers of this task queue
  # task (rebuild-cache) when there are concurrent task creations. The dry run
  # doesn't cost much, and if it passes it reduces transaction contention.
  #
  # The transaction contention can be problematic on pools with a high
  # cardinality of the dimension sets.
  #
  # TODO(vadimsh): Is this really necessary considering the code below doesn't
  # run transactionally anymore and essentially just re-executes the exact same
  # check again? Do we need to disable ndb in-process cache for this check to
  # be useful?
  task_dimensions = yield task_dimensions_key.get_async()
  if task_dimensions and not any(
      task_dimensions.assert_request(now, valid_until_ts, df)
      for df in task_dimensions_flats):
    logging.debug('TaskDimensions already contains requested dimension sets')
    raise ndb.Return(True)

  # Do an adhoc transaction instead of using datastore_utils.transaction().
  # This is because for some pools, the transaction rate may be so high that
  # it's impossible to get a good performance on the entity group.
  #
  # In practice the odds of conflict is ~nil, because it can only conflict
  # if a TaskDimensions.set has more than one item and this happens when
  # there's a hash conflict (odds 2^31) plus two concurrent task running
  # simultaneously (over _EXTEND_VALIDITY period) so we can do it in a more
  # adhoc way.
  #
  # TODO(vadimsh): Should this be a "lock" on the root entity key instead? The
  # "expiration jitter" mechanism in _assert_task_props_async should already
  # provide sufficient reduction in number of tasks per TaskDimensions entity
  # (but not per the parent entity group).
  #
  # TODO(vadimsh): This can accidentally overwrite dimension sets added
  # concurrently since memcache is not a replacement for transactions (e.g.
  # it can get wiped, the key evicted etc.).
  key = '%s:dimensions_hash:%d' % (task_dimensions_key.parent().string_id(),
                                   task_dimensions_key.integer_id())
  logging.debug('Taking task_queues_tx pseudo-lock. key=%s', key)
  res = yield ndb.get_context().memcache_add(
      key, True, time=60, namespace='task_queues_tx')
  if not res:
    # add() returns True if the entry was added, False otherwise.
    logging.warning('Failed taking pseudo-lock for %s; triggering retry', key)
    raise ndb.Return(False)
  try:
    outcome = yield _ensure_TaskDimensions_async(task_dimensions_key, now,
                                                 valid_until_ts,
                                                 task_dimensions_flats)
    logging.info('_refresh_TaskDimensions_async: %s', outcome)
  finally:
    yield ndb.get_context().memcache_delete(key, namespace='task_queues_tx')

  raise ndb.Return(True)


def _get_queues(bot_root_key):
  """Returns the known task queues as integers.

  This function is called to get the task queues to poll, as the bot is trying
  to reap a task, any task.

  It is also called while the bot is running a task, to refresh the task queues.

  Arguments:
    bot_root_key: ndb.Key to bot_management.BotRoot

  Returns:
    dimensions_hashes: list of dimension_hash for the bot
  """
  bot_id = bot_root_key.string_id()
  dimensions_hashes = memcache.get(bot_id, namespace='task_queues')
  if dimensions_hashes is not None:
    # Note: This may return stale queues. We may want to change the format to
    # include the expiration.
    logging.debug('get_queues(%s): can run from %d queues (memcache)\n%s',
                  bot_id, len(dimensions_hashes), dimensions_hashes)
    # Refresh all the keys.
    memcache.set_multi({str(d): True
                        for d in dimensions_hashes},
                       time=61,
                       namespace='task_queues_tasks')
    return dimensions_hashes

  # Retrieve all the dimensions_hash that this bot could run that have
  # actually been triggered in the past. Since this is under a root entity, this
  # should be fast.
  now = utils.utcnow()
  dimensions_hashes = sorted(
      obj.key.integer_id()
      for obj in BotTaskDimensions.query(ancestor=bot_root_key)
      if obj.valid_until_ts >= now)
  memcache.set(bot_id,
               dimensions_hashes,
               namespace='task_queues',
               time=_EXPIRATION_TIME_TASK_QUEUES)
  logging.info('get_queues(%s): Query in %.3fs: can run from %d queues\n%s',
               bot_id, (utils.utcnow() - now).total_seconds(),
               len(dimensions_hashes), dimensions_hashes)
  memcache.set_multi({str(d): True
                      for d in dimensions_hashes},
                     time=61,
                     namespace='task_queues_tasks')
  return dimensions_hashes


### WIP internal APIs.


class TaskDimensionsSets(ndb.Model):
  """A task dimensions hash and a list of dimension sets that match it.

  Root entity. Key ID is
    * `bot:<url-encoded-id>:<dimensions_hash>` - for tasks targeting a bot.
    * `pool:<url-encoded-id>:<dimensions_hash>` - for tasks targeting a pool.

  Where `<url-encoded-id>` is an URL encoding of bot or pool ID (primarily to
  quote `:`, since it is allowed in pool IDs) and `<dimensions_hash>` is a
  32 bit integer with the task dimensions hash as generated by
  hash_dimensions(...).

  Presence of this entity indicates that there's potentially a pending task
  that matches `<dimensions_hash>` and matching bots should be polling the
  corresponding TaskToRun queue.

  For tasks that use OR-ed dimensions (`{"k": "a|b"}`), the `<dimensions_hash>`
  is taken over the original dimensions (with "|" still inside), but the stored
  dimensions set are flat i.e. `sets` logically stores two separate dimension
  sets: `{"k": "a"}` and `{"k": "b"}`.

  If there's a collision on `<dimensions_hash>` (i.e. two different dimensions
  sets hash to the same value), `sets` stores dimensions sets from all colliding
  entries. Collisions are eventually resolved when bots poll for tasks: they
  get all tasks that match `<dimensions_hash>`, and then filter out ones that
  don't actually match bot's dimensions.

  A TaskDimensionsSets entity is always accompanied by TaskDimensionsInfo
  entity. TaskDimensionsSets entity stores infrequently changing (but frequently
  scanned) data, while TaskDimensionsInfo contains all data (including
  frequently changing data, like expiration timestamps). This separation reduces
  churn in TaskDimensionsSets datastore indexes (improving performance of full
  rescans that happen in _tq_rescan_matching_task_sets_async), as well as
  improves caching efficiency of direct ndb.get_multi fetches of
  TaskDimensionsSets in assert_bot(...).
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False

  # All matching dimensions sets as a list of dicts.
  #
  # Each dict defines a single set of task dimensions:
  #
  #   {
  #     "dimensions": ["k1:v1", "k1:v2", "k2:v2", ...]
  #   }
  #
  # Dimensions are represented by a sorted dedupped list of `key:value` strings.
  # These are flattened dimensions as returned by expand_dimensions_to_flats,
  # i.e. values here never have "|" in them. Such dimension sets can directly be
  # matched with bot dimensions using set operations.
  #
  # Matches dimensions stored in `TaskDimensionsInfo.sets` (sans expiration
  # time).
  #
  # The order of dicts is irrelevant. Never empty.
  sets = datastore_utils.DeterministicJsonProperty(indexed=False)

  @staticmethod
  def dimensions_to_id(dimensions):
    """Returns a string ID for this entity given the task dimensions dict.

    Arguments:
      dimensions: a dict with task dimensions prior to expansion of "|".
    """
    # Both `id` and `pool` are guaranteed to have at most 1 item. If a task
    # targets a specific bot via `id`, use `bot:` key prefix to allow this bot
    # to find the task faster, otherwise use `pool:`.
    if u'id' in dimensions:
      kind = 'bot'
      pfx = dimensions[u'id'][0]
    else:
      kind = 'pool'
      pfx = dimensions[u'pool'][0]
    dimensions_hash = hash_dimensions(dimensions)
    return '%s:%d' % (TaskDimensionsSets.id_prefix(kind, pfx), dimensions_hash)

  @staticmethod
  def id_prefix(kind, pfx):
    """Returns either `bot:<url-encoded-id>` or `pool:<url-encoded-id>`."""
    assert kind in ('bot', 'pool'), kind
    if isinstance(pfx, unicode):
      pfx = pfx.encode('utf-8')
    return '%s:%s' % (kind, urllib.quote_plus(pfx))


class TaskDimensionsInfo(ndb.Model):
  """Accompanies TaskDimensionsSets and caries expiration timestamps.

  The Parent entity is the corresponding TaskDimensionsSets. ID is integer 1.

  There things can happen to `sets`:
    * A new entry added: happens in assert_task_async.
    * Expiration of an existing entry is updated: happens in assert_task_async.
    * An expired entry is removed: happens in the cleanup cron.

  Whenever the list of sets in `sets` changes, the matching TaskDimensionsSets
  entity is updated as well (in a single transaction). This happens relatively
  infrequently. Changes to the expiry time of existing entries (which happen
  frequently) do not touch TaskDimensionsSets entity at all.
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False

  # All matching dimensions sets and their expiry time as a list of dicts.
  #
  # Each dict defines a single set of task dimensions together with a timestamp
  # when it should be removed from the datastore:
  #
  #   {
  #     "dimensions": ["k1:v1", "k1:v2", "k2:v2", ...],
  #     "expiry": 1663020417
  #   }
  #
  # Dimensions are represented by a sorted dedupped list of `key:value` strings.
  # These are flattened dimensions as returned by expand_dimensions_to_flats,
  # i.e. values here never have "|" in them. Such dimension sets can directly be
  # matched with bot dimensions using set operations.
  #
  # Expiry is specified as a integer Unix timestamp in seconds since epoch.
  # Expiry is always larger (with some wide margin) than the scheduling deadline
  # of the last task that used these dimensions and it is randomized to avoid
  # expiration stampedes.
  #
  # Matches dimensions stored in `TaskDimensionsSets.sets`.
  #
  # The order is irrelevant. Never empty.
  sets = datastore_utils.DeterministicJsonProperty(indexed=False)

  # A timestamp when this entity needs to be visited by the cleanup cron.
  #
  # The cron visits it if now() > next_cleanup_ts. This timestamp is calculated
  # based on expiry of individual dimensions sets.
  next_cleanup_ts = ndb.DateTimeProperty(indexed=True)


class BotDimensionsMatches(ndb.Model):
  """Stores what TaskDimensionsSets are matched to a bot.

  Root entity. Key ID is the bot ID.

  Created the first time the bot calls assert_bot(...). Deleted when the bot is
  expected to stop consuming tasks (e.g. it is deleted, quarantined, detected as
  dead, etc.) in cleanup_after_bot(...) .
  """
  # Disable useless in-process per-request cache to save some RAM.
  _use_cache = False

  # Bot dimensions as a sorted list of dedupped 'key:value' strings.
  #
  # If a bot reports dimension `{k: ["v1", "v2"]}`, this set will contain both
  # `k:v1` and `k:v2` strings.
  #
  # Indexed to allow finding bots that match particular task dimensions using
  # a datastore query, see _tq_update_bot_matches_async.
  dimensions = ndb.StringProperty(repeated=True, indexed=True)

  # String IDs of all matching TaskDimensionsSets.
  #
  # New entries are added via two mechanisms:
  #   * When a new TaskDimensionsSets task appears it proactively "searches"
  #     for matching bots in _tq_update_bot_matches_async. This reduces latency
  #     of scheduling of new kinds of tasks.
  #   * Periodically, and on bot dimensions change, there's a full rescan of
  #     all TaskDimensionsSets, see _tq_rescan_matching_task_sets_async.
  #
  # Entries are deleted in assert_bot(...) when their corresponding
  # TaskDimensionsSets entities disappear (which means there are no more tasks
  # matching these dimensions anymore) or when bot's dimensions no longer match
  # dimensions in TaskDimensionsSets (can happen when bot changes dimensions).
  matches = ndb.StringProperty(repeated=True, indexed=False)

  # Incremented whenever a rescan is triggered, used to skip obsolete TQ tasks.
  #
  # When a task queue task starts, if the current value of `rescan_counter` in
  # the entity is different from the value in the task payload, then this task
  # is no longer relevant and should be skipped.
  rescan_counter = ndb.IntegerProperty(default=0, indexed=False)

  # When we need to enqueue the next full rescan.
  #
  # Note: it is updated when triggering the asynchronous rescan via a task queue
  # task, not when it completes.
  next_rescan_ts = ndb.DateTimeProperty(default=utils.EPOCH, indexed=False)

  # When the last rescan was enqueued, for debugging.
  last_rescan_enqueued_ts = ndb.DateTimeProperty(indexed=False)
  # When the last rescan finished, for debugging.
  last_rescan_finished_ts = ndb.DateTimeProperty(indexed=False)
  # The last time when `matches` were cleaned up, for debugging.
  last_cleanup_ts = ndb.DateTimeProperty(indexed=False)
  # The last time a match was added via _maybe_add_match_async, for debugging.
  last_addition_ts = ndb.DateTimeProperty(indexed=False)


def _sets_to_expiry_map(sets):
  """Constructs a mapping of task dimension tuples to their expiry time.

  Arguments:
    sets: a value of `TaskDimensionsInfo.sets` entity property. In particular,
      `expiry` in dicts is populated.

  Returns:
    A dict {tuple(task dimensions list) => expiry datetime.datetime}
  """
  return {
      tuple(s['dimensions']): utils.timestamp_to_datetime(s['expiry'] * 1e6)
      for s in sets
  }


def _expiry_map_to_sets(expiry_map, with_expiry=True):
  """Converts an expiry map back into a list that can be stored in `sets`.

  Reverse of _sets_to_expiry_map(...). Optionally drops the expiry time for
  storing `sets` in TaskDimensionsSets.

  Arguments:
    expiry_map: a {tuple(task dimensions list) => datetime.datetime}.
    with_expiry: if True, populate `expiry` field in the result.

  Returns:
    A list with dicts, can be stored in `sets` entity property.
  """
  sets = []
  for dims, exp in sorted(expiry_map.items()):
    d = {'dimensions': dims}
    if with_expiry:
      d['expiry'] = int(utils.datetime_to_timestamp(exp) / 1e6)
    sets.append(d)
  return sets


def _put_task_dimensions_sets_async(sets_id, expiry_map):
  """Puts TaskDimensionsSets and TaskDimensionsInfo entities.

  Must be called in a transaction to make sure both entities stay in sync.

  Arguments:
    sets_id: string ID of TaskDimensionsSets to store under.
    expiry_map: an expiry map to store there, must not be empty.
  """
  assert ndb.in_transaction()
  assert expiry_map

  sets_key = ndb.Key(TaskDimensionsSets, sets_id)
  info_key = ndb.Key(TaskDimensionsInfo, 1, parent=sets_key)

  # Schedule a cleanup a little bit past the expiry of the entry that expires
  # first (to clean it up). Mostly to avoid hitting weird edge cases related
  # to not perfectly synchronized clocks.
  next_cleanup_ts = min(expiry_map.values()) + datetime.timedelta(minutes=5)

  return ndb.put_multi_async([
      TaskDimensionsSets(key=sets_key,
                         sets=_expiry_map_to_sets(expiry_map,
                                                  with_expiry=False)),
      TaskDimensionsInfo(
          key=info_key,
          sets=_expiry_map_to_sets(expiry_map, with_expiry=True),
          next_cleanup_ts=next_cleanup_ts,
      ),
  ])


def _delete_task_dimensions_sets_async(sets_id):
  """Deletes TaskDimensionsSets and TaskDimensionsInfo entities.

  Must be called in a transaction to make sure both entities stay in sync.

  Arguments:
    sets_id: string ID of TaskDimensionsSets to delete.
  """
  assert ndb.in_transaction()
  sets_key = ndb.Key(TaskDimensionsSets, sets_id)
  info_key = ndb.Key(TaskDimensionsInfo, 1, parent=sets_key)
  return ndb.delete_multi_async([sets_key, info_key])


@ndb.tasklet
def _assert_task_dimensions_async(task_dimensions, exp_ts):
  """Ensures there's corresponding TaskDimensionsSets stored in the datastore.

  If it is a never seen before set of dimensions, will emit a task queue task
  to find matching bots. Otherwise occasionally makes a transaction to bump
  expiration time of the stored TaskDimensionsSets entity.

  Arguments:
    task_dimensions: a dict with task dimensions (prior to expansion of "|").
    exp_ts: datetime.datetime with task expiration (aka scheduling deadline).
  """
  # This is e.g. "pool:<url-encoded-pool-id>:<number>".
  sets_id = TaskDimensionsSets.dimensions_to_id(task_dimensions)
  sets_key = ndb.Key(TaskDimensionsSets, sets_id)
  info_key = ndb.Key(TaskDimensionsInfo, 1, parent=sets_key)

  log = _Logger('assert_task(%s)', sets_id)

  # Load expiration of known dimensions sets to see if they need to be updated.
  info = yield info_key.get_async()
  expiry_map = _sets_to_expiry_map(info.sets if info else [])

  # Randomize expiration time a bit. This is done for two reasons:
  #   * Reduce transaction collisions when checking the expiry from concurrent
  #     requests (only the most unlucky one will do the transaction below).
  #   * Spread out load for TaskDimensionsSets cleanup cron a bit in case a
  #     lot of tasks appeared at once.
  # Some extra time will also be added in the transaction below, see comments
  # there.
  exp_ts += _random_timedelta_mins(0, 30)

  # This expands e.g. `{"k": "a|b"}` into `[("k:a",), ("k:b",)]`.
  expanded = [tuple(s) for s in expand_dimensions_to_flats(task_dimensions)]

  # Check all sets are known and fresh.
  fresh = True
  for dims in expanded:
    cur_expiry = expiry_map.get(dims)
    if not cur_expiry or cur_expiry < exp_ts:
      fresh = False
      break
  if fresh:
    raise ndb.Return(None)

  # Some dimensions sets are either missing or stale. We need to transactionally
  # update the entity to add them, perhaps emitting a task queue task to find
  # matching bots.
  @ndb.tasklet
  def txn():
    # Set the stored expiration time far in advance. This reduces frequency of
    # transactions that update the expiration time in case of a steady stream of
    # requests (one transaction "covers" all next ~5h of requests: they'll see
    # the entity as fresh). The downside is that we keep stuff in datastore for
    # longer than strictly necessary per `exp_ts`. But this is actually a good
    # thing for infrequent tasks with short expiration time. We **want** to keep
    # TaskDimensionsSets for them in the datastore longer (perhaps even if there
    # are no such tasks the queues) to avoid frequently creating and deleting
    # TaskDimensionsSets entities for them. Creating TaskDimensionsSets is a
    # costly operation since it requires a scan to find matching bots.
    extended_ts = exp_ts + datetime.timedelta(hours=5)

    # Load the fresh state, since we are in the transaction now.
    info = yield info_key.get_async()
    expiry_map = _sets_to_expiry_map(info.sets if info else [])

    # Bump the expiry and add new sets.
    created = []
    updated = []
    for dims in expanded:
      cur_expiry = expiry_map.get(dims)
      if not cur_expiry:
        log.info('adding %s, expiry %s', dims, extended_ts)
        expiry_map[dims] = extended_ts
        created.append(dims)
      elif cur_expiry < extended_ts:
        log.info('expiry of %s: %s => %s', dims, cur_expiry, extended_ts)
        expiry_map[dims] = extended_ts
        updated.append(dims)

    # It is possible the entity was already updated by another transaction.
    if not created and not updated:
      log.info('nothing to commit, already updated by another txn')
      raise ndb.Return(None)

    # Kick out old sets since we are going to commit the mutation anyway.
    now = utils.utcnow()
    for dims, expiry in expiry_map.items():
      if expiry < now:
        log.info('dropping %s, expired at %s', dims, expiry)
        expiry_map.pop(dims)

    # Some sets must survive, since we just added/updated fresh sets.
    assert len(expiry_map) > 0
    yield _put_task_dimensions_sets_async(sets_id, expiry_map)

    # If we added new sets, need to launch a task to find matching bots ASAP.
    # This reduces scheduling latency for new kinds of tasks. This eventually
    # calls _tq_update_bot_matches_async. Note we don't need to do this if we
    # are just updating the expiration time. The assignments of tasks to bots
    # were already done at that point.
    if created:
      log.info('enqueuing a task to find matches for %s', created)
      ok = yield utils.enqueue_task_async(
          '/internal/taskqueue/important/task_queues/update-bot-matches',
          'update-bot-matches',
          payload=utils.encode_to_json({
              'task_sets_id': sets_id,
              'dimensions': created,
              'enqueued_ts': now,
          }),
          transactional=True,
      )
      if not ok:
        raise datastore_utils.CommitError('Failed to enqueue a TQ task')

  # Do it!
  log.info('launching txn')
  yield datastore_utils.transaction_async(txn, retries=3)
  log.info('txn completed')


@ndb.tasklet
def _tq_update_bot_matches_async(task_sets_id, task_sets_dims, enqueued_ts):
  """Assigns a new task dimension set to matching bots.

  Discovers BotDimensionsMatches that can be potentially matched to any
  task dimensions set in `task_sets_dims` and transactionally updates them
  if they indeed match.

  Arguments:
    task_sets_id: string ID of TaskDimensionsSets that contains the set.
    task_sets_dims: a list with lists of flat task dimensions to match on, e.g.
      `[[`k1:a`, `k2:v1`], [`k1:a`, `k2:v2`]]`. It may have multiple elements
      when tasks is using OR dimensions.
    enqueued_ts: datetime.datetime when this task was enqueued, for debugging.

  Returns:
    True if succeeded, False if the TQ task needs to be retried.
  """
  # TODO:
  #
  # * Check that TaskDimensionsSets entity still has `task_sets_dims`.
  # * Query for matching BotDimensionsMatches using `dimensions` index.
  # * Transactionally add new ID to the list if not there yet. When doing a
  #   transaction  opportunistically kick out expired entries.
  # * Succeed only if all updates are successful.
  logging.info('%s %s %s', task_sets_id, task_sets_dims, enqueued_ts)
  raise ndb.Return(True)


@ndb.tasklet
def _cleanup_task_dimensions_async(dims_info, log):
  """Removes stale dimensions sets from a TaskDimensions[Sets|Info] entities.

  Deletes entities entirely if all sets there are stale. Called as part of the
  cleanup cron.

  Arguments:
    dims_info: a TaskDimensionsInfo entity to cleanup.
    log: _Logger to use for logs.

  Returns:
    True if cleaned up, False if already clean or gone.
  """
  now = utils.utcnow()
  sets_id = dims_info.key.parent().string_id()

  # Confirm we have something to cleanup before opening a transaction.
  expiry_map = _sets_to_expiry_map(dims_info.sets)
  if all(expiry >= now for expiry in expiry_map.values()):
    raise ndb.Return(False)

  @ndb.tasklet
  def txn():
    txn_ent = yield dims_info.key.get_async()
    if not txn_ent:
      raise ndb.Return(False)
    expiry_map = _sets_to_expiry_map(txn_ent.sets)

    changed = False
    for dims, expiry in expiry_map.items():
      if expiry < now:
        log.info('%s: dropping %s, expired at %s', sets_id, dims, expiry)
        expiry_map.pop(dims)
        changed = True

    if changed:
      if not expiry_map:
        yield _delete_task_dimensions_sets_async(sets_id)
      else:
        yield _put_task_dimensions_sets_async(sets_id, expiry_map)

    raise ndb.Return(changed)

  changed = yield datastore_utils.transaction_async(txn, retries=3)
  raise ndb.Return(changed)


@ndb.tasklet
def _assert_bot_dimensions_async(bot_dimensions):
  """Updates assignment of task queues to the bot, returns them.

  Runs as part of /bot/poll and must be fast.

  Arguments:
    bot_dimensions: a dict with bot dimensions (including "id" dimension).

  Returns:
    A list of integers with queues to poll.
  """
  # TODO:
  #
  # Fetch BotDimensionsMatches.
  #
  # Filter out stale TaskDimensionsSets:
  #    * Ones no longer present in the datastore.
  #    * Ones no longer matching bot dimensions.
  #
  # If the set changed or dimensions changed or next rescan TS arrived:
  #    * Start a txn.
  #    * If dimensions changed or next rescan arrived: enqueue a task.
  #    * Store updated set numbers, removing no longer matching ones.
  _ = bot_dimensions
  raise ndb.Return([])


@ndb.tasklet
def _tq_rescan_matching_task_sets_async(bot_id, rescan_counter, rescan_reason):
  """A task queue task that finds all matching TaskDimensionsSets for a bot.

  Arguments:
    bot_id: ID of the bot.
    rescan_counter: value of `BotDimensionsMatches.rescan_counter` when the
      task was enqueued.
    rescan_reason: a string with rescan reason, for debugging.

  Returns:
    True if succeeded, False if the TQ task needs to be retried.
  """
  # TODO:
  #
  # * Fetch BotDimensionsMatches and confirm the revision still matches. Grab
  #   `BotDimensionsMatches.dimensions`.
  # * Find *all* possible TaskDimensionsSets using a key prefix scan.
  # * Filter out ones that don't match bot dimensions.
  # * In a BotDimensionsMatches transaction:
  #    * Check `BotDimensionsMatches.dimensions` is still what we scanned for.
  #    * See what existing sets are not in the calculated list above.
  #    * Double check they are really gone.
  #    * Store the up-to-date list.
  _ = bot_id
  _ = rescan_counter
  _ = rescan_reason
  raise ndb.Return(True)


### Some generic helpers.


class _Logger(object):
  """Prefixes a string and request duration to logged messages."""

  def __init__(self, pfx, *args):
    self._pfx = pfx % args
    self._ts = time.time()

  def derive(self, sfx, *args):
    """Derives a new logger with prefix extended by the given message."""
    logger = _Logger('%s: %s', self._pfx, sfx % args)
    logger._ts = self._ts
    return logger

  def _log(self, lvl, msg, args):
    lvl('[%.2fs] %s: %s', time.time() - self._ts, self._pfx, msg % args)
    #print '[%.2fs] %s: %s' % (time.time() - self._ts, self._pfx, msg % args)

  def info(self, msg, *args):
    self._log(logging.info, msg, args)

  def warning(self, msg, *args):
    self._log(logging.warning, msg, args)

  def error(self, msg, *args):
    self._log(logging.error, msg, args)


def _random_timedelta_mins(a, b):
  """Returns a random timedelta in [a min; b min) range."""
  return datetime.timedelta(minutes=random.uniform(a, b))


class _AsyncWorkQueue(object):
  """A queue of work items with limits on concurrency.

  Used internally by _map_async.
  """

  def __init__(self, producers):
    """Creates a queue that accepts items from a number of producers.

    Arguments:
      producers: number of producers that would enqueue items. Each one is
        expected to call done() when they are done. Once all producers are done,
        the queue is not accepting new items. This eventually causes
        consume_async tasklet to exit.
    """
    assert producers > 0
    self._queue = collections.deque()
    self._producers = producers
    self._waiter = None

  def _wakeup(self):
    if self._waiter:
      waiter, self._waiter = self._waiter, None
      waiter.set_result(None)

  def enqueue(self, items):
    """Enqueues a batch of items to be processed.

    Arguments:
      items: an iterable of items, each one will eventually be passed to the
        callback in `cb` which may decide to launch a tasklet to process this
        item.
    """
    assert self._producers > 0
    if items:
      self._queue.extend(items)
      self._wakeup()

  def done(self):
    """Must be called by a producer when they are done with the queue."""
    assert self._producers > 0
    self._producers -= 1
    if not self._producers:
      self._wakeup()

  @ndb.tasklet
  def consume_async(self, cb, max_concurrency):
    """Runs a loop that dequeues items and calls the callback for each item.

    See _map_async for expected behavior of the callback.

    Returns:
      True if all futures returned by the callback resolved into True.
    """
    ok = True
    running = []
    while self._producers or self._queue:
      # While have stuff in the queue, keep launching and executing it.
      while self._queue:
        fut = cb(self._queue.popleft())
        if not isinstance(fut, ndb.Future):
          continue
        running.append(fut)

        # If running too much stuff, wait for some of it to finish. What we'd
        # really like is ndb.Future.wait_any_async(running), but there's no
        # async variant of wait_any in Python 2 ndb library (there's one in
        # Python 3 ndb). So instead just wait for the oldest future to finish
        # and then collect all the ones that finished during that time.
        if len(running) >= max_concurrency:
          yield running[0]
          pending = []
          for f in running:
            if f.done():
              ok = ok and f.get_result()
            else:
              pending.append(f)
          running = pending

      # If the queue is drained and there are no more producers, we are done.
      if not self._producers:
        break

      # If the queue is drained, but there are producers, wait for more items
      # to appear or when the last producer drops.
      assert not self._waiter
      self._waiter = ndb.Future()
      yield self._waiter

    # Wait for the completion of the rest of the items.
    oks = yield running
    raise ndb.Return(ok and all(oks))


@ndb.tasklet
def _map_async(queries,
               cb,
               max_concurrency=100,
               page_size=1000,
               timeout=300,
               max_pages=None):
  """Applies a callback to results of a bunch of queries.

  This is roughly similar to ndb.Query.map_async, except it tries to make
  progress on errors and timeouts and it limits number of concurrent tasklets
  to avoid grinding to a halt when processing large sets.

  Arguments:
    queries: a list of (ndb.Query, _Logger) with queries to fetch results of.
    cb: a callback that takes a fetched entity and optionally returns a future
      to wait on. The future must resolve in a boolean, where True indicates
      the item was processed successfully and False if the processing failed.
      If the callback doesn't return a future, the item is assumed to be
      processed successfully. The callback must not raise exceptions, they
      will abort the whole thing. The callback may be called multiple times
      with the same entity if this entity is returned by multiple queries.
    max_concurrency: a limit on number of concurrently pending futures.
    page_size: a page size for datastore queries.
    timeout: how long (in seconds) to run the query loop before giving up.
    max_pages: a limit on number of pages to fetch (mostly for tests).

  Returns:
    True if all queries finished to completion and all fetched items were
    processed successfully.
  """
  queue = _AsyncWorkQueue(len(queries))

  @ndb.tasklet
  def scan(q, logger):
    # For some reason queries that use has_next_async() API have a hard deadline
    # of 60s regardless of query options. This is not enough for busy servers.
    # Instead we'll use an explicitly paginated query.
    try:
      logger.info('scanning')
      deadline = utils.time_time() + timeout
      count = 0
      pages = 0
      cursor = None
      more = True
      while more:
        rpc_deadline = deadline - utils.time_time()
        if rpc_deadline < 0 or (max_pages and pages >= max_pages):
          raise ndb.Return(False)
        try:
          page, cursor, more = yield q.fetch_page_async(page_size,
                                                        start_cursor=cursor,
                                                        deadline=rpc_deadline)
          # Avoid favoring items that appear earlier in the page. It is
          # important when _map_async is retried, e.g. as a part of TQ task
          # retry. Gives some opportunity for tail items to be processed. Mostly
          # useful for larger page sizes.
          random.shuffle(page)
          queue.enqueue(page)
          count += len(page)
          pages += 1
        except (datastore_errors.BadRequestError, datastore_errors.Timeout):
          logger.error('scan timed out, visited %d items', count)
          raise ndb.Return(False)
      logger.info('scan completed, visited %d items', count)
      raise ndb.Return(True)
    finally:
      queue.done()

  # Run scans that enqueue items into a queue, and in parallel consume items
  # from this queue.
  futs = [scan(q, logger) for q, logger in queries]
  futs.append(queue.consume_async(cb, max_concurrency))
  ok = yield futs
  raise ndb.Return(all(ok))


### Public APIs.


def expand_dimensions_to_flats(dimensions, is_bot_dim=False):
  """Expands |dimensions| to a series of dimensions_flat.

  If OR is not used, the result contains exactly one element. Otherwise, it
  expands the OR dimension into a series of basic dimensions, then flattens each
  one of them.

  Returns: a list of dimensions_flat expanded from |dimensions|. A
  dimensions_flat is a sorted list of '<key>:<value>' of the basic dimensions.

  This function can be called with invalid dimensions that are reported by the
  bot. Tolerate them, but trim dimensions longer than 321 characters (the limit
  is 64+256+1=321). This is important, otherwise handling the returned values
  that are too long can throw while trying to store this in the datastore.

  The challenge here is that we're handling unicode strings, but we need to
  count in term of utf-8 bytes while being efficient.

  According to https://en.wikipedia.org/wiki/UTF-8, the longest UTF-8 encoded
  character is 4 bytes.

  Keys are strictly a subset of ASCII, thus valid keys are at most 64 bytes.

  Values can be any unicode string limited to 256 characters. So the worst
  case is 1024 bytes for a valid value.

  This means that the maximum valid string is 64+1+1024 = 1089 bytes, where 1 is
  the ':' character which is one byte.

  One problem is that because surrogate code points are used to encode
  characters outside the base plane in UTF-16, a UCS2 build of Python may use
  two Unicode code points for one characters, so len('😬') returns 2, even
  though it returns 1 on a UCS4 build.

  So it means that the limit is effectively halved for non-BMP characters,
  depending on the python build used.

  Silently remove duplicate dimensions, for the same reason as for long ones.
  """
  dimensions_kv = list(dimensions.items())
  for i, (k, v) in enumerate(dimensions_kv):
    if isinstance(v, (str, unicode)):
      assert is_bot_dim, (k, v)
      dimensions_kv[i] = (k, [
          v,
      ])
  cur_dimensions_flat = []
  result = []
  cutoff = config.DIMENSION_KEY_LENGTH + 1 + config.DIMENSION_VALUE_LENGTH

  def gen(ki, vi):
    if ki == len(dimensions_kv):
      # Remove duplicate dimensions. While invalid, we want to make sure they
      # can be stored without throwing an exception.
      result.append(sorted(set(cur_dimensions_flat)))
      return

    key, values = dimensions_kv[ki]
    if vi == len(values):
      gen(ki + 1, 0)
      return

    for v in values[vi].split(OR_DIM_SEP):
      flat = u'%s:%s' % (key, v)
      if len(flat) > cutoff:
        assert is_bot_dim, flat
        # An ellipsis is codepoint U+2026 which is encoded with 3 bytes in
        # UTF-8. We're still well below the 1500 bytes limit. Datastore uses
        # UTF-8.
        flat = flat[:cutoff] + u'…'

      cur_dimensions_flat.append(flat)
      gen(ki, vi + 1)
      cur_dimensions_flat.pop()

  gen(0, 0)
  return result


def bot_dimensions_to_flat(dimensions):
  """Returns a flat '<key>:<value>' sorted list of dimensions."""
  try:
    expanded = expand_dimensions_to_flats(dimensions, is_bot_dim=True)
  except AttributeError as e:
    logging.exception(
        "crbug.com/1133117: failed to call expand_dimensions_to_flats for %s",
        dimensions)
    raise e
  assert len(expanded) == 1, dimensions
  return expanded[0]


def hash_dimensions(dimensions):
  """Returns a 32 bits int that is a hash of the request dimensions specified.

  Dimensions values still have "|" in them, i.e. this is calculated prior to
  expansion of OR-ed dimensions into a disjunction of dimension sets.

  Arguments:
    dimensions: dict(str, [str])

  The return value is guaranteed to be a non-zero int so it can be used as a key
  id in a ndb.Key.
  """
  # This horrible code is the product of micro benchmarks.
  # TODO(maruel): This is incorrect, as it can confuse keys and values. But
  # changing the algo is non-trivial.
  data = ''
  for k, values in sorted(dimensions.items()):
    data += k.encode('utf8')
    data += '\000'
    assert isinstance(values, (list, tuple)), values
    for v in values:
      data += v.encode('utf8')
      data += '\000'
  return _hash_data(data)


def assert_bot(bot_root_key, bot_dimensions):
  """Prepares BotTaskDimensions entities as needed, fetches matching queues.

  Coupled with assert_task_async(), enables assignment of tasks to bots by
  putting tasks into logical queues (represented by queue numbers), and
  assigning each bot a list of queues it needs to poll tasks from.

  Arguments:
    bot_root_key: ndb.Key to bot_management.BotRoot.
    bot_dimensions: dictionary of the bot dimensions.

  Returns:
    A list of integers with queues to poll.
  """
  # Run old and new implementation concurrently. The old one is still used
  # for real, the new one just used to pre-fill entities before the switch
  # to use it for real.
  matches_fut = _assert_bot_dimensions_async(bot_dimensions)
  _assert_bot_old_async(bot_root_key, bot_dimensions).get_result()
  queues = _get_queues(bot_root_key)
  try:
    _ = matches_fut.get_result()
  except (apiproxy_errors.DeadlineExceededError, datastore_utils.CommitError):
    logging.error('_assert_bot_dimensions_async commit error')
  return queues


def freshen_up_queues(bot_root_key):
  """Refreshes liveness of memcache entries for queues polled by the bot.

  This is needed by probably_has_capacity(...) check.

  Arguments:
    bot_root_key: ndb.Key to bot_management.BotRoot.
  """
  # TODO: Maintain some lightweight best-effort memcache structure on top of
  # BotDimensionsMatches.
  _get_queues(bot_root_key)


def cleanup_after_bot(bot_root_key):
  """Removes all BotDimensions and BotTaskDimensions for this bot.

  Arguments:
    bot_root_key: ndb.Key to bot_management.BotRoot

  Do not clean up TaskDimensions. There could be pending tasks and there's a
  possibility that a bot with the same ID could come up afterward (low chance in
  practice but it's a possibility). In this case, if TaskDimensions is deleted,
  the pending task would not be correctly run even when a bot comes back online
  as assert_bot_async() would fail to create the corresponding
  BotTaskDimensions.
  """
  q = BotTaskDimensions.query(ancestor=bot_root_key).iter(keys_only=True)
  futures = ndb.delete_multi_async(q)
  futures.append(ndb.Key(BotDimensions, 1, parent=bot_root_key).delete_async())
  futures.append(
      ndb.Key(BotDimensionsMatches, bot_root_key.string_id()).delete_async())
  _flush_futures(futures)


@ndb.tasklet
def assert_task_async(request):
  """Makes sure the TaskRequest dimensions, for each TaskProperties, are listed
  as a known queue.

  This function must be called before storing the TaskRequest in the DB.

  When a cache miss occurs, a task queue is triggered.

  Warning: the task will not be run until the task queue ran, which causes a
  user visible delay. There is no SLA but expected range is normally seconds at
  worst. This only occurs on new kind of requests, which is not that often in
  practice.
  """
  # Note that TaskRequest may not be stored in the datastore yet, but it is
  # fully populated at this point.
  exp_ts = request.created_ts
  futures = []
  for i in range(request.num_task_slices):
    t = request.task_slice(i)
    exp_ts += datetime.timedelta(seconds=t.expiration_secs)
    # Run old and new implementation concurrently. The old one is still used
    # for real, the new one just used to pre-fill entities before the switch
    # to use it for real.
    futures.append(_assert_task_props_old_async(t.properties, exp_ts))
    futures.append(
        _assert_task_dimensions_async(t.properties.dimensions, exp_ts))
  yield futures


def probably_has_capacity(dimensions):
  """Returns True if there is likely a live bot to serve this request.

  There's a risk of collision, that is it could return True even if there is no
  capacity. The risk is of 2^30 different dimensions sets.

  It is only looking at the task queues, not at the actual bots.

  Returns:
    True or False if the capacity is cached, None otherwise.
  """
  dimensions_hash = str(hash_dimensions(dimensions))
  # Sadly, the fact that the key is not there doesn't mean that this task queue
  # is dead. For example:
  # - memcache could have been cleared manually or could be malfunctioning.
  # - in the case where a single bot can service the dimensions, the bot may not
  #   have been polling for N+1 seconds.
  return memcache.get(dimensions_hash, namespace='task_queues_tasks')


def set_has_capacity(dimensions, seconds):
  """Registers the fact that this task request dimensions set has capacity.

  Arguments:
    seconds: the amount of time this 'fact' should be kept.
  """
  dimensions_hash = str(hash_dimensions(dimensions))
  memcache.set(
      dimensions_hash, True, time=seconds, namespace='task_queues_tasks')


@ndb.tasklet
def rebuild_task_cache_async(payload):
  """Rebuilds the TaskDimensions cache.

  This function is called in two cases:
  - A new kind of task request dimensions never seen before
  - The TaskDimensions.valid_until_ts is close to expiration

  It is a cache miss, query all the bots and check for the ones which can run
  the task.

  Warning: There's a race condition, where the TaskDimensions query could be
  missing some instances due to eventual consistency in the BotInfo query. This
  only happens when there's new request dimensions set AND a bot that can run
  this task recently showed up.

  Runtime expectation: the scale on the number of bots that can run the task,
  via BotInfo.dimensions_flat filtering. As there can be tens of thousands of
  bots that can run the task, this can take a long time to store all the
  entities on a new kind of request. As such, it must be called in the backend.

  Arguments:
  - payload: dict as created in assert_task_async() with:
    - 'dimensions': dict of task dimensions to refresh
    - 'dimensions_hash': precalculated hash for dimensions
    - 'valid_until_ts': expiration_ts + _EXTEND_VALIDITY for how long this cache
      is valid

  Returns:
    True if everything was processed, False if it needs to be retried.
  """
  data = json.loads(payload)
  logging.debug('rebuild_task_cache(%s)', data)
  task_dimensions = data[u'dimensions']
  task_dimensions_hash = int(data[u'dimensions_hash'])
  valid_until_ts = utils.parse_datetime(data[u'valid_until_ts'])
  task_dimensions_key = _get_task_dimensions_key(task_dimensions_hash,
                                                 task_dimensions)
  now = utils.utcnow()

  expanded_task_dimensions_flats = expand_dimensions_to_flats(task_dimensions)

  success = False
  try:
    # Scan through matching bots and create/refresh their BotTaskDimensions.
    yield [
        _refresh_all_BotTaskDimensions_async(now, valid_until_ts, df,
                                             task_dimensions_hash)
        for df in expanded_task_dimensions_flats
    ]
    # Done updating, now add expanded_task_dimensions_flats to TaskDimensions.
    success = yield _refresh_TaskDimensions_async(
        now, valid_until_ts, expanded_task_dimensions_flats,
        task_dimensions_key)
  finally:
    # Any of the calls above could throw. Log how far long we processed.
    duration = (utils.utcnow()-now).total_seconds()
    logging.debug(
        'rebuild_task_cache(%d) in %.3fs\n%s\ndimensions_flat size=%d',
        task_dimensions_hash, duration, task_dimensions,
        len(expanded_task_dimensions_flats))
  raise ndb.Return(success)


def cron_tidy_tasks():
  """Removes stale TaskDimensions."""
  _tidy_stale_TaskDimensions().get_result()


def cron_tidy_bots():
  """Removes stale BotTaskDimensions."""
  _tidy_stale_BotTaskDimensions().get_result()


def update_bot_matches_async(payload):
  """Assigns new task dimension set to matching bots.

  Task queue task handler, part of assert_task_async(...) implementation.
  """
  logging.info('TQ task payload:\n%s', payload)
  payload = json.loads(payload)
  return _tq_update_bot_matches_async(
      payload['task_sets_id'], payload['dimensions'],
      utils.parse_datetime(payload['enqueued_ts']))


def rescan_matching_task_sets_async(payload):
  """A task queue task that finds all matching TaskDimensionsSets for a bot."""
  logging.info('TQ task payload:\n%s', payload)
  payload = json.loads(payload)
  return _tq_rescan_matching_task_sets_async(payload['bot_id'],
                                             payload['rescan_counter'],
                                             payload['rescan_reason'])


@ndb.tasklet
def tidy_task_dimension_sets_async():
  """Removes expired task dimension sets from the datastore.

  Returns:
    True if cleaned up everything, False if something failed.
  """
  log = _Logger('tidy_task_dims')

  updated = []

  @ndb.tasklet
  def cleanup_async(dim_info):
    sets_id = dim_info.key.parent().string_id()
    try:
      cleaned = yield _cleanup_task_dimensions_async(dim_info, log)
      if cleaned:
        updated.append(sets_id)
      raise ndb.Return(True)
    except (apiproxy_errors.DeadlineExceededError, datastore_utils.CommitError):
      log.warning('error cleaning %s', sets_id)
      raise ndb.Return(False)

  q = TaskDimensionsInfo.query(
      TaskDimensionsInfo.next_cleanup_ts < utils.utcnow())
  ok = yield _map_async([(q, log)], cleanup_async)
  log.info('cleaned %d', len(updated))

  # Retry the whole thing if something failed or the query timed out. This is
  # faster than waiting for the next cron tick. Will also give some monitoring
  # signal.
  if not ok:
    log.error('need a retry')
  raise ndb.Return(ok)
