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

import datetime
import hashlib
import json
import logging
import random
import struct

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

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
# cron_tidy_stale. This is needed to reduce contention between cron_tidy_stale
# and assert_bot_async. assert_bot_async will delete BotTaskDimensions as soon
# as their expire, and cron_tidy_stale will delete remaining ones (e.g. dead
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
    s = self._match_request_flat(task_dimensions_flat)
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

  def match_request(self, dimensions):
    """Confirms that this instance actually stores this set.

    Note that `dimensions` values here may still contain "|" inside.

    Returns:
      Matching TaskDimensionsSet or None if there's no match.
    """
    flat = []
    for k, values in dimensions.items():
      for v in values:
        flat.append(u'%s:%s' % (k, v))
    # TODO(vadimsh): This is wrong. _match_request_flat expects flattened
    # dimensions with "|" already gone, but `flat` may still have "|" in them.
    return self._match_request_flat(flat)

  def match_bot(self, bot_dimensions_set):
    """Returns the TaskDimensionsSet that matches bot_dimensions_set or None.

    Arguments:
      - bot_dimensions_set a set of "key:value" pairs with bot dimensions.
    """
    for s in self.sets:
      if bot_dimensions_set.issuperset(s.dimensions_flat):
        return s
    return None

  def _match_request_flat(self, task_dimensions_flat):
    """Returns the stored TaskDimensionsSet that equals task_dimensions_flat."""
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
  obj = yield task_dimensions_key.get_async()
  if not obj:
    obj = TaskDimensions(key=task_dimensions_key)
    outcome = 'created'
  # Force all elements in `task_dimensions_flats` to be evaluated
  updated = [
      obj.assert_request(now, valid_until_ts, df)
      for df in task_dimensions_flats
  ]
  if any(updated):
    if obj.sets:
      outcome = outcome or 'updated'
      yield obj.put_async()
    else:
      outcome = 'deleted'
      yield obj.key.delete_async()
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
  while (yield qit.has_next_async()):
    key = qit.next()
    cleanup_by_root.setdefault(key.root, []).append(key)

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
  logging.info('_tidy_stale_TaskDimensions(): deleted %d', sum(deleted))


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
  while (yield qit.has_next_async()):
    key = qit.next()
    cleanup_by_root.setdefault(key.root, []).append(key)

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
  logging.info('_tidy_stale_BotTaskDimensions(): deleted %d', sum(deleted))


@ndb.tasklet
def _assert_task_props_async(properties, expiration_ts):
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
  obj = yield task_dimensions_key.get_async()
  if obj:
    # Reduce the check to be 5~10 minutes earlier to help reduce an attack on
    # task queues when there's a strong on-going load of tasks happening. This
    # jitter is essentially removed from _EXTEND_VALIDITY window.
    jitter = datetime.timedelta(seconds=random.randint(5*60, 10*60))
    valid_until_ts = expiration_ts - jitter
    s = obj.match_request(properties.dimensions)
    if s:
      if s.valid_until_ts >= valid_until_ts:
        logging.debug('assert_task_async(%d): hit. valid_until_ts(%s)',
                      dimensions_hash, s.valid_until_ts)
        raise ndb.Return(None)
      logging.info(
          'assert_task_async(%d): set.valid_until_ts(%s) < expected(%s); '
          'triggering rebuild-task-cache', dimensions_hash, s.valid_until_ts,
          valid_until_ts)
    else:
      # TaskDimensions(dimensions_hash) actually contains a list of dimension
      # dicts all matching `dimensions_hash` (regardless of collisions). The
      # enqueued task will just add entries to this list.
      logging.info(
          'assert_task_async(%d): failed to match the dimensions; triggering '
          'rebuild-task-cache', dimensions_hash)
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
  obj = yield task_dimensions_key.get_async()
  if obj and not any(
      obj.assert_request(now, valid_until_ts, df)
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


@ndb.tasklet
def assert_bot_async(bot_root_key, bot_dimensions):
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
  obj = yield ndb.Key(BotDimensions, 1, parent=bot_root_key).get_async()
  if (obj and obj.dimensions_flat == bot_dimensions_to_flat(bot_dimensions) and
      obj.valid_until_ts > now):
    # Cache hit, no need to look further.
    logging.debug(
        'assert_bot_async: cache hit. bot_id: %s, valid_until: %s, '
        'bot_dimensions: %s', bot_root_key.string_id(), obj.valid_until_ts,
        bot_dimensions)
    raise ndb.Return(None)

  matches = yield _rebuild_bot_cache_async(bot_dimensions, bot_root_key)
  raise ndb.Return(matches)


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
    futures.append(_assert_task_props_async(t.properties, exp_ts))
  yield futures


def get_queues(bot_root_key):
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
    logging.debug(
        'get_queues(%s): can run from %d queues (memcache)\n%s',
        bot_id, len(dimensions_hashes), dimensions_hashes)
    # Refresh all the keys.
    memcache.set_multi(
        {str(d): True for d in dimensions_hashes},
        time=61, namespace='task_queues_tasks')
    return dimensions_hashes

  # Retrieve all the dimensions_hash that this bot could run that have
  # actually been triggered in the past. Since this is under a root entity, this
  # should be fast.
  now = utils.utcnow()
  dimensions_hashes = sorted(
      obj.key.integer_id()
      for obj in BotTaskDimensions.query(ancestor=bot_root_key)
      if obj.valid_until_ts >= now)
  memcache.set(
      bot_id,
      dimensions_hashes,
      namespace='task_queues',
      time=_EXPIRATION_TIME_TASK_QUEUES)
  logging.info(
      'get_queues(%s): Query in %.3fs: can run from %d queues\n%s',
      bot_id, (utils.utcnow()-now).total_seconds(),
      len(dimensions_hashes), dimensions_hashes)
  memcache.set_multi(
      {str(d): True for d in dimensions_hashes},
      time=61, namespace='task_queues_tasks')
  return dimensions_hashes


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


def cron_tidy_stale():
  """Removes stale BotTaskDimensions and TaskDimensions."""
  # TODO(vadimsh): This can be two separate crons (to avoid OOMing).
  for f in [_tidy_stale_TaskDimensions(), _tidy_stale_BotTaskDimensions()]:
    f.get_result()
