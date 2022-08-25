# coding=utf-8
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Task entity that describe when a task is to be scheduled.

This module doesn't do the scheduling itself. It only describes the tasks ready
to be scheduled.

Graph of the schema:

    +--------Root-------+
    |TaskRequest        |                                        task_request.py
    |  +--------------+ |
    |  |TaskProperties| |
    |  |  +--------+  | |
    |  |  |FilesRef|  | |
    |  |  +--------+  | |
    |  +--------------+ |
    |id=<based on epoch>|
    +-------------------+
        |
        |
        v
    +--------------+     +--------------+
    |TaskToRunShard| ... |TaskToRunShard|
    |id=<composite>| ... |id=<composite>|
    +--------------+     +--------------+
"""

import collections
import datetime
import heapq
import logging
import random
import time

from google.appengine.api import datastore_errors
from google.appengine.runtime import apiproxy_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import bot_management
from server import config
from server import task_pack
from server import task_queues
from server import task_request
from server.constants import OR_DIM_SEP
import ts_mon_metrics


### Models.

N_SHARDS = 16  # the number of TaskToRunShards


class _TaskToRunBase(ndb.Model):
  """Defines a TaskRequest slice ready to be scheduled on a bot.

  Each TaskRequest results in one or more TaskToRunShard entities (one per
  slice). They are created sequentially, one by one, as the task progresses
  through its slices. Each TaskToRunShard is eventually either picked up by
  a bot for execution or expires. Each TaskToRunShard picked up for execution
  has an associated task_result.TaskResult entity (expired ones don't).

  A TaskToRunShard can be in two states:
  - reapable: queue_number and expiration_ts are both not None.
  - consumed: queue_number and expiration_ts are both None.

  The entity starts its life in reapable state and then transitions to consumed
  state either by being picked up by a bot for execution or when it expires.
  Consumed state is final.

  This entity must be kept small and contain the minimum amount of data (in
  particular indexes) for two reasons:
  - It is created and transactionally mutated at high QPS.
  - Each bot_reap_task(...) runs a datastore scan for available TaskToRunShard.

  The key id is:
  - lower 4 bits is the try number. The only supported values are 1.
  - next 5 bits are TaskResultSummary.current_task_slice (shifted by 4 bits).
  - rest is 0.

  TODO(crbug.com/1065101): Try number is always 1 now. Swarming-level retries
  have been removed. Anything related to try numbers is dead code that needs to
  be cleaned up.

  This is a base class of TaskToRunShard, and get_shard_kind() should be used
  to get a TaskToRunShard kind. The shard number is derived deterministically by
  calculating dimensions hash % N_SHARDS.
  """
  # This entity is used in transactions. It is not worth using either cache.
  # https://cloud.google.com/appengine/docs/standard/python/ndb/cache
  _use_cache = False
  _use_memcache = False

  # Used to know when the entity is enqueued. The very first TaskToRunShard
  # has the same value as TaskRequest.created_ts, but following ones (when using
  # task slices) have created_ts set at the time the new entity is created.
  created_ts = ndb.DateTimeProperty(indexed=False)

  # Copy of dimensions from the corresponding task slice of TaskRequest.
  # Used to quickly check if a bot can reap this TaskToRunShard right after
  # fetching it from a datastore query.
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict,
                                                         indexed=False)

  # Everything above is immutable, everything below is mutable.

  # Moment by which this TaskToRunShard has to be claimed by a bot.
  # It is based on TaskSlice.expiration_ts. It is used to figure out when to
  # fallback on the next task slice. It is scanned by a cron job and thus needs
  # to be indexed.
  # Reset to None when the TaskToRunShard is consumed.
  expiration_ts = ndb.DateTimeProperty()

  # Delay from the expiration_ts to the actual expired time in seconds.
  # This is set at expiration process. Exclusively for monitoring.
  expiration_delay = ndb.FloatProperty(indexed=False)

  # Priority and request creation timestamp are mixed together to allow queries
  # to order the results by this field to allow sorting by priority first, and
  # then timestamp. See _gen_queue_number() for details. This value is only set
  # when the task is available to run, i.e.
  # ndb.TaskResult.query(ancestor=self.key).get().state==AVAILABLE.
  # Reset to None when the TaskToRunShard is consumed.
  queue_number = ndb.IntegerProperty()

  @property
  def task_slice_index(self):
    """Returns the TaskRequest.task_slice() index this entity represents."""
    return task_to_run_key_slice_index(self.key)

  @property
  def try_number(self):
    """Returns the try number, 1 or 2."""
    return task_to_run_key_try_number(self.key)

  @property
  def is_reapable(self):
    """Returns True if the task is ready to be scheduled."""
    return bool(self.queue_number)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is parent to the task to run."""
    return task_to_run_key_to_request_key(self.key)

  @property
  def run_result_key(self):
    """Returns the TaskRunResult ndb.Key that will be created for this
    TaskToRunShard once reaped.
    """
    summary_key = task_pack.request_key_to_result_summary_key(
        self.request_key)
    return task_pack.result_summary_key_to_run_result_key(
        summary_key, self.try_number)

  @property
  def task_id(self):
    """Returns an encoded task id for this TaskToRunShard.

    Note: this includes the try_number but not the task_slice_index.
    """
    return task_pack.pack_run_result_key(self.run_result_key)

  def to_dict(self):
    """Purely used for unit testing."""
    out = super(_TaskToRunBase, self).to_dict(exclude=['dimensions'])
    # Consistent formatting makes it easier to reason about.
    if out['queue_number']:
      out['queue_number'] = '0x%016x' % out['queue_number']
    out['try_number'] = self.try_number
    out['task_slice_index'] = self.task_slice_index
    return out

  def _pre_put_hook(self):
    super(_TaskToRunBase, self)._pre_put_hook()

    if self.expiration_ts is None and self.queue_number:
      raise datastore_errors.BadValueError(
          ('%s.queue_number must be None when expiration_ts is None' %
           self.__class__.__name__))


# Instantiate all TaskToRunShard<X> classes.
_TaskToRunShards = {
    shard: type('TaskToRunShard%d' % shard, (_TaskToRunBase, ), {})
    for shard in range(N_SHARDS)
}


def get_shard_kind(shard):
  """Returns a TaskToRunShard kind for the given shard number."""
  assert shard < N_SHARDS, 'Shard number must be < %d, but %d is given' % (
      N_SHARDS, shard)
  return _TaskToRunShards[shard]


### Private functions.


def _gen_queue_number(dimensions_hash, timestamp, priority):
  """Generates a 63 bit packed value used for TaskToRunShard.queue_number.

  Arguments:
  - dimensions_hash: 32 bit integer to classify in a queue.
  - timestamp: datetime.datetime when the TaskRequest was filed in. This value
        is used for FIFO or LIFO ordering (depending on configuration) with a
        100ms granularity; the year is ignored.
  - priority: priority of the TaskRequest. It's a 8 bit integer. Lower is higher
        priority.

  Returns:
    queue_number is a 63 bit integer with dimension_hash, timestamp at 100ms
    resolution plus priority.
  """
  # dimensions_hash should be 32 bits but on AppEngine, which is using 32 bits
  # python, it is silently upgraded to long.
  assert isinstance(dimensions_hash, (int, long)), repr(dimensions_hash)
  assert 0 < dimensions_hash <= 0xFFFFFFFF, hex(dimensions_hash)
  assert isinstance(timestamp, datetime.datetime), repr(timestamp)
  task_request.validate_priority(priority)

  # Ignore the year.

  if config.settings().use_lifo:
    next_year = datetime.datetime(timestamp.year + 1, 1, 1)
    # It is guaranteed to fit 32 bits but upgrade to long right away to ensure
    # assert works.
    t = long(round((next_year - timestamp).total_seconds() * 10.))
  else:
    year_start = datetime.datetime(timestamp.year, 1, 1)
    # It is guaranteed to fit 32 bits but upgrade to long right away to ensure
    # assert works.
    t = long(round((timestamp - year_start).total_seconds() * 10.))

  assert 0 <= t <= 0x7FFFFFFF, (hex(t), dimensions_hash, timestamp, priority)
  # 31-22 == 9, leaving room for overflow with the addition.
  # 0x3fc00000 is the priority mask.
  # It is important that priority mixed with time is an addition, not a bitwise
  # or.
  low_part = (long(priority) << 22) + t
  assert 0 <= low_part <= 0xFFFFFFFF, '0x%X is out of band' % (low_part)
  # int may be 32 bits, upgrade to long for consistency, albeit this is
  # significantly slower.
  high_part = long(dimensions_hash) << 31
  return high_part | low_part


def _queue_number_order_priority(v):
  """Returns the number to be used as a comparison for priority.

  Lower values are more important. The queue priority is the lowest 31 bits,
  of which the top 9 bits are the task priority, and the rest is the timestamp
  which may overflow in the task priority.
  """
  return v.queue_number & 0x7FFFFFFF


def _queue_number_priority(v):
  """Returns the task's priority.

  There's an overflow of 1 bit, as part of the timestamp overflows on the laster
  part of the year, so the result is between 0 and 330. See _gen_queue_number()
  for the details.
  """
  return int(_queue_number_order_priority(v) >> 22)


def _memcache_to_run_key(to_run_key):
  """Encodes TaskToRunShard key as a string to address it in the memcache.

  See Claim for more explanation.
  """
  request_key = task_to_run_key_to_request_key(to_run_key)
  return '%x-%d-%d' % (
      request_key.integer_id(),
      task_to_run_key_try_number(to_run_key),
      task_to_run_key_slice_index(to_run_key))


class _QueryStats(object):
  """Statistics for a yield_next_available_task_to_dispatch() loop."""
  claimed = 0
  mismatch = 0
  stale = 0
  total = 0
  visited = 0

  def __str__(self):
    return ('%d total, %d visited, %d already claimed, %d stale, '
            '%d dimensions mismatch') % (self.total, self.visited, self.claimed,
                                         self.stale, self.mismatch)


def _get_task_to_run_query(dimensions_hash):
  """Returns a ndb.Query of TaskToRunShard within this dimensions_hash queue.
  """
  # dimensions_hash should be 32 bits but on AppEngine, which is using 32 bits
  # python, it is silently upgraded to long.
  assert isinstance(dimensions_hash, (int, long)), repr(dimensions_hash)
  # See _gen_queue_number() as of why << 31. This query cannot use the key
  # because it is not a root entity.
  def _query(kind):
    opts = ndb.QueryOptions(deadline=30)
    return kind.query(default_options=opts).order(
        kind.queue_number).filter(
            kind.queue_number >= (dimensions_hash << 31),
            kind.queue_number < ((dimensions_hash + 1) << 31))

  return [_query(get_shard_kind(dimensions_hash % N_SHARDS))]


class _TaskToRunItem(object):
  __slots__ = ('key', 'ttr')

  def __init__(self, ttr):
    self.key = _queue_number_order_priority(ttr)
    self.ttr = ttr

  def __cmp__(self, other):
    return cmp(self.key, other.key)


class _PriorityQueue(object):
  def __init__(self):
    self._heap = []

  def add_heapified(self, ttr):
    heapq.heappush(self._heap, _TaskToRunItem(ttr))

  def add_unordered(self, ttr):
    self._heap.append(_TaskToRunItem(ttr))

  def heapify(self):
    heapq.heapify(self._heap)

  def pop(self):
    return heapq.heappop(self._heap).ttr

  def size(self):
    return len(self._heap)

  def empty(self):
    return not self._heap


_MC_CLIENT = memcache.Client()


class _ActiveQuery(object):
  DEADLINE = 60

  def __init__(self, query, dim_hash, bot_id, stats, bot_dims_matcher):
    self._query = query
    self._dim_hash = dim_hash
    self._bot_id = bot_id
    self._stats = stats
    self._bot_dims_matcher = bot_dims_matcher
    self._page_size = 10
    self._future = self._fetch_and_filter(None)

  @property
  def dim_hash(self):
    return self._dim_hash

  def ready(self):
    return self._future and self._future.done()

  def page(self):
    results, _cursor, _more = self._future.get_result()
    return results

  def advance(self):
    _results, cursor, more = self._future.get_result()
    if more:
      # We start with a small page size to get pending items ASAP to start
      # processing them sooner. If the backlog is large (i.e. we end up fetching
      # many pages), make pages larger with each iteration to reduce number of
      # datastore calls we make.
      self._page_size = min(50, self._page_size + 5)
      self._future = self._fetch_and_filter(cursor)
    else:
      self._future = None
    return more

  @ndb.tasklet
  def _fetch_and_filter(self, cursor):
    """Fetches a page of query results and filters out unusable items.

    A TaskToRunShard is unusable if either queue_number is None, it was
    already claimed by some other bot based on the state in memcache, or
    dimensions doesn't actually match bot one's.

    Yields:
      ([list of potentially consumable TaskToRunShard], cursor, more).
    """
    fetched, cursor, more = yield self._query.fetch_page_async(
        self._page_size, start_cursor=cursor, deadline=self.DEADLINE)

    self._stats.total += len(fetched)

    # The query is eventually consistent. It is possible TaskToRunShard was
    # already processed and its queue_number was cleared. Filter out all such
    # entries.
    fresh = []
    for ttr in fetched:
      if ttr.is_reapable:
        fresh.append(ttr)
      else:
        self._log('TaskToRunShard %s (slice %d) is stale', ttr.task_id,
                  ttr.task_slice_index)
    self._stats.stale += len(fetched) - len(fresh)

    # Filter out all entries that have already been claimed by other bots. This
    # is an optimization to skip claimed entries as fast as possible. The bots
    # will then contest on remaining entries in Claim.obtain(...), and finally
    # in the datastore transaction.
    available = []
    if fresh:
      keys = [_memcache_to_run_key(ttr.key) for ttr in fresh]
      taken = yield _MC_CLIENT.get_multi_async(keys, namespace=Claim._NAMESPACE)
      for i in xrange(len(keys)):
        if not taken.get(keys[i]):
          available.append(fresh[i])
      self._stats.claimed += len(fresh) - len(available)

    # Older TaskToRunShard entities don't have `dimensions` populated. Back fill
    # them based on TaskRequest. This should be relatively rare and eventually
    # can be removed.
    #
    # TODO(vadimsh): Remove when there are no more hits in production,
    # approximately Sep 7 2022.
    request_keys = [
        task_to_run_key_to_request_key(ttr.key) for ttr in available
        if not ttr.dimensions
    ]
    if request_keys:
      self._log('missing_dims: %r',
                [ttr.task_id for ttr in available if not ttr.dimensions])
      requests = yield ndb.get_multi_async(request_keys)
      idx = 0
      for ttr in available:
        if not ttr.dimensions:
          props = requests[idx].task_slice(ttr.task_slice_index).properties
          ttr.dimensions = props.dimensions
          idx += 1

    # The queue_number hash may have conflicts or the queues returned by
    # get_queues(...) aren't matching the bot anymore (if this cache is stale).
    # Filter out all requests that don't match bot dimensions.
    matched = []
    for ttr in available:
      if self._bot_dims_matcher(ttr.dimensions):
        matched.append(ttr)
      else:
        self._log('TaskToRunShard %s (slice %d) dimensions mismatch',
                  ttr.task_id, ttr.task_slice_index)
    self._stats.mismatch += len(available) - len(matched)

    if fetched:
      self._log(
          'got %d items (%d stale, %d already claimed, '
          '%d mismatch, %d backfilled dims)', len(fetched),
          len(fetched) - len(fresh),
          len(fresh) - len(available),
          len(available) - len(matched), len(request_keys))
    if not more:
      self._log('exhausted')
    raise ndb.Return((matched, cursor, more))

  def _log(self, msg, *args):
    logging.debug('_ActiveQuery(%s, %d): %s', self._bot_id, self._dim_hash,
                  msg % args)


def _yield_potential_tasks(bot_id, pool, stats, bot_dims_matcher):
  """Queries all the known task queues in parallel and yields the task in order
  of priority.

  The ordering is opportunistic, not strict. There's a risk of not returning
  exactly in the priority order depending on index staleness and query execution
  latency. The number of queries is unbounded.

  Yields:
    TaskToRunShard entities, trying to yield the highest priority one first.
    To have finite execution time, starts yielding results once one of these
    conditions are met:
    - 1 second elapsed; in this case, continue iterating in the background
    - First page of every query returned
    - All queries exhausted
  """
  bot_root_key = bot_management.get_root_key(bot_id)
  dim_hashes = task_queues.get_queues(bot_root_key)

  # Keep track how many queues we scan in parallel.
  ts_mon_metrics.on_scheduler_scan(pool, len(dim_hashes))

  try:
    # Start fetching first pages of each per dimension set query. Note that
    # the default ndb.EVENTUAL_CONSISTENCY is used so stale items may be
    # returned. It's handled specifically by consumers of this function.
    queries = []
    for d in dim_hashes:
      for q in _get_task_to_run_query(d):
        queries.append(_ActiveQuery(q, d, bot_id, stats, bot_dims_matcher))

    # Run tasklets for at most 1 sec or until all fetches are done.
    #
    # TODO(vadimsh): Why only 1 sec? This also makes priority-based sorting kind
    # of useless for loaded multi-queue bots, since we can't control what
    # queries manage to finish first within 1 sec.
    start = time.time()
    while (time.time() - start) < 1 and not all(q.ready() for q in queries):
      r = ndb.eventloop.run0()
      if r is None:
        break
      if r > 0:
        time.sleep(r)

    # Log how many items we actually got. On loaded servers it will actually
    # be 0 pretty often, since 1 sec is not that much.
    logging.debug(
        '_yield_potential_tasks(%s): waited %.3fs for %d items from queues '
        '%s', bot_id,
        time.time() - start,
        sum(len(q.page()) for q in queries if q.ready()),
        [q.dim_hash for q in queries])

    # A priority queue with TaskToRunShard ordered by the priority+timestamp,
    # extracted from queue_number using _queue_number_order_priority.
    queue = _PriorityQueue()

    def _poll_queries(queries, use_heap):
      enqueue = queue.add_heapified if use_heap else queue.add_unordered

      # Get items from pages we already fetched and add them into `queue`. We
      # don't block here, just pick what already was fetched.
      pending = []
      changed = False
      for q in queries:
        # Retain in the pending list if still working on some page.
        if not q.ready():
          pending.append(q)
          continue

        runs = q.page()
        if runs:
          # TaskToRunShard submitted within the same 100ms interval at the same
          # priority have the same queue sorting number (see _gen_queue_number).
          # By shuffling them here we reduce chances of multiple bots contesting
          # over the same items. This is effective only for queues with more
          # than 10 tasks per second.
          random.shuffle(runs)

          # Add them to the local priority queue.
          changed = True
          for r in runs:
            enqueue(r)

        # Start fetching the next page, if any.
        if q.advance():
          pending.append(q)
        else:
          # The queue has no more results, don't add it to `pending`.
          changed = True

      # Log the final stats if anything changed.
      if changed:
        logging.debug(
            '_yield_potential_tasks(%s): %s items pending. active queues %s',
            bot_id, queue.size(), [q.dim_hash for q in pending])
      return pending

    # Pick up all first pages we managed to fetch in 1 sec above and put them
    # into the list in whatever order they were fetched.
    queries = _poll_queries(queries, use_heap=False)

    # Transform the list into an actual priority queue before polling from it.
    queue.heapify()

    while not queue.empty() or queries:
      # Grab the top-priority item and let the caller try to process it.
      if not queue.empty():
        yield queue.pop()
      else:
        # No pending items, but there are some pending futures. Run one step of
        # ndb event loop to move things forward.
        ndb.eventloop.run1()
      # Poll for any new query pages, adding fetched items to the local queue.
      # Do it only if the local queue is sufficiently shallow. If it is not,
      # then the consumer is too slow. Adding more items will just result in
      # eventually running out of memory.
      if queue.size() < 1000:
        queries = _poll_queries(queries, use_heap=True)
    logging.debug('_yield_potential_tasks(%s): all queues exhausted', bot_id)

  except apiproxy_errors.DeadlineExceededError as e:
    # This is normally due to: "The API call datastore_v3.RunQuery() took too
    # long to respond and was cancelled."
    # At that point, the Cloud DB index is not able to sustain the load. So the
    # best thing to do is to back off a bit and not return any task to the bot
    # for this poll.
    logging.error(
        'Failed to yield a task due to an RPC timeout. Returning no '
        'task to the bot: %s', e)


### Public API.


def request_to_task_to_run_key(request, try_number, task_slice_index):
  """Returns the ndb.Key for a TaskToRunShard from a TaskRequest."""
  assert 1 <= try_number <= 2, try_number
  assert 0 <= task_slice_index < request.num_task_slices
  h = request.task_slice(task_slice_index).properties.dimensions_hash
  kind = get_shard_kind(h % N_SHARDS)
  return ndb.Key(kind, try_number | (task_slice_index << 4), parent=request.key)


def task_to_run_key_to_request_key(to_run_key):
  """Returns the ndb.Key for a TaskToRunShard from a TaskRequest key."""
  assert to_run_key.kind().startswith('TaskToRunShard'), to_run_key
  return to_run_key.parent()


def task_to_run_key_slice_index(to_run_key):
  """Returns the TaskRequest.task_slice() index this TaskToRunShard entity
  represents as pending.
  """
  return to_run_key.integer_id() >> 4


def task_to_run_key_try_number(to_run_key):
  """Returns the try number, 1 or 2."""
  return to_run_key.integer_id() & 15


def new_task_to_run(request, task_slice_index):
  """Returns a fresh new TaskToRunShard for the task ready to be scheduled.

  Returns:
    Unsaved TaskToRunShard entity.
  """
  assert 0 <= task_slice_index < 64, task_slice_index
  created = request.created_ts
  if task_slice_index:
    created = utils.utcnow()
  # TODO(maruel): expiration_ts is based on request.created_ts but it could be
  # enqueued sooner or later. crbug.com/781021
  offset = 0
  for i in range(task_slice_index + 1):
    offset += request.task_slice(i).expiration_secs
  exp = request.created_ts + datetime.timedelta(seconds=offset)
  dims = request.task_slice(task_slice_index).properties.dimensions
  h = task_queues.hash_dimensions(dims)
  qn = _gen_queue_number(h, request.created_ts, request.priority)
  kind = get_shard_kind(h % N_SHARDS)
  key = request_to_task_to_run_key(request, 1, task_slice_index)
  return kind(key=key,
              created_ts=created,
              dimensions=dims,
              queue_number=qn,
              expiration_ts=exp)


def dimensions_matcher(bot_dimensions):
  """Returns a predicate that can check if request dimensions match bot's ones.

  Assumes request dimensions have been validated already.

  Returns:
    func(request_dimensions) -> bool.
  """
  assert isinstance(bot_dimensions, dict), bot_dimensions
  bot_flat = frozenset(task_queues.bot_dimensions_to_flat(bot_dimensions))

  def matcher(request_dimensions):
    assert isinstance(request_dimensions, dict), request_dimensions
    for key, vals in request_dimensions.iteritems():
      # Here if key='k' and vals=['a', 'b|c'], we should check that
      #   ('k:a' in bot_flat) AND ('k:b' in bot_flat OR 'k:c' in bot_flat)
      for val in vals:
        match = any(u'%s:%s' % (key, variant) in bot_flat
                    for variant in val.split(OR_DIM_SEP))
        if not match:
          logging.warning('Mismatch: bot %r, req %r', bot_dimensions,
                          request_dimensions)
          return False
    return True

  return matcher


class Claim(object):
  """Represents a claim on a TaskToRunShard."""
  _NAMESPACE = 'task_to_run'

  __slots__ = ('_key', '_expiry', '_released')

  @classmethod
  def obtain(cls, to_run_key, duration=15):
    """Atomically marks a TaskToRunShard as "claimed" in the memcache.

    Returns a Claim object if TaskToRunShard was claimed by the caller, or None
    if it was already claimed by someone else. When claimed, the caller has up
    to `duration` seconds to do anything they want with TaskToRunShard until
    someone else is able to claim it. The caller may also release the claim
    sooner by calling `release` on the returned Claim object (or using it as
    a context manager).

    This is essentially a best-effort self-expiring lock. It exists to reduce
    contention in datastore transactions that touch the TaskToRunShard entity,
    in particular in the very hot `yield_next_available_task_to_dispatch` loop.
    It is an optimization. It does not by itself guarantee correctness and
    should not be used for that (use datastore transactions instead).

    Returns:
      Claim if the TaskToRunShard was claimed by us or None if by someone else.
    """
    assert duration > 1, duration
    expiry = utils.time_time() + duration
    key = _memcache_to_run_key(to_run_key)
    if not memcache.add(key, True, time=duration, namespace=cls._NAMESPACE):
      return None
    return cls(key, expiry)

  @classmethod
  def check(cls, to_run_key):
    """Returns True if there's an existing claim on TaskToRunShard."""
    key = _memcache_to_run_key(to_run_key)
    val = ndb.get_context().memcache_get(key,
                                         namespace=cls._NAMESPACE).get_result()
    return val is not None

  def __init__(self, key, expiry):
    self._key = key
    self._expiry = expiry
    self._released = False

  def release(self):
    """Releases the claim prematurely.

    This call is totally optional and should be used **only** if it is OK
    for the TaskToRunShard to be claimed by someone else immediately.

    In particular, if the TaskToRunShard reached its final consumed state (and
    won't be mutated anymore), it is fine (and even faster) to not explicitly
    release the claim and let it expire naturally.

    Note: this is a best-effort method and it may theoretically release "wrong"
    claim if the memcache was flushed recently, but this is fine, since the
    entire Claim mechanism is best effort and should not be used to guarantee
    correctness (use datastore transactions for that).
    """
    # Don't touch soon-to-be-expired claims: there is a high chance that our
    # indiscriminate memcache.delete(...) below will actually delete a wrong
    # claim set by someone else after our claim expires. Note that 2 sec was
    # picked arbitrarily.
    if not self._released and self._expiry - utils.time_time() > 2:
      memcache.delete(self._key, namespace=self._NAMESPACE)
      self._released = True

  def __enter__(self):
    return self

  def __exit__(self, _exc_type, _exc_val, _exc_tb):
    self.release()


def yield_next_available_task_to_dispatch(bot_id, pool, bot_dims_matcher):
  """Yields next available TaskToRunShard in roughly decreasing order of
  priority.

  All yielded items are already claimed via Claim.obtain(...). The caller should
  do necessary datastore transactions to finalize the assignment and remove
  TaskToRunShard from the queue.

  Arguments:
  - bot_id: id of the bot to poll tasks for.
  - pool: this bot's pool for monitoring metrics.
  - bot_dims_matcher: a predicate that checks if task dimensions match bot's
      dimensions.
  """
  now = utils.utcnow()
  stats = _QueryStats()
  try:
    for ttr in _yield_potential_tasks(bot_id, pool, stats, bot_dims_matcher):
      # Stop searching after too long, since the odds of the request blowing
      # up right after succeeding in reaping a task is not worth the dangling
      # task request that will stay in limbo until the cron job reaps it and
      # retry it. The current handlers are given 60s to complete. By limiting
      # search to 40s, it gives 20s to complete the reaping and complete the
      # HTTP request.
      if (utils.utcnow() - now).total_seconds() > 40.:
        logging.debug(
            'yield_next_available_task_to_dispatch(%s): exit by timeout',
            bot_id)
        return

      # Try to claim this TaskToRunShard. Only one bot will pass this. It then
      # will have ~15s to submit a transaction that assigns the TaskToRunShard
      # to this bot before another bot will be able to try that.
      if Claim.obtain(ttr.key):
        logging.debug(
            'yield_next_available_task_to_dispatch(%s): ready to reap %s',
            bot_id, ttr.task_id)
        stats.visited += 1
        yield ttr

  finally:
    logging.debug(
        'yield_next_available_task_to_dispatch(%s) in %.3fs: %s',
        bot_id, (utils.utcnow() - now).total_seconds(), stats)
    ts_mon_metrics.on_scheduler_visits(pool=pool,
                                       claimed=stats.claimed,
                                       mismatch=stats.mismatch,
                                       stale=stats.stale,
                                       total=stats.total,
                                       visited=stats.visited)


def yield_expired_task_to_run():
  """Yields the expired TaskToRunShard still marked as available."""
  # The query fetches tasks that reached expiration time recently
  # to avoid fetching all past tasks. It uses a large batch size
  # since the entities are very small and to reduce RPC overhead.
  def _query(kind):
    now = utils.utcnow()
    # The backsearch here is just to ensure that we find entities that we forgot
    # about before because the cron job couldn't keep up. In practice
    # expiration_ts should not be more than 1 minute old (as the cron job runs
    # every minutes) but keep it high in case there's an outage.
    cut_off = now - datetime.timedelta(hours=24)
    return kind.query(kind.expiration_ts < now,
                      kind.expiration_ts > cut_off,
                      default_options=ndb.QueryOptions(batch_size=256))

  total = 0
  try:
    for shard in range(N_SHARDS):
      for task in _query(get_shard_kind(shard)):
        yield task
        total += 1
  finally:
    logging.debug('Yielded %d tasks', total)


def get_task_to_runs(request, slice_until):
  """Get TaskToRunShard entities by TaskRequest"""
  shards = set()
  for slice_index in range(slice_until + 1):
    if len(request.task_slices) <= slice_index:
      break
    h = request.task_slice(slice_index).properties.dimensions_hash
    shards.add(h % N_SHARDS)

  to_runs = []
  for shard in shards:
    runs = get_shard_kind(shard).query(ancestor=request.key).fetch()
    to_runs.extend(runs)

  return to_runs
