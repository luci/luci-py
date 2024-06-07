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
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import datastore_utils
from components import utils

from proto.config import pools_pb2
from server import bot_management
from server import config
from server import rbe
from server import task_pack
from server import task_queues
from server import task_request
from server.constants import OR_DIM_SEP
import ts_mon_metrics


### Models.

N_SHARDS = 16  # the number of TaskToRunShards

# Swarming used to have internal retries. These were removed and now the only
# permitted value is 1
# see https://crbug.com/1065101
LEGACY_TRY_NUMBER = 1


class _TaskToRunBase(ndb.Model):
  """Defines a TaskRequest slice ready to be scheduled on a bot.

  Each TaskRequest results in one or more TaskToRunShard entities (one per
  slice). They are created sequentially, one by one, as the task progresses
  through its slices. Each TaskToRunShard is eventually either picked up by
  a bot for execution or expires. Each TaskToRunShard picked up for execution
  has an associated task_result.TaskResult entity (expired ones don't).

  A TaskToRunShard can either be in "native mode" (dispatched via the native
  Swarming scheduler implemented in this code base) or in "RBE mode" (dispatched
  via the remote RBE scheduler service). This is controlled by rbe_reservation
  field.

  TODO(vadimsh): Does it make sense to support "whichever scheduler grabs first"
  mode for tasks? It isn't too hard to implemented.

  A TaskToRunShard (regardless of mode) can be in two states:
  - reapable
    - Native mode: queue_number and expiration_ts are both not None.
    - RBE mode: claim_id is None and expiration_ts is not None.
  - consumed:
    - Native mode: queue_number and expiration_ts are both None.
    - RBE mode: claim_id is not None and expiration_ts is None.

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

  This is a base class of TaskToRunShard, and get_shard_kind() should be used
  to get a TaskToRunShard kind. The shard number is derived deterministically by
  calculating dimensions hash % N_SHARDS.
  """
  # This entity is used in transactions. It is not worth using either cache.
  # https://cloud.google.com/appengine/docs/standard/python/ndb/cache
  _use_cache = False
  _use_memcache = False

  # Used to know when the entity is enqueued.
  #
  # The very first TaskToRunShard has the same value as TaskRequest.created_ts,
  # but following ones (when using task slices) have created_ts set at the time
  # the new entity is created.
  #
  # Used in both native and RBE mode.
  created_ts = ndb.DateTimeProperty(indexed=False)

  # Copy of dimensions from the corresponding task slice of TaskRequest.
  #
  # Used to quickly check if a bot can reap this TaskToRunShard right after
  # fetching it from a datastore query.
  #
  # Used in both native and RBE mode.
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict,
                                                         indexed=False)

  # RBE reservation name that is (or will be) handling this TaskToRunShard.
  #
  # If set, then TaskToRunShard is in RBE mode. If not, then in native
  # mode. TaskToRunShard in RBE mode are always (transactionally) created with
  # a Task Queue task to actually dispatch them to RBE scheduler.
  #
  # It is derived from TaskRequest and slice index in new_task_to_run.
  rbe_reservation = ndb.StringProperty(required=False, indexed=False)

  # Everything above is immutable, everything below is mutable.

  # Moment by which this TaskToRunShard has to be claimed by a bot.
  #
  # Used in both native and RBE mode.
  #
  # It is based on TaskSlice.expiration_ts. It is used to figure out when to
  # fallback on the next task slice. It is scanned by a cron job and thus needs
  # to be indexed.
  #
  # Reset to None when the TaskToRunShard is consumed.
  expiration_ts = ndb.DateTimeProperty()

  # Delay from the expiration_ts to the actual expired time in seconds.
  #
  # Used in both native and RBE mode.
  #
  # This is set at expiration process if the slice expired by reaching its
  # deadline. Unset if the slice expired because there were no bots that could
  # run it.
  #
  # Exclusively for monitoring.
  expiration_delay = ndb.FloatProperty(indexed=False)

  # A magical number by which bots and tasks find one another.
  #
  # Used only in native mode. Always None and unused in RBE mode.
  #
  # Priority and request creation timestamp are mixed together to allow queries
  # to order the results by this field to allow sorting by priority first, and
  # then timestamp. See _gen_queue_number() for details. This value is only set
  # when the task is available to run, i.e.
  # ndb.TaskResult.query(ancestor=self.key).get().state==AVAILABLE.
  #
  # Reset to None when the TaskToRunShard is consumed.
  queue_number = ndb.IntegerProperty()

  # A non-None if some bot claimed this TaskToRunShard and will execute it.
  #
  # Used only in RBE mode. Always None in native mode.
  #
  # It is an opaque ID supplied by the bot when it attempts to claim this
  # entity. If TaskToRunShard is already claimed and `claim_id` matches the one
  # supplied by the bot, then it means this bot has actually claimed the entity
  # already and now just retries the call.
  #
  # Never reset once set.
  claim_id = ndb.StringProperty(required=False, indexed=False)

  def consume(self, claim_id):
    """Moves TaskToRun into non-reapable state (e.g. when canceling)."""
    self.claim_id = claim_id
    self.expiration_ts = None
    self.queue_number = None

  @property
  def shard_index(self):
    """Returns the index of TaskToRunShard extracting it from the key."""
    kind = self.key.kind()
    assert kind.startswith('TaskToRunShard'), kind
    return int(kind[14:])

  @property
  def task_slice_index(self):
    """Returns the TaskRequest.task_slice() index this entity represents."""
    return task_to_run_key_slice_index(self.key)

  @property
  def is_reapable(self):
    """Returns True if the task is ready to be picked up for execution."""
    return self.expiration_ts is not None

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
    return task_pack.result_summary_key_to_run_result_key(summary_key)

  @property
  def task_id(self):
    """Returns an encoded task id for this TaskToRunShard."""
    return task_pack.pack_run_result_key(self.run_result_key)

  def to_dict(self):
    """Purely used for unit testing."""
    out = super(
        _TaskToRunBase,
        self).to_dict(exclude=['claim_id', 'dimensions', 'rbe_reservation'])
    # Consistent formatting makes it easier to reason about.
    if out['queue_number']:
      out['queue_number'] = '0x%016x' % out['queue_number']
    out['task_slice_index'] = self.task_slice_index
    return out

  def _pre_put_hook(self):
    super(_TaskToRunBase, self)._pre_put_hook()
    if self.rbe_reservation is None:
      ex = self.expiration_ts is not None
      qn = self.queue_number is not None
      if ex != qn:
        raise datastore_errors.BadValueError(
            'TaskToRun: queue_number and expiration_ts should either be both '
            'set or unset')


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


def _gen_queue_number(dimensions_hash, timestamp, priority,
                      scheduling_algorithm):
  """Generates a 63 bit packed value used for TaskToRunShard.queue_number.

  Arguments:
    dimensions_hash: 32 bit integer to classify in a queue.
    timestamp: datetime.datetime when the TaskRequest was filed in. This value
        is used for FIFO or LIFO ordering (depending on configuration) with a
        100ms granularity; the year is ignored.
    priority: priority of the TaskRequest. It's a 8 bit integer. Lower is higher
        priority.
    scheduling_algorithm: The algorithm to use to schedule this task,
        using the Pool.SchedulingAlgorithm enum in pools.proto.

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

  if scheduling_algorithm == pools_pb2.Pool.SCHEDULING_ALGORITHM_LIFO:
    next_year = datetime.datetime(timestamp.year + 1, 1, 1)
    # It is guaranteed to fit 32 bits but upgrade to long right away to ensure
    # assert works.
    t = long(round((next_year - timestamp).total_seconds() * 10.))
  else:
    # Default scheduling algorithm is FIFO to avoid confusion.
    assert scheduling_algorithm in (
        pools_pb2.Pool.SCHEDULING_ALGORITHM_UNKNOWN,
        pools_pb2.Pool.SCHEDULING_ALGORITHM_FIFO,
    ), ('Unknown Pool.SchedulingAlgorithm: %d' % scheduling_algorithm)
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
  return '%x-%d-%d' % (request_key.integer_id(), LEGACY_TRY_NUMBER,
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
    return kind.query().order(kind.queue_number).filter(
        kind.queue_number >= (dimensions_hash << 31), kind.queue_number <
        ((dimensions_hash + 1) << 31))

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
  def __init__(self, query, dim_hash, bot_id, stats, bot_dims_matcher,
               deadline):
    self._query = query
    self._dim_hash = dim_hash
    self._bot_id = bot_id
    self._stats = stats
    self._bot_dims_matcher = bot_dims_matcher
    self._deadline = deadline
    self._canceled = False
    self._pages = 0
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

  def cancel(self):
    self._canceled = True

  @property
  def canceled(self):
    return self._canceled

  @ndb.tasklet
  def _fetch_and_filter(self, cursor):
    """Fetches a page of query results and filters out unusable items.

    A TaskToRunShard is unusable if either queue_number is None, it was
    already claimed by some other bot based on the state in memcache, or
    dimensions doesn't actually match bot one's.

    Yields:
      ([list of potentially consumable TaskToRunShard], cursor, more).
    """
    deadline = (self._deadline - utils.utcnow()).total_seconds()
    if not self._canceled and deadline < 1.0:
      self._log('not enough time to make an RPC, canceling this query')
      self._canceled = True

    if not self._canceled:
      try:
        self._pages += 1
        self._log('fetching page #%d (deadline %.3fs)', self._pages, deadline)
        fetched, cursor, more = yield self._query.fetch_page_async(
            self._page_size, start_cursor=cursor, deadline=deadline)
      except (apiproxy_errors.DeadlineExceededError, datastore_errors.Timeout):
        # TODO(vadimsh): Maybe it makes sense to use a smaller RPC deadline and
        # instead retry the call a bunch of times until reaching the overall
        # deadline?
        self._log('RPC deadline exceeded, canceling this query')
        self._canceled = True

    # Exit ASAP if the query was cancelled while waiting in fetch_page_async.
    if self._canceled:
      raise ndb.Return(([], None, False))

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

    # The queue_number hash may have conflicts or the queues being polled aren't
    # matching the bot anymore (if this cache is stale). Filter out all requests
    # that don't match bot dimensions.
    matched = []
    for ttr in available:
      if self._bot_dims_matcher(ttr.dimensions):
        matched.append(ttr)
      else:
        self._log('TaskToRunShard %s (slice %d) dimensions mismatch',
                  ttr.task_id, ttr.task_slice_index)
    self._stats.mismatch += len(available) - len(matched)

    if fetched:
      self._log('got %d items (%d stale, %d already claimed, %d mismatch)',
                len(fetched),
                len(fetched) - len(fresh),
                len(fresh) - len(available),
                len(available) - len(matched))
    if not more:
      self._log('exhausted')
    raise ndb.Return((matched, cursor, more))

  def _log(self, msg, *args):
    logging.debug('_ActiveQuery(%s, %d): %s', self._bot_id, self._dim_hash,
                  msg % args)


def _yield_potential_tasks(bot_id, pool, queues, stats, bot_dims_matcher,
                           deadline):
  """Queries given task queues in parallel and yields the tasks in order
  of priority until all queues are exhausted or the deadline is reached.

  The ordering is opportunistic, not strict. There's a risk of not returning
  exactly in the priority order depending on index staleness and query execution
  latency. The number of queries is unbounded.

  Arguments:
    bot_id: id of the bot to poll tasks for.
    pool: this bot's pool for monitoring metrics.
    queues: a list of integers with dimensions hashes of queues to poll.
    stats: a _QueryStats object to update in-place.
    bot_dims_matcher: a predicate that checks if task dimensions match bot's
        dimensions.
    deadline: datetime.datetime when to give up.

  Yields:
    TaskToRunShard entities, trying to yield the highest priority one first.
    To have finite execution time, starts yielding results once one of these
    conditions are met:
    - 1 second elapsed; in this case, continue iterating in the background
    - First page of every query returned
    - All queries exhausted

  Raises:
    ScanDeadlineError if reached the deadline before exhausting queues.
  """
  if utils.utcnow() >= deadline:
    logging.debug('_yield_potential_tasks(%s): skipping due to deadline',
                  bot_id)
    raise ScanDeadlineError('skipped', 'No time left to run the scan at all')

  # Keep track how many queues we scan in parallel.
  ts_mon_metrics.on_scheduler_scan(pool, len(queues))

  # Start fetching first pages of each per dimension set query. Note that
  # the default ndb.EVENTUAL_CONSISTENCY is used so stale items may be
  # returned. It's handled specifically by consumers of this function.
  queries = []
  for d in queues:
    for q in _get_task_to_run_query(d):
      queries.append(
          _ActiveQuery(q, d, bot_id, stats, bot_dims_matcher, deadline))

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
      time.time() - start, sum(len(q.page()) for q in queries if q.ready()),
      [q.dim_hash for q in queries])

  # We may be running of time already if the initial fetch above was
  # particularly slow.
  if utils.utcnow() >= deadline:
    for q in queries:
      q.cancel()
    logging.debug('_yield_potential_tasks(%s): deadline before the poll loop',
                  bot_id)
    raise ScanDeadlineError('initializing', 'Deadline before the poll loop')

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
  active = _poll_queries(queries, use_heap=False)

  # Transform the list into an actual priority queue before polling from it.
  queue.heapify()

  while not queue.empty() or active:
    # Grab the top-priority item and let the caller try to process it.
    if not queue.empty():
      yield queue.pop()
    else:
      # No pending items, but there are some pending futures. Run one step of
      # ndb event loop to move things forward.
      ndb.eventloop.run1()
    # On the overall deadline asynchronously cancel remaining queries and exit.
    if utils.utcnow() >= deadline:
      for q in active:
        q.cancel()
      break
    # Poll for any new query pages, adding fetched items to the local queue.
    # Do it only if the local queue is sufficiently shallow. If it is not,
    # then the consumer is too slow. Adding more items will just result in
    # eventually running out of memory.
    if queue.size() < 1000:
      active = _poll_queries(active, use_heap=True)

  canceled = [q for q in queries if q.canceled]
  if canceled:
    logging.debug(
        '_yield_potential_tasks(%s): deadline in queues %s, dropping %d items',
        bot_id, [q.dim_hash for q in canceled], queue.size())
    raise ScanDeadlineError('fetching', 'Deadline fetching queues')

  if utils.utcnow() >= deadline:
    logging.debug(
        '_yield_potential_tasks(%s): deadline processing, dropping %d items',
        bot_id, queue.size())
    raise ScanDeadlineError('processing', 'Deadline processing fetched items')

  logging.debug('_yield_potential_tasks(%s): all queues exhausted', bot_id)


### Public API.


class ScanDeadlineError(Exception):
  """Raised when yield_next_available_task_to_dispatch reaches the deadline
  before exhausting pending queues.

  Has a string-valued code that is used as a monitoring metric field.
  """

  def __init__(self, code, msg):
    super(ScanDeadlineError, self).__init__(msg)
    self.code = code


def request_to_task_to_run_key(request, task_slice_index):
  """Returns the ndb.Key for a TaskToRunShard from a TaskRequest."""
  assert 0 <= task_slice_index < request.num_task_slices
  h = request.task_slice(task_slice_index).properties.dimensions_hash
  kind = get_shard_kind(h % N_SHARDS)
  return ndb.Key(kind,
                 LEGACY_TRY_NUMBER | (task_slice_index << 4),
                 parent=request.key)


def task_to_run_key_to_request_key(to_run_key):
  """Returns the ndb.Key for a TaskToRunShard from a TaskRequest key."""
  assert to_run_key.kind().startswith('TaskToRunShard'), to_run_key
  return to_run_key.parent()


def task_to_run_key_slice_index(to_run_key):
  """Returns the TaskRequest.task_slice() index this TaskToRunShard entity
  represents as pending.
  """
  return to_run_key.integer_id() >> 4


def task_to_run_key_from_parts(request_key, shard_index, entity_id):
  """Returns TaskToRun key given its parts.

  Arguments:
    request_key: parent TaskRequest entity key.
    shard_index: index of TaskToRunShard entity class.
    entity_id: int64 with TaskToRunShard entity key.
  """
  assert request_key.kind() == 'TaskRequest', request_key
  return ndb.Key(get_shard_kind(shard_index), entity_id, parent=request_key)


def new_task_to_run(request, task_slice_index):
  """Returns a fresh new TaskToRunShard for the task ready to be scheduled.

  Returns:
    Unsaved TaskToRunShard entity.
  """
  assert 0 <= task_slice_index < 64, task_slice_index
  assert request.scheduling_algorithm is not None

  # This is used to track how long TaskToRunShard was pending.
  created = request.created_ts
  if task_slice_index:
    created = utils.utcnow()

  # TODO(vadimsh): These expiration timestamps may end up significantly larger
  # than expected if slices are skipped quickly without waiting due to
  # "no capacity" condition. For example, if 4 slices with 1h expiration all
  # were skipped quickly, the fifth one ends up with effectively +4h of extra
  # expiration time.
  offset = 0
  for i in range(task_slice_index + 1):
    offset += request.task_slice(i).expiration_secs
  exp = request.created_ts + datetime.timedelta(seconds=offset)

  dims = request.task_slice(task_slice_index).properties.dimensions
  h = task_queues.hash_dimensions(dims)
  kind = get_shard_kind(h % N_SHARDS)
  key = request_to_task_to_run_key(request, task_slice_index)

  # Queue number and RBE reservation ID are mutually exclusive.
  queue_number = None
  rbe_reservation = None
  if request.rbe_instance:
    rbe_reservation = rbe.gen_rbe_reservation_id(request, task_slice_index)
  else:
    queue_number = _gen_queue_number(h, request.created_ts, request.priority,
                                     request.scheduling_algorithm)

  return kind(key=key,
              created_ts=created,
              dimensions=dims,
              rbe_reservation=rbe_reservation,
              expiration_ts=exp,
              queue_number=queue_number)


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
  def obtain(cls, to_run_key, duration=60):
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


def yield_next_available_task_to_dispatch(bot_id, pool, queues,
                                          bot_dims_matcher, deadline):
  """Yields next available TaskToRunShard in roughly decreasing order of
  priority.

  All yielded items are already claimed via Claim.obtain(...). The caller should
  do necessary datastore transactions to finalize the assignment and remove
  TaskToRunShard from the queue.

  Arguments:
    bot_id: id of the bot to poll tasks for.
    pool: this bot's pool for monitoring metrics.
    queues: a list of integers with dimensions hashes of queues to poll.
    bot_dims_matcher: a predicate that checks if task dimensions match bot's
        dimensions.
    deadline: datetime.datetime when to give up.

  Raises:
    ScanDeadlineError if reached the deadline before clearing queues.
  """
  now = utils.utcnow()
  stats = _QueryStats()
  try:
    for ttr in _yield_potential_tasks(bot_id, pool, queues, stats,
                                      bot_dims_matcher, deadline):
      # Try to claim this TaskToRunShard. Only one bot will pass this. It then
      # will have ~60s to submit a transaction that assigns the TaskToRunShard
      # to this bot before another bot will be able to try that.
      if Claim.obtain(ttr.key):
        logging.debug(
            'yield_next_available_task_to_dispatch(%s): ready to reap %s',
            bot_id, ttr.task_id)
        stats.visited += 1
        yield ttr
      else:
        logging.debug(
            'yield_next_available_task_to_dispatch(%s): skipping claimed %s',
            bot_id, ttr.task_id)
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


def yield_expired_task_to_run(delay_sec):
  """Yields the expired TaskToRunShard still marked as available."""
  # The query fetches tasks that reached expiration time recently
  # to avoid fetching all past tasks. It uses a large batch size
  # since the entities are very small and to reduce RPC overhead.
  def _query(kind):
    now = utils.utcnow() - datetime.timedelta(seconds=delay_sec)
    # The backsearch here is just to ensure that we find entities that we forgot
    # about before because the cron job couldn't keep up. In practice
    # expiration_ts should not be more than 1 minute old (as the cron job runs
    # every minutes) but keep it high in case there's an outage.
    cut_off = now - datetime.timedelta(hours=24)
    return kind.query(kind.expiration_ts < now,
                      kind.expiration_ts > cut_off,
                      default_options=ndb.QueryOptions(batch_size=256))

  # Shuffle the order in which we visit shards to give all shards equal chance
  # to be visited first (matters if there's a backlog).
  shards = list(range(N_SHARDS))
  random.shuffle(shards)

  total = 0
  try:
    for shard in shards:
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
