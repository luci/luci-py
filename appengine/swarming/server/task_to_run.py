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
        v
    +--------------------+
    |TaskToRun           |
    |id=<dimensions_hash>|
    +--------------------+
"""

import datetime
import logging
import time

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import utils
from server import task_queues
from server import task_request


### Models.


class TaskToRun(ndb.Model):
  """Defines a TaskRequest ready to be scheduled on a bot.

  This specific request for a specific task can be executed multiple times,
  each execution will create a new child task_result.TaskResult of
  task_result.TaskResultSummary.

  This entity must be kept small and contain the minimum data to enable the
  queries for two reasons:
  - it is updated inside a transaction for each scheduling event, e.g. when a
    bot gets assigned this task item to work on.
  - all the ones currently active are fetched at once in a cron job.

  The key id is the value of 'dimensions_hash' that is generated with
  task_queues.hash_dimensions(), parent is TaskRequest.
  """
  # Moment by which the task has to be requested by a bot. Copy of TaskRequest's
  # TaskRequest.expiration_ts to enable queries when cleaning up stale jobs.
  expiration_ts = ndb.DateTimeProperty(required=True)

  # Everything above is immutable, everything below is mutable.

  # priority and request creation timestamp are mixed together to allow queries
  # to order the results by this field to allow sorting by priority first, and
  # then timestamp. See _gen_queue_number() for details. This value is only set
  # when the task is available to run, i.e.
  # ndb.TaskResult.query(ancestor=self.key).get().state==AVAILABLE.
  # If this task it not ready to be scheduled, it must be None.
  queue_number = ndb.IntegerProperty()

  @property
  def is_reapable(self):
    """Returns True if the task is ready to be scheduled."""
    return bool(self.queue_number)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is parent to the task to run."""
    return task_to_run_key_to_request_key(self.key)

  def to_dict(self):
    out = super(TaskToRun, self).to_dict()
    out['dimensions_hash'] = self.key.integer_id()
    return out


### Private functions.


def _gen_queue_number(dimensions_hash, timestamp, priority):
  """Generates a 64 bit packed value used for TaskToRun.queue_number.

  Arguments:
  - dimensions_hash: 31 bit integer to classify in a queue.
  - timestamp: datetime.datetime when the TaskRequest was filed in. This value
        is used for FIFO ordering with a 100ms granularity; the year is ignored.
  - priority: priority of the TaskRequest. It's a 8 bit integer. Lower is higher
        priority.

  Returns:
    queue_number is a 63 bit integer with dimension_hash, timestamp at 100ms
    resolution plus priority.
  """
  assert isinstance(dimensions_hash, (int, long)), repr(dimensions_hash)
  assert dimensions_hash > 0 and dimensions_hash <= 0xFFFFFFFF, hex(
      dimensions_hash)
  assert isinstance(timestamp, datetime.datetime), repr(timestamp)
  assert isinstance(priority, int), repr(priority)
  task_request.validate_priority(priority)
  assert 0 <= priority <= 255, 'Just for clarity, validate_priority() checks it'

  # Ignore the year.
  year_start = datetime.datetime(timestamp.year, 1, 1)
  t = int(round((timestamp - year_start).total_seconds() * 10.))
  assert t >= 0 and t <= 0x7FFFFFFF, (
      hex(t), dimensions_hash, timestamp, priority)
  # 31-22 == 9, leaving room for overflow with the addition.
  # 0x3fc00000 is the priority mask.
  # It is important that priority mixed with time is an addition, not a bitwise
  # or.
  low_part = (priority << 22) + t
  high_part = dimensions_hash << 31
  return high_part | low_part


def _queue_number_priority(q):
  """Returns the number to be used as a comparision for priority.

  The higher the more important.
  """
  return q & 0x7FFFFFFF


def _memcache_to_run_key(task_key):
  """Functional equivalent of task_result.pack_result_summary_key()."""
  request_key = task_to_run_key_to_request_key(task_key)
  return '%x' % request_key.integer_id()


def _lookup_cache_is_taken(task_key):
  """Queries the quick lookup cache to reduce DB operations."""
  assert not ndb.in_transaction()
  key = _memcache_to_run_key(task_key)
  return bool(memcache.get(key, namespace='task_to_run'))


class _QueryStats(object):
  """Statistics for a yield_next_available_task_to_dispatch() loop."""
  broken = 0
  cache_lookup = 0
  deadline = None
  expired = 0
  hash_mismatch = 0
  ignored = 0
  no_queue = 0
  real_mismatch = 0
  too_long = 0
  total = 0

  def __str__(self):
    return (
        '%d total, %d exp %d no_queue, %d hash mismatch, %d cache negative, '
        '%d dimensions mismatch, %d ignored, %d broken, '
        '%d not executable by deadline (UTC %s)') % (
        self.total,
        self.expired,
        self.no_queue,
        self.hash_mismatch,
        self.cache_lookup,
        self.real_mismatch,
        self.ignored,
        self.broken,
        self.too_long,
        self.deadline)


def _validate_query_item(bot_dimensions, deadline, stats, now, task_key):
  """Validates the TaskToRun and update stats.

  Returns:
    None if the task_key cannot be reaped by this bot.
    tuple(TaskRequest, TaskToRun) if this is a good candidate to reap.
  """
  stats.total += 1
  # Verify TaskToRun is what is expected. Play defensive here.
  try:
    validate_to_run_key(task_key)
  except ValueError as e:
    logging.error(str(e))
    stats.broken += 1
    return

  # Do this after the basic weeding out but before fetching TaskRequest.
  if _lookup_cache_is_taken(task_key):
    stats.cache_lookup += 1

  # Ok, it's now worth taking a real look at the entity.
  task = task_key.get(use_cache=False)

  # DB operations are slow, double check memcache again.
  if _lookup_cache_is_taken(task_key):
    stats.cache_lookup += 1

  # It is possible for the index to be inconsistent since it is not executed in
  # a transaction, no problem.
  if not task.queue_number:
    stats.no_queue += 1
    return

  # It expired. A cron job will cancel it eventually. Since 'now' is saved
  # before the query, an expired task may still be reaped even if technically
  # expired if the query is very slow. This is on purpose so slow queries do not
  # cause exagerate expirations.
  if task.expiration_ts < now:
    stats.expired += 1
    return

  # The hash may have conflicts. Ensure the dimensions actually match by
  # verifying the TaskRequest. There's a probability of 2**-31 of conflicts,
  # which is low enough for our purpose. The reason use_cache=False is otherwise
  # it'll create a buffer bloat.
  request = task.request_key.get(use_cache=False)
  if not match_dimensions(request.properties.dimensions, bot_dimensions):
    stats.real_mismatch += 1
    return

  # If the bot has a deadline, don't allow it to reap the task unless it can be
  # completed before the deadline. We have to assume the task takes the
  # theoretical maximum amount of time possible, which is governed by
  # execution_timeout_secs. An isolated task's download phase is not subject to
  # this limit, so we need to add io_timeout_secs. When a task is signalled that
  # it's about to be killed, it receives a grace period as well.
  # grace_period_secs is given by run_isolated to the task execution process, by
  # task_runner to run_isolated, and by bot_main to the task_runner. Lastly, add
  # a few seconds to account for any overhead.
  #
  # Give an exemption to the special terminate task because it doesn't actually
  # run anything.
  if deadline is not None and not request.properties.is_terminate:
    if not request.properties.execution_timeout_secs:
      # Task never times out, so it cannot be accepted.
      stats.too_long += 1
      return
    max_task_time = (request.properties.execution_timeout_secs +
                     (request.properties.io_timeout_secs or 600) +
                     3 * (request.properties.grace_period_secs or 30) +
                     10)
    if deadline <= utils.utcnow() + datetime.timedelta(seconds=max_task_time):
      stats.too_long += 1
      return

  # It's a valid task! Note that in the meantime, another bot may have reaped
  # it.
  return request, task


def _yield_pages_async(q, size):
  """Given a ndb.Query, yields ndb.Future that returns pages of results
  asynchronously.
  """
  next_cursor = [None]
  should_continue = [True]
  def fire(page, result):
    results, cursor, more = page.get_result()
    result.set_result(results)
    next_cursor[0] = cursor
    should_continue[0] = more

  while should_continue[0]:
    page_future = q.fetch_page_async(size, start_cursor=next_cursor[0])
    result_future = ndb.Future()
    page_future.add_immediate_callback(fire, page_future, result_future)
    yield result_future
    result_future.get_result()


def _get_task_to_run_query(dimensions_hash):
  """Returns a ndb.Query of TaskToRun within this dimensions_hash queue."""
  opts = ndb.QueryOptions(keys_only=True, deadline=15)
  return TaskToRun.query(default_options=opts).order(
          TaskToRun.queue_number).filter(
              TaskToRun.queue_number >= (dimensions_hash << 31),
              TaskToRun.queue_number < ((dimensions_hash+1) << 31))


def _yield_potential_tasks(bot_id):
  """Queries all the known task queues in parallel and yields the task in order
  of priority.

  The ordering is opportunistic, not strict. There's a risk of not returning
  exactly in the priority order depending on index staleness and query execution
  latency. The number of queries is unbounded.

  Yields:
    TaskToRun entities, trying to yield the highest priority one first.  To have
    finite execution time, starts yielding results once one of these conditions
    are met:
    - 1 second elapsed; in this case, continue iterating in the background
    - First page of every query returned
    - All queries exhausted
  """
  potential_dimensions_hashes = task_queues.get_queues(bot_id)
  # Note that the default ndb.EVENTUAL_CONSISTENCY is used so stale items may be
  # returned. It's handled specifically by consumers of this function.
  start = time.time()
  queries = [_get_task_to_run_query(d) for d in potential_dimensions_hashes]
  yielders = [_yield_pages_async(q, 10) for q in queries]
  # We do care about the first page of each query so we cannot merge all the
  # results of every query insensibly.
  futures = []

  for y in yielders:
    futures.append(next(y, None))

  while (time.time() - start) < 1 and not all(f.done() for f in futures if f):
    r = ndb.eventloop.run0()
    if r is None:
      break
    time.sleep(r)
  logging.debug(
      'Waited %.3fs for %d futures, %d completed',
      time.time() - start, sum(1 for f in futures if f.done()), len(futures))
  items = []
  for i, f in enumerate(futures):
    if f and f.done():
      items.extend(f.get_result())
      futures[i] = next(yielders[i], None)
  items.sort(key=lambda k: _queue_number_priority(k.id()), reverse=True)

  # It is possible that there is no items yet, in case all futures are taking
  # more than 1 second.
  # It is possible that all futures are done if every queue has less than 10
  # task pending.
  while any(futures) or items:
    if items:
      yield items[0]
      items = items[1:]
    else:
      # Let activity happen.
      ndb.eventloop.run1()

    # Continue iteration.
    changed = False
    for i, f in enumerate(futures):
      if f and f.done():
        items.extend(f.get_result())
        futures[i] = next(yielders[i], None)
        changed = True
    if changed:
      items.sort(key=lambda k: _queue_number_priority(k.id()), reverse=True)


### Public API.


def request_to_task_to_run_key(request):
  """Returns the ndb.Key for a TaskToRun from a TaskRequest."""
  return ndb.Key(
      TaskToRun, task_queues.hash_dimensions(request.properties.dimensions),
      parent=request.key)


def task_to_run_key_to_request_key(task_key):
  """Returns the ndb.Key for a TaskToRun from a TaskRequest key."""
  if task_key.kind() != 'TaskToRun':
    raise ValueError('Expected key to TaskToRun, got %s' % task_key.kind())
  return task_key.parent()


def gen_queue_number(request):
  """Returns the value to use for TaskToRun.queue_number based on request.

  It is exported so a task can be retried by task_scheduler.
  """
  return _gen_queue_number(
      task_queues.hash_dimensions(request.properties.dimensions),
      request.created_ts,
      request.priority)


def new_task_to_run(request):
  """Returns a fresh new TaskToRun for the task ready to be scheduled.

  Returns:
    Unsaved TaskToRun entity.
  """
  return TaskToRun(
      key=request_to_task_to_run_key(request),
      queue_number=gen_queue_number(request),
      expiration_ts=request.expiration_ts)


def validate_to_run_key(task_key):
  """Validates a ndb.Key to a TaskToRun entity. Raises ValueError if invalid."""
  # This also validates the key kind.
  request_key = task_to_run_key_to_request_key(task_key)
  key_id = task_key.integer_id()
  if not key_id or key_id >= 2**32:
    raise ValueError(
        'TaskToRun key id should be between 1 and 2**32, found %s' %
        task_key.id())
  task_request.validate_request_key(request_key)


def match_dimensions(request_dimensions, bot_dimensions):
  """Returns True if the bot dimensions satisfies the request dimensions."""
  assert isinstance(request_dimensions, dict), request_dimensions
  assert isinstance(bot_dimensions, dict), bot_dimensions
  if frozenset(request_dimensions).difference(bot_dimensions):
    return False
  for key, required in request_dimensions.iteritems():
    bot_value = bot_dimensions[key]
    if isinstance(bot_value, (list, tuple)):
      if required not in bot_value:
        return False
    elif required != bot_value:
      return False
  return True


def set_lookup_cache(task_key, is_available_to_schedule):
  """Updates the quick lookup cache to mark an item as available or not.

  This cache is a blacklist of items that are already reaped, so it is not worth
  trying to reap it with a DB transaction. This saves on DB contention when a
  high number (>1000) of concurrent bots with similar dimension are reaping
  tasks simultaneously. In this case, there is a high likelihood that multiple
  concurrent HTTP handlers are trying to reap the exact same task
  simultaneously. This blacklist helps reduce the contention.
  """
  # Set the expiration time for items in the negative cache as 2 minutes. This
  # copes with significant index inconsistency but do not clog the memcache
  # server with unneeded keys.
  cache_lifetime = 120

  assert not ndb.in_transaction()
  key = _memcache_to_run_key(task_key)
  if is_available_to_schedule:
    # The item is now available, so remove it from memcache.
    memcache.delete(key, namespace='task_to_run')
  else:
    memcache.set(key, True, time=cache_lifetime, namespace='task_to_run')


def yield_next_available_task_to_dispatch(bot_dimensions, deadline):
  """Yields next available (TaskRequest, TaskToRun) in decreasing order of
  priority.

  Once the caller determines the task is suitable to execute, it must use
  reap_task_to_run(task.key) to mark that it is not to be scheduled anymore.

  Performance is the top most priority here.

  Arguments:
  - bot_dimensions: dimensions (as a dict) defined by the bot that can be
      matched.
  - deadline: UTC timestamp (as an int) that the bot must be able to
      complete the task by. None if there is no such deadline.
  """
  assert len(bot_dimensions['id']) == 1, bot_dimensions
  # List of all the valid dimensions hashed.
  now = utils.utcnow()
  stats = _QueryStats()
  stats.deadline = deadline
  bot_id = bot_dimensions[u'id'][0]
  try:
    for task_key in _yield_potential_tasks(bot_id):
      duration = (utils.utcnow() - now).total_seconds()
      if duration > 40.:
        # Stop searching after too long, since the odds of the request blowing
        # up right after succeeding in reaping a task is not worth the dangling
        # task request that will stay in limbo until the cron job reaps it and
        # retry it. The current handlers are given 60s to complete. By limiting
        # search to 40s, it gives 20s to complete the reaping and complete the
        # HTTP request.
        return
      # _validate_query_item() returns (request, task) if it's worth reaping.
      item = _validate_query_item(
          bot_dimensions, deadline, stats, now, task_key)
      if item:
        yield item[0], item[1]
        # If the code is still executed, it means that the task reaping wasn't
        # successful.
        stats.ignored += 1
  finally:
    logging.debug(
        'yield_next_available_task_to_dispatch(%s) in %.3fs: %s',
        bot_id, (utils.utcnow() - now).total_seconds(), stats)


def yield_expired_task_to_run():
  """Yields all the expired TaskToRun still marked as available."""
  # The reason it is done this way as an iteration over all the pending entities
  # instead of using a composite index with 'queue_number' and 'expiration_ts'
  # is that TaskToRun entities are very hot and it is important to not require
  # composite indexes on it. It is expected that the number of pending task is
  # 'relatively low', in the orders of 100,000 entities.
  now = utils.utcnow()
  for task in TaskToRun.query(TaskToRun.queue_number > 0):
    if task.expiration_ts < now:
      yield task
