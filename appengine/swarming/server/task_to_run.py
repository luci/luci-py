# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task entity that describe when a task is to be scheduled.

This module doesn't do the scheduling itself. It only describes the tasks ready
to be scheduled.

Graph of the schema:
     <See task_request.py>
               ^
               |
    +---------------------+
    |TaskRequest          |  (task_request.py)
    |    +--------------+ |
    |    |TaskProperties| |
    |    +--------------+ |
    |id=<based on epoch>  |
    +---------------------+
               ^
               |
    +-----------------------+
    |TaskToRun              |
    |id=<hash of dimensions>|
    +-----------------------+
"""

import datetime
import hashlib
import itertools
import logging
import struct

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import utils
from server import task_request


# Maximum product search space for dimensions for a bot.
MAX_DIMENSIONS = 16384


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
  _hash_dimensions(), parent is TaskRequest.
  """
  # Moment by which the task has to be requested by a bot. Copy of TaskRequest's
  # TaskRequest.expiration_ts to enable queries when cleaning up stale jobs.
  expiration_ts = ndb.DateTimeProperty(required=True)

  # Everything above is immutable, everything below is mutable.

  # priority and request creation timestamp are mixed together to allow queries
  # to order the results by this field to allow sorting by priority first, and
  # then timestamp. See _gen_queue_number_key() for details. This value is only
  # set when the task is available to run, i.e.
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


def _gen_queue_number_key(timestamp, priority):
  """Generates a 64 bit packed value used for TaskToRun.queue_number.

  The lower value the higher importance.

  Arguments:
  - timestamp: datetime.datetime when the TaskRequest was filed in.
  - priority: priority of the TaskRequest.

  Returns:
    queue_number is a 64 bit integer:
    - 1 bit highest order bit set to 0 to detect overflow.
    - 8 bits of priority.
    - 47 bits at 1ms resolution is 2**47 / 365 / 24 / 60 / 60 / 1000 = 4462
      years.
    - The last 8 bits is currently unused and set to 0.
  """
  assert isinstance(timestamp, datetime.datetime)
  task_request.validate_priority(priority)
  now = utils.milliseconds_since_epoch(timestamp)
  assert 0 <= now < 2**47, hex(now)
  # Assumes the system runs a 64 bits version of python.
  return priority << 55 | now << 8


def _explode_list(values):
  """Yields all the combinations in the dict values for items which are list.

  Examples:
  - values={'a': [1, 2]} yields {'a': 1}, {'a': 2}
  - values={'a': [1, 2], 'b': [3, 4]} yields {'a': 1, 'b': 3}, {'a': 2, 'b': 3},
    {'a': 1, 'b': 4}, {'a': 2, 'b': 4}
  """
  key_has_list = frozenset(
      k for k, v in values.iteritems() if isinstance(v, list))
  if not key_has_list:
    yield values
    return

  all_keys = frozenset(values)
  key_others = all_keys - key_has_list
  non_list_dict = {k: values[k] for k in key_others}
  options = [[(k, v) for v in values[k]] for k in sorted(key_has_list)]
  for i in itertools.product(*options):
    out = non_list_dict.copy()
    out.update(i)
    yield out


def _powerset(dimensions):
  """Yields the product of all the possible combinations in dimensions.

  Starts with the most restrictive set and goes down to the less restrictive
  ones. See unit test TaskToRunPrivateTest.test_powerset for examples.
  """
  keys = sorted(dimensions)
  for i in itertools.chain.from_iterable(
      itertools.combinations(keys, r) for r in xrange(len(keys), -1, -1)):
    for i in _explode_list({k: dimensions[k] for k in i}):
      yield i


def _hash_dimensions(dimensions_json):
  """Returns a 32 bits int that is a hash of the dimensions specified.

  dimensions_json must already be encoded as json.

  The return value is guaranteed to be non-zero so it can be used as a key id in
  a ndb.Key.
  """
  digest = hashlib.md5(dimensions_json).digest()
  # Note that 'L' means C++ unsigned long which is (usually) 32 bits and
  # python's int is 64 bits.
  return int(struct.unpack('<L', digest[:4])[0]) or 1


def _memcache_to_run_key(task_key):
  """Functional equivalent of task_result.pack_result_summary_key()."""
  request_key = task_to_run_key_to_request_key(task_key)
  return '%x' % request_key.integer_id()


def _lookup_cache_is_taken(task_key):
  """Queries the quick lookup cache to reduce DB operations."""
  assert not ndb.in_transaction()
  key = _memcache_to_run_key(task_key)
  return bool(memcache.get(key, namespace='task_to_run'))


### Public API.


def request_to_task_to_run_key(request):
  """Returns the ndb.Key for a TaskToRun from a TaskRequest."""
  assert isinstance(request, task_request.TaskRequest), request
  dimensions_json = utils.encode_to_json(request.properties.dimensions)
  return ndb.Key(
      TaskToRun, _hash_dimensions(dimensions_json), parent=request.key)


def task_to_run_key_to_request_key(task_key):
  """Returns the ndb.Key for a TaskToRun from a TaskRequest key."""
  if task_key.kind() != 'TaskToRun':
    raise ValueError('Expected key to TaskToRun, got %s' % task_key.kind())
  return task_key.parent()


def gen_queue_number_key(request):
  """Returns the value to use for TaskToRun.queue_number based on request."""
  return _gen_queue_number_key(request.created_ts, request.priority)


def new_task_to_run(request):
  """Returns a fresh new TaskToRun for the task ready to be scheduled.

  Returns:
    Unsaved TaskToRun entity.
  """
  return TaskToRun(
      key=request_to_task_to_run_key(request),
      queue_number=gen_queue_number_key(request),
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


def dimensions_powerset_count(dimensions):
  """Returns the number of combinations possible with the dimensions."""
  out = 1
  for i in dimensions.itervalues():
    if isinstance(i, basestring):
      # When a dimension value is a string, it can be in two states: "present"
      # or "not present", so the product is 2.
      out *= 2
    else:
      # When a dimension value is a list, it can be in len(values) + 1 states:
      # one of each state or "not present".
      out *= len(i) + 1
  return out


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


def is_task_reapable(task_key, queue_number):
  """Returns TaskToRun if it could be updated.

  This function enforces that TaskToRun.queue_number member is not in the value
  specified.

  Arguments:
  - task_key: ndb.Key to TaskToRun entity to update.
  - queue_number: new queue_number value that it would be set to.

  Returns:
    TaskToRun entity if bool(.queue_number) is not equivalent to the
    bool(|queue_number)| passed as an argument, None if the entity had
    .queue_number already set to the same True/False state.
  """
  assert task_key.kind() == 'TaskToRun', task_key
  # Refresh the item.
  task = task_key.get(use_cache=False)
  if bool(queue_number) == bool(task.queue_number):
    # Too bad, someone else reaped it in the meantime.
    return None
  return task


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


def yield_next_available_task_to_dispatch(bot_dimensions):
  """Yields next available (TaskRequest, TaskToRun) in decreasing order of
  priority.

  Once the caller determines the task is suitable to execute, it must use
  reap_task_to_run(task.key) to mark that it is not to be scheduled anymore.

  Performance is the top most priority here.

  Arguments:
  - bot_dimensions: dimensions (as a dict) defined by the bot that can be
      matched.
  """
  # List of all the valid dimensions hashed.
  accepted_dimensions_hash = frozenset(
      _hash_dimensions(utils.encode_to_json(i))
      for i in _powerset(bot_dimensions))
  now = utils.utcnow()
  broken = 0
  cache_lookup = 0
  expired = 0
  hash_mismatch = 0
  ignored = 0
  no_queue = 0
  real_mismatch = 0
  total = 0
  # Be very aggressive in fetching the largest amount of items as possible. Note
  # that we use the default ndb.EVENTUAL_CONSISTENCY so stale items may be
  # returned. It's handled specifically.
  # - 100/200 gives 2s~40s of query time for 1275 items.
  # - 250/500 gives 2s~50s of query time for 1275 items.
  # - 50/500 gives 3s~20s of query time for 1275 items. (Slower but less
  #   variance). Spikes in 20s~40s are rarer.
  # The problem here are:
  # - Outliers, some shards are simply slower at executing the query.
  # - Median time, which we should optimize.
  # - Abusing batching will slow down this query.
  #
  # TODO(maruel): Measure query performance with stats_framework!!
  # TODO(maruel): Use fetch_page_async() + ndb.get_multi_async() +
  # memcache.get_multi_async() to do pipelined processing. Should greatly reduce
  # the effect of latency on the total duration of this function. I also suspect
  # using ndb.get_multi() will return fresher objects than what is returned by
  # the query.
  opts = ndb.QueryOptions(batch_size=50, prefetch_size=500, keys_only=True)
  try:
    # Interestingly, the filter on .queue_number>0 is required otherwise all the
    # None items are returned first.
    q = TaskToRun.query(default_options=opts).order(
        TaskToRun.queue_number).filter(TaskToRun.queue_number > 0)
    for task_key in q:
      duration = (utils.utcnow() - now).total_seconds()
      if duration > 40.:
        # Stop searching after too long, since the odds of the request blowing
        # up right after succeeding in reaping a task is not worth the dangling
        # task request that will stay in limbo until the cron job reaps it and
        # retry it. The current handlers are given 60s to complete. By using
        # 40s, it gives 20s to complete the reaping and complete the HTTP
        # request.
        return

      total += 1
      # Verify TaskToRun is what is expected. Play defensive here.
      try:
        validate_to_run_key(task_key)
      except ValueError as e:
        logging.error(str(e))
        broken += 1
        continue

      # integer_id() == dimensions_hash.
      if task_key.integer_id() not in accepted_dimensions_hash:
        hash_mismatch += 1
        continue

      # Do this after the basic weeding out but before fetching TaskRequest.
      if _lookup_cache_is_taken(task_key):
        cache_lookup += 1
        continue

      # Ok, it's now worth taking a real look at the entity.
      task = task_key.get(use_cache=False)

      # DB operations are slow, double check memcache again.
      if _lookup_cache_is_taken(task_key):
        cache_lookup += 1
        continue

      # It is possible for the index to be inconsistent since it is not executed
      # in a transaction, no problem.
      if not task.queue_number:
        no_queue += 1
        continue

      # It expired. A cron job will cancel it eventually. Since 'now' is saved
      # before the query, an expired task may still be reaped even if
      # technically expired if the query is very slow. This is on purpose so
      # slow queries do not cause exagerate expirations.
      if task.expiration_ts < now:
        expired += 1
        continue

      # The hash may have conflicts. Ensure the dimensions actually match by
      # verifying the TaskRequest. There's a probability of 2**-31 of conflicts,
      # which is low enough for our purpose. The reason use_cache=False is
      # otherwise it'll create a buffer bloat.
      request = task.request_key.get(use_cache=False)
      if not match_dimensions(request.properties.dimensions, bot_dimensions):
        real_mismatch += 1
        continue

      # It's a valid task! Note that in the meantime, another bot may have
      # reaped it.
      yield request, task
      ignored += 1
  finally:
    duration = (utils.utcnow() - now).total_seconds()
    logging.info(
        '%d/%s in %5.2fs: %d total, %d exp %d no_queue, %d hash mismatch, '
        '%d cache negative, %d dimensions mismatch, %d ignored, %d broken',
        opts.batch_size,
        opts.prefetch_size,
        duration,
        total,
        expired,
        no_queue,
        hash_mismatch,
        cache_lookup,
        real_mismatch,
        ignored,
        broken)


def yield_expired_task_to_run():
  """Yields all the expired TaskToRun still marked as available."""
  now = utils.utcnow()
  for task in TaskToRun.query().filter(TaskToRun.queue_number > 0):
    if task.expiration_ts < now:
      yield task


def abort_task_to_run(task):
  """Updates a TaskToRun to note it should not be scheduled for work.

  Arguments:
  - task: TaskToRun entity to update.
  """
  # TODO(maruel): Add support to kill an on-going task and update the
  # corresponding TaskRunResult.
  # TODO(maruel): Add stats.
  assert not ndb.in_transaction()
  assert isinstance(task, TaskToRun), task
  task.queue_number = None
  task.put()
  # Add it to the negative cache.
  set_lookup_cache(task.key, False)


def retry(request, now):
  """Retries a task at its original priority.

  If the task expired, it will not be retried.

  Returns the TaskToRun entity to save in the DB on success.
  """
  assert ndb.in_transaction()
  if now > request.expiration_ts:
    return None
  to_run = request_to_task_to_run_key(request).get()
  if to_run.queue_number:
    return None

  # The original queue_number is used but expiration_ts is unchanged.
  to_run.queue_number = gen_queue_number_key(request)
  return to_run
