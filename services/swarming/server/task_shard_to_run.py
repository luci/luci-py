# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tasks shard entities that describe when a shard is to be scheduled.

This module doesn't do the scheduling itself. It only describes the shards to
ready to be scheduled.

Graph of the schema:
   <See task_request.py>
             ^
             |
  +---------------------+
  |TaskRequest          |  (task_request.py)
  |    +--------------+ |<..........................+
  |    |TaskProperties| |                           .
  |    +--------------+ |<..+                       .
  +---------------------+   .                       .
                            .                       .
     +--------Root-------+  . +--------Root-------+ .
     |TaskShardToRunShard|  . |TaskShardToRunShard| .
     +-------------------+  . +-------------------+ .
               ^            .          ^            .
               |            .          |            .
        +--------------+    .   +--------------+    .
        |TaskShardToRun|....+   |TaskShardToRun|....+
        +--------------+        +--------------+
                    ^              ^
                    |              |
                   <See task_result.py>
"""

import datetime
import hashlib
import itertools
import logging
import struct

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import task_common
from server import task_request


class TaskShardToRun(ndb.Model):
  """Defines a shard to run for a TaskRequest.

  This specific request for a specific shard can be executed multiple times,
  each execution will create a new child task_result.TaskShardResult.

  This entity must be kept small and contain the minimum data to enable the
  queries for two reasons:
  - it is updated inside a transaction for each scheduling event, e.g. when a
    bot gets assigned this task item to work on.
  - all the ones currently active are fetched at once in a cron job.

  The key is derived from TaskRequest's key. The low order byte is the shard
  number + 1.

  Warning: ordering by key is not useful. This is because of the root entity
  sharding, which shuffle the base key.
  """
  # The shortened hash of TaskRequests.dimensions_json. This value is generated
  # with _hash_dimensions().
  dimensions_hash = ndb.IntegerProperty(indexed=False, required=True)

  # Moment by which the task has to be requested by a bot. Copy of TaskRequest's
  # TaskRequest.expiration_ts to enable queries when cleaning up stale jobs.
  expiration_ts = ndb.DateTimeProperty(required=True)

  # Everything above is immutable, everything below is mutable.

  # priority and request creation timestamp are mixed together to allow queries
  # to order the results by this field to allow sorting by priority first, and
  # then timestamp. See _gen_queue_number_key() for details. This value is only
  # set when the task is available to run, i.e.
  # ndb.TaskShardResult.query(ancestor=self.key).get().state==AVAILABLE.
  # If this task shard it not ready to be scheduled, it must be None.
  queue_number = ndb.IntegerProperty()

  @property
  def shard_number(self):
    """Returns a 0-based number that determines the subset of the task to run.
    """
    return shard_to_run_key_to_shard_number(self.key)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to shard to run."""
    return shard_to_run_key_to_request_key(self.key)

  def to_dict(self):
    """Encodes .dimension_has as hex to make it json compatible."""
    out = super(TaskShardToRun, self).to_dict()
    out['shard_number'] = self.shard_number
    return out


def _gen_queue_number_key(timestamp, priority, shard_id):
  """Generates a 64 bit packed value used for TaskShardToRun.queue_number.

  The lower value the higher importance.

  Arguments:
  - timestamp: datetime.datetime when the TaskRequest was filed in.
  - priority: priority of the TaskRequest.
  - shard_id: 1-based shard id.

  Returns:
    queue_number is a 64 bit integer:
    - 1 bit highest order bit set to 0 to detect overflow.
    - 8 bits of priority.
    - 47 bits at 1ms resolution is 2**47 / 365 / 24 / 60 / 60 / 1000 = 4462
      years.
    - The last 8 bits is the shard number.
  """
  assert isinstance(timestamp, datetime.datetime)
  task_common.validate_priority(priority)
  # shard_id must fit a single byte.
  assert 1 <= shard_id and not (shard_id & ~0xFF)
  now = task_common.milliseconds_since_epoch(timestamp)
  assert 0 <= now < 2**47, hex(now)
  # Assumes the system runs a 64 bits version of python.
  return priority << 55 | now << 8 | shard_id


def _explode_list(values):
  """Yields all the permutations in the dict values for items which are list.

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
  """Yields the product of all the possible permutations in dimensions.

  Starts with the most restrictive set and goes down to the less restrictive
  ones. See unit test TaskShardToRunPrivateTest.test_powerset for examples.
  """
  keys = sorted(dimensions)
  for i in itertools.chain.from_iterable(
      itertools.combinations(keys, r) for r in xrange(len(keys), -1, -1)):
    for i in _explode_list({k: dimensions[k] for k in i}):
      yield i


def _put_shard_to_run(shard_to_run_key, queue_number):
  """Updates a TaskShardToRun.queue_number value.

  This function enforces that TaskShardToRun.queue_number member is toggled and
  this function must run inside a transaction.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.
  - queue_number: new queue_number value.

  Returns:
    True if succeeded, False if no modification was committed to the DB.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  assert ndb.in_transaction()
  # Refresh the item.
  shard_to_run = shard_to_run_key.get()
  if bool(queue_number) == bool(shard_to_run.queue_number):
    # Too bad, someone else reaped it in the meantime.
    return False
  shard_to_run.queue_number = queue_number
  shard_to_run.put()
  return True


def _hash_dimensions(dimensions_json):
  """Returns a 32 bits int that is a hash of the dimensions specified.

  dimensions_json must already be encoded as json. This is because
  TaskProperties already has the json encoded data available.
  """
  digest = hashlib.md5(dimensions_json).digest()
  # Note that 'L' means C++ unsigned long which is (usually) 32 bits and
  # python's int is 64 bits.
  return int(struct.unpack('<L', digest[:4])[0])


def _match_dimensions(request_dimensions, bot_dimensions):
  """Returns True if the bot dimensions satisfies the request."""
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


def _set_lookup_cache(shard_to_run_key, is_available_to_schedule):
  """Updates the quick lookup cache to mark an item as available or not.

  This cache is a blacklist of items that are already reaped, so it is not worth
  trying to reap it with a DB transaction. This saves on DB contention when a
  high number (>1000) of concurrent bots with similar dimension are reaping
  tasks simultaneously. In this case, there is a high likelihood that multiple
  concurrent HTTP handlers are trying to reap the exact same task
  simultaneously. This blacklist helps reduce the contention.
  """
  key = '%x' % shard_to_run_key.integer_id()
  if is_available_to_schedule:
    # The item is now available, so remove it from memcache.
    memcache.delete(key, namespace='shard_to_run')
  else:
    # Save the item for 5 minutes. This copes with significant index
    # inconsistency but do not clog the memcache server with unneeded keys.
    memcache.set(key, True, time=300, namespace='shard_to_run')


def _lookup_cache_is_taken(shard_to_run_key):
  """Queries the quick lookup cache to reduce DB operations."""
  key = '%x' % shard_to_run_key.integer_id()
  return bool(memcache.get(key, namespace='shard_to_run'))


### Public API.


def request_key_to_shard_to_run_key(request_key, shard_id):
  """Returns the ndb.Key for a TaskShardToRun from a TaskRequest key.

  Arguments:
  - request_key: ndb.Key to a TaskRequest.
  - shard_id: must be 1 based.
  """
  if shard_id < 1 or shard_id > task_common.MAXIMUM_SHARDS:
    raise ValueError('shard_id must be 1 based and fit a single byte.')
  request_id = request_key.integer_id()
  # Ensure shard_id fits a single byte and that this bit is free in request_id.
  assert not (request_id & 0xFF) and not (shard_id & ~0xFF)
  key_id = request_id | shard_id
  return shard_id_to_key(key_id)


def shard_id_to_key(key_id):
  """Returns the ndb.Key for a TaskShardToRun from a key to a raw id."""
  # key_id is likely to be user-provided.
  if not (key_id & 0xFF):
    raise ValueError('Can\'t pass a request id as a shard id')
  if key_id < 0x100 or key_id >= 2**64:
    raise ValueError('Invalid shard id')
  parent = datastore_utils.hashed_shard_key(
      str(key_id), task_common.SHARDING_LEVEL, 'TaskShardToRunShard')
  return ndb.Key(TaskShardToRun, key_id, parent=parent)


def shard_to_run_key_to_request_key(shard_to_run_key):
  """Returns the ndb.Key for a TaskShardToRun from a TaskRequest key.

  Arguments:
  - request_key: ndb.Key to a TaskRequest.
  """
  # Mask the low order byte.
  return task_request.id_to_request_key(
      shard_to_run_key.integer_id() & 0xFFFFFFFFFFFFFF00)


def shard_to_run_key_to_shard_number(shard_to_run_key):
  """Returns the 0-based shard number for a ndb.Key to a TaskShardToRun."""
  return (shard_to_run_key.integer_id() - 1) & 0xFF


def new_shards_to_run_for_request(request):
  """Returns a fresh new TaskShardToRun for each shard ready to be scheduled.

  Each shard has a slightly less priority so the shard will be reaped in order.
  This makes more sense.

  Returns:
    Yet to be stored TaskShardToRun entities.
  """
  dimensions_json = utils.encode_to_json(request.properties.dimensions)
  return [
    TaskShardToRun(
        key=request_key_to_shard_to_run_key(request.key, i+1),
        queue_number=_gen_queue_number_key(
          request.created_ts, request.priority, i+1),
        dimensions_hash=_hash_dimensions(dimensions_json),
        expiration_ts=request.expiration_ts)
    for i in xrange(request.properties.number_shards)
  ]


def yield_next_available_shard_to_dispatch(bot_dimensions):
  """Yields next available (TaskRequest, TaskShardToRun) in decreasing order of
  priority.

  Once the caller determines the shard is suitable to execute, it must update
  TaskShardToRun.queue_number to None to mark that it is not to be scheduled
  anymore.

  Performance is the top most priority here.

  Arguments:
  - bot_dimensions: dimensions (as a dict) defined by the bot that can be
      matched.
  """
  # List of all the valid dimensions hashed.
  accepted_dimensions_hash = frozenset(
      _hash_dimensions(utils.encode_to_json(i))
      for i in _powerset(bot_dimensions))
  now = task_common.utcnow()
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
  # TODO(maruel): Use a keys_only=True query + fetch_page_async() +
  # ndb.get_multi_async() + memcache.get_multi_async() to do pipelined
  # processing. Should greatly reduce the effect of latency on the total
  # duration of this function. I also suspect using ndb.get_multi() will return
  # fresher objects than what is returned by the query.
  opts = ndb.QueryOptions(batch_size=50, prefetch_size=500)
  try:
    # Interestingly, the filter on .queue_number>0 is required otherwise all the
    # None items are returned first.
    q = TaskShardToRun.query(default_options=opts).order(
        TaskShardToRun.queue_number).filter(TaskShardToRun.queue_number > 0)
    for shard_to_run in q:
      total += 1
      # Verify TaskRequestShard is what is expected. Play defensive here.
      shard_to_run_shard_key = shard_to_run.key.parent()
      if (not shard_to_run_shard_key.string_id() or
          len(shard_to_run_shard_key.string_id()) !=
              task_common.SHARDING_LEVEL):
        logging.error(
            'Found %s which is not on proper sharding level %d',
            shard_to_run.key, task_common.SHARDING_LEVEL)
        broken += 1
        continue

      # It is possible for the index to be inconsistent since it is not executed
      # in a transaction, no problem.
      if not shard_to_run.queue_number:
        no_queue += 1
        continue
      # It expired. A cron job will cancel it eventually. Since 'now' is saved
      # before the query, an expired task may still be reaped even if
      # technically expired if the query is very slow. This is on purpose so
      # slow queries do not cause exagerate expirations.
      if shard_to_run.expiration_ts < now:
        expired += 1
        continue
      if shard_to_run.dimensions_hash not in accepted_dimensions_hash:
        hash_mismatch += 1
        continue

      # Do this after the basic weeding out but before fetching TaskRequest.
      if _lookup_cache_is_taken(shard_to_run.key):
        cache_lookup += 1
        continue

      # The hash may have conflicts. Ensure the dimensions actually match by
      # verifying the TaskRequest. There's a probability of 2**-31 of conflicts,
      # which is low enough for our purpose.
      request = shard_to_run.request_key.get()
      if not _match_dimensions(request.properties.dimensions, bot_dimensions):
        real_mismatch += 1
        continue

      yield request, shard_to_run
      ignored += 1
  finally:
    duration = (task_common.utcnow() - now).total_seconds()
    logging.info(
        '%d/%s in %5.2fs: %d total, %d exp %d no_queue, %d hash mismatch, '
        '%d cache negative, %d dimensions mismatch, %d ignored, %d broken',
        opts.batch_size,
        opts.prefetch_size,
        duration,
        total,
        expired,
        no_queue,
        cache_lookup,
        hash_mismatch,
        real_mismatch,
        ignored,
        broken)


def yield_expired_shard_to_run():
  """Yields all the expired TaskShardToRun still marked as available."""
  # Do not use a projection query, since the TaskShardToRun entities are hot,
  # thus are likely to be found in cache and projection queries disable the
  # cache.
  q = TaskShardToRun.query().filter(TaskShardToRun.queue_number > 0)
  now = task_common.utcnow()
  for shard_to_run in q:
    if shard_to_run.expiration_ts < now:
      yield shard_to_run


def retry_shard_to_run(shard_to_run_key):
  """Updates a TaskShardToRun to note it should run again.

  The modification is done in a transaction to ensure .queue_number is toggled.

  Uses the original timestamp for priority, so the task shard request doesn't
  get starved on retries.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.

  Returns:
    True if succeeded.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  assert not ndb.in_transaction()
  request = shard_to_run_key_to_request_key(shard_to_run_key).get()
  queue_number = _gen_queue_number_key(
      request.created_ts,
      request.priority,
      shard_to_run_key_to_shard_number(shard_to_run_key) + 1)
  try:
    result = ndb.transaction(
        lambda: _put_shard_to_run(shard_to_run_key, queue_number),
        retries=1)
    if result:
      _set_lookup_cache(shard_to_run_key, True)
    return result
  except datastore_errors.TransactionFailedError:
    return False


def reap_shard_to_run(shard_to_run_key):
  """Updates a TaskShardToRun to note it should not be run again.

  If running inside a transaction, reuse it. Otherwise runs it in a transaction
  to ensure .queue_number is toggled.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.
  - request: TaskRequest with necessary details.

  Returns:
    True if succeeded.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  if ndb.in_transaction():
    result = _put_shard_to_run(shard_to_run_key, None)
  else:
    try:
      # Don't retry, it's not worth. Just move on to the next task shard.
      result = ndb.transaction(
          lambda: _put_shard_to_run(shard_to_run_key, None),
          retries=0)
    except datastore_errors.TransactionFailedError:
      # Ignore transaction failures, it's seen if the object had been reaped by
      # another bot concurrently.
      return False

  if result:
    # Note this fact immediately in memcache to reduce DB contention.
    _set_lookup_cache(shard_to_run_key, False)
  return result


def abort_shard_to_run(shard_to_run):
  """Updates a TaskShardToRun to note it should not be scheduled for work.

  Arguments:
  - shard_to_run: TaskShardToRun entity to update.

  Returns:
    Always True.
  """
  assert not ndb.in_transaction()
  assert isinstance(shard_to_run, TaskShardToRun), shard_to_run
  shard_to_run.queue_number = None
  shard_to_run.put()
  _set_lookup_cache(shard_to_run.key, False)
  return True
