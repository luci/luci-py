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
  dimensions_hash = ndb.IntegerProperty(indexed=False)

  # Moment by which the task has to be requested by a bot. Copy of TaskRequest's
  # TaskRequest.expiration_ts to enable queries when cleaning up stale jobs.
  expiration_ts = ndb.DateTimeProperty()

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


@ndb.transactional(retries=1)  # pylint: disable=E1120
def _put_shard_to_run(shard_to_run_key, queue_number):
  """Updates a TaskShardToRun.queue_number value.

  This function enforces that TaskShardToRun.queue_number member is toggled.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.
  - queue_number: new queue_number value.

  Returns:
    True if succeeded, False if no modification was committed to the DB.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  shard_to_run = shard_to_run_key.get()
  if bool(queue_number) == bool(shard_to_run.queue_number):
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
  assert key_id & 0xFF
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
  return [
    TaskShardToRun(
        key=request_key_to_shard_to_run_key(request.key, i+1),
        queue_number=_gen_queue_number_key(
          request.created_ts, request.priority, i+1),
        dimensions_hash=_hash_dimensions(request.properties.dimensions_json),
        expiration_ts=request.expiration_ts)
    for i in xrange(request.properties.number_shards)
  ]


def yield_next_available_shard_to_dispatch(bot_dimensions):
  """Yields next available (TaskRequest, TaskShardToRun) in decreasing order of
  priority.

  Once the caller determines the shard is suitable to execute, it must update
  TaskShardToRun.queue_number to None to mark that it is not to be scheduled
  anymore.

  Arguments:
  - bot_dimensions: dimensions (as a dict) defined by the bot that can be
      matched.
  """
  # List of all the valid dimensions hashed.
  accepted_dimensions = frozenset(
      _hash_dimensions(utils.encode_to_json(i))
      for i in _powerset(bot_dimensions))
  now = task_common.utcnow()
  for shard_to_run in TaskShardToRun.query().order(TaskShardToRun.queue_number):
    # It is possible for the index to be inconsistent since it is not executed
    # in a transaction, no problem.
    if not shard_to_run.queue_number:
      continue
    # It expired. A cron job will cancel it eventually. Since 'now' is saved
    # before the query, an expired task may still be reaped even if technically
    # expired if the query is very slow. This is on purpose so slow queries do
    # not cause exagerate expirations.
    if shard_to_run.expiration_ts < now:
      continue
    if shard_to_run.dimensions_hash in accepted_dimensions:
      # The hash may have conflicts. Ensure the dimensions actually match by
      # verifying the TaskRequest. There's a probability of 2**-31 of conflicts,
      # which is low enough for our purpose.
      request = shard_to_run.request_key.get()
      if _match_dimensions(request.properties.dimensions(), bot_dimensions):
        logging.info('Chose %d', shard_to_run.key.integer_id())
        yield request, shard_to_run


def all_expired_shard_to_run():
  """Returns an ndb.Query for all the available expired TaskShardToRun."""
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

  Runs it in a transaction to ensure .queue_number is toggled.

  Uses the original timestamp for priority, so the task shard request doesn't
  get starved on retries.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.

  Returns:
    True if succeeded.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  request = shard_to_run_key_to_request_key(shard_to_run_key).get()
  queue_number = _gen_queue_number_key(
      request.created_ts,
      request.priority,
      shard_to_run_key_to_shard_number(shard_to_run_key) + 1)
  try:
    return _put_shard_to_run(shard_to_run_key, queue_number)
  except ndb.TransactionFailedError:
    return False


def reap_shard_to_run(shard_to_run_key):
  """Updates a TaskShardToRun to note it should not be run again.

  Runs it in a transaction to ensure .queue_number is toggled.

  Arguments:
  - shard_to_run_key: ndb.Key to TaskShardToRun entity to update.
  - request: TaskRequest with necessary details.

  Returns:
    True if succeeded.
  """
  assert isinstance(shard_to_run_key, ndb.Key), shard_to_run_key
  try:
    return _put_shard_to_run(shard_to_run_key, None)
  except ndb.TransactionFailedError:
    return False


def abort_shard_to_run(shard_to_run):
  """Updates a TaskShardToRun to note it should not be scheduled for work.

  Arguments:
  - shard_to_run: TaskShardToRun entity to update.

  Returns:
    Always True.
  """
  assert isinstance(shard_to_run, TaskShardToRun), shard_to_run
  shard_to_run.queue_number = None
  shard_to_run.put()
  return True
