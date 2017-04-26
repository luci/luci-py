# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Ambient task queues generated from the actual load.

This means that the task queues are deduced by the actual load, they are never
explicitly defined. They are eventually deleted by a cron job once no incoming
task with the exact set of dimensions is triggered anymore.

Used to optimize scheduling.

          +---------+
          |BotRoot  |     <bot_management.py>
          |id=bot_id|
          +---------+
              |
              |
              +--------------------+
              |                    |
              v                    v
    +-------------------+     +-------------------+
    |BotTaskDimensions  | ... |BotTaskDimensions  |
    |id=<dimension_hash>| ... |id=<dimension_hash>|
    +-------------------+     +-------------------+

    +-------Root------------+
    |TaskDimensionsRoot     |  (not stored)
    |id=<pool:foo or id:foo>|
    +-----------------------+
              |
              v
    +-------------------+
    |TaskDimensions     |
    |id=<dimension_hash>|
    +-------------------+
"""

import datetime

from google.appengine.ext import ndb


# Frequency at which these entities must be refreshed. This value is a trade off
# between constantly updating BotTaskDimensions and TaskDimensions vs keeping
# them alive for longer than necessary, causing unnecessary queries in
# get_queues() users.
_ADVANCE = datetime.timedelta(hours=1)


### Models.


class BotTaskDimensions(ndb.Model):
  """Stores the precalculated hashes for this bot.

  Parent is BotRoot.
  key id is <dimensions_hash>.

  This hash could be conflicting different properties set, but this doesn't
  matter at this level because disambiguation is done in TaskDimensions
  entities.

  The number of stored entities is:
    <number of bots> x <TaskDimensions each bot support>

  The actual number if a direct function of the variety of the TaskDimensions.
  """
  # Validity time, at which this entity should be considered irrelevant.
  valid_until_ts = ndb.DateTimeProperty()


class TaskDimensionsRoot(ndb.Model):
  """Ghost root entity to group kinds of tasks to a common root.

  This root entity is not stored in the DB.

  id is either 'id:<value>' or 'pool:<value>'. For a request dimensions set that
  specifies both keys, one TaskDimensions is listed in each root.
  """
  pass


class TaskDimensionsSet(ndb.Model):
  """Embedded struct to store a set of dimensions.

  This entity is not stored, it is contained inside TaskDimensions.sets.
  """
  # 'key:value' strings. This is stored to enable match(). This is important as
  # the dimensions_hash can be colliding! In this case, a *set* of
  # dimensions_flat can exist.
  dimensions_flat = ndb.StringProperty(repeated=True, indexed=False)

  def match(self, bot_dimensions):
    """Returns True if this bot can run this request dimensions set."""
    for d in self.dimensions_flat:
      key, value = d.split(':', 1)
      if value not in bot_dimensions.get(key, []):
        return False
    return True


class TaskDimensions(ndb.Model):
  """List dimensions for each kind of task.

  Parent is TaskDimensionsRoot
  key id is <dimensions_hash>

  A single dimensions_hash may represent multiple independent queues in a single
  root. This is because the hash is very compressed (32 bits). This is handled
  specifically here by having one set of TaskDimensionsSet per 'set'.

  The worst case of having hash collision is unneeded scanning for unrelated
  tasks in get_queues(). This is bad but not the end of the world.

  It is only a function of the number of different tasks, so it is not expected
  to be very large, only in the few hundreds. The exception is when one task per
  bot is triggered, which leads to have at <number of bots> TaskDimensions
  entities.
  """
  # Validity time, at which this entity should be considered irrelevant.
  # Entities with valid_until_ts in the past are considered inactive and are not
  # used. valid_until_ts is set in assert_task() to "TaskRequest.expiration_ts +
  # _ADVANCE". It is updated when an assert_task() call sees that valid_until_ts
  # becomes lower than TaskRequest.expiration_ts for a later task. This enables
  # not updating the entity too frequently, at the cost of keeping a dead queue
  # "alive" for a bit longer than strictly necessary.
  valid_until_ts = ndb.DateTimeProperty()

  # One or multiple sets of request dimensions this dimensions_hash represents.
  sets = ndb.LocalStructuredProperty(TaskDimensionsSet, repeated=True)

  def confirm(self, dimensions):
    """Confirms that this instance actually stores this set."""
    return self.confirm_flat(
        '%s:%s' % (k, v) for k, v in dimensions.iteritems())

  def confirm_flat(self, dimensions_flat):
    """Confirms that this instance actually stores this set."""
    x = frozenset(dimensions_flat)
    return any(not x.difference(s.dimensions_flat) for s in self.sets)

  def match(self, bot_dimensions):
    """Returns True if this bot can run one of the request dimensions sets
    represented by this dimensions_hash.
    """
    return any(s.match(bot_dimensions) for s in self.sets)


### Private APIs.


### Public APIs.


def hash_dimensions(dimensions):
  """Returns a 32 bits int that is a hash of the request dimensions specified.

  Arguments:
    dimensions: dict(str, str)

  The return value is guaranteed to be non-zero so it can be used as a key id in
  a ndb.Key.
  """
  del dimensions


def assert_bot(bot_dimensions):
  """Prepares the dimensions for the queues."""
  assert len(bot_dimensions[u'id']) == 1, bot_dimensions


def assert_task(request):
  """Makes sure the TaskRequest dimensions are listed as a known queue.

  This function must be called before storing the TaskRequest in the DB.

  When a cache miss occurs, a task queue is triggered.

  Warning: the task will not be run until the task queue ran, which causes a
  user visible delay. This only occurs on new kind of requests, which is not
  that often in practice.
  """
  assert not request.key, request.key


def get_queues(bot_id):
  """Queries all the known task queues in parallel and yields the task in order
  of priority.

  The ordering is opportunistic, not strict.
  """
  assert isinstance(bot_id, unicode), repr(bot_id)
  return []


def rebuild_task_cache(dimensions_hash, dimensions_flat):
  """Rebuilds the TaskDimensions cache.

  This code implicitly depends on bot_management.bot_event() being called for
  the bots.

  This function is called in two cases:
  - A new kind of dimensions never seen before
  - The TaskDimensions.valid_until_ts expired

  It is a cache miss, query all the bots and check for the ones which can run
  the task.

  Warning: There's a race condition, where the TaskDimensions query could be
  missing some instances due to eventually coherent consistency in the BotInfo
  query. This only happens when there's new request dimensions set AND a bot
  that can run this task recently showed up.

  Runtime expectation: the scale on the number of bots that can run the task,
  via BotInfo.dimensions_flat filtering. As there can be tens of thousands of
  bots that can run the task, this can take a long time to store all the
  entities on a new kind of request. As such, it must be called in the backend.
  """
  del dimensions_hash
  del dimensions_flat


def tidy_stale():
  """Searches for all stale BotTaskDimensions and TaskDimensions and delete
  them.

  Their .valid_until_ts is compared to the current time and the entity is
  deleted if it's older.

  The number of entities processed is expected to be relatively low, in the few
  tens at most.
  """
  pass
