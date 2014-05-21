# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task execution shard result and result summary models.

This module doesn't do the scheduling itself. It only describes the shards
results.

Graph of the schema for TaskResultSummary. It represents the overall result for
the TaskRequest:
  <See task_request.py>
            ^
            |
  +---------------------+
  |TaskRequest          |
  |    +--------------+ |
  |    |TaskProperties| |
  |    +--------------+ |
  +---------------------+
         ^
         |
  +-------------------------+
  |TaskResultSummary        |
  |                         |
  |  +--------------------+ |
  |  |TaskShardResultInner| |
  |  +--------------------+ |
  |                         |
  |  +--------------------+ |
  |  |TaskShardResultInner| |
  |  +--------------------+ |
  |   (...)                 |
  +-------------------------+

Graph of the schema for TaskShardResult. It represents the result for the
TaskShardToRun:
  <See task_shard_to_run.py>
        ^
        |
  +--------------+
  |TaskShardToRun|
  +--------------+
         ^
         |
  +---------------+
  |TaskShardResult|
  +---------------+
"""

import datetime
import logging

from google.appengine.api import datastore_errors
from google.appengine.ext import deferred
from google.appengine.ext import ndb

from components import utils
from server import result_helper
from server import task_common
from server import task_request
from server import task_shard_to_run


class State(object):
  """States in which a task shard can be.

  It's in fact an enum. Values must be in decreasing order of importance. That
  is, if a task request has two shards with results A and B, the value min(A,B)
  should be the one that best summarizes the overall request's state.
  """
  RUNNING = 0x10
  PENDING = 0x20
  EXPIRED = 0x30
  TIMED_OUT = 0x40
  BOT_DIED = 0x50
  CANCELED = 0x60
  COMPLETED = 0x70

  STATES = (RUNNING, PENDING, EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, COMPLETED)
  STATES_RUNNING = (RUNNING, PENDING)
  STATES_NOT_RUNNING = (EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, COMPLETED)
  STATES_DONE = (TIMED_OUT, COMPLETED)
  STATES_ABANDONED = (EXPIRED, BOT_DIED, CANCELED)

  _NAMES = {
    RUNNING: 'Running',
    PENDING: 'Pending',
    EXPIRED: 'Expired (failed to find a bot before the request\'s expiration)',
    TIMED_OUT: 'One of the executed command timed out',
    BOT_DIED: 'Bot died while running the task. Either the task killed the bot '
      'or the bot suicided',
    CANCELED: 'User canceled the request',
    COMPLETED: 'Completed',
  }

  @classmethod
  def to_string(cls, state):
    """Returns a user-readable string representing a State."""
    if state not in cls._NAMES:
      raise ValueError('Invalid state %s' % state)
    return cls._NAMES[state]


class StateProperty(ndb.IntegerProperty):
  """State of a single task shard as a model property."""
  def __init__(self, **kwargs):
    # pylint: disable=E1002
    super(StateProperty, self).__init__(choices=State.STATES, **kwargs)


def _calc_modified_ts(result_summary):
  """Calculates TaskResultSummary.modified_ts with the latest value of all the
  shards.
  """
  if any(i.modified_ts for i in result_summary.shards):
    return max(i.modified_ts for i in result_summary.shards if i.modified_ts)
  return None


def _calc_task_state(result_summary):
  return min(i.task_state for i in result_summary.shards)


def _calc_task_failure(result_summary):
  return bool(any(i.task_failure for i in result_summary.shards))


def _calc_internal_failure(result_summary):
  return bool(any(i.internal_failure for i in result_summary.shards))


def _calc_done_ts(result_summary):
  return (
      _calc_modified_ts(result_summary)
      if _calc_task_state(result_summary) in State.STATES_NOT_RUNNING else None)


def _validate_not_pending(prop, value):
  if value == State.PENDING:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError('%s cannot be PENDING' % prop._name)


class TaskShardResult(ndb.Model):
  """Contains the aggregate results for a TaskShardToRun (a single shard).

  Parent is a TaskShardToRun. Key id is 1 on the initial try, then increases on
  automatic retries (if they are required).

  This includes results for one specific shard.

  Existence of this entity means a bot requested a task and started executing
  it. Everything beside created_ts and bot_id can be modified. Multiple
  TaskShardResult can be generated for the same TaskShardToRun, it happens when
  a bot dies, in case of internal failure or automatic retries.
  """
  # Time when a bot reaped this task shard.
  started_ts = ndb.DateTimeProperty(auto_now_add=True)

  # Bot that ran this task shard.
  bot_id = ndb.StringProperty()

  # Everything above is immutable, everything below is mutable.

  # Used to synchronize the data with TaskResultSummary(
  #   id=1, parent=self.task_request_key).shards[self.shard_number].modified_ts
  # This entity is updated everytime the bot sends data so it is equivalent to
  # 'last_ping'.
  modified_ts = ndb.DateTimeProperty(auto_now=True)

  # Current state of this task.
  task_state = StateProperty(
      default=State.RUNNING, validator=_validate_not_pending)

  # Records that the task failed, e.g. one process had a non-zero exit code. The
  # task may be retried if desired to weed out flakiness.
  task_failure = ndb.BooleanProperty(default=False)

  # Internal infrastructure failure, in which case the task should be retried
  # automatically if possible.
  internal_failure = ndb.BooleanProperty(default=False)

  # Time when the bot completed the task shard. Note that if the job
  # was improperly handled, for example state is BOT_DIED, abandoned_ts is
  # used instead of completed_ts.
  completed_ts = ndb.DateTimeProperty()
  abandoned_ts = ndb.DateTimeProperty()

  # Aggregated outputs. Ordered by command.
  outputs = ndb.KeyProperty(
      repeated=True, kind=result_helper.Results, indexed=False)

  # Aggregated exit codes. Ordered by command.
  exit_codes = ndb.IntegerProperty(repeated=True, indexed=False)

  # TODO(maruel): Remove, for compatibility with old code only. \/ \/ \/
  @property
  def created(self):
    return self.request_key.get().created_ts

  @property
  def dimensions(self):
    return utils.encode_to_json(self.request_key.get().properties.dimensions)

  @property
  def name(self):
    return self.request_key.get().name

  @property
  def started(self):
    return self.started_ts

  @property
  def ended(self):
    return self.completed_ts or self.abandoned_ts

  @property
  def ran_successfully(self):
    return not self.task_failure

  @property
  def aborted(self):
    return self.task_state in State.STATES_ABANDONED

  @property
  def requestor(self):
    return self.request_key.get().user

  automatic_retry_count = 0

  @property
  def machine_id(self):
    return self.bot_id

  def GetAsDict(self):
    request = self.request_key.get()
    return {
      'config_instance_index': 0,
      'config_name': request.name,
      'num_config_instances': 1,
      'request': {},
    }
  # TODO(maruel): Remove, for compatibility with old code only. ^^^^

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this shard."""
    return shard_result_key_to_request_key(self.key)

  @property
  def shard_to_run_key(self):
    """Returns the TaskShardToRun ndb.Key that is parent of this shard."""
    return self.key.parent()

  @property
  def try_number(self):
    """Retry number this task shard. 1 based."""
    return self.key.integer_id()

  def to_dict(self):
    out = super(TaskShardResult, self).to_dict()
    out['outputs'] = out['outputs'] or []
    out['exit_codes'] = out['exit_codes'] or []
    return out

  def duration(self):
    """Returns the runtime for this shard or None if not applicable.

    Shards abandoned or not started yet are not applicable and return None.
    """
    if self.abandoned_ts:
      return None
    end = self.completed_ts or task_common.utcnow()
    return end - self.started_ts

  def to_string(self):
    return state_to_string(self)

  def _pre_put_hook(self):
    """Use extra validation that cannot be validated throught 'validator'.

    A transaction is required to ensure the TaskResultSummary is coherent with
    its TaskShardResult. This permits to add the taskqueue that will enforce
    coherency to be added within the same transaction.
    """
    assert ndb.in_transaction()
    if self.task_state == State.EXPIRED and self.task_failure:
      raise datastore_errors.BadValueError(
          'Unexpected State, a task can\'t fail if it hasn\'t started yet')
    if self.task_state == State.TIMED_OUT and not self.task_failure:
      raise datastore_errors.BadValueError('Timeout implies task failure')


class TaskShardResultInner(ndb.Model):
  """Represents summary results of a shard, embedded in a TaskResultSummary."""
  # Represent the try attempt of the task shard. Starts at 1.
  try_number = ndb.IntegerProperty(indexed=False)

  # Used as the basis to know if it is up to date by comparing with
  # TaskResultSummary.modified_shard_ts.
  modified_ts = ndb.DateTimeProperty(indexed=False)

  # State of the last TaskShardResult run.
  task_state = StateProperty(default=State.PENDING, indexed=False)
  # See TaskShardResult.task_failure.
  task_failure = ndb.BooleanProperty(default=False)
  # See TaskShardResult.internal_failure.
  internal_failure = ndb.BooleanProperty(default=False)

  # Time when a bot reaped the corresponding TaskShardToRun and when the
  # TaskShardResult was marked as completed. Note that if the job was improperly
  # handled, for example state is BOT_DIED, abandoned_ts is used instead of
  # completed_ts.
  started_ts = ndb.DateTimeProperty(indexed=False)
  completed_ts = ndb.DateTimeProperty(indexed=False)
  abandoned_ts = ndb.DateTimeProperty(indexed=False)

  def duration(self):
    """Returns the runtime for this shard or None if not applicable.

    Shards abandoned or not started yet are not applicable and return None.
    """
    if not self.started_ts or self.abandoned_ts:
      return None
    end = self.completed_ts or task_common.utcnow()
    return end - self.started_ts

  def update_from_shard_result(self, shard_result):
    """Updates itself from a TaskShardResult.

    Returns:
      True if the data was modified.
    """
    # TODO(maruel): Test the situation where a shard is retried, but the bot
    # running the previous try somehow reappears and reports success, the job
    # should still be marked as success, not as running.
    if self.modified_ts and self.modified_ts > shard_result.modified_ts:
      diff = (self.modified_ts - shard_result.modified_ts).total_seconds()
      logging.error(
          'Unexpected datastore inconsistency. %s-%s = %fs in the future',
          self.modified_ts, shard_result.modified_ts, diff)
      return False
    # TODO(maruel): Check all the properties?
    if self.modified_ts == shard_result.modified_ts:
      return False
    # Copy the common values.
    self.abandoned_ts = shard_result.abandoned_ts
    self.completed_ts = shard_result.completed_ts
    self.internal_failure = shard_result.internal_failure
    self.modified_ts = shard_result.modified_ts
    self.started_ts = shard_result.started_ts
    self.task_failure = shard_result.task_failure
    self.task_state = shard_result.task_state
    self.try_number = shard_result.try_number
    return True


class TaskResultSummary(ndb.Model):
  """Contains the aggregate results for a TaskRequest.

  Parent is a TaskRequest. Key id is always 1.

  This includes results for all shards. This entity is basically a cache. As
  such, it is eventually consistent.

  It bases the consistency of its content on the TaskShardResult.modified_ts
  value of each of these sources, so this entity doesn't need to be saved inside
  a transaction.

  It's primary purpose is for status pages listing all the active tasks or
  recently completed tasks.
  """
  # Summary of the state of each shard. The .modified_ts is used for internal
  # consistency.
  shards = ndb.LocalStructuredProperty(
      TaskShardResultInner, repeated=True, indexed=False, compressed=True)

  # Last time modified including the TaskShardResultInner.modified_ts.
  modified_ts = ndb.ComputedProperty(_calc_modified_ts)

  # Calculated property from all the task shard states.
  task_state = ndb.ComputedProperty(_calc_task_state)
  # True if any shard has .task_failure = True
  task_failure = ndb.ComputedProperty(_calc_task_failure)
  # True if any shard has .internal_failure = True
  internal_failure = ndb.ComputedProperty(_calc_internal_failure)

  # Time at which the task changed to a value in State.STATES_DONE or
  # State.STATES_ABANDONED and has no shard scheduled for execution.
  done_ts = ndb.ComputedProperty(_calc_done_ts)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is parent of this result summary."""
    return self.key.parent()

  def shard_result_key(self, shard_id):
    """Returns the ndb.Key for the corresponding TaskShardResult.

    Arguments:
      - shard_id: must be 1 based.
    """
    if shard_id < 1:
      raise IndexError('Need positive value')
    try_number = self.shards[shard_id-1].try_number
    if not try_number:
      return None
    shard_to_run_key = task_shard_to_run.request_key_to_shard_to_run_key(
        self.request_key, shard_id)
    return ndb.Key(TaskShardResult, try_number, parent=shard_to_run_key)

  def duration(self):
    """Returns the total runtime of this request."""
    return sum(
        filter(None, (s.duration() for s in self.shards)),
        datetime.timedelta())

  def to_string(self):
    return state_to_string(self)


class TaskSyncMarker(ndb.Model):
  """Used by task_update_result_summary to note where to start the scan.

  It is a singleton with key id 1.
  """
  modified_ts = ndb.DateTimeProperty(indexed=False)


def _task_update_result_summary(request_id):
  """Updates TaskResultSummary for a TaskRequest key.

  Meant to be run from a task queue so it doesn't slow down HTTP request
  handlers and can be run from the backend.

  It loads the following entities:
  - TaskRequest
  - TaskResultSummary
  - All TaskShardResult

  If a TaskShardResult is modified in the meantime, it could be missed. It will
  be caught on the next execution of this task queue.
  """
  request_key = task_request.id_to_request_key(request_id)
  summary_key = request_key_to_result_summary_key(request_key)
  request, result_summary = ndb.get_multi((request_key, summary_key))
  if request is None:
    logging.error('TaskRequest doesn\'t exist anymore.\n%s', request_key)
    return None
  # TODO(maruel): An option is to use
  # TaskShardResult.query(ancestor=request_key). It is much less efficient but
  # works to get all the needed data even in case of retries. Retries are a
  # problem. The other problem is that it fetches the data for all retries,
  # which we do not want since we only care about the last execution.
  keys = [
    shard_to_run_key_to_shard_result_key(
        task_shard_to_run.request_key_to_shard_to_run_key(request_key, 1), 1),
  ]

  changed = False
  if not result_summary:
    result_summary = new_result_summary(request)
    changed = True

  for i, shard_result in enumerate(ndb.get_multi(keys)):
    if shard_result:
      changed |= result_summary.shards[i].update_from_shard_result(shard_result)

  if changed:
    result_summary.put()
  return result_summary


def _yield_requests_to_scan(start, page_size):
  """Yields tuple(TaskRequest, datetime) where the TaskResultSummary should be
  checked for consistency in sync_all_result_summary().

  The range is between start and now minus a tolerance.
  """
  # Set a tolerance level of 2 minutes. This means it is expected that most task
  # queues will complete within two minutes and the DB will have time to become
  # consistent.
  # TODO(maruel): Tune the value.
  cutoff = task_common.utcnow() - datetime.timedelta(seconds=2*60)
  if start and cutoff >= start:
    return

  # Note that prefetch_size is only for the first batch.
  opts = ndb.QueryOptions(
      projection=('modified_ts',),
      batch_size=page_size,
      prefetch_size=500)
  # TODO(maruel): Does order() requires a composite index?
  q = TaskShardResult.query(default_options=opts).order(
      TaskShardResult.modified_ts)
  q = q.filter(TaskShardResult.modified_ts <= cutoff)
  if start:
    q = q.filter(TaskShardResult.modified_ts >= start)

  for shard_result in q:
    yield shard_result.request_key, shard_result.modified_ts


def _sync_batch_of_result_summary(items):
  """Syncs a batch of TaskResultSummary as needed.

  Batching is used to reduce the effect of the DB latency.
  TODO(maruel): Async functions will help further.

  Arguments:
  - items: list of tuple(ndb.Key, datetime). The Key is to a TaskRequest. The
        datetime is the minimum modified_ts value that TaskResultSummary must
        have or it will be synced.

  Returns:
    Number of TaskResultSummary updated.
  """
  num_fixed = 0
  summary_keys = (
    request_key_to_result_summary_key(request_key) for request_key, _ in items
  )
  for i, result_summary in enumerate(ndb.get_multi(summary_keys)):
    request_key, modified_ts = items[i]
    if (result_summary and
        result_summary.modified_ts and
        result_summary.modified_ts >= modified_ts):
      # It's fine.
      continue
    logging.error(
        'Found inconsistent TaskResultSummary\n%s',
        request_key.integer_id())
    # Fix the stale (or missing) TaskResultSummary right here inline.
    result_summary = _task_update_result_summary(request_key.integer_id())
    num_fixed += 1
  return num_fixed


### Public API.


def state_to_string(state):
  """Returns a user-readable string representing a State."""
  out = State.to_string(state.task_state)
  if state.task_failure:
    out += ' (Task failed)'
  if state.internal_failure:
    out += ' (Internal failure)'
  return out


def request_key_to_result_summary_key(request_key):
  """Returns the TaskResultSummary ndb.Key for this TaskRequest.key.

  Arguments:
    request_key: ndb.Key for a TaskRequest entity.

  Returns:
    ndb.Key for the corresponding TaskResultSummary entity.
  """
  assert isinstance(request_key, ndb.Key)
  assert not (request_key.integer_id() & 0xFF)
  return ndb.Key(TaskResultSummary, 1, parent=request_key)


def shard_result_key_to_request_key(shard_result_key):
  """Returns the TaskRequest ndb.Key for this TaskShardResult key."""
  assert isinstance(shard_result_key, ndb.Key)
  return task_shard_to_run.shard_to_run_key_to_request_key(
      shard_result_key.parent())


def shard_to_run_key_to_shard_result_key(shard_to_run_key, try_number):
  """Returns the TaskShardResult ndb.Key for this TaskShardToRun.key.

  Arguments:
    shard_to_run_key: ndb.Key for a TaskShardToRun entity.
    try_number: the try on which TaskShardResult was created for. The first try
        is 1, the second is 2, etc.

  Returns:
    ndb.Key for the corresponding TaskShardResult entity.
  """
  if try_number != 1:
    raise NotImplementedError(
        'Try number(%d) != 1 is not yet implemented' % try_number)
  assert isinstance(shard_to_run_key, ndb.Key)
  return ndb.Key(TaskShardResult, try_number, parent=shard_to_run_key)


def new_result_summary(request):
  """Returns the new and only TaskResultSummary for a TaskRequest.

  The caller must save it in the DB.
  """
  shards = [TaskShardResultInner()]
  return TaskResultSummary(
      key=request_key_to_result_summary_key(request.key), shards=shards)


def new_shard_result(shard_to_run_key, try_number, bot_id):
  """Returns the new and only TaskShardResult for a TaskShardToRun.

  It is to be created when a task shard is actually starting to be run.
  """
  return TaskShardResult(
      key=shard_to_run_key_to_shard_result_key(shard_to_run_key, try_number),
      bot_id=bot_id)


def yield_shard_results_without_update():
  """Yields all the TaskShardResult where the bot died recently.

  In practice it is returning a ndb.Query but this is equivalent.
  """
  # If a bot didn't ping recently, it is considered dead.
  deadline = task_common.utcnow() - task_common.BOT_PING_TOLERANCE
  q = TaskShardResult.query().filter(TaskShardResult.modified_ts < deadline)
  q = q.filter(TaskShardResult.task_state == State.RUNNING)
  return q


@ndb.transactional
def put_shard_result(shard_result):
  shard_result.put()
  _enqueue_update_result_summary(shard_result.request_key)


def terminate_shard_result(shard_result, state):
  """Puts a TaskShardResult in dead state."""
  assert state in State.STATES_ABANDONED, state
  shard_result.task_state = state
  shard_result.abandoned_ts = task_common.utcnow()
  put_shard_result(shard_result)


### Task queue work.


def _enqueue_update_result_summary(request_key):
  """Adds a deferred to asynchronously update a single TaskResultSummary."""
  deferred.defer(
      _task_update_result_summary,
      request_key.integer_id(),
      _queue='update-result-summary',
      _transactional=True)


def sync_all_result_summary():
  """Syncs all TaskResultSummary with their TaskShardResult as needed.

  Only scans the TaskResultSummary where a TaskShardResult was updated since
  last scan. This is noted in entity TaskSyncMarker.
  """
  marker = TaskSyncMarker.get_by_id(1)
  marker_ts = (marker and marker.modified_ts) or None

  try:
    # Process a few items at a time to save on DB latency. It is a list of
    # tuple(request_key, datetime).
    items_in_flight = []
    num_fixed = 0
    for request_key, modified_ts in _yield_requests_to_scan(marker_ts, 100):
      items_in_flight.append((request_key, modified_ts))
      if len(items_in_flight) == 20:
        num_fixed += _sync_batch_of_result_summary(items_in_flight)
        marker_ts = items_in_flight[-1][1]
        items_in_flight = []
    if items_in_flight:
      num_fixed += _sync_batch_of_result_summary(items_in_flight)
      marker_ts = items_in_flight[-1][1]
    return num_fixed
  finally:
    # Note where to start searching next time.
    if marker_ts:
      TaskSyncMarker(id=1, modified_ts=marker_ts).put()
