# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task execution result models.

This module doesn't do the scheduling itself. It only describes the entities to
store tasks results.

Graph of the schema for TaskResultSummary and TaskRunResult. TaskResultSummary
represents the overall result for the TaskRequest taking in account retries.
TaskRunResult represents the result for one 'try'. There can be multiple tries
for one job, for example if a bot dies.

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
        +-----------------+
        |TaskResultSummary|
        +-----------------+
              ^      ^
              |      |
  +-------------+  +-------------+
  |TaskRunResult|  |TaskRunResult|
  +-------------+  +-------------+
"""

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from server import result_helper
from server import task_common
from server import task_request


class State(object):
  """States in which a task can be.

  It's in fact an enum. Values should be in decreasing order of importance.
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
  """State of a single task as a model property."""
  def __init__(self, **kwargs):
    # pylint: disable=E1002
    super(StateProperty, self).__init__(choices=State.STATES, **kwargs)


def _validate_not_pending(prop, value):
  if value == State.PENDING:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError('%s cannot be PENDING' % prop._name)


class _TaskResultCommon(ndb.Model):
  """Contains properties that is common to both TaskRunResult and
  TaskResultSummary.

  It is not meant to be instantiated on its own.
  """
  # Bot that ran this task.
  bot_id = ndb.StringProperty()

  # This entity is updated everytime the bot sends data so it is equivalent to
  # 'last_ping'.
  modified_ts = ndb.DateTimeProperty(auto_now=True)

  # Records that the task failed, e.g. one process had a non-zero exit code. The
  # task may be retried if desired to weed out flakiness.
  failure = ndb.BooleanProperty(default=False)

  # Internal infrastructure failure, in which case the task should be retried
  # automatically if possible.
  internal_failure = ndb.BooleanProperty(default=False)

  # Aggregated outputs. Ordered by command.
  outputs = ndb.KeyProperty(
      repeated=True, kind=result_helper.Results, indexed=False)

  # Aggregated exit codes. Ordered by command.
  exit_codes = ndb.IntegerProperty(repeated=True, indexed=False)

  # Time when a bot reaped this task.
  started_ts = ndb.DateTimeProperty()

  # Time when the bot completed the task. Note that if the job was improperly
  # handled, for example state is BOT_DIED, abandoned_ts is used instead of
  # completed_ts.
  completed_ts = ndb.DateTimeProperty()
  abandoned_ts = ndb.DateTimeProperty()

  @property
  def ended_ts(self):
    return self.completed_ts or self.abandoned_ts

  @property
  def pending(self):
    """Returns the timedelta that the task has been pending to be scheduled or
    None if not started yet.
    """
    if self.started_ts and self.created_ts:
      return self.started_ts - self.created_ts

  def get_outputs(self):
    """Returns the actual outputs as strings in a list."""
    # TODO(maruel): Rework this so all the entities can be fetched in parallel.
    return [o.get().GetResults() for o in self.outputs]

  def duration(self):
    """Returns the runtime for this task or None if not applicable.

    Task abandoned or not started yet are not applicable and return None.
    """
    if not self.started_ts or self.abandoned_ts:
      return None
    end = self.completed_ts or task_common.utcnow()
    return end - self.started_ts

  def to_string(self):
    return state_to_string(self)

  def to_dict(self):
    out = super(_TaskResultCommon, self).to_dict()
    out['exit_codes'] = out['exit_codes'] or []
    out['outputs'] = out['outputs'] or []
    return out


class TaskRunResult(_TaskResultCommon):
  """Contains the results for a TaskToRun scheduled on a bot.

  Parent is a TaskResultSummary. Key id is 1 on the initial try, then increases
  linearly on automatic retries (if they are required, for example there could
  be one automatic retry when the bot dies). Multiple TaskRunResult can be
  children for the same TaskResultSummary; it happens when a bot dies, in case
  of internal failure or optional automatic retries on failure=True.

  Existence of this entity means a bot requested a task and started executing
  it. Everything beside created_ts and bot_id can be modified.
  """
  # Current state of this task.
  state = StateProperty(default=State.RUNNING, validator=_validate_not_pending)

  @property
  def key_string(self):
    return task_common.pack_run_result_key(self.key)

  @property
  def created_ts(self):
    # TODO(maruel): This property is not efficient at lookup time so it is
    # probably better to duplicate the data. The trade off is that TaskRunResult
    # is saved a lot. Maybe we'll need to rethink this, maybe TaskRunSummary
    # wasn't a great idea after all.
    return self.request_key.get().created_ts

  @property
  def name(self):
    # TODO(maruel): This property is not efficient at lookup time so it is
    # probably better to duplicate the data. The trade off is that TaskRunResult
    # is saved a lot. Maybe we'll need to rethink this, maybe TaskRunSummary
    # wasn't a great idea after all.
    return self.request_key.get().name

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this entity."""
    return result_summary_key_to_request_key(self.result_summary_key)

  @property
  def result_summary_key(self):
    """Returns the TaskToRun ndb.Key that is parent of this entity."""
    return run_result_key_to_result_summary_key(self.key)

  @property
  def try_number(self):
    """Retry number this task. 1 based."""
    return self.key.integer_id()

  def to_dict(self):
    out = super(TaskRunResult, self).to_dict()
    out['try_number'] = self.try_number
    return out

  def _pre_put_hook(self):
    """Use extra validation that cannot be validated throught 'validator'."""
    super(TaskRunResult, self)._pre_put_hook()
    if self.state == State.EXPIRED and self.failure:
      raise datastore_errors.BadValueError(
          'Unexpected State, a task can\'t fail if it hasn\'t started yet')
    if self.state == State.TIMED_OUT and not self.failure:
      raise datastore_errors.BadValueError('Timeout implies task failure')


class TaskResultSummary(_TaskResultCommon):
  """Represents the overall result of a task.

  Parent is a TaskRequest. Key id is always 1.

  This includes the relevant result taking in account all tries. This entity is
  basically a cache.

  It's primary purpose is for status pages listing all the active tasks or
  recently completed tasks.
  """
  # These properties are directly copied from TaskRequest. They are only copied
  # here to simplify searches with the Web UI. They are immutable.
  created_ts = ndb.DateTimeProperty(required=True)
  name = ndb.StringProperty(required=True)
  user = ndb.StringProperty(required=True)

  # State of this task. The value from TaskRunResult will be copied over.
  state = StateProperty(default=State.PENDING)

  # Represent the last try attempt of the task. Starts at 1.
  try_number = ndb.IntegerProperty()

  @property
  def key_string(self):
    return task_common.pack_result_summary_key(self.key)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this entity."""
    return result_summary_key_to_request_key(self.key)

  def set_from_run_result(self, rhs):
    """Copies all the properties from another instance deriving from
    _TaskResultCommon.
    """
    for property_name in _TaskResultCommon._properties:
      setattr(self, property_name, getattr(rhs, property_name))
    # Include explicit support for 'state' and 'try_number'.
    # pylint: disable=W0201
    self.state = rhs.state
    self.try_number = rhs.try_number


### Public API.


def state_to_string(state_obj):
  """Returns a user-readable string representing a State."""
  out = State.to_string(state_obj.state)
  if state_obj.failure:
    out += ' (Task failed)'
  if state_obj.internal_failure:
    out += ' (Internal failure)'
  return out


def request_key_to_result_summary_key(request_key):
  """Returns the TaskResultSummary ndb.Key for this TaskRequest.key."""
  assert request_key.kind() == 'TaskRequest', request_key
  assert request_key.integer_id(), request_key
  assert not (request_key.integer_id() & 0xFF), request_key
  return ndb.Key(TaskResultSummary, 1, parent=request_key)


def result_summary_key_to_request_key(result_summary_key):
  """Returns the TaskRequest ndb.Key for this TaskResultSummmary key."""
  assert result_summary_key.kind() == 'TaskResultSummary', result_summary_key
  return result_summary_key.parent()


def result_summary_key_to_run_result_key(result_summary_key, try_number):
  """Returns the TaskRunResult ndb.Key for this TaskResultSummary.key.

  Arguments:
    result_summary_key: ndb.Key for a TaskResultSummary entity.
    try_number: the try on which TaskRunResult was created for. The first try
        is 1, the second is 2, etc.

  Returns:
    ndb.Key for the corresponding TaskRunResult entity.
  """
  assert result_summary_key.kind() == 'TaskResultSummary', result_summary_key
  if try_number < 1:
    raise ValueError('Try number(%d) must be above 0' % try_number)
  if try_number != 1:
    # https://code.google.com/p/swarming/issues/detail?id=108
    raise NotImplementedError(
        'Try number(%d) != 1 is not yet implemented' % try_number)
  return ndb.Key(TaskRunResult, try_number, parent=result_summary_key)


def run_result_key_to_result_summary_key(run_result_key):
  """Returns the TaskResultSummary ndb.Key for this TaskRunResult.key.
  """
  assert run_result_key.kind() == 'TaskRunResult', run_result_key
  return run_result_key.parent()


def new_result_summary(request):
  """Returns the new and only TaskResultSummary for a TaskRequest.

  The caller must save it in the DB.
  """
  return TaskResultSummary(
      key=request_key_to_result_summary_key(request.key),
      created_ts=request.created_ts,
      name=request.name,
      user=request.user)


def new_run_result(request, try_number, bot_id):
  """Returns a new TaskRunResult for a TaskRequest.

  The caller must save it in the DB.
  """
  assert isinstance(request, task_request.TaskRequest)
  summary_key = request_key_to_result_summary_key(request.key)
  return TaskRunResult(
      key=result_summary_key_to_run_result_key(summary_key, try_number),
      bot_id=unicode(bot_id),
      started_ts=task_common.utcnow())


def yield_run_results_with_dead_bot():
  """Yields all the TaskRunResult where the bot died recently.

  In practice it is returning a ndb.Query but this is equivalent.
  """
  # If a bot didn't ping recently, it is considered dead.
  deadline = task_common.utcnow() - task_common.BOT_PING_TOLERANCE
  q = TaskRunResult.query().filter(TaskRunResult.modified_ts < deadline)
  return q.filter(TaskRunResult.state == State.RUNNING)


def prepare_put_run_result(run_result):
  """Prepares the entity to be saved.

  It returns the updated TaskRunResult and TaskResultSummary to be saved.
  """
  # TODO(maruel): Test the situation where a shard is retried, but the bot
  # running the previous try somehow reappears and reports success, the job
  # should still be marked as success, not as running.
  assert isinstance(run_result, TaskRunResult)
  result_summary = run_result.result_summary_key.get()
  result_summary.set_from_run_result(run_result)
  return (run_result, result_summary)
