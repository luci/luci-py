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

The stdout of each command in TaskResult.properties.commands is saved inside
TaskOutput. It is chunked in TaskOutputChunk to fit the entity size limit.

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
                ^
                |
              +----------+
              |TaskOutput|
              +----------+
                ^       ^
                |       |
    +---------------+  +---------------+
    |TaskOutputChunk|  |TaskOutputChunk|
    +---------------+  +---------------+
"""

import logging

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import utils
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
    EXPIRED: 'Expired',
    TIMED_OUT: 'Execution timed out',
    BOT_DIED: 'Bot died',
    CANCELED: 'User canceled',
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


def _calculate_failure(result_common):
  # When a command times out, there may not be any exit code, it is still a user
  # process failure mode, not an infrastructure failure mode.
  return (
      any(result_common.exit_codes or []) or
      result_common.state == State.TIMED_OUT)


class TaskOutput(ndb.Model):
  """Phantom entity to represent a command output stored as small chunks.

  Parent is TaskRunResult. Key id is the command's index + 1, because id 0 is
  invalid.

  Child entities TaskOutputChunk are aggregated for the whole output.

  This entity doesn't actually exist in the DB. It only exists to make
  categories.
  """
  # The maximum size for each TaskOutputChunk.chunk. The rationale is that
  # appending data to an entity requires reading it first, so it must not be too
  # big. On the other hand, having thousands of small entities is pure overhead.
  # TODO(maruel): This value was selected from guts feeling. Do proper load
  # testing to find the best value.
  # TODO(maruel): This value should be stored in the entity for future-proofing.
  # It can't be changed until then.
  CHUNK_SIZE = 100*1024

  # Maximum content saved in a TaskOutput.
  # It is a safe-guard for tasks that sends way too much data. 100Mb should be
  # enough stdout.
  PUT_MAX_CONTENT = 100*1024*1024

  # Maximum number of chunks.
  PUT_MAX_CHUNKS = PUT_MAX_CONTENT / CHUNK_SIZE

  # Hard limit on the amount of data returned by get_output_async() at once.
  # Eventually, we'll want to add support for chunked fetch, if desired. Because
  # CHUNK_SIZE is hardcoded, it's not exactly 16Mb.
  FETCH_MAX_CONTENT = 16*1000*1024

  # Maximum number of chunks to fetch at once.
  FETCH_MAX_CHUNKS = FETCH_MAX_CONTENT / CHUNK_SIZE

  # It is easier if there is no remainder for efficiency.
  assert (PUT_MAX_CONTENT % CHUNK_SIZE) == 0
  assert (FETCH_MAX_CONTENT % CHUNK_SIZE) == 0

  @classmethod
  @ndb.tasklet
  def get_output_async(cls, output_key, number_chunks):
    """Returns the stdout for a single command as a ndb.Future."""
    # TODO(maruel): Save number_chunks locally in this entity.
    if not number_chunks:
      raise ndb.Return(None)

    number_chunks = min(number_chunks, cls.FETCH_MAX_CHUNKS)

    # TODO(maruel): Always get one more than necessary, in case number_chunks
    # is invalid. If there's an unexpected TaskOutputChunk entity present,
    # continue fetching for more incrementally.
    parts = []
    for f in ndb.get_multi_async(
        _output_key_to_output_chunk_key(output_key, i)
        for i in xrange(number_chunks)):
      chunk = yield f
      parts.append(chunk.chunk if chunk else None)

    # Trim ending empty chunks.
    while parts and not parts[-1]:
      parts.pop()

    # parts is now guaranteed to not end with an empty chunk.
    # Replace any missing chunk.
    for i in xrange(len(parts)):
      if not parts[i]:
        parts[i] = '\x00' * cls.CHUNK_SIZE
    raise ndb.Return(''.join(parts))


class TaskOutputChunk(ndb.Model):
  """Represents a chunk of a command output.

  Parent is TaskOutput. Key id is monotonically increasing starting from 1,
  since 0 is not a valid id.

  Each entity except the last one must have exactly
  len(self.chunk) == self.CHUNK_SIZE.
  """
  chunk = ndb.BlobProperty(default='', compressed=True)
  # gaps is a series of 2 integer pairs, which specifies the part that are
  # invalid. Normally it should be empty. All values are relative to the start
  # of this chunk offset.
  gaps = ndb.IntegerProperty(repeated=True, indexed=False)

  @property
  def chunk_number(self):
    return self.key.integer_id() - 1


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
  failure = ndb.ComputedProperty(_calculate_failure)

  # Internal infrastructure failure, in which case the task should be retried
  # automatically if possible.
  internal_failure = ndb.BooleanProperty(default=False)

  # Number of TaskOutputChunk entities for each output for each command. Set to
  # 0 when no output has been collected for a specific index. Ordered by
  # command.
  # TODO(maruel): Move into TaskOutput.
  stdout_chunks = ndb.IntegerProperty(repeated=True, indexed=False)

  # Aggregated exit codes. Ordered by command.
  exit_codes = ndb.IntegerProperty(repeated=True, indexed=False)

  # Aggregated durations in seconds. Ordered by command.
  durations = ndb.FloatProperty(repeated=True, indexed=False)

  # Time when a bot reaped this task.
  started_ts = ndb.DateTimeProperty()

  # Time when the bot completed the task. Note that if the job was improperly
  # handled, for example state is BOT_DIED, abandoned_ts is used instead of
  # completed_ts.
  completed_ts = ndb.DateTimeProperty()
  abandoned_ts = ndb.DateTimeProperty()

  @property
  def can_be_canceled(self):
    return self.state == State.PENDING

  @property
  def ended_ts(self):
    return self.completed_ts or self.abandoned_ts

  @property
  def pending(self):
    """Returns the timedelta the task spent pending to be scheduled or None if
    not started yet."""
    if self.started_ts:
      return self.started_ts - self.created_ts

  def pending_now(self):
    """Returns the timedelta the task spent pending to be scheduled as of now.
    """
    return (self.started_ts or utils.utcnow()) - self.created_ts

  @property
  def priority(self):
    # TODO(maruel): This property is not efficient at lookup time so it is
    # probably better to duplicate the data. The trade off is that TaskRunResult
    # is saved a lot. Maybe we'll need to rethink this, maybe TaskRunSummary
    # wasn't a great idea after all.
    return self.request_key.get().priority

  @property
  def duration(self):
    """Returns the timedelta the task spent executing.

    Task abandoned or not yet completed are not applicable and return None.
    """
    if not self.started_ts or not self.completed_ts:
      return None
    return self.completed_ts - self.started_ts

  def duration_now(self):
    """Returns the timedelta the task spent executing as of now.

    Task abandoned is not applicable and return None.
    """
    if not self.started_ts or self.abandoned_ts:
      return None
    return (self.completed_ts or utils.utcnow()) - self.started_ts

  def to_string(self):
    return state_to_string(self)

  def to_dict(self):
    out = super(_TaskResultCommon, self).to_dict()
    out['durations'] = out['durations'] or []
    out['exit_codes'] = out['exit_codes'] or []
    out['outputs'] = self.get_outputs()
    # Make the output consistent independent if using the old format or the new
    # one.
    # TODO(maruel): Remove the old format once the DBs have been cleared on the
    # server.
    del out['stdout_chunks']
    return out

  def _pre_put_hook(self):
    """Use extra validation that cannot be validated throught 'validator'."""
    super(_TaskResultCommon, self)._pre_put_hook()
    if self.state == State.EXPIRED:
      if self.failure or self.exit_codes:
        raise datastore_errors.BadValueError(
            'Unexpected State, a task can\'t fail if it hasn\'t started yet')
      if not self.internal_failure:
        raise datastore_errors.BadValueError(
            'Unexpected State, EXPIRED is internal failure')

    if self.state == State.TIMED_OUT and not self.failure:
      raise datastore_errors.BadValueError('Timeout implies task failure')

  def _get_outputs(self, run_result_key):
    """Returns the actual outputs as a list of strings."""
    # TODO(maruel): Make this function async.
    if not run_result_key or not self.stdout_chunks:
      # The task was not reaped or no output was streamed yet.
      return []

    # Fetch everything in parallel.
    futures = [
      self._get_command_output_async(run_result_key, command_index)
      for command_index in xrange(len(self.stdout_chunks))
    ]
    return [future.get_result() for future in futures]

  @ndb.tasklet
  def _get_command_output_async(self, run_result_key, command_index):
    """Returns the stdout for a single command as a ndb.Future.

    Use out.get_result() to get the data as a str or None if no output is
    present.
    """
    if not run_result_key or command_index >= len(self.stdout_chunks or []):
      # The task was not reaped or no output was streamed for this index yet.
      raise ndb.Return(None)

    number_chunks = self.stdout_chunks[command_index]
    if not number_chunks:
      raise ndb.Return(None)

    output_key = _run_result_key_to_output_key(run_result_key, command_index)
    out = yield TaskOutput.get_output_async(output_key, number_chunks)
    raise ndb.Return(out)


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

  def append_output(self, command_index, output, output_chunk_start):
    """Appends output to the stdout of the command.

    Returns the entities to save.
    """
    while len(self.stdout_chunks) <= command_index:
      # The reason for this to be a loop is that items could be handled out of
      # order.
      self.stdout_chunks.append(0)
    entities, self.stdout_chunks[command_index] = _output_append(
        _run_result_key_to_output_key(self.key, command_index),
        self.stdout_chunks[command_index],
        output,
        output_chunk_start)
    assert self.stdout_chunks[command_index] <= TaskOutput.PUT_MAX_CHUNKS
    return entities

  def get_outputs(self):
    return self._get_outputs(self.key)

  def get_command_output_async(self, command_index):
    return self._get_command_output_async(self.key, command_index)

  def to_dict(self):
    out = super(TaskRunResult, self).to_dict()
    out['try_number'] = self.try_number
    return out


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

  @property
  def run_result_key(self):
    if not self.try_number:
      return None
    return result_summary_key_to_run_result_key(self.key, self.try_number)

  def get_outputs(self):
    return self._get_outputs(self.run_result_key)

  def get_command_output_async(self, command_index):
    return self._get_command_output_async(self.run_result_key, command_index)

  def set_from_run_result(self, run_result):
    """Copies all the relevant properties from a TaskRunResult into this
    TaskResultSummary.
    """
    assert isinstance(run_result, TaskRunResult), run_result
    for property_name in _TaskResultCommon._properties:
      if isinstance(
          getattr(_TaskResultCommon, property_name), ndb.ComputedProperty):
        continue
      setattr(self, property_name, getattr(run_result, property_name))
    # Include explicit support for 'state' and 'try_number'. TaskRunResult.state
    # is a ComputedProperty so it can't be copied as-is, and try_number is a
    # generated property.
    # pylint: disable=W0201
    self.state = run_result.state
    self.try_number = run_result.try_number

  def need_update_from_run_result(self, run_result):
    """Returns True if set_from_run_result() would modify this instance.

    E.g. they are different and TaskResultSummary needs to be updated from the
    corresponding TaskRunResult.
    """
    assert isinstance(run_result, TaskRunResult), run_result
    # A previous try is still sending update. Ignore it from a result summary
    # PoV.
    if self.try_number and self.try_number > run_result.try_number:
      return False

    for property_name in _TaskResultCommon._properties:
      if isinstance(
          getattr(_TaskResultCommon, property_name), ndb.ComputedProperty):
        continue
      if getattr(self, property_name) != getattr(run_result, property_name):
        return True
    # Include explicit support for 'state' and 'try_number'. TaskRunResult.state
    # is a ComputedProperty so it can't be copied as-is, and try_number is a
    # generated property.
    # pylint: disable=W0201
    return (
        self.state != run_result.state or
        self.try_number != run_result.try_number)


### Private stuff.


def _run_result_key_to_output_key(run_result_key, command_index):
  """Returns a ndb.key to a TaskOutput. command_index is zero-indexed."""
  assert run_result_key.kind() == 'TaskRunResult', run_result_key
  assert command_index >= 0, command_index
  return ndb.Key(TaskOutput, command_index+1, parent=run_result_key)


def _output_key_to_output_chunk_key(output_key, chunk_number):
  """Returns a ndb.key to a TaskOutputChunk.

  Both command_index and chunk_number are zero-indexed.
  """
  assert output_key.kind() == 'TaskOutput', output_key
  assert chunk_number >= 0, chunk_number
  return ndb.Key(TaskOutputChunk, chunk_number+1, parent=output_key)


def _output_append(output_key, number_chunks, output, output_chunk_start):
  """Appends output to a TaskOutput in TaskOutputChunk entities.

  Creates new TaskOutputChunk entities as necessary as children of
  TaskRunResult/TaskOutput.

  It silently drops saving the output if it goes over ~16Mb. The hard limit is
  32Mb but HTML escaping can expand the raw data a bit, so just store half of
  the limit to be on the safe side.

  TODO(maruel): This is because AppEngine can't do response over 32Mb and at
  this point, it's probably just a ton of junk. Figure out a way to better
  implement this if necessary.

  Does one DB read by key and no puts. It's the responsibility of the caller to
  save the entities.

  Arguments:
    output_key: ndb.Key to TaskOutput that is the parent of TaskOutputChunk.
    number_chunks: Current number of TaskOutputChunk instances. If 0, this means
        there is not data yet.
    output: Actual content to append.
    output_chunk_start: Index of the data to be written to.

  Returns:
    A tuple of (list of entities to save, number_chunks). The number_chunks is
    the number of TaskOutputChunk instances for this output.
  """
  assert output and isinstance(output, str), output
  assert output_key.kind() == 'TaskOutput', output_key

  # Split everything in small bits.
  chunks = []
  while output:
    chunk_number = output_chunk_start / TaskOutput.CHUNK_SIZE
    if chunk_number >= TaskOutput.PUT_MAX_CHUNKS:
      # TODO(maruel): Log into TaskOutput that data was dropped.
      logging.error('Dropping %d of output', len(output))
      break
    key = _output_key_to_output_chunk_key(output_key, chunk_number)
    start = output_chunk_start % TaskOutput.CHUNK_SIZE
    next_start = TaskOutput.CHUNK_SIZE - start
    chunks.append((key, start, output[:next_start]))
    output = output[next_start:]
    number_chunks = max(number_chunks, chunk_number + 1)
    output_chunk_start = (chunk_number+1)*TaskOutput.CHUNK_SIZE

  if not chunks:
    return [], number_chunks

  # Get the TaskOutputChunk from the DB. Normally it would be only one entity
  # (the last incomplete one) but this code supports arbitrary overwrite.
  #
  # number_chunks should normally be used to skip entities that are assumed to
  # not be present but we don't assume the number_chunks is valid for safety.
  #
  # This means an unneeded get() is done on the missing chunk.
  entities = ndb.get_multi(i[0] for i in chunks)

  # Update the entities.
  for i in xrange(len(chunks)):
    key, start, output = chunks[i]
    if not entities[i]:
      # Fill up for missing entities.
      entities[i] = TaskOutputChunk(key=key)
    chunk = entities[i]
    # Magically combine everything.
    end = start + len(output)
    if len(chunk.chunk) < start:
      # Insert blank data automatically.
      chunk.gaps.extend((len(chunk.chunk), start))
      chunk.chunk = chunk.chunk + '\x00' * (start-len(chunk.chunk))

    # Strip gaps that are being written to.
    new_gaps = []
    for i in xrange(0, len(chunk.gaps), 2):
      # All values are relative to the starting offset of the chunk itself.
      gap_start = chunk.gaps[i]
      gap_end = chunk.gaps[i+1]
      # If the gap overlaps the chunk being written, strip it. Cases:
      #   Gap:     |   |
      #   Chunk: |   |
      if start <= gap_start <= end and end <= gap_end:
        gap_start = end

      #   Gap:     |   |
      #   Chunk:     |   |
      if gap_start <= start and start <= gap_end <= end:
        gap_end = start

      #   Gap:       |  |
      #   Chunk:   |      |
      if start <= gap_start <= end and start <= gap_end <= end:
        continue

      #   Gap:     |      |
      #   Chunk:     |  |
      if gap_start < start < gap_end and gap_start <= end <= gap_end:
        # Create a hole.
        new_gaps.extend((gap_start, start))
        new_gaps.extend((end, gap_end))
      else:
        new_gaps.extend((gap_start, gap_end))

    chunk.gaps = new_gaps
    chunk.chunk = chunk.chunk[:start] + output + chunk.chunk[end:]
  return entities, number_chunks


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
  if try_number > 2:
    # https://code.google.com/p/swarming/issues/detail?id=108
    raise NotImplementedError(
        'Try number(%d) > 2 is not yet implemented' % try_number)
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
      bot_id=bot_id,
      started_ts=utils.utcnow())


def yield_run_results_with_dead_bot():
  """Yields all the TaskRunResult where the bot died recently.

  In practice it is returning a ndb.Query but this is equivalent.
  """
  # If a bot didn't ping recently, it is considered dead.
  deadline = utils.utcnow() - task_common.BOT_PING_TOLERANCE
  q = TaskRunResult.query().filter(TaskRunResult.modified_ts < deadline)
  return q.filter(TaskRunResult.state == State.RUNNING)


def prepare_put_run_result(run_result):
  """Prepares the entity to be saved.

  Return:
    list(TaskRunResult, TaskResultSummary) to be saved. It's possible that the
    list only has TaskRunResult when it's updating an try_number that is lower
    than what TaskResultSummary is at.
  """
  assert isinstance(run_result, TaskRunResult)
  result_summary = run_result.result_summary_key.get()
  if (result_summary.try_number and
      result_summary.try_number > run_result.try_number):
    # The situation where a shard is retried, but the bot running the previous
    # try somehow reappears and reports success, the result must still show the
    # last try's result.
    return [run_result]

  result_summary.set_from_run_result(run_result)
  return [run_result, result_summary]
