# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task execution result models.

This module doesn't do the scheduling itself. It only describes the entities to
store tasks results.

- TaskResultSummary represents the overall result for the TaskRequest taking in
  account retries.
- TaskRunResult represents the result for one 'try'. There can
  be multiple tries for one job, for example if a bot dies.
- The stdout of each command in TaskResult.properties.commands is saved inside
  TaskOutput.
- It is chunked in TaskOutputChunk to fit the entity size limit.

Graph of schema:

               +--------Root---------+
               |TaskRequest          |
               |    +--------------+ |   (task_request.py)
               |    |TaskProperties| |
               |    +--------------+ |
               |id=<based on epoch>  |
               +---------------------+
                          ^
                          |
                          |
                  +-----------------+
                  |TaskResultSummary|
                  |id=1             |
                  +-----------------+
                       ^          ^
                       |          |
                       |          |
               +-------------+  +-------------+
               |TaskRunResult|  |TaskRunResult|
               |id=1 <try #> |  |id=2         |
               +-------------+  +-------------+
                ^           ^           ...
                |           |
        +----------------+  +----------+
        |TaskOutput      |  |TaskOutput| ...
        |id=1 <cmd index>|  |id=2      |
        +----------------+  +----------+
                 ^      ^        ...
                 |      |
    +---------------+  +---------------+
    |TaskOutputChunk|  |TaskOutputChunk| ...
    |id=1           |  |id=2           |
    +---------------+  +---------------+
"""

import datetime
import logging

from google.appengine.api import datastore_errors
from google.appengine.api import search
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from components import utils
from server import task_request


# Amount of time after which a bot is considered dead. In short, if a bot has
# not ping in the last 5 minutes while running a task, it is considered dead.
BOT_PING_TOLERANCE = datetime.timedelta(seconds=5*60)


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

  # Bot version (as a hash) of the code running the task.
  bot_version = ndb.StringProperty()

  # Active server version(s). Note that during execution, the active server
  # version may have changed, this list will list all versions seen as the task
  # was updated.
  server_versions = ndb.StringProperty(repeated=True)

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
    """Returns True if the task is in a state that can be canceled."""
    # TOOD(maruel): To be able to add State.RUNNING, the following must be done:
    # task_scheduler.cancel_task() must be strictly a transaction relative to
    # task_scheduler.bot_kill_task() and task_scheduler.bot_update_task().
    #
    # The tricky part is to keep this code performant. On the other hand, all
    # the entities under the transaction (TaskToRun, TaskResultSummary and
    # TaskRunResult) are under the same entity root, so it's definitely
    # feasible, likely using a transaction is not a problem in practice. The
    # important part would be to ensure that TaskOuputChunks are not also stored
    # as part of the transaction, since they do not need to.
    # https://code.google.com/p/swarming/issues/detail?id=62
    return self.state == State.PENDING

  @property
  def duration(self):
    """Returns the timedelta the task spent executing.

    Task abandoned or not yet completed are not applicable and return None.
    """
    if not self.started_ts or not self.completed_ts:
      return None
    return self.completed_ts - self.started_ts

  def duration_now(self, now):
    """Returns the timedelta the task spent executing as of now.

    Similar to .duration except that its return value is not deterministic. Task
    abandoned is not applicable and return None.
    """
    if not self.started_ts or self.abandoned_ts:
      return None
    return (self.completed_ts or now) - self.started_ts

  @property
  def ended_ts(self):
    return self.completed_ts or self.abandoned_ts

  @property
  def is_pending(self):
    """Returns True if the task is still pending. Mostly for html view."""
    return self.state == State.PENDING

  @property
  def is_running(self):
    """Returns True if the task is still pending. Mostly for html view."""
    return self.state == State.RUNNING

  @property
  def pending(self):
    """Returns the timedelta the task spent pending to be scheduled.

    Returns None if not started yet or if the task was deduped from another one.
    """
    if not self.deduped_from and self.started_ts:
      return self.started_ts - self.created_ts
    return None

  def pending_now(self, now):
    """Returns the timedelta the task spent pending to be scheduled as of now.

    Similar to .pending except that its return value is not deterministic.
    """
    if self.deduped_from:
      return None
    return (self.started_ts or now) - self.created_ts

  @property
  def priority(self):
    # TODO(maruel): This property is not efficient at lookup time so it is
    # probably better to duplicate the data. The trade off is that TaskRunResult
    # is saved a lot. Maybe we'll need to rethink this, maybe TaskRunSummary
    # wasn't a great idea after all.
    return self.request_key.get().priority

  @property
  def run_result_key(self):
    """Returns the active TaskRunResult key."""
    raise NotImplementedError()

  def to_string(self):
    return state_to_string(self)

  def to_dict(self):
    out = super(_TaskResultCommon, self).to_dict()
    # stdout_chunks is an implementation detail.
    out.pop('stdout_chunks')
    out['id'] = self.key_string
    return out

  def signal_server_version(self, server_version):
    """Adds `server_version` to self.server_versions if relevant."""
    if not self.server_versions or self.server_versions[-1] != server_version:
      self.server_versions.append(server_version)

  def get_outputs(self):
    """Yields the actual outputs as a generator of strings."""
    # TODO(maruel): Make this function async.
    if not self.run_result_key or not self.stdout_chunks:
      # The task was not reaped or no output was streamed yet.
      return []

    # Fetch everything in parallel.
    futures = [
      self.get_command_output_async(command_index)
      for command_index in xrange(len(self.stdout_chunks))
    ]
    return (future.get_result() for future in futures)

  @ndb.tasklet
  def get_command_output_async(self, command_index):
    """Returns the stdout for a single command as a ndb.Future.

    Use out.get_result() to get the data as a str or None if no output is
    present.
    """
    assert isinstance(command_index, int), command_index
    if (not self.run_result_key or
        command_index >= len(self.stdout_chunks or [])):
      # The task was not reaped or no output was streamed for this index yet.
      raise ndb.Return(None)

    number_chunks = self.stdout_chunks[command_index]
    if not number_chunks:
      raise ndb.Return(None)

    output_key = _run_result_key_to_output_key(
        self.run_result_key, command_index)
    out = yield TaskOutput.get_output_async(output_key, number_chunks)
    raise ndb.Return(out)

  def _pre_put_hook(self):
    """Use extra validation that cannot be validated throught 'validator'."""
    super(_TaskResultCommon, self)._pre_put_hook()
    if self.state == State.EXPIRED:
      if self.failure or self.exit_codes:
        raise datastore_errors.BadValueError(
            'Unexpected State, a task can\'t fail if it hasn\'t started yet')

    if self.state == State.TIMED_OUT and not self.failure:
      raise datastore_errors.BadValueError('Timeout implies task failure')


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

  # A task run execution can't be definition be deduped from another task.
  # Still, setting this property always to None simplifies a lot the
  # presentation layer.
  deduped_from = None

  @property
  def key_string(self):
    return pack_run_result_key(self.key)

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
  def run_result_key(self):
    return self.key

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
  # here to simplify searches with the Web UI and to enable DB queries based on
  # both user and results properties (e.g. all requests from X which succeeded).
  # They are immutable.
  # TODO(maruel): Investigate what is worth copying over.
  created_ts = ndb.DateTimeProperty(required=True)
  name = ndb.StringProperty(required=True)
  user = ndb.StringProperty(required=True)

  # Value of TaskRequest.properties.properties_hash only when these conditions
  # are met:
  # - TaskRequest.properties.idempotent is True
  # - self.state == State.COMPLETED
  # - self.failure == False
  # - self.internal_failure == False
  properties_hash = ndb.BlobProperty(indexed=True)

  # State of this task. The value from TaskRunResult will be copied over.
  state = StateProperty(default=State.PENDING)

  # Represent the last try attempt of the task. Starts at 1 EXCEPT when the
  # results were deduped, in this case it's 0.
  try_number = ndb.IntegerProperty()

  # Set to the task run id when the task result was retrieved from another task.
  # A task run id is a reference to a TaskRunResult generated via
  # pack_run_result_key(). The reason so store the packed version instead of
  # KeyProperty is that it's much shorter and it futureproofing refactoring of
  # the entity hierarchy.
  #
  # Note that when it's set, there's no TaskRunResult child since there was no
  # run.
  deduped_from = ndb.StringProperty(indexed=False)

  @property
  def key_string(self):
    return pack_result_summary_key(self.key)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this entity."""
    return result_summary_key_to_request_key(self.key)

  @property
  def run_result_key(self):
    if self.deduped_from:
      # Return the run results for the original task.
      return unpack_run_result_key(self.deduped_from)

    if not self.try_number:
      return None
    return result_summary_key_to_run_result_key(self.key, self.try_number)

  def reset_to_pending(self):
    """Resets this entity to pending state."""
    self.durations = []
    self.exit_codes = []
    self.internal_failure = False
    self.state = State.PENDING
    self.started_ts = None

  def set_from_run_result(self, run_result, request):
    """Copies all the relevant properties from a TaskRunResult into this
    TaskResultSummary.

    If the task completed, succeeded and is idempotent, self.properties_hash is
    set.
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

    if (self.state == State.COMPLETED and not self.failure and
        not self.internal_failure and request.properties.idempotent):
      # Signal the results are valid and can be reused.
      self.properties_hash = request.properties.properties_hash
      assert self.properties_hash

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

  def to_dict(self):
    out = super(TaskResultSummary, self).to_dict()
    if out['properties_hash']:
      out['properties_hash'] = out['properties_hash'].encode('hex')
    return out


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
      logging.error('Dropping output\n%d bytes were lost', len(output))
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
  if state_obj.deduped_from:
    return 'Deduped'
  out = State.to_string(state_obj.state)
  if state_obj.failure:
    out += ' (failed)'
  if state_obj.internal_failure:
    out += ' (internal failure)'
  return out


def request_key_to_result_summary_key(request_key):
  """Returns the TaskResultSummary ndb.Key for this TaskRequest.key."""
  assert request_key.kind() == 'TaskRequest', request_key
  assert request_key.integer_id(), request_key
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


def pack_result_summary_key(result_summary_key):
  """Returns TaskResultSummary ndb.Key encoded, safe to use in HTTP requests.
  """
  assert result_summary_key.kind() == 'TaskResultSummary'
  request_key = result_summary_key_to_request_key(result_summary_key)
  return task_request.request_key_to_id(request_key) + '0'


def pack_run_result_key(run_result_key):
  """Returns TaskRunResult ndb.Key encoded, safe to use in HTTP requests.
  """
  assert run_result_key.kind() == 'TaskRunResult'
  request_key = result_summary_key_to_request_key(
      run_result_key_to_result_summary_key(run_result_key))
  key_id = task_request.request_key_to_id(request_key)
  return key_id + '%x' % run_result_key.integer_id()


def unpack_result_summary_key(packed_key):
  """Returns the TaskResultSummary ndb.Key from a packed key.

  The expected format of |packed_key| is %x.
  """
  request_key = task_request.request_id_to_key(packed_key[:-1])
  run_id = int(packed_key[-1], 16)
  if run_id & 0xff:
    raise ValueError('Can\'t reference to a specific try result.')
  return request_key_to_result_summary_key(request_key)


def unpack_run_result_key(packed_key):
  """Returns the TaskRunResult ndb.Key from a packed key.

  The expected format of |packed_key| is %x.
  """
  request_key = task_request.request_id_to_key(packed_key[:-1])
  run_id = int(packed_key[-1], 16)
  if not run_id:
    raise ValueError('Can\'t reference to the overall task result.')
  result_summary_key = request_key_to_result_summary_key(request_key)
  return result_summary_key_to_run_result_key(result_summary_key, run_id)


def new_result_summary(request):
  """Returns the new and only TaskResultSummary for a TaskRequest.

  The caller must save it in the DB.
  """
  return TaskResultSummary(
      key=request_key_to_result_summary_key(request.key),
      created_ts=request.created_ts,
      name=request.name,
      user=request.user)


def new_run_result(request, try_number, bot_id, bot_version):
  """Returns a new TaskRunResult for a TaskRequest.

  The caller must save it in the DB.
  """
  assert isinstance(request, task_request.TaskRequest)
  summary_key = request_key_to_result_summary_key(request.key)
  return TaskRunResult(
      key=result_summary_key_to_run_result_key(summary_key, try_number),
      bot_id=bot_id,
      started_ts=utils.utcnow(),
      bot_version=bot_version,
      server_versions=[utils.get_app_version()])


def yield_run_result_keys_with_dead_bot():
  """Yields all the TaskRunResult ndb.Key where the bot died recently.

  In practice it is returning a ndb.QueryIterator but this is equivalent.
  """
  # If a bot didn't ping recently, it is considered dead.
  deadline = utils.utcnow() - BOT_PING_TOLERANCE
  q = TaskRunResult.query().filter(TaskRunResult.modified_ts < deadline)
  return q.filter(TaskRunResult.state == State.RUNNING).iter(keys_only=True)


def get_tasks(task_name, task_tags, cursor_str, limit, sort, state):
  """Returns TaskResultSummary entities for this query.

  This function is synchronous.

  Arguments:
    task_name: search for task name whole word.
    task_tags: list of search for one or multiple task tags.
    cursor_str: query-dependent string encoded cursor to continue a previous
        search.
    limit: Maximum number of items to return.
    sort: get_result_summary_query() argument. Only used is both task_name and
        task_tags are empty.
    state: get_result_summary_query() argument. Only used is both task_name and
        task_tags are empty.

  Returns:
    tuple(list of tasks, str encoded cursor, updated sort, updated state)
  """
  if task_tags:
    # Tag based search. Override the flags.
    sort = 'created_ts'
    state = 'all'
    # Only the TaskRequest has the tags. So first query all the keys to
    # requests; then fetch the TaskResultSummary.
    order = datastore_query.PropertyOrder(
        sort, datastore_query.PropertyOrder.DESCENDING)
    query = task_request.TaskRequest.query().order(order)
    task_tags = task_tags[:]
    tags_filter = task_request.TaskRequest.tags == task_tags.pop(0)
    while task_tags:
      tags_filter = ndb.AND(
          tags_filter, task_request.TaskRequest.tags == task_tags.pop(0))
    query = query.filter(tags_filter)
    cursor = datastore_query.Cursor(urlsafe=cursor_str)
    requests, cursor, more = query.fetch_page(
        limit, start_cursor=cursor, keys_only=True)
    keys = [request_key_to_result_summary_key(k) for k in requests]
    tasks = ndb.get_multi(keys)
    cursor_str = cursor.urlsafe() if cursor and more else None
  elif task_name:
    # Task name based word based search. Override the flags.
    sort = 'created_ts'
    state = 'all'
    tasks, cursor_str = search_by_name(task_name, cursor_str, limit)
  else:
    # Normal listing.
    queries, query = get_result_summary_query(sort, state)
    if queries:
      # When multiple queries are used, we can't use a cursor.
      cursor_str = None

      # Take the first |limit| items for each query. This is not efficient,
      # worst case is fetching N * limit entities.
      futures = [q.fetch_async(limit) for q in queries]
      lists = sum((f.get_result() for f in futures), [])
      tasks = sorted(lists, key=lambda i: i.created_ts, reverse=True)[:limit]
    else:
      # Normal efficient behavior.
      cursor = datastore_query.Cursor(urlsafe=cursor_str)
      tasks, cursor, more = query.fetch_page(limit, start_cursor=cursor)
      cursor_str = cursor.urlsafe() if cursor and more else None

  return tasks, cursor_str, sort, state


def get_result_summary_query(sort, state):
  """Generates one or many ndb.Query to return TaskResultSummary in the order
  and state specified.

  There can be multiple queries when a single query may not be able to return
  all results.
  TODO(maruel): Update entities so multiple queries code path is not necessary
  anymore.

  Arguments:
    sort: One valid TaskResultSummary property that can be used for sorting.
    state: One of the known state to filter on.

  Returns:
    tuple(queries, query); one is valid and the other is None.
  """
  query = TaskResultSummary.query()
  if sort:
    order = datastore_query.PropertyOrder(
        sort, datastore_query.PropertyOrder.DESCENDING)
    query = query.order(order)

  if state == 'pending':
    return None, query.filter(TaskResultSummary.state == State.PENDING)

  if state == 'running':
    return None, query.filter(TaskResultSummary.state == State.RUNNING)

  if state == 'pending_running':
    # This is a special case that sends two concurrent queries under the hood.
    # ndb.OR() doesn't work when order() is used, it requires __key__ sorting.
    # This is not efficient, so the DB should be updated accordingly to be
    # able to support pagination.
    queries = [
      query.filter(TaskResultSummary.state == State.PENDING),
      query.filter(TaskResultSummary.state == State.RUNNING),
    ]
    return queries, None

  if state == 'completed':
    return None, query.filter(TaskResultSummary.state == State.COMPLETED)

  if state == 'completed_success':
    query = query.filter(TaskResultSummary.state == State.COMPLETED)
    return None, query.filter(TaskResultSummary.failure == False)

  if state == 'completed_failure':
    query = query.filter(TaskResultSummary.state == State.COMPLETED)
    return None, query.filter(TaskResultSummary.failure == True)

  if state == 'expired':
    return None, query.filter(TaskResultSummary.state == State.EXPIRED)

  # TODO(maruel): This is never set until the new bot API is writen.
  # https://code.google.com/p/swarming/issues/detail?id=117
  if state == 'timed_out':
    return None, query.filter(TaskResultSummary.state == State.TIMED_OUT)

  if state == 'bot_died':
    return None, query.filter(TaskResultSummary.state == State.BOT_DIED)

  if state == 'canceled':
    return None, query.filter(TaskResultSummary.state == State.CANCELED)

  if state == 'all':
    return None, query

  raise ValueError('Invalid state')


def search_by_name(word, cursor_str, limit):
  """Returns TaskResultSummary in -created_ts order containing the word."""
  cursor = search.Cursor(web_safe_string=cursor_str, per_result=True)
  index = search.Index(name='requests')

  def item_to_id(item):
    for field in item.fields:
      if field.name == 'id':
        return field.value

  # The code is structured to handle incomplete entities but still return
  # 'limit' items. This is done by fetching a few more entities than necessary,
  # then keeping track of the cursor per item so the right cursor can be
  # returned.
  opts = search.QueryOptions(limit=limit + 5, cursor=cursor)
  results = index.search(search.Query('name:%s' % word, options=opts))
  result_summary_keys = []
  cursors = []
  for item in results.results:
    value = item_to_id(item)
    if value:
      result_summary_keys.append(unpack_result_summary_key(value))
      cursors.append(item.cursor)

  # Handle None result value. See make_request() for details about how this can
  # happen.
  tasks = []
  cursor = None
  for task, c in zip(ndb.get_multi(result_summary_keys), cursors):
    if task:
      cursor = c
      tasks.append(task)
      if len(tasks) == limit:
        # Drop the rest.
        break
  else:
    if len(cursors) == limit + 5:
      while len(tasks) < limit:
        # Go into the slow path, seems like we got a lot of corrupted items.
        opts = search.QueryOptions(limit=limit-len(tasks) + 5, cursor=cursor)
        results = index.search(search.Query('name:%s' % word, options=opts))
        if not results.results:
          # Nothing else.
          cursor = None
          break
        for item in results.results:
          value = item_to_id(item)
          if value:
            cursor = item.cursor
            task = unpack_result_summary_key(value).get()
            if task:
              tasks.append(task)
              if len(tasks) == limit:
                break

  cursor_str = cursor.web_safe_string if cursor else None
  return tasks, cursor_str
