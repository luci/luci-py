# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Task execution result models.

This module doesn't do the scheduling itself. It only describes the entities to
store tasks results.

- TaskResultSummary represents the overall result for the TaskRequest taking in
  account retries.
- TaskRunResult represents the result for one 'try'. There can
  be multiple tries for one job, for example if a bot dies.
- The stdout of the task is saved under TaskOutput, chunked in TaskOutputChunk
  entities to fit the entity size limit.

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
                  |  +--------+     |
                  |  |FilesRef|     |
                  |  +--------+     |
                  |                 |
                  |id=1             |
                  +-----------------+
                       ^          ^
                       |          |
                       |          |
               +-------------+  +-------------+
               |TaskRunResult|  |TaskRunResult|
               |  +--------+ |  |  +--------+ |
               |  |FilesRef| |  |  |FilesRef| |
               |  +--------+ |  |  +--------+ |
               |id=1 <try #> |  |id=2         |
               +-------------+  +-------------+
                ^           ^           ...
                |           |
       +-----------------+ +----------------+
       |TaskOutput       | |PerformanceStats|
       |id=1 (not stored)| |id=1            |
       +-----------------+ +----------------+
                 ^      ^
                 |      |
    +---------------+  +---------------+
    |TaskOutputChunk|  |TaskOutputChunk| ...
    |id=1           |  |id=2           |
    +---------------+  +---------------+
"""

import collections
import datetime
import logging
import random
import re

from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import large
from server import task_pack
from server import task_request

import cipd

# Amount of time after which a bot is considered dead. In short, if a bot has
# not ping in the last 5 minutes while running a task, it is considered dead.
BOT_PING_TOLERANCE = datetime.timedelta(seconds=5*60)


class State(object):
  """States in which a task can be.

  It's in fact an enum. Values should be in decreasing order of importance.
  """
  RUNNING = 0x10    # 16
  PENDING = 0x20    # 32
  EXPIRED = 0x30    # 48
  TIMED_OUT = 0x40  # 64
  BOT_DIED = 0x50   # 80
  CANCELED = 0x60   # 96
  COMPLETED = 0x70  # 112

  STATES = (RUNNING, PENDING, EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, COMPLETED)
  STATES_RUNNING = (RUNNING, PENDING)
  STATES_NOT_RUNNING = (EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, COMPLETED)
  STATES_EXCEPTIONAL = (EXPIRED, TIMED_OUT, BOT_DIED, CANCELED)
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


def _validate_task_summary_id(_prop, value):
  """Validates a task_id looks valid without fetching the entity."""
  if not value:
    return None
  task_pack.unpack_result_summary_key(value)
  return value


class LargeIntegerArray(ndb.BlobProperty):
  """Contains a large integer array as compressed by large."""
  def __init__(self, **kwargs):
    # pylint: disable=E1002
    super(LargeIntegerArray, self).__init__(
        indexed=False, compressed=False, **kwargs)

  def _do_validate(self, value):
    return super(LargeIntegerArray, self)._do_validate(value) or None


def _calculate_failure(result_common):
  # When the task command times out, there may not be any exit code, it is still
  # a user process failure mode, not an infrastructure failure mode.
  return (
      bool(result_common.exit_code) or
      result_common.state == State.TIMED_OUT)


class TaskOutput(ndb.Model):
  """Phantom entity to represent the task output stored as small chunks.

  Parent is TaskRunResult. Key id is 1.

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
    """Returns the stdout for the task as a ndb.Future."""
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
  """Represents a chunk of the task output.

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


class OperationStats(ndb.Model):
  """Statistics for an operation.

  This entity is not stored in the DB. It is only embedded in PerformanceStats.
  """
  # Duration of the isolation operation in seconds.
  duration = ndb.FloatProperty(indexed=False)
  # Initial cache size, if applicable.
  initial_number_items = ndb.IntegerProperty(indexed=False)
  initial_size = ndb.IntegerProperty(indexed=False)
  # Items operated on.
  # These buffers are compressed as deflate'd delta-encoded varints. See
  # large.py for the code to handle these.
  items_cold = LargeIntegerArray()
  items_hot = LargeIntegerArray()

  @property
  def items_cold_array(self):
    # It seems like it's impossible to add it as a method to LargeIntegerArray
    # in a way that works in jinja2 templates.
    return large.unpack(self.items_cold or '')

  @property
  def items_hot_array(self):
    return large.unpack(self.items_hot or '')


class PerformanceStats(ndb.Model):
  """Statistics for an isolated task from the point of view of the bot.

  Parent is TaskRunResult. Key id is 1.

  Data from the client who uploaded the initial data is not tracked here, since
  it is generally operating on data that is used across multiple different
  tasks.

  This entity only exist for isolated tasks. For "raw command task", this entity
  is not stored, since there's nothing to note.
  """
  # Miscellaneous overhead in seconds, in addition to the overhead from
  # package_installation.duration, isolated_download.duration and
  # isolated_upload.duration
  bot_overhead = ndb.FloatProperty(indexed=False)
  # Results installing CIPD packages before the task.
  package_installation = ndb.LocalStructuredProperty(OperationStats)
  # Runtime dependencies download operation before the task.
  isolated_download = ndb.LocalStructuredProperty(OperationStats)
  # Results uploading operation after the task.
  isolated_upload = ndb.LocalStructuredProperty(OperationStats)

  @property
  def is_valid(self):
    return self.bot_overhead is not None

  def _pre_put_hook(self):
    if self.bot_overhead is None:
      raise datastore_errors.BadValueError(
          'PerformanceStats.bot_overhead is required')


class CipdPins(ndb.Model):
  """Specifies which CIPD client and packages were actually installed.

  A part of _TaskResultCommon.
  """
  # CIPD package of CIPD client to use.
  # client_package.package_name and version are provided.
  # client_package.path will be None.
  client_package = ndb.LocalStructuredProperty(task_request.CipdPackage)

  # List of packages to install in $CIPD_PATH prior task execution.
  packages = ndb.LocalStructuredProperty(task_request.CipdPackage,
                                         repeated=True)


class _TaskResultCommon(ndb.Model):
  """Contains properties that is common to both TaskRunResult and
  TaskResultSummary.

  It is not meant to be instantiated on its own.

  TODO(maruel): Overhaul this entity:
  - Get rid of TaskOutput as it is not needed anymore (?)
  """
  # Bot that ran this task.
  bot_id = ndb.StringProperty()

  # Bot version (as a hash) of the code running the task.
  bot_version = ndb.StringProperty()

  # Bot dimensions at the moment the bot reaped the task. Not set for old tasks.
  bot_dimensions = datastore_utils.DeterministicJsonProperty(
      json_type=dict, compressed=True)

  # Active server version(s). Note that during execution, the active server
  # version may have changed, this list will list all versions seen as the task
  # was updated.
  server_versions = ndb.StringProperty(repeated=True)

  # This entity is updated everytime the bot sends data so it is equivalent to
  # 'last_ping'.
  modified_ts = ndb.DateTimeProperty()

  # Records that the task failed, e.g. one process had a non-zero exit code. The
  # task may be retried if desired to weed out flakiness.
  failure = ndb.ComputedProperty(_calculate_failure)

  # Internal infrastructure failure, in which case the task should be retried
  # automatically if possible.
  internal_failure = ndb.BooleanProperty(default=False)

  # Number of TaskOutputChunk entities for the output.
  stdout_chunks = ndb.IntegerProperty(indexed=False)

  # Process exit code.
  exit_code = ndb.IntegerProperty(indexed=False, name='exit_codes')

  # Task duration in seconds as seen by the process who started the child task,
  # excluding all overheads. If the task was not isolated, this is the value
  # returned by task_runner. If the task was isolated, this is the value
  # returned by run_isolated.
  duration = ndb.FloatProperty(indexed=False, name='durations')

  # Time when a bot reaped this task.
  started_ts = ndb.DateTimeProperty()

  # Time when the bot completed the task. Note that if the job was improperly
  # handled, for example state is BOT_DIED, abandoned_ts is used instead of
  # completed_ts.
  completed_ts = ndb.DateTimeProperty()
  abandoned_ts = ndb.DateTimeProperty()

  # Children tasks that were triggered by this task. This is set when the task
  # reentrantly creates other Swarming tasks. Note that the task_id is to a
  # TaskResultSummary.
  children_task_ids = ndb.StringProperty(
      validator=_validate_task_summary_id, repeated=True)

  # File outputs of the task. Only set if TaskRequest.properties.sources_ref is
  # set. The isolateserver and namespace should match.
  outputs_ref = ndb.LocalStructuredProperty(task_request.FilesRef)

  # The pinned versions of all the CIPD packages used in the task.
  cipd_pins = ndb.LocalStructuredProperty(CipdPins)

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
  def duration_as_seen_by_server(self):
    """Returns the timedelta the task spent executing, including server<->bot
    communication overhead.

    This is the task duration as seen by the server, not by the bot.

    Task abandoned or not yet completed are not applicable and return None.
    """
    if not self.started_ts or not self.completed_ts:
      return None
    return self.completed_ts - self.started_ts

  def duration_now(self, now):
    """Returns the timedelta the task spent executing as of now, including
    overhead while running but excluding overhead after running..
    """
    if self.duration is not None:
      return datetime.timedelta(seconds=self.duration)
    if not self.started_ts or self.abandoned_ts:
      return None
    return (self.completed_ts or now) - self.started_ts

  @property
  def ended_ts(self):
    return self.completed_ts or self.abandoned_ts

  @property
  def is_exceptional(self):
    """Returns True if the task is in an exceptional state. Mostly for html
    view.
    """
    return self.state in State.STATES_EXCEPTIONAL

  @property
  def is_pending(self):
    """Returns True if the task is still pending. Mostly for html view."""
    return self.state == State.PENDING

  @property
  def is_running(self):
    """Returns True if the task is still pending. Mostly for html view."""
    return self.state == State.RUNNING

  @property
  def performance_stats(self):
    """Returns the PerformanceStats associated with this task results.

    Returns an empty instance if none is available.
    """
    # Keeps a cache. It's still paying the full latency cost of a DB fetch.
    if not hasattr(self, '_performance_stats_cache'):
      key = None if self.deduped_from else self.performance_stats_key
      # pylint: disable=attribute-defined-outside-init
      stats = (key.get() if key else None) or PerformanceStats()
      stats.isolated_download = stats.isolated_download or OperationStats()
      stats.isolated_upload = stats.isolated_upload or OperationStats()
      stats.package_installation = (
          stats.package_installation or OperationStats())
      self._performance_stats_cache = stats
    return self._performance_stats_cache

  @property
  def overhead_package_installation(self):
    """Returns the overhead from package installation in timedelta."""
    perf = self.performance_stats
    if perf.package_installation.duration is not None:
      return datetime.timedelta(seconds=perf.package_installation.duration)

  @property
  def overhead_isolated_inputs(self):
    """Returns the overhead from isolated setup in timedelta."""
    perf = self.performance_stats
    if perf.isolated_download.duration is not None:
      return datetime.timedelta(seconds=perf.isolated_download.duration)

  @property
  def overhead_isolated_outputs(self):
    """Returns the overhead from isolated results upload in timedelta."""
    perf = self.performance_stats
    if perf.isolated_upload.duration is not None:
      return datetime.timedelta(seconds=perf.isolated_upload.duration)

  @property
  def overhead_server(self):
    """Returns the overhead from server<->bot communication in timedelta."""
    perf = self.performance_stats
    if perf.bot_overhead is not None:
      duration = (self.duration or 0.) + (perf.bot_overhead or 0.)
      duration += (perf.isolated_download.duration or 0.)
      duration += (perf.isolated_upload.duration or 0.)
      out = (
          (self.duration_as_seen_by_server or datetime.timedelta()) -
          datetime.timedelta(seconds=duration))
      if out.total_seconds() >= 0:
        return out

  @property
  def overhead_task_runner(self):
    """Returns the overhead from task_runner in timedelta, excluding isolated
    overhead.

    This is purely bookeeping type of overhead.
    """
    perf = self.performance_stats
    if perf.bot_overhead is not None:
      return datetime.timedelta(seconds=perf.bot_overhead)

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
  def request(self):
    """Returns the TaskRequest that is related to this entity."""
    # Keeps a cache. It's still paying the full latency cost of a DB fetch.
    if not hasattr(self, '_request_cache'):
      # pylint: disable=attribute-defined-outside-init
      self._request_cache = self.request_key.get()
    return self._request_cache

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
    out['id'] = self.task_id
    return out

  def signal_server_version(self, server_version):
    """Adds `server_version` to self.server_versions if relevant."""
    if not self.server_versions or self.server_versions[-1] != server_version:
      self.server_versions.append(server_version)

  def get_output(self):
    """Returns the output, either as str or None if no output is present."""
    return self.get_output_async().get_result()

  @ndb.tasklet
  def get_output_async(self):
    """Returns the stdout as a ndb.Future.

    Use out.get_result() to get the data as a str or None if no output is
    present.
    """
    if not self.run_result_key or not self.stdout_chunks:
      # The task was not reaped or no output was streamed for this index yet.
      raise ndb.Return(None)

    output_key = _run_result_key_to_output_key(self.run_result_key)
    out = yield TaskOutput.get_output_async(output_key, self.stdout_chunks)
    raise ndb.Return(out)

  def validate(self, request):
    """Validation that requires the task_request.

    Full validation includes calling this method, and the checks in
    _pre_put_hook.

    Raises ValueError if this is invalid, otherwise returns None.
    """
    props = request.properties

    if props.cipd_input and self.cipd_pins:
      with cipd.pin_check_fn(None, None) as check:
        check(props.cipd_input.client_package, self.cipd_pins.client_package)
        if len(props.cipd_input.packages) != len(self.cipd_pins.packages):
          raise ValueError('Mismatched package lengths')
        for a, b in zip(props.cipd_input.packages, self.cipd_pins.packages):
          check(a, b)

  def _pre_put_hook(self):
    """Use extra validation that cannot be validated throught 'validator'."""
    super(_TaskResultCommon, self)._pre_put_hook()
    if self.state == State.EXPIRED:
      if self.failure or self.exit_code is not None:
        raise datastore_errors.BadValueError(
            'Unexpected State, a task can\'t fail if it hasn\'t started yet')

    if self.state == State.TIMED_OUT and not self.failure:
      raise datastore_errors.BadValueError('Timeout implies task failure')

    if not self.modified_ts:
      raise datastore_errors.BadValueError('Must update .modified_ts')

    if (self.duration is None) != (self.exit_code is None):
        raise datastore_errors.BadValueError(
            'duration and exit_code must both be None or not None')
    if self.state in State.STATES_DONE:
      if self.duration is None:
        raise datastore_errors.BadValueError(
            'duration and exit_code must be set with state %s' %
            State.to_string(self.state))
    elif self.state != State.BOT_DIED:
      # With BOT_DIED, it can be either ways.
      if self.duration is not None:
        raise datastore_errors.BadValueError(
            'duration and exit_code must not be set with state %s' %
            State.to_string(self.state))

    if self.deduped_from:
      if self.state != State.COMPLETED:
        raise datastore_errors.BadValueError(
            'state(%d) must be COMPLETED on deduped task %s' %
            (self.state, self.deduped_from))
      if self.failure:
        raise datastore_errors.BadValueError(
            'failure can\'t be True on deduped task %s' % self.deduped_from)

    self.children_task_ids = sorted(
        set(self.children_task_ids), key=lambda x: int(x, 16))

  @classmethod
  def _properties_fixed(cls):
    """Returns all properties with their member name, excluding computed
    properties.
    """
    return [
      prop._code_name for prop in cls._properties.itervalues()
      if not isinstance(prop, ndb.ComputedProperty)
    ]


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

  # Effective cost of this task.
  cost_usd = ndb.FloatProperty(indexed=False, default=0.)

  # A task run execution can't by definition save any cost.
  cost_saved_usd = None

  # A task run execution can't by definition be deduped from another task.
  # Still, setting this property always to None simplifies a lot the
  # presentation layer.
  deduped_from = None

  @property
  def created_ts(self):
    return self.request.created_ts

  @property
  def performance_stats_key(self):
    return task_pack.run_result_key_to_performance_stats_key(self.key)

  @property
  def name(self):
    return self.request.name

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this entity."""
    return task_pack.result_summary_key_to_request_key(self.result_summary_key)

  @property
  def result_summary_key(self):
    """Returns the TaskToRun ndb.Key that is parent of this entity."""
    return task_pack.run_result_key_to_result_summary_key(self.key)

  @property
  def run_result_key(self):
    return self.key

  @property
  def task_id(self):
    return task_pack.pack_run_result_key(self.key)

  @property
  def try_number(self):
    """Retry number this task. 1 based."""
    return self.key.integer_id()

  def append_output(self, output, output_chunk_start):
    """Appends output to the stdout.

    Returns the entities to save.
    """
    entities, self.stdout_chunks = _output_append(
        _run_result_key_to_output_key(self.key),
        self.stdout_chunks,
        output,
        output_chunk_start)
    assert self.stdout_chunks <= TaskOutput.PUT_MAX_CHUNKS
    return entities

  def to_dict(self):
    out = super(TaskRunResult, self).to_dict()
    out['try_number'] = self.try_number
    return out


class TaskResultSummary(_TaskResultCommon):
  """Represents the overall result of a task.

  Parent is a TaskRequest. Key id is always 1.

  This includes the relevant result taking in account all tries. This entity is
  basically a cache plus a bunch of indexes to speed up common queries.

  It's primary purpose is for status pages listing all the active tasks or
  recently completed tasks.
  """
  # These properties are directly copied from TaskRequest. They are only copied
  # here to simplify searches with the Web UI and to enable DB queries based on
  # both user and results properties (e.g. all requests from X which succeeded).
  # They are immutable.
  # TODO(maruel): Investigate what is worth copying over.
  created_ts = ndb.DateTimeProperty(required=True)
  name = ndb.StringProperty()
  user = ndb.StringProperty()
  tags = ndb.StringProperty(repeated=True)

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

  # Effective cost of this task for each try. Use self.cost_usd for the sum.
  # It's empty on deduped task, since nothing was executed.
  costs_usd = ndb.FloatProperty(repeated=True, indexed=False)

  # Cost saved for deduped task. This is the value of TaskResultSummary.cost_usd
  # from self.deduped_from.
  cost_saved_usd = ndb.FloatProperty(indexed=False)

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
  def cost_usd(self):
    """Returns the sum of the cost of each try."""
    return sum(self.costs_usd) if self.costs_usd else 0.

  @property
  def performance_stats_key(self):
    key = self.run_result_key
    if key:
      return task_pack.run_result_key_to_performance_stats_key(key)

  @property
  def request_key(self):
    """Returns the TaskRequest ndb.Key that is related to this entity."""
    return task_pack.result_summary_key_to_request_key(self.key)

  @property
  def run_result_key(self):
    if self.deduped_from:
      # Return the run results for the original task.
      return task_pack.unpack_run_result_key(self.deduped_from)

    if not self.try_number:
      return None
    return task_pack.result_summary_key_to_run_result_key(
        self.key, self.try_number)

  @property
  def task_id(self):
    return task_pack.pack_result_summary_key(self.key)

  def reset_to_pending(self):
    """Resets this entity to pending state."""
    self.cipd_pins = None
    self.duration = None
    self.exit_code = None
    self.internal_failure = False
    self.outputs_ref = None
    self.started_ts = None
    self.state = State.PENDING

  def set_from_run_result(self, run_result, request):
    """Copies all the relevant properties from a TaskRunResult into this
    TaskResultSummary.

    If the task completed, succeeded and is idempotent, self.properties_hash is
    set.
    """
    assert isinstance(run_result, TaskRunResult), run_result
    for property_name in _TaskResultCommon._properties_fixed():
      setattr(self, property_name, getattr(run_result, property_name))
    # Include explicit support for 'state' and 'try_number'. TaskRunResult.state
    # is a ComputedProperty so it can't be copied as-is, and try_number is a
    # generated property.
    # pylint: disable=W0201
    self.state = run_result.state
    self.try_number = run_result.try_number

    while len(self.costs_usd) < run_result.try_number:
      self.costs_usd.append(0.)
    self.costs_usd[run_result.try_number-1] = run_result.cost_usd

    if (self.state == State.COMPLETED and
        not self.failure and
        not self.internal_failure and
        request.properties.idempotent and
        not self.deduped_from):
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

    for property_name in _TaskResultCommon._properties_fixed():
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


class TagValues(ndb.Model):
  tag = ndb.StringProperty()
  values = ndb.StringProperty(repeated=True)


class TagAggregation(ndb.Model):
  """Has all dimensions that are currently in use."""
  tags = ndb.LocalStructuredProperty(TagValues, repeated=True)

  ts = ndb.DateTimeProperty()

  # We only store one of these entities. Use this key to refer to any instance.
  KEY = ndb.Key('TagAggregation', 'current')


### Private stuff.


def _run_result_key_to_output_key(run_result_key):
  """Returns a ndb.key to a TaskOutput."""
  assert run_result_key.kind() == 'TaskRunResult', run_result_key
  return ndb.Key(TaskOutput, 1, parent=run_result_key)


def _output_key_to_output_chunk_key(output_key, chunk_number):
  """Returns a ndb.key to a TaskOutputChunk.

  Is chunk_number zero-indexed.
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


def _sort_property(sort):
  """Returns a datastore_query.PropertyOrder based on 'sort'."""
  if sort not in ('created_ts', 'modified_ts', 'completed_ts', 'abandoned_ts'):
    raise ValueError('Unexpected sort %r' % sort)
  if sort == 'created_ts':
    return datastore_query.PropertyOrder(
        '__key__', datastore_query.PropertyOrder.ASCENDING)
  return datastore_query.PropertyOrder(
      sort, datastore_query.PropertyOrder.DESCENDING)


def _datetime_to_key(date):
  """Converts a datetime.datetime to a ndb.Key to a task_request.TaskRequest."""
  if not date:
    return None
  assert isinstance(date, datetime.datetime), date
  return task_request.convert_to_request_key(date)


def _filter_query(cls, query, start, end, sort, state):
  """Filters a query by creation time, state and order."""
  # Inequalities are <= and >= because keys are in reverse chronological
  # order.
  start_key = _datetime_to_key(start)
  if start_key:
    query = query.filter(TaskRunResult.key <= start_key)
  end_key = _datetime_to_key(end)
  if end_key:
    query = query.filter(TaskRunResult.key >= end_key)
  query = query.order(_sort_property(sort))

  if sort != 'created_ts' and (start or end):
    raise ValueError('Cannot both sort and use timestamp filtering')

  if state == 'all':
    return query

  if state == 'pending':
    return query.filter(cls.state == State.PENDING)

  if state == 'running':
    return query.filter(cls.state == State.RUNNING)

  if state == 'pending_running':
    # cls.state <= State.PENDING would work.
    return query.filter(
        ndb.OR(
            cls.state == State.PENDING,
            cls.state == State.RUNNING))

  if state == 'completed':
    return query.filter(cls.state == State.COMPLETED)

  if state == 'completed_success':
    query = query.filter(cls.state == State.COMPLETED)
    return query.filter(cls.failure == False)

  if state == 'completed_failure':
    query = query.filter(cls.state == State.COMPLETED)
    return query.filter(cls.failure == True)

  if state == 'deduped':
    query = query.filter(cls.state == State.COMPLETED)
    return query.filter(cls.try_number == 0)

  if state == 'expired':
    return query.filter(cls.state == State.EXPIRED)

  if state == 'timed_out':
    return query.filter(cls.state == State.TIMED_OUT)

  if state == 'bot_died':
    return query.filter(cls.state == State.BOT_DIED)

  if state == 'canceled':
    return query.filter(cls.state == State.CANCELED)

  raise ValueError('Invalid state')


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


def new_result_summary(request):
  """Returns the new and only TaskResultSummary for a TaskRequest.

  The caller must save it in the DB.
  """
  return TaskResultSummary(
      key=task_pack.request_key_to_result_summary_key(request.key),
      created_ts=request.created_ts,
      name=request.name,
      user=request.user,
      tags=request.tags)


def new_run_result(request, try_number, bot_id, bot_version, bot_dimensions):
  """Returns a new TaskRunResult for a TaskRequest.

  The caller must save it in the DB.
  """
  assert isinstance(request, task_request.TaskRequest)
  summary_key = task_pack.request_key_to_result_summary_key(request.key)
  return TaskRunResult(
      key=task_pack.result_summary_key_to_run_result_key(
          summary_key, try_number),
      bot_dimensions=bot_dimensions,
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
  q = TaskRunResult.query(TaskRunResult.modified_ts < deadline)
  return q.filter(TaskRunResult.state == State.RUNNING).iter(keys_only=True)


def get_run_results_query(start, end, sort, state, bot_id):
  """Returns TaskRunResult.query() with these filters.

  Arguments:
    start: Earliest creation date of retrieved tasks.
    end: Most recent creation date of retrieved tasks, normally None.
    sort: Order to use. Must default to 'created_ts' to use the default. Cannot
        be used along start and end.
    state: One of State enum value as str. Use 'all' to get all tasks.
    bot_id: (required) bot id to filter on.
  """
  if not bot_id:
    raise ValueError('bot_id is required')
  query = TaskRunResult.query(TaskRunResult.bot_id == bot_id)
  return _filter_query(TaskRunResult, query, start, end, sort, state)


def get_result_summaries_query(start, end, sort, state, tags):
  """Returns TaskResultSummary.query() with these filters.

  Arguments:
    start: Earliest creation date of retrieved tasks.
    end: Most recent creation date of retrieved tasks, normally None.
    sort: Order to use. Must default to 'created_ts' to use the default. Cannot
        be used along start and end.
    state: One of State enum value as str. Use 'all' to get all tasks.
    tags: List of search for one or multiple task tags.
  """
  query = TaskResultSummary.query()
  # Filter by one or more tags.
  if tags:
    # Add TaskResultSummary indexes if desired.
    if sort != 'created_ts':
      raise ValueError(
          'Add needed indexes for sort:%s and tags if desired' % sort)
    tags_filter = TaskResultSummary.tags == tags[0]
    for tag in tags[1:]:
      tags_filter = ndb.AND(tags_filter, TaskResultSummary.tags == tag)
    query = query.filter(tags_filter)
  return _filter_query(TaskResultSummary, query, start, end, sort, state)
