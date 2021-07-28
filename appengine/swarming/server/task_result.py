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
    +-----------------+
    |TaskResultSummary|
    |  +--------+     |
    |  |FilesRef|     |
    |  +--------+     |
    |id=1             |
    +-----------------+
        |
        +----------------+
        |                |
        v                v
    +-------------+  +-------------+
    |TaskRunResult|  |TaskRunResult|
    |  +--------+ |  |  +--------+ |
    |  |FilesRef| |  |  |FilesRef| |
    |  +--------+ |  |  +--------+ |
    |id=1 <try #> |  |id=2         |
    +-------------+  +-------------+
        |
        +--------------------+
        |                    |
        v                    v
    +-----------------+  +----------------+
    |TaskOutput       |  |PerformanceStats|
    |id=1 (not stored)|  |id=1            |
    +-----------------+  +----------------+
        |
        +------------ ... ----+
        |                     |
        v                     v
    +---------------+     +---------------+
    |TaskOutputChunk| ... |TaskOutputChunk|
    |id=1           | ... |id=N           |
    +---------------+     +---------------+
"""

import collections
import datetime
import logging
import random
import re

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from server import bq_state
from server import large
from server import resultdb
from server import task_pack
from server import task_request
from server.constants import OR_DIM_SEP


class State(object):
  """Represents the current task state.

  For documentation, see the comments in the swarming_rpcs.TaskState enum, which
  is using the same values:
  https://cs.chromium.org/chromium/infra/luci/appengine/swarming/swarming_rpcs.py?q=TaskState\(

  It's in fact an enum.
  """
  RUNNING = 0x10  # 16
  PENDING = 0x20  # 32
  EXPIRED = 0x30  # 48
  TIMED_OUT = 0x40  # 64
  BOT_DIED = 0x50  # 80
  CANCELED = 0x60  # 96
  COMPLETED = 0x70  # 112
  KILLED = 0x80  # 128
  NO_RESOURCE = 0x100  # 256

  STATES = (RUNNING, PENDING, EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, COMPLETED,
            KILLED, NO_RESOURCE)
  # State will mutate again. Anything else means not queued, not running.
  STATES_RUNNING = (RUNNING, PENDING)
  # Abnormal termination.
  STATES_EXCEPTIONAL = (
      EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, KILLED, NO_RESOURCE)
  # Task ran and is done running.
  STATES_DONE = (TIMED_OUT, COMPLETED, KILLED)
  # Task didn't run (except for BOT_DIED, which may or may not have run).
  STATES_ABANDONED = (EXPIRED, BOT_DIED, CANCELED, NO_RESOURCE)

  _NAMES = {
      RUNNING: 'Running',
      PENDING: 'Pending',
      EXPIRED: 'Expired',
      TIMED_OUT: 'Execution timed out',
      BOT_DIED: 'Bot died',
      CANCELED: 'User canceled',
      COMPLETED: 'Completed',
      KILLED: 'Killed',
      NO_RESOURCE: 'No resource available',
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
  return (bool(result_common.exit_code) or
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
  # TODO(maruel): This value should be stored in the entity for future-proofing.
  # It can't be changed until then.
  CHUNK_SIZE = 100*1024

  # Maximum number of chunks.
  PUT_MAX_CHUNKS = 1024

  # Maximum content size saved in a TaskOutput.
  @classmethod
  def PUT_MAX_CONTENT(cls):
    return cls.PUT_MAX_CHUNKS * cls.CHUNK_SIZE


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

  It is immutable.
  """
  # Duration of the isolation operation in seconds.
  duration = ndb.FloatProperty(indexed=False)

  def to_proto(self, out):
    """Converts self to a swarming_pb2.CASEntriesStats."""
    if self.duration:
      out.duration.FromTimedelta(datetime.timedelta(seconds=self.duration))
    return out


class CASOperationStats(OperationStats):
  """Statistics for an CAS downloads/uploads operation.
  """
  # Initial cache size, if applicable.
  initial_number_items = ndb.IntegerProperty(indexed=False)
  initial_size = ndb.IntegerProperty(indexed=False)
  # Items operated on.
  # These buffers are compressed as deflate'd delta-encoded varints. See
  # large.py for the code to handle these.
  items_cold = LargeIntegerArray()
  items_hot = LargeIntegerArray()

  # Cached precalculated values.
  _num_items_cold = None
  _total_bytes_items_cold = None
  _num_items_hot = None
  _total_bytes_items_hot = None

  @property
  def num_items_cold(self):
    """Numbers of missing items from the bot local isolated cache."""
    self._ensure_cache()
    return self._num_items_cold

  @property
  def total_bytes_items_cold(self):
    """Total size in bytes of all missing items from the bot local isolated
    cache.
    """
    self._ensure_cache()
    return self._total_bytes_items_cold

  @property
  def num_items_hot(self):
    """Numbers of items already present in the bot local isolated cache."""
    self._ensure_cache()
    return self._num_items_hot

  @property
  def total_bytes_items_hot(self):
    """Total size in bytes of all items already present in the bot local
    isolated cache.
    """
    self._ensure_cache()
    return self._total_bytes_items_hot

  def to_dict(self):
    out = super(OperationStats, self).to_dict()
    out['num_items_cold'] = self.num_items_cold
    out['total_bytes_items_cold'] = self.total_bytes_items_cold
    out['num_items_hot'] = self.num_items_hot
    out['total_bytes_items_hot'] = self.total_bytes_items_hot
    return out

  def to_proto(self, out):
    """Converts self to a swarming_pb2.TaskOverheadStats."""
    out = super(CASOperationStats, self).to_proto(out)

    if self.items_cold:
      out.cold.num_items = self.num_items_cold
      out.cold.total_bytes_items = self.total_bytes_items_cold

    if self.items_hot:
      out.hot.num_items = self.num_items_hot
      out.hot.total_bytes_items = self.total_bytes_items_hot

    # TODO(maruel): Put initial_number_items and initial_size in the bot
    # snapshot for now, until this is better restructured later.
    # https://crbug.com/850560
    return out

  def _ensure_cache(self):
    if self._num_items_cold is None and self.items_cold:
      items_cold = large.unpack(self.items_cold)
      self._num_items_cold = len(items_cold)
      self._total_bytes_items_cold = sum(items_cold)
    if self._num_items_hot is None and self.items_hot:
      items_hot = large.unpack(self.items_hot)
      self._num_items_hot = len(items_hot)
      self._total_bytes_items_hot = sum(items_hot)


class PerformanceStats(ndb.Model):
  """Statistics for an isolated task from the point of view of the bot.

  Parent is TaskRunResult. Key id is 1.

  Data from the client who uploaded the initial data is not tracked here, since
  it is generally operating on data that is used across multiple different
  tasks.

  This entity only exist for isolated tasks. For "raw command task", this entity
  is not stored, since there's nothing to note.
  """
  # Total overhead in seconds, including the overheads from
  # package_installation.duration, isolated_download.duration and
  # isolated_upload.duration and others.
  bot_overhead = ndb.FloatProperty(indexed=False)
  # Cache trimming before the dependency installations.
  cache_trim = ndb.LocalStructuredProperty(OperationStats)
  # Results installing CIPD packages before the task.
  package_installation = ndb.LocalStructuredProperty(OperationStats)
  # Named cache install operation before the task.
  named_caches_install = ndb.LocalStructuredProperty(OperationStats)
  # Named cache uninstall operation after the task.
  named_caches_uninstall = ndb.LocalStructuredProperty(OperationStats)
  # Runtime dependencies download operation before the task.
  isolated_download = ndb.LocalStructuredProperty(CASOperationStats)
  # Results uploading operation after the task.
  isolated_upload = ndb.LocalStructuredProperty(CASOperationStats)
  # Cleanup work dirs after the task.
  cleanup = ndb.LocalStructuredProperty(OperationStats)

  @property
  def is_valid(self):
    return self.bot_overhead is not None

  def to_dict(self):
    # to_dict() doesn't correctly call overriden to_dict() on
    # LocalStructuredProperty.
    out = super(PerformanceStats, self).to_dict(exclude=[
        'cache_trim',
        'package_installation',
        'named_caches_install',
        'named_caches_uninstall',
        'isolated_download',
        'isolated_upload',
        'cleanup',
    ])
    out['cache_trim'] = self.cache_trim.to_dict()
    out['package_installation'] = self.package_installation.to_dict()
    out['named_caches_install'] = self.named_caches_install.to_dict()
    out['named_caches_uninstall'] = self.named_caches_uninstall.to_dict()
    out['isolated_download'] = self.isolated_download.to_dict()
    out['isolated_upload'] = self.isolated_upload.to_dict()
    out['cleanup'] = self.cleanup.to_dict()
    return out

  def to_proto(self, out):
    """Converts self to a swarming_pb2.TaskPerformance"""
    if not self.bot_overhead:
      return
    # out.cost_usd is not set here.

    # Total overhead.
    out.total_overhead.FromTimedelta(
        datetime.timedelta(seconds=self.bot_overhead))

    # Setup overheads.
    setup_dur = 0
    if self.cache_trim.duration:
      self.cache_trim.to_proto(out.setup_overhead.cache_trim)
      setup_dur += self.cache_trim.duration
    if self.package_installation.duration:
      self.package_installation.to_proto(out.setup_overhead.cipd)
      setup_dur += self.package_installation.duration
    if self.named_caches_install.duration:
      self.named_caches_install.to_proto(out.setup_overhead.named_cache)
      setup_dur += self.named_caches_install.duration
    if self.isolated_download.duration:
      # TODO(crbug.com/1202053): deprecate setup field.
      self.isolated_download.to_proto(out.setup)
      self.isolated_download.to_proto(out.setup_overhead.cas)
      setup_dur += self.isolated_download.duration
    if setup_dur:
      out.setup.duration.FromTimedelta(datetime.timedelta(seconds=setup_dur))
      out.setup_overhead.duration.FromTimedelta(
          datetime.timedelta(seconds=setup_dur))

    # Teardown overheads.
    teardown_dur = 0
    if self.isolated_upload.duration:
      # TODO(crbug.com/1202053): deprecate teardown field.
      self.isolated_upload.to_proto(out.teardown)
      self.isolated_upload.to_proto(out.teardown_overhead.cas)
      teardown_dur += self.isolated_upload.duration
    if self.named_caches_uninstall.duration:
      self.named_caches_uninstall.to_proto(out.teardown_overhead.named_cache)
      teardown_dur += self.named_caches_uninstall.duration
    if self.cleanup.duration:
      self.cleanup.to_proto(out.teardown_overhead.cleanup)
      teardown_dur += self.cleanup.duration
    if teardown_dur:
      out.teardown.duration.FromTimedelta(
          datetime.timedelta(seconds=teardown_dur))
      out.teardown_overhead.duration.FromTimedelta(
          datetime.timedelta(seconds=teardown_dur))

    # Other overheads.
    other_overhead = self.bot_overhead - setup_dur - teardown_dur
    out.other_overhead.FromTimedelta(datetime.timedelta(seconds=other_overhead))

  def _pre_put_hook(self):
    if self.bot_overhead is None:
      raise datastore_errors.BadValueError(
          'PerformanceStats.bot_overhead is required')


class ResultDBInfo(ndb.Model):
  """ResultDB related properties."""
  # ResultDB hostname, e.g. "results.api.cr.dev"
  hostname = ndb.StringProperty()

  # e.g. "invocations/task-chromium-swarm.appspot.com-deadbeef1"
  # None if the integration was not enabled for this task.
  #
  # If the task was deduplicated, this equals invocation name of the original
  # task.
  invocation = ndb.StringProperty()

  def to_proto(self, out):
    if self.hostname:
      out.hostname = self.hostname
    if self.invocation:
      out.invocation = self.invocation


class CipdPins(ndb.Model):
  """Specifies which CIPD client and packages were actually installed.

  A part of _TaskResultCommon.
  """
  # CIPD package of CIPD client used.
  # client_package.package_name and version are provided.
  # client_package.path will be None.
  client_package = ndb.LocalStructuredProperty(task_request.CipdPackage)

  # List of packages that were installed.
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

  # Process exit code. May be missing when task_runner dies and bot_main tries
  # to recover the task and in some cases with state TIMED_OUT.
  exit_code = ndb.IntegerProperty(indexed=False, name='exit_codes')

  # Task duration in seconds as seen by the process who started the child task,
  # excluding all overheads.
  duration = ndb.FloatProperty(indexed=False, name='durations')

  # Time when a bot reaped this task.
  started_ts = ndb.DateTimeProperty()

  # Time when the task was not considered for execution anymore, or the bot
  # completed its execution.
  #
  # For old entities prior to 2019-02-01, this value can be unset.
  completed_ts = ndb.DateTimeProperty()
  # Set when a task had an internal failure, timed out or was killed by a client
  # request.
  abandoned_ts = ndb.DateTimeProperty()

  # Children tasks that were triggered by this task. This is set when the task
  # reentrantly creates other Swarming tasks. Note that the task_id is to a
  # TaskResultSummary.
  children_task_ids = ndb.StringProperty(
      validator=_validate_task_summary_id, repeated=True)

  # DEPRECATED. Isolate server is being migrated to RBE-CAS. cas_output_ref will
  # be used instead.
  # File outputs of the task. Only set if TaskRequest.properties.inputs_ref is
  # set. The isolateserver and namespace should match.
  outputs_ref = ndb.LocalStructuredProperty(task_request.FilesRef)

  # Reference to the root of the output files.
  # Only set if TaskRequest.properties.cas_input_root is set.
  cas_output_root = ndb.LocalStructuredProperty(task_request.CASReference)

  # The pinned versions of all the CIPD packages used in the task.
  cipd_pins = ndb.LocalStructuredProperty(CipdPins)

  # Index in the TaskRequest.task_slices that this entity is current waiting on,
  # running or ran.
  current_task_slice = ndb.IntegerProperty(indexed=False, default=0)

  # ResultDB related information.
  resultdb_info = ndb.LocalStructuredProperty(ResultDBInfo)

  @property
  def can_be_canceled(self):
    """Returns True if the task is in a state that can be canceled."""
    return self.state in State.STATES_RUNNING

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
    # TODO(maruel): 2020-07-01: Remove and use completed_ts.
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
      stats.cache_trim = stats.cache_trim or OperationStats()
      stats.named_caches_install = (
          stats.named_caches_install or OperationStats())
      stats.named_caches_uninstall = (
          stats.named_caches_uninstall or OperationStats())
      stats.package_installation = (
          stats.package_installation or OperationStats())
      stats.isolated_download = stats.isolated_download or CASOperationStats()
      stats.isolated_upload = stats.isolated_upload or CASOperationStats()
      stats.cleanup = stats.cleanup or OperationStats()
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

  @property
  def task_state(self):
    """Returns the swarming.pb2.TaskState."""
    # https://crbug.com/915342: PENDING_DEDUPING
    # https://crbug.com/796757: RUNNING_OVERHEAD_SETUP
    # https://crbug.com/813412: RUNNING_OVERHEAD_TEARDOWN
    # https://crbug.com/916560: TERMINATING
    # https://crbug.com/902807: DUT_FAILURE
    # https://crbug.com/916553: BOT_DISAPPEARED
    # https://crbug.com/916559: PREEMPTED
    # https://crbug.com/916556: TIMED_OUT_SILENCE
    # https://crbug.com/916553: MISSING_INPUTS
    # https://crbug.com/916562: LOAD_SHED
    # https://crbug.com/916557: RESOURCE_EXHAUSTED
    if self.deduped_from:
      return swarming_pb2.DEDUPED
    if self.internal_failure:
      return swarming_pb2.RAN_INTERNAL_FAILURE
    if self.state == State.PENDING:
      return swarming_pb2.PENDING
    if self.state == State.RUNNING:
      return swarming_pb2.RUNNING
    if self.state == State.COMPLETED:
      return swarming_pb2.COMPLETED
    if self.state == State.TIMED_OUT:
      return swarming_pb2.TIMED_OUT
    if self.state == State.KILLED:
      return swarming_pb2.KILLED
    if self.state == State.EXPIRED:
      return swarming_pb2.EXPIRED
    if self.state == State.CANCELED:
      return swarming_pb2.CANCELED
    if self.state == State.NO_RESOURCE:
      return swarming_pb2.NO_RESOURCE
    # Internal error.
    return swarming_pb2.TASK_STATE_INVALID

  def to_string(self):
    return state_to_string(self)

  def to_dict(self, **kwargs):
    out = super(_TaskResultCommon, self).to_dict(**kwargs)
    # stdout_chunks is an implementation detail.
    out.pop('stdout_chunks')
    out['id'] = self.task_id
    return out

  def to_proto(self, out, append_root_ids=False):
    """Converts self to a swarming_pb2.TaskResult"""
    self.request.to_proto(out.request, append_root_ids=append_root_ids)
    if self.created_ts:
      # Can only be unset in test case.
      out.create_time.FromDatetime(self.created_ts)
    if self.started_ts:
      out.start_time.FromDatetime(self.started_ts)
    if self.completed_ts:
      out.end_time.FromDatetime(self.completed_ts)
    if self.abandoned_ts:
      out.abandon_time.FromDatetime(self.abandoned_ts)
      # Stay compatible with old entities.
      if not self.completed_ts:
        out.end_time.FromDatetime(self.abandoned_ts)
    if self.duration:
      out.duration.FromTimedelta(datetime.timedelta(seconds=self.duration))
    out.state = self.task_state
    # Convert state to category.
    out.state_category = self.task_state & 0xF0
    if self.try_number is not None:
      out.try_number = self.try_number
    out.current_task_slice = self.current_task_slice
    if self.bot_dimensions:
      # TODO(maruel): Keep a complete snapshot. This is a bit clunky at the
      # moment. https://crbug.com/850560
      for key, values in sorted(self.bot_dimensions.items()):
        dst = out.bot.dimensions.add()
        dst.key = key
        dst.values.extend(values)
        if key == u'id':
          out.bot.bot_id = values[0]
        elif key == u'pool':
          out.bot.pools.extend(values)
    out.server_versions.extend(self.server_versions)
    out.children_task_ids.extend(self.children_task_ids)
    if self.deduped_from:
      out.deduped_from = self.deduped_from
    key = self.result_summary_key
    if key:
      out.task_id = task_pack.pack_result_summary_key(key)
    key = self.run_result_key
    if key:
      out.run_id = task_pack.pack_run_result_key(key)

    # TODO(maruel): Make sure we enforce the same CIPD server for all TaskSlice.
    if self.request.num_task_slices:
      props = self.request.task_slice(0).properties
      if props.cipd_input:
        out.cipd_pins.server = props.cipd_input.server
    if self.cipd_pins:
      if self.cipd_pins.client_package:
        self.cipd_pins.client_package.to_proto(out.cipd_pins.client_package)
      for pkg in self.cipd_pins.packages:
        dst = out.cipd_pins.packages.add()
        pkg.to_proto(dst)

    if self.cost_usd:
      out.performance.cost_usd = self.cost_usd
    if self.performance_stats:
      self.performance_stats.to_proto(out.performance)
    if self.resultdb_info:
      self.resultdb_info.to_proto(out.resultdb_info)
    if self.exit_code is not None:
      out.exit_code = self.exit_code
    if self.outputs_ref:
      self.outputs_ref.to_proto(out.outputs)
    if self.cas_output_root:
      self.cas_output_root.to_proto(out.cas_output_root)

  def signal_server_version(self, server_version):
    """Adds `server_version` to self.server_versions if relevant."""
    if not self.server_versions or self.server_versions[-1] != server_version:
      self.server_versions.append(server_version)

  def get_output(self, offset, length):
    """Returns the stdout content for this task.

    Arguments:
      offset: offset in the stream which start returning the data.
      length: chunk size to return. If 0, fetch the whole content.

    Returns:
      str with content, None if there wasn't any content.
    """
    run_result_key = self.run_result_key
    if not run_result_key or not self.stdout_chunks:
      # The task was not reaped or no output was streamed yet.
      return None

    chunk_size = TaskOutput.CHUNK_SIZE
    length = length or (self.stdout_chunks * chunk_size - offset)

    # Determine which chunks to fetch.
    first_chunk = offset / chunk_size
    end = offset + length
    last_chunk = min((end + chunk_size-1) / chunk_size, self.stdout_chunks)

    # Retrieve the subset of TaskOutputChunk needed.
    output_key = _run_result_key_to_output_key(run_result_key)
    keys = [
        _output_key_to_output_chunk_key(output_key, i)
        for i in range(first_chunk, last_chunk)
    ]
    void = None
    parts = []
    for e in ndb.get_multi(keys):
      if e:
        parts.append(e.chunk)
      else:
        if not void:
          void = '\x00' * TaskOutput.CHUNK_SIZE
        parts.append(void)

    # Process the output.
    start_offset = offset % chunk_size
    end_offset = end - (first_chunk * chunk_size)
    return ''.join(parts)[start_offset:end_offset]

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

    if self.state in State.STATES_DONE:
      if self.duration is None:
        raise datastore_errors.BadValueError(
            'duration must be set with state %s' %
            State.to_string(self.state))
      # Allow exit_code to be missing for TIMED_OUT.
      if self.state != State.TIMED_OUT:
        if self.exit_code is None:
          raise datastore_errors.BadValueError(
              'exit_code must be set with state %s' %
              State.to_string(self.state))
    elif self.state != State.BOT_DIED:
      # Allow duration and exit_code to be either missing or set for BOT_DIED,
      # but they should be not present for any running/pending states.
      if self.duration is not None:
        raise datastore_errors.BadValueError(
            'duration must not be set with state %s' % State.to_string(
                self.state))
      if self.exit_code is not None:
        raise datastore_errors.BadValueError(
            'exit_code must not be set with state %s' %
            State.to_string(self.state))

    if self.state not in State.STATES_RUNNING:
      if self.completed_ts is None:
        raise datastore_errors.BadValueError(
            'completed_ts must be set with state %s' %
            State.to_string(self.state))
      # When a task is deduped, its creation time is after the completed time,
      # because original timestamps are used.
      if not self.deduped_from and self.completed_ts < self.created_ts:
        raise datastore_errors.BadValueError(
            'completed_ts must be equal or after created_ts')
    if self.abandoned_ts:
      if self.abandoned_ts < self.created_ts:
        raise datastore_errors.BadValueError(
            'abandoned_ts must be equal or after created_ts')

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
      prop._code_name for prop in cls._properties.values()
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

  # A user requested for the task to be canceled while running, which leads to
  # KILLED. It is set to true only for the time between the user request and
  # this task to be set to state == KILLED.
  killing = ndb.BooleanProperty(indexed=False)

  # A task run execution can't by definition save any cost.
  cost_saved_usd = None

  # A task run execution can't by definition be deduped from another task.
  # Still, setting this property always to None simplifies a lot the
  # presentation layer.
  deduped_from = None

  # Specifies the time after which the bot is considered dead. That is,
  # if a bot has not sent an update after this time while running the task,
  # it is considered dead. It is set after every ping from the bot if the
  # task is RUNNING and set to None once the task terminates.
  dead_after_ts = ndb.DateTimeProperty()

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

  def _pre_put_hook(self):
    super(TaskRunResult, self)._pre_put_hook()
    if not self.started_ts:
      raise datastore_errors.BadValueError('Must update .started_ts')
    if self.dead_after_ts:
      if self.state != State.RUNNING:
        raise datastore_errors.BadValueError('.dead_after_ts should be None')
    elif self.state == State.RUNNING:
      raise datastore_errors.BadValueError('Must update .dead_after_ts')


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
  priority = ndb.IntegerProperty(indexed=False)

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

  # Delay from TaskRequest.expiratoin_ts to the actual expired time.
  expiration_delay = ndb.FloatProperty(indexed=False)

  # Previous state, will be set in _pre_put_hook and compared in _post_pre_hook
  # with post state. It can be used for state transition actions. e.g. logging,
  # sending metrics
  _prev_state = None

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
  def result_summary_key(self):
    return self.key

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

  def _pre_put_hook(self):
    super(TaskResultSummary, self)._pre_put_hook()
    self._cache_prev_state()

  def _cache_prev_state(self):
    """Stores previous state."""
    # Note: Skip when the current state is running or pending
    # since it's used only by _send_job_completed_metric at the time
    # of implementing.
    if self.state in State.STATES_RUNNING:
      return
    # Don't use process cache to retrieve the original object
    orig = self.key.get(use_cache=False)
    if not orig:
      return
    self._prev_state = orig.state

  def _post_put_hook(self, future):
    super(TaskResultSummary, self)._post_put_hook(future)
    # Ensure that no errors are raised from future.check_success()
    # See the document for check_success()
    # https://cloud.google.com/appengine/docs/standard/python/ndb/futureclass
    future.check_success()
    self._send_job_completed_metric()
    self._call_finalize_invocation()

  def _call_finalize_invocation(self):
    """Call FinalizeInvocation to ResultDB."""
    if self.state in State.STATES_RUNNING:
      return

    if not self._prev_state or self._prev_state not in State.STATES_RUNNING:
      # self._prev_state becomes None when NO RESOURCE case.
      return

    if self.request.resultdb_update_token:
      run_id = task_pack.pack_run_result_key(
          task_pack.result_summary_key_to_run_result_key(self.key, 1))
      # TODO(crbug.com/1065139): remove get_result() if ndb.toplevel works fine.
      resultdb.finalize_invocation_async(
          run_id, self.request.resultdb_update_token).get_result()

  def _send_job_completed_metric(self):
    """Sends metric 'job/completed'"""
    # Skip when the current state is running or pending
    if self.state in State.STATES_RUNNING:
      return
    # Skip when the previous state was alerady done
    if self._prev_state and self._prev_state not in State.STATES_RUNNING:
      return
    prev_state = None
    if self._prev_state:
      prev_state = State.to_string(self._prev_state)
    logging.debug(
        '_send_job_completed_metric: '
        'Task completed. prev_state:"%s", current_state:"%s".\n'
        'Sending metric...', prev_state, State.to_string(self.state))
    import ts_mon_metrics
    ts_mon_metrics.on_task_completed(self)

  def reset_to_pending(self):
    """Resets this entity to pending state."""
    self.cipd_pins = None
    self.duration = None
    self.exit_code = None
    self.internal_failure = False
    self.outputs_ref = None
    self.cas_output_root = None
    self.started_ts = None
    self.state = State.PENDING

  def set_from_run_result(self, run_result, request):
    """Copies all the relevant properties from a TaskRunResult into this
    TaskResultSummary.

    If the task completed, succeeded and is idempotent, self.properties_hash is
    set.
    """
    assert ndb.in_transaction()
    assert isinstance(request, task_request.TaskRequest), request
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

    # Update the automatic tags, removing the ones from the other
    # TaskProperties.
    t = request.task_slice(run_result.current_task_slice or 0)
    if run_result.current_task_slice != self.current_task_slice:
      self.tags = task_request.get_automatic_tags(
          request, run_result.current_task_slice)
    if (self.state == State.COMPLETED and
        not self.failure and
        not self.internal_failure and
        t.properties.idempotent and
        not self.deduped_from):
      # Signal the results are valid and can be reused. If the request has a
      # SecretBytes, it is GET, which is a performance concern.
      self.properties_hash = t.properties_hash(request)

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
    return super(TaskResultSummary, self).to_dict(exclude=['properties_hash'])


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

  It silently drops saving the output if it goes over ~100Mib.

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
      logging.warning('Dropping output\n%d bytes were lost', len(output))
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
  for i, (key, start, output_chunk) in enumerate(chunks):
    if not entities[i]:
      # Fill up for missing entities.
      entities[i] = TaskOutputChunk(key=key)
    chunk = entities[i]
    # Magically combine everything.
    end = start + len(output_chunk)
    if len(chunk.chunk) < start:
      # Insert blank data automatically.
      chunk.gaps.extend((len(chunk.chunk), start))
      chunk.chunk = chunk.chunk + '\x00' * (start-len(chunk.chunk))

    # Strip gaps that are being written to.
    new_gaps = []
    for j in range(0, len(chunk.gaps), 2):
      # All values are relative to the starting offset of the chunk itself.
      gap_start = chunk.gaps[j]
      gap_end = chunk.gaps[j + 1]
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
    chunk.chunk = chunk.chunk[:start] + output_chunk + chunk.chunk[end:]
  return entities, number_chunks


def _outputchunk_key_to_request(output_chunk_key):
  """Returns the ndb.Key for the TaskRequest."""
  summary_key = output_chunk_key.parent().parent().parent()
  return task_pack.result_summary_key_to_request_key(summary_key)


def _sort_property(sort):
  """Returns a datastore_query.PropertyOrder based on 'sort'."""
  if sort not in ('created_ts', 'modified_ts', 'completed_ts', 'abandoned_ts',
                  'started_ts'):
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


def _filter_query(cls, q, start, end, sort, state):
  """Filters a query by creation time, state and order."""
  # Inequalities are <= and >= because keys are in reverse chronological
  # order.
  start_key = _datetime_to_key(start)
  if start_key:
    q = q.filter(TaskRunResult.key <= start_key)
  end_key = _datetime_to_key(end)
  if end_key:
    q = q.filter(TaskRunResult.key >= end_key)
  q = q.order(_sort_property(sort))

  if sort != 'created_ts' and (start or end):
    raise ValueError('Cannot both sort and use timestamp filtering')

  if state == 'all':
    return q

  if state == 'pending':
    return q.filter(cls.state == State.PENDING)

  if state == 'running':
    return q.filter(cls.state == State.RUNNING)

  if state == 'pending_running':
    # cls.state <= State.PENDING would work.
    return q.filter(
        ndb.OR(
            cls.state == State.PENDING,
            cls.state == State.RUNNING))

  if state == 'completed':
    return q.filter(cls.state == State.COMPLETED)

  if state == 'completed_success':
    q = q.filter(cls.state == State.COMPLETED)
    # pylint: disable=singleton-comparison
    return q.filter(cls.failure == False)

  if state == 'completed_failure':
    q = q.filter(cls.state == State.COMPLETED)
    # pylint: disable=singleton-comparison
    return q.filter(cls.failure == True)

  if state == 'deduped':
    q = q.filter(cls.state == State.COMPLETED)
    return q.filter(cls.try_number == 0)

  if state == 'expired':
    return q.filter(cls.state == State.EXPIRED)

  if state == 'timed_out':
    return q.filter(cls.state == State.TIMED_OUT)

  if state == 'bot_died':
    return q.filter(cls.state == State.BOT_DIED)

  if state == 'canceled':
    return q.filter(cls.state == State.CANCELED)

  if state == 'killed':
    return q.filter(cls.state == State.KILLED)

  if state == 'no_resource':
    return q.filter(cls.state == State.NO_RESOURCE)

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
      server_versions=[utils.get_app_version()],
      user=request.user,
      tags=request.tags,
      priority=request.priority)


def new_run_result(request, to_run, bot_id, bot_version, bot_dimensions,
                   resultdb_info):
  """Returns a new TaskRunResult for a TaskRequest.

  Initializes only the immutable parts.

  The caller must save it in the DB.
  """
  assert isinstance(request, task_request.TaskRequest)
  summary_key = task_pack.request_key_to_result_summary_key(request.key)
  return TaskRunResult(
      key=task_pack.result_summary_key_to_run_result_key(
          summary_key, to_run.try_number),
      bot_dimensions=bot_dimensions,
      bot_id=bot_id,
      bot_version=bot_version,
      resultdb_info=resultdb_info,
      current_task_slice=to_run.task_slice_index,
      server_versions=[utils.get_app_version()])


def yield_result_summary_by_parent_task_id(parent_task_id):
  """Yields child TaskResultSummary entities by parent task id."""
  q = task_request.yield_request_keys_by_parent_task_id(parent_task_id)
  for request_key in q:
    result_summary_key = (
        task_pack.request_key_to_result_summary_key(request_key))
    yield result_summary_key.get()


def yield_active_run_result_keys():
  """Yields all the TaskRunResult ndb.Key of running tasks.

  In practice it is returning a ndb.QueryIterator but this is equivalent.
  """
  q = TaskRunResult.query(TaskRunResult.completed_ts == None)
  return q.iter(keys_only=True)


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
  # Disable the in-process local cache. This is important, as there can be up to
  # a thousand entities loaded in memory, and this is a pure memory leak, as
  # there's no chance this specific instance will need these again, therefore
  # this leads to 'Exceeded soft memory limit' AppEngine errors.
  q = TaskRunResult.query(
      TaskRunResult.bot_id == bot_id,
      default_options=ndb.QueryOptions(use_cache=False))
  return _filter_query(TaskRunResult, q, start, end, sort, state)


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
  # Disable the in-process local cache. This is important, as there can be up to
  # a thousand entities loaded in memory, and this is a pure memory leak, as
  # there's no chance this specific instance will need these again, therefore
  # this leads to 'Exceeded soft memory limit' AppEngine errors.
  q = TaskResultSummary.query(
      default_options=ndb.QueryOptions(use_cache=False))
  # Filter by one or more tags.
  if tags:
    # Add TaskResultSummary indexes if desired.
    if sort != 'created_ts':
      raise ValueError(
          'Add needed indexes for sort:%s and tags if desired' % sort)
    for tag in tags:
      parts = tag.split(':', 1)
      if len(parts) != 2 or any(i.strip() != i or not i for i in parts):
        raise ValueError('Invalid tags')
      values = parts[1].split(OR_DIM_SEP)
      separated_tags = ['%s:%s' % (parts[0], v) for v in values]
      q = q.filter(TaskResultSummary.tags.IN(separated_tags))

  return _filter_query(TaskResultSummary, q, start, end, sort, state)


def cron_update_tags():
  """Populates TagAggregation entities."""
  seen = {}
  now = utils.utcnow()
  count = 0
  end = now - datetime.timedelta(hours=1)
  q = TaskResultSummary.query(TaskResultSummary.modified_ts > end)
  cursor = None
  more = True
  while more:
    tasks, cursor, more = q.fetch_page(1000, start_cursor=cursor)
    count += len(tasks)
    for t in tasks:
      for i in t.tags:
        k, v = i.split(':', 1)
        s = seen.setdefault(k, set())
        if s is not None:
          s.add(v)
          # 128 is an arbitrary large number to avoid OOM.
          if len(s) >= 128:
            logging.info('Limiting tag %s because there are too many', k)
            seen[k] = None
    logging.debug('Fetched tags from %d tasks', count)

  tags = [
    TagValues(tag=k, values=sorted(values or []))
    for k, values in sorted(seen.items())
  ]
  logging.info('From %d tasks, saw %d tags', count, len(tags))
  TagAggregation(key=TagAggregation.KEY, tags=tags, ts=now).put()
  return len(tags)


def task_bq_run(start, end):
  """Sends TaskRunResult to BigQuery swarming.task_results_run table.

  Multiple queries are run one after the other. This is because ndb.OR() cannot
  be used when the subqueries are inequalities on different fields.
  """
  def _convert(e):
    """Returns a tuple(bq_key, row)."""
    out = swarming_pb2.TaskResult()
    e.to_proto(out, append_root_ids=True)
    return (e.task_id, out)

  total = 0
  seen = set()

  # Completed
  q = TaskRunResult.query(
      TaskRunResult.completed_ts >= start,
      TaskRunResult.completed_ts <= end,
      # Disable cache for consistency.
      default_options=ndb.QueryOptions(use_cache=False, use_memcache=False))
  cursor = None
  more = True
  while more:
    entities, cursor, more = q.fetch_page(
        bq_state.RAW_LIMIT, start_cursor=cursor)
    rows = [_convert(e) for e in entities]
    seen.update(e.task_id for e in entities)
    total += len(rows)
    bq_state.send_to_bq('task_results_run', rows)

  return total


def task_bq_summary(start, end):
  """Sends TaskResultSummary to BigQuery swarming.task_results_summary table.

  Multiple queries are run one after the other. This is because ndb.OR() cannot
  be used when the subqueries are inequalities on different fields.
  """
  def _convert(e):
    """Returns a tuple(bq_key, row)."""
    out = swarming_pb2.TaskResult()
    e.to_proto(out, append_root_ids=True)
    if not out.HasField('end_time'):
      logging.warning('crbug.com/1064833: task %s does not have end_time %s',
                      e.task_id, out)
    return (e.task_id, out)

  total = 0
  seen = set()

  # Completed
  q = TaskResultSummary.query(
      TaskResultSummary.completed_ts >= start,
      TaskResultSummary.completed_ts <= end,
      # Disable cache for consistency.
      default_options=ndb.QueryOptions(use_cache=False, use_memcache=False))
  cursor = None
  more = True
  while more:
    entities, cursor, more = q.fetch_page(
        bq_state.RAW_LIMIT, start_cursor=cursor)
    rows = [_convert(e) for e in entities]
    seen.update(e.task_id for e in entities)
    total += len(rows)
    bq_state.send_to_bq('task_results_summary', rows)

  return total
