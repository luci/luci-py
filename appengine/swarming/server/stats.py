# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Generates statistics out of logs. Contains the backend code.

It's important to keep logs concise for general performance concerns. Each http
handler should strive to do only one stats log entry per request.
"""

import json
import logging

import webapp2
from google.appengine.ext import ndb

from components import decorators
from components import stats_framework
from components import utils
from server import task_pack
from server import task_to_run


### Models


class _SnapshotBucketBase(ndb.Model):
  """Statistics for a specific bucket, meant to be subclassed.

  TODO(maruel): Add data about the runs (TaskRunResult), which will be higher
  than the number of tasks (TaskResultSummary) in case of retries. Until
  implemented, both values will match.

  TODO(maruel): Use sampling to get median, average, 95%, 99%, 99.9% instead of
  the raw sum for a few values. The average is not much representative.
  """
  tasks_enqueued = ndb.IntegerProperty(default=0)

  tasks_started = ndb.IntegerProperty(default=0)
  tasks_pending_secs = ndb.FloatProperty(default=0)

  tasks_active = ndb.IntegerProperty(default=0)
  # TODO(maruel): Add:
  # Number of tasks pending at the start of this moment. It only makes sense for
  # minute resolution.
  # tasks_pending = ndb.IntegerProperty(default=0)

  tasks_completed = ndb.IntegerProperty(default=0)
  tasks_completed = ndb.IntegerProperty(default=0)
  tasks_total_runtime_secs = ndb.FloatProperty(default=0)

  tasks_bot_died = ndb.IntegerProperty(default=0)
  tasks_request_expired = ndb.IntegerProperty(default=0)

  @property
  def tasks_avg_pending_secs(self):
    if self.tasks_started:
      return round(self.tasks_pending_secs / float(self.tasks_started), 3)
    return 0.

  @property
  def tasks_avg_runtime_secs(self):
    if self.tasks_completed:
      return round(
          self.tasks_total_runtime_secs / float(self.tasks_completed), 3)
    return 0.

  def to_dict(self):
    out = super(_SnapshotBucketBase, self).to_dict()
    out['tasks_avg_pending_secs'] = self.tasks_avg_pending_secs
    out['tasks_avg_runtime_secs'] = self.tasks_avg_runtime_secs
    return out


class _SnapshotForUser(_SnapshotBucketBase):
  """Statistics for a specific bucket of TaskRequest.user.

  This is useful to see if a specific user hogs the system.
  """
  user = ndb.StringProperty()

  def accumulate(self, rhs):
    assert self.user == rhs.user
    stats_framework.accumulate(self, rhs, ['user'])


class _SnapshotForDimensions(_SnapshotBucketBase):
  """Statistics for a specific bucket of TaskRequest.properties.dimensions."""
  # Dimensions are saved as json encoded. JSONProperty is not used so it is
  # easier to compare and sort entries containing dicts.
  dimensions = ndb.StringProperty()

  # TODO(maruel): This will become large (thousands). Something like a
  # bloomfilter + number could help. On the other hand, it's highly compressible
  # and this instance is zlib compressed. Run an NN thousands bots load tests
  # and determine if it becomes a problem.
  bot_ids = ndb.StringProperty(repeated=True)

  bot_ids_bad = ndb.StringProperty(repeated=True)

  @property
  def bots_active(self):
    return len(self.bot_ids)

  @property
  def bots_inactive(self):
    return len(self.bot_ids_bad)

  def accumulate(self, rhs):
    assert self.dimensions == rhs.dimensions
    stats_framework.accumulate(
        self, rhs, ['bot_ids', 'bot_ids_bad', 'dimensions'])
    self.bot_ids = sorted(set(self.bot_ids) | set(rhs.bot_ids))
    self.bot_ids_bad = sorted(set(self.bot_ids_bad) | set(rhs.bot_ids_bad))

  def to_dict(self):
    out = super(_SnapshotForDimensions, self).to_dict()
    out['bots_active'] = self.bots_active
    out['bots_inactive'] = self.bots_inactive
    del out['bot_ids']
    del out['bot_ids_bad']
    return out


class _Snapshot(ndb.Model):
  """A snapshot of statistics for the specific time frame.

  It has references to _SnapshotForDimensions which holds the
  dimensions-specific data.

  TODO(maruel): Add tasks_total_runtime_secs, tasks_avg_runtime_secs.
  """
  # General HTTP details.
  http_requests = ndb.IntegerProperty(default=0)
  http_failures = ndb.IntegerProperty(default=0)

  # TODO(maruel): This will become large (thousands). Something like a
  # bloomfilter + number could help. On the other hand, it's highly compressible
  # and this instance is zlib compressed. Run an NN thousands bots load tests
  # and determine if it becomes a problem.
  bot_ids = ndb.StringProperty(repeated=True)

  # Bots that are quarantined.
  # TODO(maruel): Also add bots that are considered dead at that moment. That
  # wil be a useful metric.
  bot_ids_bad = ndb.StringProperty(repeated=True)

  # Buckets the statistics per dimensions.
  buckets = ndb.LocalStructuredProperty(_SnapshotForDimensions, repeated=True)
  # Per user statistics.
  users = ndb.LocalStructuredProperty(_SnapshotForUser, repeated=True)

  @property
  def bots_active(self):
    return len(self.bot_ids)

  @property
  def bots_inactive(self):
    return len(self.bot_ids_bad)

  # Sums.

  @property
  def tasks_enqueued(self):
    return sum(i.tasks_enqueued for i in self.buckets)

  @property
  def tasks_started(self):
    return sum(i.tasks_started for i in self.buckets)

  @property
  def tasks_pending_secs(self):
    return sum(i.tasks_pending_secs for i in self.buckets)

  @property
  def tasks_active(self):
    return sum(i.tasks_active for i in self.buckets)

  @property
  def tasks_completed(self):
    return sum(i.tasks_completed for i in self.buckets)

  @property
  def tasks_total_runtime_secs(self):
    return sum(i.tasks_total_runtime_secs for i in self.buckets)

  @property
  def tasks_bot_died(self):
    return sum(i.tasks_bot_died for i in self.buckets)

  @property
  def tasks_request_expired(self):
    return sum(i.tasks_request_expired for i in self.buckets)

  @property
  def tasks_avg_pending_secs(self):
    started = 0
    pending_secs = 0.
    for i in self.buckets:
      started += i.tasks_started
      pending_secs += i.tasks_pending_secs
    if started:
      return round(pending_secs / float(started), 3)
    return 0.

  @property
  def tasks_avg_runtime_secs(self):
    completed = 0
    runtime_secs = 0.
    for i in self.buckets:
      completed += i.tasks_completed
      runtime_secs += i.tasks_total_runtime_secs
    if completed:
      return round(runtime_secs / float(completed), 3)
    return 0.

  def get_dimensions(self, dimensions_json):
    """Returns a _SnapshotForDimensions instance for this dimensions key.

    Arguments:
      dimensions_json: json encoded dict representing the dimensions.

    Creates one if necessary and keeps self.buckets sorted.
    """
    # TODO(maruel): A binary search could be done since the list is always
    # sorted.
    for i in self.buckets:
      if i.dimensions == dimensions_json:
        return i

    new_item = _SnapshotForDimensions(dimensions=dimensions_json)
    self.buckets.append(new_item)
    self.buckets.sort(key=lambda i: i.dimensions)
    return new_item

  def get_user(self, user):
    """Returns a _SnapshotForUser instance for this user.

    Creates one if necessary and keeps self.users sorted.
    """
    # TODO(maruel): A binary search could be done since the list is always
    # sorted.
    for i in self.users:
      if i.user == user:
        return i

    new_item = _SnapshotForUser(user=user)
    self.users.append(new_item)
    self.users.sort(key=lambda i: i.user)
    return new_item

  def accumulate(self, rhs):
    """Accumulates data from rhs into self."""
    stats_framework.accumulate(
        self, rhs, ['bot_ids', 'bot_ids_bad', 'buckets', 'users'])
    self.bot_ids = sorted(set(self.bot_ids) | set(rhs.bot_ids))
    self.bot_ids_bad = sorted(set(self.bot_ids_bad) | set(rhs.bot_ids_bad))
    lhs_dimensions = dict((i.dimensions, i) for i in self.buckets)
    rhs_dimensions = dict((i.dimensions, i) for i in rhs.buckets)
    for key in set(rhs_dimensions):
      if key not in lhs_dimensions:
        # Make a copy of the right hand side so no aliasing occurs.
        lhs_dimensions[key] = rhs_dimensions[key].__class__()
        # Call the root method directly so 'enhancements' do not get in the way.
        lhs_dimensions[key].populate(**ndb.Model.to_dict(rhs_dimensions[key]))
      else:
        lhs_dimensions[key].accumulate(rhs_dimensions[key])
    self.buckets = sorted(
        lhs_dimensions.itervalues(), key=lambda i: i.dimensions)

    lhs_users = dict((i.user, i) for i in self.users)
    rhs_users = dict((i.user, i) for i in rhs.users)
    for key in set(rhs_users):
      if key not in lhs_users:
        # Make a copy of the right hand side so no aliasing occurs.
        lhs_users[key] = rhs_users[key].__class__()
        # Call the root method directly so 'enhancements' do not get in the way.
        lhs_users[key].populate(**ndb.Model.to_dict(lhs_users[key]))
      else:
        lhs_users[key].accumulate(rhs_users[key])
    self.users = sorted(lhs_users.itervalues(), key=lambda i: i.user)

  def to_dict(self):
    """Returns the summary only, not the buckets."""
    return {
      'bots_active': self.bots_active,
      'bots_inactive': self.bots_inactive,
      'http_failures': self.http_failures,
      'http_requests': self.http_requests,
      'tasks_active': self.tasks_active,
      'tasks_avg_pending_secs': self.tasks_avg_pending_secs,
      'tasks_avg_runtime_secs': self.tasks_avg_runtime_secs,
      'tasks_bot_died': self.tasks_bot_died,
      'tasks_completed': self.tasks_completed,
      'tasks_enqueued': self.tasks_enqueued,
      'tasks_pending_secs': self.tasks_pending_secs,
      'tasks_request_expired': self.tasks_request_expired,
      'tasks_started': self.tasks_started,
      'tasks_total_runtime_secs': self.tasks_total_runtime_secs,
    }


### Utility


# Valid actions that can be logged.
_VALID_ACTIONS = frozenset(
  [
    'bot_active',
    'bot_inactive',
    # run_* relates to a TaskRunResult. It can happen multiple time for a single
    # task, when the task is retried automatically.
    'run_bot_died',
    'run_completed',
    # TODO(maruel): Add: 'run_enqueued'.
    'run_started',
    'run_updated',
    # task_* relates to a TaskResultSummary.
    'task_completed',  # Comes after a non-retried 'run_completed'.
    'task_enqueued',  # Implies run_enqueued.
    'task_request_expired',
  ])

# Mapping from long to compact key names. This reduces space usage.
_KEY_MAPPING = {
  'action': 'a',
  'bot_id': 'bid',
  'dimensions': 'd',
  'pending_ms': 'pms',
  'run_id': 'rid',
  'runtime_ms': 'rms',
  'task_id': 'tid',
  'user': 'u',
}

_REVERSE_KEY_MAPPING = {v:k for k, v in _KEY_MAPPING.iteritems()}


def _assert_list(actual, expected):
  actual = sorted(actual)
  if actual != expected:
    raise ValueError('%s != %s' % (','.join(actual), ','.join(expected)))


def _mark_task_as_active(extras, tasks_active):
  task_id = extras.get('task_id')
  if not task_id:
    # Crudely zap out the retries for now.
    # https://code.google.com/p/swarming/issues/detail?id=108
    task_id = extras['run_id'][:-2] + '00'
  dimensions_json = utils.encode_to_json(extras['dimensions'])
  tasks_active.setdefault(dimensions_json, set()).add(task_id)


def _mark_bot_and_task_as_active(extras, bots_active, tasks_active):
  """Notes both the bot id and the task id as active in this time span."""
  bots_active.setdefault(extras['bot_id'], extras['dimensions'])
  _mark_task_as_active(extras, tasks_active)


def _ms_to_secs(raw):
  """Converts a str representing a value in ms into seconds.

  See task_scheduler._secs_to_ms() for the reverse conversion.
  """
  return float(raw) * 0.001


def _pack_entry(**kwargs):
  """Packs an entry so it can be logged as a statistic in the logs.

  Specifically process 'dimensions' if present to remove the key 'hostname'.
  """
  assert kwargs['action'] in _VALID_ACTIONS, kwargs
  dimensions = kwargs.get('dimensions')
  if dimensions:
    kwargs['dimensions'] = {
      k: v for k, v in dimensions.iteritems() if k != 'hostname'
    }
  packed = {_KEY_MAPPING[k]: v for k, v in kwargs.iteritems()}
  return utils.encode_to_json(packed)


def _unpack_entry(line):
  return {_REVERSE_KEY_MAPPING[k]: v for k, v in json.loads(line).iteritems()}


def _parse_line(line, values, bots_active, bots_inactive, tasks_active):
  """Updates a Snapshot instance with a processed statistics line if relevant.

  This function is a big switch case, so while it is long and will get longer,
  it is relatively easy to read, as long as the keys are kept sorted!
  """
  try:
    try:
      extras = _unpack_entry(line)
      action = extras.pop('action')
    except (KeyError, ValueError):
      data = line.split('; ')
      action = data.pop(0)
      extras = dict(i.split('=', 1) for i in data)

    # Preemptively reduce copy-paste.
    d = None
    if 'dimensions' in extras and action not in ('bot_active', 'bot_inactive'):
      # Skip bot_active because we don't want complex dimensions to be created
      # implicitly.
      dimensions_json = utils.encode_to_json(extras['dimensions'])
      d = values.get_dimensions(dimensions_json)
    u = None
    if 'user' in extras:
      u = values.get_user(extras['user'])

    # Please keep 'action == 'foo' conditions sorted!
    if action == 'bot_active':
      if sorted(extras) != ['bot_id', 'dimensions']:
        raise ValueError(','.join(sorted(extras)))

      bots_active[extras['bot_id']] = extras['dimensions']
      return True

    if action == 'bot_inactive':
      if sorted(extras) != ['bot_id', 'dimensions']:
        raise ValueError(','.join(sorted(extras)))

      bots_inactive[extras.get('bot_id') or 'unknown'] = extras['dimensions']
      return True

    if action == 'run_bot_died':
      _assert_list(extras, ['bot_id', 'dimensions', 'run_id', 'user'])
      d.tasks_bot_died += 1
      u.tasks_bot_died += 1
      return True

    if action == 'run_completed':
      _assert_list(
          extras, ['bot_id', 'dimensions', 'run_id', 'runtime_ms', 'user'])
      _mark_bot_and_task_as_active(extras, bots_active, tasks_active)
      d.tasks_completed += 1
      d.tasks_total_runtime_secs += _ms_to_secs(extras['runtime_ms'])
      u.tasks_completed += 1
      u.tasks_total_runtime_secs += _ms_to_secs(extras['runtime_ms'])
      return True

    if action == 'run_started':
      _assert_list(
          extras, ['bot_id', 'dimensions', 'pending_ms', 'run_id', 'user'])
      _mark_bot_and_task_as_active(extras, bots_active, tasks_active)
      d.tasks_started += 1
      d.tasks_pending_secs += _ms_to_secs(extras['pending_ms'])
      u.tasks_started += 1
      u.tasks_pending_secs += _ms_to_secs(extras['pending_ms'])
      return True

    if action == 'run_updated':
      _assert_list(extras, ['bot_id', 'dimensions', 'run_id'])
      _mark_bot_and_task_as_active(extras, bots_active, tasks_active)
      return True

    # TODO(maruel): Ignore task_completed for now, since it is a duplicate of
    # run_completed.
    if action == 'task_completed':
      _assert_list(extras, ['dimensions', 'pending_ms', 'task_id', 'user'])
      # TODO(maruel): Add pending_ms as the total latency to run tasks, versus
      # the amount of time that was spent actually running the task. This gives
      # the infrastructure wall-clock time overhead.
      #d.tasks_completed += 1
      #u.tasks_completed += 1
      return True

    if action == 'task_enqueued':
      _assert_list(extras, ['dimensions', 'task_id', 'user'])
      _mark_task_as_active(extras, tasks_active)
      d.tasks_enqueued += 1
      u.tasks_enqueued += 1
      return True

    if action == 'task_request_expired':
      _assert_list(extras, ['dimensions', 'task_id', 'user'])
      d.tasks_request_expired += 1
      u.tasks_request_expired += 1
      return True

    logging.error('Unknown stats action\n%s', line)
    return False
  except (TypeError, ValueError) as e:
    logging.error('Failed to parse stats line\n%s\n%s', line, e)
    return False


def _post_process(snapshot, bots_active, bots_inactive, tasks_active):
  """Completes the _Snapshot instance with additional data."""
  for dimensions_json, tasks in tasks_active.iteritems():
    snapshot.get_dimensions(dimensions_json).tasks_active = len(tasks)

  snapshot.bot_ids = sorted(bots_active)
  snapshot.bot_ids_bad = sorted(bots_inactive)
  for bot_id, dimensions in bots_active.iteritems():
    # Looks at the current buckets, do not create one.
    for bucket in snapshot.buckets:
      # If this bot matches these dimensions, mark it as a member of this group.
      if task_to_run.match_dimensions(
          json.loads(bucket.dimensions), dimensions):
        # This bot could be used for requests on this dimensions filter.
        if not bot_id in bucket.bot_ids:
          bucket.bot_ids.append(bot_id)
          bucket.bot_ids.sort()

  for bot_id, dimensions in bots_inactive.iteritems():
    # Looks at the current buckets, do not create one.
    for bucket in snapshot.buckets:
      # If this bot matches these dimensions, mark it as a member of this group.
      if task_to_run.match_dimensions(
          json.loads(bucket.dimensions), dimensions):
        # This bot could be used for requests on this dimensions filter.
        if not bot_id in bucket.bot_ids_bad:
          bucket.bot_ids_bad.append(bot_id)
          bucket.bot_ids_bad.sort()


def _extract_snapshot_from_logs(start_time, end_time):
  """Returns a _Snapshot from the processed logs for the specified interval.

  The data is retrieved from logservice via stats_framework.
  """
  snapshot = _Snapshot()
  total_lines = 0
  parse_errors = 0
  bots_active = {}
  bots_inactive = {}
  tasks_active = {}

  for entry in stats_framework.yield_entries(start_time, end_time):
    snapshot.http_requests += 1
    if entry.request.status >= 500:
      snapshot.http_failures += 1

    for l in entry.entries:
      if _parse_line(l, snapshot, bots_active, bots_inactive, tasks_active):
        total_lines += 1
      else:
        parse_errors += 1

  _post_process(snapshot, bots_active, bots_inactive, tasks_active)
  logging.debug(
      '_extract_snapshot_from_logs(%s, %s): %d lines, %d errors',
      start_time, end_time, total_lines, parse_errors)
  return snapshot


### Public API


STATS_HANDLER = stats_framework.StatisticsFramework(
    'global_stats', _Snapshot, _extract_snapshot_from_logs)


def add_entry(**kwargs):
  """Formatted statistics log entry so it can be processed for statistics."""
  stats_framework.add_entry(_pack_entry(**kwargs))


def add_run_entry(action, run_result_key, **kwargs):
  """Action about a TaskRunResult."""
  assert action.startswith('run_'), action
  run_id = task_pack.pack_run_result_key(run_result_key)
  return add_entry(action=action, run_id=run_id, **kwargs)


def add_task_entry(action, result_summary_key, **kwargs):
  """Action about a TaskRequest/TaskResultSummary."""
  assert action.startswith('task_'), action
  task_id = task_pack.pack_result_summary_key(result_summary_key)
  return add_entry(action=action, task_id=task_id, **kwargs)


### Handlers


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  @decorators.require_cronjob
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    i = STATS_HANDLER.process_next_chunk(stats_framework.TOO_RECENT)
    if i is not None:
      msg = 'Processed %d minutes' % i
      logging.info(msg)
      self.response.write(msg)
