# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates statistics out of logs. Contains the backend code.

It's important to keep logs concise for general performance concerns. Each http
handler should strive to do only one stats log entry per request.
"""

import logging

import webapp2
from google.appengine.api import logservice
from google.appengine.ext import ndb
from google.appengine.runtime import DeadlineExceededError

from components import decorators
from components import stats_framework


### Models


class _SnapshotBucketBase(ndb.Model):
  """Statistics for a specific bucket, meant to be subclassed.
  """
  requests_enqueued = ndb.IntegerProperty(default=0)
  shards_enqueued = ndb.IntegerProperty(default=0)

  # TODO(maruel): Use sampling to get median, average, etc instead of the raw
  # sum.
  shards_started = ndb.IntegerProperty(default=0)
  shards_pending_secs = ndb.FloatProperty(default=0)

  shards_active = ndb.IntegerProperty(default=0)

  requests_completed = ndb.IntegerProperty(default=0)
  shards_completed = ndb.IntegerProperty(default=0)
  shards_total_runtime_secs = ndb.FloatProperty(default=0)

  shards_bot_died = ndb.IntegerProperty(default=0)
  shards_request_expired = ndb.IntegerProperty(default=0)

  @property
  def shards_avg_pending_secs(self):
    if self.shards_started:
      return round(self.shards_pending_secs / float(self.shards_started), 3)
    return 0.

  @property
  def shards_avg_runtime_secs(self):
    if self.shards_completed:
      return round(
          self.shards_total_runtime_secs / float(self.shards_completed), 3)
    return 0.

  def to_dict(self):
    out = super(_SnapshotBucketBase, self).to_dict()
    out['shards_avg_pending_secs'] = self.shards_avg_pending_secs
    out['shards_avg_runtime_secs'] = self.shards_avg_runtime_secs
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
  dimensions = ndb.StringProperty()

  # TODO(maruel): This will become large (thousands). Something like a
  # bloomfilter + number could help. On the other hand, it's highly compressible
  # and this instance is zlib compressed. Run an NN thousands bots load tests
  # and determine if it becomes a problem.
  bot_ids = ndb.StringProperty(repeated=True)

  @property
  def bots_active(self):
    return len(self.bot_ids)

  def accumulate(self, rhs):
    assert self.dimensions == rhs.dimensions
    stats_framework.accumulate(self, rhs, ['bot_ids', 'dimensions'])
    self.bot_ids = sorted(set(self.bot_ids) | set(rhs.bot_ids))

  def to_dict(self):
    out = super(_SnapshotForDimensions, self).to_dict()
    out['bots_active'] = self.bots_active
    del out['bot_ids']
    return out


class _Snapshot(ndb.Model):
  """A snapshot of statistics for the specific time frame.

  It has references to _SnapshotForDimensions which holds the
  dimensions-specific data.
  """
  # General HTTP details.
  http_requests = ndb.IntegerProperty(default=0)
  http_failures = ndb.IntegerProperty(default=0)

  # TODO(maruel): This will become large (thousands). Something like a
  # bloomfilter + number could help. On the other hand, it's highly compressible
  # and this instance is zlib compressed. Run an NN thousands bots load tests
  # and determine if it becomes a problem.
  bot_ids = ndb.StringProperty(repeated=True)

  buckets = ndb.LocalStructuredProperty(_SnapshotForDimensions, repeated=True)
  users = ndb.LocalStructuredProperty(_SnapshotForUser, repeated=True)

  @property
  def bots_active(self):
    return len(self.bot_ids)

  # Sums.

  @property
  def requests_enqueued(self):
    return sum(i.requests_enqueued for i in self.buckets)

  @property
  def requests_completed(self):
    return sum(i.requests_completed for i in self.buckets)

  @property
  def shards_enqueued(self):
    return sum(i.shards_enqueued for i in self.buckets)

  @property
  def shards_started(self):
    return sum(i.shards_started for i in self.buckets)

  @property
  def shards_pending_secs(self):
    return sum(i.shards_pending_secs for i in self.buckets)

  @property
  def shards_active(self):
    return sum(i.shards_active for i in self.buckets)

  @property
  def shards_completed(self):
    return sum(i.shards_completed for i in self.buckets)

  @property
  def shards_total_runtime_secs(self):
    return sum(i.shards_total_runtime_secs for i in self.buckets)

  @property
  def shards_bot_died(self):
    return sum(i.shards_bot_died for i in self.buckets)

  @property
  def shards_request_expired(self):
    return sum(i.shards_request_expired for i in self.buckets)

  @property
  def shards_avg_pending_secs(self):
    started = 0
    pending_secs = 0.
    for i in self.buckets:
      started += i.shards_started
      pending_secs += i.shards_pending_secs
    if started:
      return round(pending_secs / float(started), 3)
    return 0.

  @property
  def shards_avg_runtime_secs(self):
    completed = 0
    runtime_secs = 0.
    for i in self.buckets:
      completed += i.shards_completed
      runtime_secs += i.shards_total_runtime_secs
    if completed:
      return round(runtime_secs / float(completed), 3)
    return 0.

  def get_dimensions(self, dimensions):
    """Returns a _SnapshotForDimensions instance for this dimensions key.

    Creates one if necessary and keeps self.buckets sorted.
    """
    dimensions_map = dict((i.dimensions, i) for i in self.buckets)
    if not dimensions in dimensions_map:
      i = _SnapshotForDimensions(dimensions=dimensions)
      self.buckets.append(i)
      self.buckets.sort(key=lambda i: i.dimensions)
      return i
    return dimensions_map[dimensions]

  def get_user(self, user):
    """Returns a _SnapshotForUser instance for this user.

    Creates one if necessary and keeps self.users sorted.
    """
    user_map = dict((i.user, i) for i in self.users)
    if not user in user_map:
      i = _SnapshotForUser(user=user)
      self.users.append(i)
      self.users.sort(key=lambda i: i.user)
      return i
    return user_map[user]

  def accumulate(self, rhs):
    # TODO(maruel): Pre-populate all dimensions being used in the past N hours.
    # In particular, pre-populate the list of bot_ids available per popular
    # request types, like 'os=foo'.
    stats_framework.accumulate(self, rhs, ['bot_ids', 'buckets', 'users'])
    self.bot_ids = sorted(set(self.bot_ids) | set(rhs.bot_ids))
    lhs_dimensions = dict((i.dimensions, i) for i in self.buckets)
    rhs_dimensions = dict((i.dimensions, i) for i in rhs.buckets)
    for key in set(lhs_dimensions) | set(rhs_dimensions):
      if key not in lhs_dimensions:
        lhs_dimensions[key] = rhs_dimensions[key]
      elif key in rhs_dimensions:
        lhs_dimensions[key].accumulate(rhs_dimensions[key])
    self.buckets = lhs_dimensions.values()

  def to_dict(self):
    """Returns the summary only, not the buckets."""
    return {
      'bots_active': self.bots_active,
      'dimensions': [i.dimensions for i in self.buckets],
      'http_failures': self.http_failures,
      'http_requests': self.http_requests,
      'requests_completed': self.requests_completed,
      'requests_enqueued': self.requests_enqueued,
      'shards_active': self.shards_active,
      'shards_avg_pending_secs': self.shards_avg_pending_secs,
      'shards_avg_runtime_secs': self.shards_avg_runtime_secs,
      'shards_bot_died': self.shards_bot_died,
      'shards_completed': self.shards_completed,
      'shards_enqueued': self.shards_enqueued,
      'shards_pending_secs': self.shards_pending_secs,
      'shards_request_expired': self.shards_request_expired,
      'shards_total_runtime_secs': self.shards_total_runtime_secs,
      'shards_started': self.shards_started,
      'users': [i.user for i in self.users],
    }


### Utility


_VALID_ACTIONS = frozenset(
  [
    'bot_active',
    'request_enqueued',
    'request_completed',
    'shard_bot_died',
    'shard_completed',
    'shard_request_expired',
    'shard_started',
    'shard_updated',
  ])


def _assert_list(actual, expected):
  actual = sorted(actual)
  if actual != expected:
    raise ValueError('%s != %s' % (','.join(actual), ','.join(expected)))


def _mark_bot_and_shard_as_active(extras, bots_active, shards_active):
  """Notes both the bot id and the shard id as active in this time span."""
  bots_active.setdefault(extras['bot_id'], extras['dimensions'])
  shards_active.setdefault(extras['dimensions'], set()).add(extras['shard_id'])


def _ms_to_secs(raw):
  """Converts a str representing a value in ms into seconds.

  See task_scheduler._secs_to_ms() for the reverse conversion.
  """
  return float(raw) * 0.001


def _parse_line(line, values, bots_active, shards_active):
  """Updates a Snapshot instance with a processed statistics line if relevant.

  This function is a big switch case, so while it is long and will get longer,
  it is relatively easy to read, as long as the keys are kept sorted!
  """
  try:
    data = line.split('; ')
    action = data.pop(0)
    extras = dict(i.split('=', 1) for i in data)

    # Preemptively reduce copy-paste.
    d = None
    if 'dimensions' in extras:
      d = values.get_dimensions(extras['dimensions'])
    u = None
    if 'user' in extras:
      u = values.get_user(extras['user'])

    # Please keep 'action == 'foo' conditions sorted!
    if action == 'bot_active':
      if sorted(extras) != ['bot_id', 'dimensions']:
        raise ValueError(','.join(sorted(extras)))

      bots_active[extras['bot_id']] = extras['dimensions']
      return True

    if action == 'request_enqueued':
      _assert_list(extras, ['dimensions', 'req_id', 'shards', 'user'])
      # Convert the request id to shard ids.
      num_shards = int(extras['shards'])
      req_id = int(extras['req_id'], 16)
      for i in xrange(num_shards):
        shards_active.setdefault(extras['dimensions'], set()).add(
            '%x-1' % (req_id + i + 1))
      d.requests_enqueued += 1
      d.shards_enqueued += num_shards
      u.requests_enqueued += 1
      u.shards_enqueued += num_shards
      return True

    if action == 'request_completed':
      _assert_list(extras, ['dimensions', 'req_id', 'user'])
      d.requests_completed += 1
      u.requests_completed += 1
      return True

    if action == 'shard_bot_died':
      _assert_list(extras, ['bot_id', 'dimensions', 'shard_id', 'user'])
      d.shards_bot_died += 1
      u.shards_bot_died += 1
      return True

    if action == 'shard_completed':
      _assert_list(
          extras, ['bot_id', 'dimensions', 'runtime_ms', 'shard_id', 'user'])
      _mark_bot_and_shard_as_active(extras, bots_active, shards_active)
      d.shards_completed += 1
      d.shards_total_runtime_secs += _ms_to_secs(extras['runtime_ms'])
      u.shards_completed += 1
      u.shards_total_runtime_secs += _ms_to_secs(extras['runtime_ms'])
      return True

    if action == 'shard_request_expired':
      _assert_list(extras, ['dimensions', 'shard_id', 'user'])
      d.shards_request_expired += 1
      u.shards_request_expired += 1
      return True

    if action == 'shard_started':
      _assert_list(
          extras, ['bot_id', 'dimensions', 'pending_ms', 'shard_id', 'user'])
      _mark_bot_and_shard_as_active(extras, bots_active, shards_active)
      d.shards_started += 1
      d.shards_pending_secs += _ms_to_secs(extras['pending_ms'])
      u.shards_started += 1
      u.shards_pending_secs += _ms_to_secs(extras['pending_ms'])
      return True

    if action == 'shard_updated':
      _assert_list(extras, ['bot_id', 'dimensions', 'shard_id'])
      _mark_bot_and_shard_as_active(extras, bots_active, shards_active)
      return True

    logging.error('Unknown stats action\n%s', line)
    return False
  except (TypeError, ValueError) as e:
    logging.error('Failed to parse stats line\n%s\n%s', line, e)
    return False


def _post_process(snapshot, bots_active, shards_active):
  snapshot.bot_ids = sorted(bots_active)
  for dimensions, shards in shards_active.iteritems():
    snapshot.get_dimensions(dimensions).shards_active = len(shards)

  # Ignore hostname dimension requests. These are usually maintenance requests
  # and are not useful.
  for bucket in snapshot.buckets[:]:
    if bucket.dimensions.startswith('hostname_'):
      snapshot.buckets.remove(bucket)


def _extract_snapshot_from_logs(start_time, end_time):
  """Returns a _Snapshot from the processed logs for the specified interval.

  The data is retrieved from logservice via stats_framework.
  """
  snapshot = _Snapshot()
  total_lines = 0
  parse_errors = 0
  bots_active = {}
  shards_active = {}

  for entry in stats_framework.yield_entries(start_time, end_time):
    snapshot.http_requests += 1
    if entry.request.status >= 500:
      snapshot.http_failures += 1

    for l in entry.entries:
      if _parse_line(l, snapshot, bots_active, shards_active):
        total_lines += 1
      else:
        parse_errors += 1

  _post_process(snapshot, bots_active, shards_active)
  logging.debug('Parsed %d lines, %d errors', total_lines, parse_errors)
  return snapshot


### Public API


STATS_HANDLER = stats_framework.StatisticsFramework(
    'global_stats', _Snapshot, _extract_snapshot_from_logs)


def add_entry(action, **kwargs):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  assert action in _VALID_ACTIONS, action
  # The stats framework doesn't do escaping yet, so refuse separator '; ' in the
  # values.
  assert not any(
      ';' in k or ';' in str(v) for k, v in kwargs.iteritems()), kwargs
  cmd = action + ''.join(
      '; %s=%s' % (k, v) for k, v in sorted(kwargs.iteritems()))
  stats_framework.add_entry(cmd)


def add_request_entry(action, request_key, **kwargs):
  """Action about a TaskRequest."""
  assert action.startswith('request_'), action
  assert request_key.kind() == 'TaskRequest', request_key
  return add_entry(action, req_id='%x' % request_key.integer_id(), **kwargs)


def add_shard_entry(action, shard_result_key, **kwargs):
  """Action about a TaskShardResult."""
  assert action.startswith('shard_'), action
  assert shard_result_key.kind() == 'TaskShardResult', shard_result_key
  shard_id = '%x-%d' % (
      shard_result_key.parent().integer_id(), shard_result_key.integer_id())
  return add_entry(action, shard_id=shard_id, **kwargs)


def pack_dimensions(dimensions):
  """Returns densely packed dimensions. Always ignore 'hostname'."""
  return '/'.join(
      '%s_%s' % (k, v)
      for k, v in sorted(dimensions.iteritems()) if k != 'hostname')


### Handlers


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  @decorators.require_cronjob
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    try:
      i = STATS_HANDLER.process_next_chunk(stats_framework.TOO_RECENT)
    except (DeadlineExceededError, logservice.Error):
      # The job will be retried.
      self.response.status_code = 500
      return

    msg = 'Processed %d minutes' % i
    logging.info(msg)
    self.response.write(msg)
