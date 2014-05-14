# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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
from server import task_common


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
  # Dimensions are saved as json encoded. JSONProperty is not used so it is
  # easier to compare and sort entries containing dicts.
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

  TODO(maruel): Add requests_total_runtime_secs, requests_avg_runtime_secs.
  """
  # General HTTP details.
  http_requests = ndb.IntegerProperty(default=0)
  http_failures = ndb.IntegerProperty(default=0)

  # TODO(maruel): This will become large (thousands). Something like a
  # bloomfilter + number could help. On the other hand, it's highly compressible
  # and this instance is zlib compressed. Run an NN thousands bots load tests
  # and determine if it becomes a problem.
  bot_ids = ndb.StringProperty(repeated=True)

  # Buckets the statistics per dimensions.
  buckets = ndb.LocalStructuredProperty(_SnapshotForDimensions, repeated=True)
  # Per user statistics.
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
    stats_framework.accumulate(self, rhs, ['bot_ids', 'buckets', 'users'])
    self.bot_ids = sorted(set(self.bot_ids) | set(rhs.bot_ids))
    lhs_dimensions = dict((i.dimensions, i) for i in self.buckets)
    rhs_dimensions = dict((i.dimensions, i) for i in rhs.buckets)
    for key in set(rhs_dimensions):
      if key not in lhs_dimensions:
        # Make a copy of the right hand side so no aliasing occurs.
        lhs_dimensions[key] = rhs_dimensions[key].__class__()
        lhs_dimensions[key].populate(**rhs_dimensions[key].to_dict())
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
        lhs_users[key].populate(**rhs_users[key].to_dict())
      else:
        lhs_users[key].accumulate(rhs_users[key])
    self.users = sorted(lhs_users.itervalues(), key=lambda i: i.user)

  def to_dict(self):
    """Returns the summary only, not the buckets."""
    return {
      'bots_active': self.bots_active,
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
    }


### Utility


# Valid actions that can be logged.
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

# Mapping from long to compact key names. This reduces space usage.
_KEY_MAPPING = {
  'action': 'a',
  'bot_id': 'bid',
  'dimensions': 'd',
  'pending_ms': 'pms',
  'req_id': 'rid',
  'runtime_ms': 'rms',
  'number_shards': 'ns',
  'shard_id': 'sid',
  'user': 'u',
}

_REVERSE_KEY_MAPPING = {v:k for k, v in _KEY_MAPPING.iteritems()}


def _assert_list(actual, expected):
  actual = sorted(actual)
  if actual != expected:
    raise ValueError('%s != %s' % (','.join(actual), ','.join(expected)))


def _mark_bot_and_shard_as_active(extras, bots_active, shards_active):
  """Notes both the bot id and the shard id as active in this time span."""
  bots_active.setdefault(extras['bot_id'], extras['dimensions'])
  dimensions_json = utils.encode_to_json(extras['dimensions'])
  shards_active.setdefault(dimensions_json, set()).add(extras['shard_id'])


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


def _parse_line(line, values, bots_active, shards_active):
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
    if 'dimensions' in extras and action != 'bot_active':
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

    if action == 'request_enqueued':
      _assert_list(extras, ['dimensions', 'number_shards', 'req_id', 'user'])
      # Convert the request id to shard ids.
      num_shards = int(extras['number_shards'])
      req_id = int(extras['req_id'], 16)
      for i in xrange(num_shards):
        shards_active.setdefault(dimensions_json, set()).add(
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
  """Completes the _Snapshot instance with additional data."""
  for dimensions_json, shards in shards_active.iteritems():
    snapshot.get_dimensions(dimensions_json).shards_active = len(shards)

  snapshot.bot_ids = sorted(bots_active)
  for bot_id, dimensions in bots_active.iteritems():
    # Looks at the current buckets, do not create one.
    for bucket in snapshot.buckets:
      # If this bot matches these dimensions, mark it as a member of this group.
      if task_common.match_dimensions(
          json.loads(bucket.dimensions), dimensions):
        # This bot could be used for requests on this dimensions filter.
        if not bot_id in bucket.bot_ids:
          bucket.bot_ids.append(bot_id)
          bucket.bot_ids.sort()


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


def add_entry(**kwargs):
  """Formatted statistics log entry so it can be processed for statistics."""
  stats_framework.add_entry(_pack_entry(**kwargs))


def add_request_entry(action, request_key, **kwargs):
  """Action about a TaskRequest."""
  assert action.startswith('request_'), action
  assert request_key.kind() == 'TaskRequest', request_key
  return add_entry(
      action=action, req_id='%x' % request_key.integer_id(), **kwargs)


def add_shard_entry(action, shard_result_key, **kwargs):
  """Action about a TaskShardResult."""
  assert action.startswith('shard_'), action
  assert shard_result_key.kind() == 'TaskShardResult', shard_result_key
  shard_id = '%x-%d' % (
      shard_result_key.parent().integer_id(), shard_result_key.integer_id())
  return add_entry(action=action, shard_id=shard_id, **kwargs)


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
