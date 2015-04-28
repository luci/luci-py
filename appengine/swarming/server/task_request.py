# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tasks definition.

Each user request creates a new TaskRequest. The TaskRequest instance saves the
metadata of the request, e.g. who requested it, when why, etc. It links to the
actual data of the request in a TaskProperties. The TaskProperties represents
everything needed to run the task.

This means if two users request an identical task, it can be deduped
accordingly and efficiently by the scheduler.

Note that the mere existence of a TaskRequest in the db doesn't mean it will be
scheduled, see task_scheduler.py for the actual scheduling. Registering tasks
and scheduling are kept separated to keep the immutable and mutable models in
separate files.


Overview of transactions:
- TaskRequest() are created inside a transaction.


Graph of the schema:

    +--------Root---------+
    |TaskRequest          |
    |    +--------------+ |
    |    |TaskProperties| |
    |    +--------------+ |
    |id=<based on epoch>  |
    +---------------------+
               ^
               |
    <See task_to_run.py and task_result.py>

TaskProperties is embedded in TaskRequest. TaskProperties is still declared as a
separate entity to clearly declare the boundary for task request deduplication.
"""


import datetime
import hashlib
import logging
import random
import re

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import utils
from server import task_pack


# Maximum acceptable priority value, which is effectively the lowest priority.
MAXIMUM_PRIORITY = 255


# One day in seconds. Add 10s to account for small jitter.
_ONE_DAY_SECS = 24*60*60 + 10


# Minimum value for timeouts.
_MIN_TIMEOUT_SECS = 1 if utils.is_local_dev_server() else 30


# The world started on 2010-01-01 at 00:00:00 UTC. The rationale is that using
# EPOCH (1970) means that 40 years worth of keys are wasted.
#
# Note: This creates a 'naive' object instead of a formal UTC object. Note that
# datetime.datetime.utcnow() also return naive objects. That's python.
_BEGINING_OF_THE_WORLD = datetime.datetime(2010, 1, 1, 0, 0, 0, 0)


# Parameters for make_request().
# The content of the 'data' parameter. This relates to the context of the
# request, e.g. who wants to run a task.
_REQUIRED_DATA_KEYS = frozenset(
    ['name', 'priority', 'properties', 'scheduling_expiration_secs', 'tags',
     'user'])
_EXPECTED_DATA_KEYS = frozenset(
    ['name', 'parent_task_id', 'priority', 'properties',
      'scheduling_expiration_secs', 'tags', 'user'])
# The content of 'properties' inside the 'data' parameter. This relates to the
# task itself, e.g. what to run.
_REQUIRED_PROPERTIES_KEYS= frozenset(
    ['commands', 'data', 'dimensions', 'env', 'execution_timeout_secs',
     'io_timeout_secs'])
_EXPECTED_PROPERTIES_KEYS = frozenset(
    ['commands', 'data', 'dimensions', 'env', 'execution_timeout_secs',
     'grace_period_secs', 'idempotent', 'io_timeout_secs'])


### Properties validators must come before the models.


def _validate_command(prop, value):
  """Validates TaskProperties.command."""
  # pylint: disable=W0212
  if not value:
    # required=True would still accept [].
    raise datastore_errors.BadValueError('%s is required' % prop._name)
  def check(line):
    return isinstance(line, list) and all(isinstance(j, unicode) for j in line)

  if not all(check(i) for i in value):
    raise TypeError(
        '%s must be a list of commands, each a list of arguments' % prop._name)


def _validate_data(prop, value):
  """Validates TaskProperties.data and sort the URLs."""
  def check(i):
    return (
        isinstance(i, list) and len(i) == 2 and
        isinstance(i[0], unicode) and isinstance(i[1], unicode))

  if not all(check(i) for i in value):
    # pylint: disable=W0212
    raise TypeError('%s must be a list of (url, file)' % prop._name)
  return sorted(value)


def _validate_dict_of_strings(prop, value):
  """Validates TaskProperties.dimension and TaskProperties.env."""
  if not all(
         isinstance(k, unicode) and isinstance(v, unicode)
         for k, v in value.iteritems()):
    # pylint: disable=W0212
    raise TypeError('%s must be a dict of strings' % prop._name)


def _validate_expiration(prop, value):
  """Validates TaskRequest.expiration_ts."""
  now = utils.utcnow()
  offset = int(round((value - now).total_seconds()))
  if not (_MIN_TIMEOUT_SECS <= offset <= _ONE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%s, %ds from now) must effectively be between %ds and one day '
        'from now (%s)' %
        (prop._name, value, offset, _MIN_TIMEOUT_SECS, now))


def _validate_grace(prop, value):
  """Validates grace_period_secs in TaskProperties."""
  if not (0 <= value <= _ONE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between %ds and one day' % (prop._name, value, 0))


def _validate_priority(_prop, value):
  """Validates TaskRequest.priority."""
  validate_priority(value)
  return value


def _validate_task_run_id(_prop, value):
  """Validates a task_id looks valid without fetching the entity."""
  if not value:
    return None
  task_pack.unpack_run_result_key(value)
  return value


def _validate_timeout(prop, value):
  """Validates timeouts in seconds in TaskProperties."""
  if not (_MIN_TIMEOUT_SECS <= value <= _ONE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between %ds and one day' %
            (prop._name, value, _MIN_TIMEOUT_SECS))


def _validate_tags(prop, value):
  """Validates and sorts TaskRequest.tags."""
  if not ':' in value:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s must be key:value form, not %s' % (prop._name, value))


def _validate_args(prop, value):
  if not (isinstance(value, list) and all(
      isinstance(arg, unicode) for arg in value)):
    raise datastore_errors.BadValueError(
        '%s must be a list of unicode strings, not %r' % (prop._name, value))


def _validate_isolated(prop, value):
  if value and not frozenset(value).issubset('0123456789abcdef'):
    raise datastore_errors.BadValueError(
        '%s must be lowercase hex, not %s' % (prop._name, value))


def _validate_hostname(prop, value):
  labels = value.split('.')
  if not (
      all(re.match(r'[a-zA-Z\d\-]{1,63}$', label) for label in labels)
      and 0 < len(value) < 254):
    raise datastore_errors.BadValueError(
        '%s must be valid hostname, not %s' % (prop._name, value))


### Models.


class TaskProperties(ndb.Model):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  This model is immutable.

  New-style TaskProperties supports invocation of run_isolated. When this
  behavior is desired, .commands and .data must be omitted; instead, the members
  .isolated, .isolatedserver, .namespace, and (optionally) .extra_args should
  be supplied instead.
  """
  # Hashing algorithm used to hash TaskProperties to create its key.
  HASHING_ALGO = hashlib.sha1

  # Commands to run. It is a list of lists. Each command is run one after the
  # other. Encoded json.
  commands = datastore_utils.DeterministicJsonProperty(
      validator=_validate_command, json_type=list)

  # List of (URLs, local file) for the bot to download. Encoded as json. Must be
  # sorted by URLs. Optional.
  data = datastore_utils.DeterministicJsonProperty(
      validator=_validate_data, json_type=list)

  # Filter to use to determine the required properties on the bot to run on. For
  # example, Windows or hostname. Encoded as json. Optional but highly
  # recommended.
  dimensions = datastore_utils.DeterministicJsonProperty(
      validator=_validate_dict_of_strings, json_type=dict)

  # Environment variables. Encoded as json. Optional.
  env = datastore_utils.DeterministicJsonProperty(
      validator=_validate_dict_of_strings, json_type=dict)

  # Maximum duration the bot can take to run this task. It's named hard_timeout
  # in the bot.
  execution_timeout_secs = ndb.IntegerProperty(
      validator=_validate_timeout, required=True)

  # Extra arguments to supply to the command `python run_isolated ... .`
  extra_args = datastore_utils.DeterministicJsonProperty(
      validator=_validate_args, json_type=list)

  # Grace period is the time between signaling the task it timed out and killing
  # the process. During this time the process should clean up itself as quickly
  # as possible, potentially uploading partial results back.
  grace_period_secs = ndb.IntegerProperty(validator=_validate_grace, default=30)

  # Bot controlled timeout for new bytes from the subprocess. If a subprocess
  # doesn't output new data to stdout for .io_timeout_secs, consider the command
  # timed out. Optional.
  io_timeout_secs = ndb.IntegerProperty(validator=_validate_timeout)

  # If True, the task can safely be served results from a previously succeeded
  # task.
  idempotent = ndb.BooleanProperty(default=False)

  # The hash of an isolated archive.
  isolated = ndb.StringProperty(validator=_validate_isolated)

  # The hostname of the isolated server to use.
  isolatedserver = ndb.StringProperty(validator=_validate_hostname)

  # Namespace on the isolate server.
  namespace = ndb.StringProperty()

  @property
  def properties_hash(self):
    """Calculates the hash for this entity IFF the task is idempotent.

    It uniquely identifies the TaskProperties instance to permit deduplication
    by the task scheduler. It is None if the task is not idempotent.

    Returns:
      Hash as a compact byte str.
    """
    if not self.idempotent:
      return None
    return self.HASHING_ALGO(utils.encode_to_json(self)).digest()


class TaskRequest(ndb.Model):
  """Contains a user request.

  Key id is a decreasing integer based on time since utils.EPOCH plus some
  randomness on lower order bits. See _new_request_key() for the complete gory
  details.

  There is also "old style keys" which inherit from a fake root entity
  TaskRequestShard.

  TODO(maruel): Remove support 2015-02-01.

  This model is immutable.
  """
  # Time this request was registered. It is set manually instead of using
  # auto_now_add=True so that expiration_ts can be set very precisely relative
  # to this property.
  created_ts = ndb.DateTimeProperty(required=True)

  # The name for this task request. It's only for description.
  name = ndb.StringProperty(required=True)

  # Authenticated client that triggered this task.
  authenticated = auth.IdentityProperty()

  # Which user to blame for this task.
  user = ndb.StringProperty(default='')

  # The actual properties are embedded in this model.
  properties = ndb.LocalStructuredProperty(
      TaskProperties, compressed=True, required=True)

  # Priority of the task to be run. A lower number is higher priority, thus will
  # preempt requests with lower priority (higher numbers).
  priority = ndb.IntegerProperty(
      indexed=False, validator=_validate_priority, required=True)

  # If the task request is not scheduled by this moment, it will be aborted by a
  # cron job. It is saved instead of scheduling_expiration_secs so finding
  # expired jobs is a simple query.
  expiration_ts = ndb.DateTimeProperty(
      indexed=True, validator=_validate_expiration, required=True)

  # Tags that specify the category of the task.
  tags = ndb.StringProperty(repeated=True, validator=_validate_tags)

  # Set when a task (the parent) reentrantly create swarming tasks. Must be set
  # to a valid task_id pointing to a TaskRunResult or be None.
  parent_task_id = ndb.StringProperty(validator=_validate_task_run_id)

  @property
  def scheduling_expiration_secs(self):
    """Reconstructs this value from expiration_ts and created_ts."""
    return (self.expiration_ts - self.created_ts).total_seconds()

  def to_dict(self):
    """Converts properties_hash to hex so it is json serializable."""
    out = super(TaskRequest, self).to_dict()
    properties_hash = self.properties.properties_hash
    out['properties_hash'] = (
        properties_hash.encode('hex') if properties_hash else None)
    return out

  def _pre_put_hook(self):
    """Adds automatic tags."""
    super(TaskRequest, self)._pre_put_hook()
    self.tags.append('priority:%s' % self.priority)
    self.tags.append('user:%s' % self.user)
    for key, value in self.properties.dimensions.iteritems():
      self.tags.append('%s:%s' % (key, value))
    self.tags = sorted(set(self.tags))


def _new_request_key():
  """Returns a valid ndb.Key for this entity.

  Task id is a 64 bits integer represented as a string to the user:
  - 1 highest order bits set to 0 to keep value positive.
  - 43 bits is time since _BEGINING_OF_THE_WORLD at 1ms resolution.
    It is good for 2**43 / 365.3 / 24 / 60 / 60 / 1000 = 278 years or 2010+278 =
    2288. The author will be dead at that time.
  - 16 bits set to a random value or a server instance specific value. Assuming
    an instance is internally consistent with itself, it can ensure to not reuse
    the same 16 bits in two consecutive requests and/or throttle itself to one
    request per millisecond.
    Using random value reduces to 2**-15 the probability of collision on exact
    same timestamp at 1ms resolution, so a maximum theoretical rate of 65536000
    requests/sec but an effective rate in the range of ~64k requests/sec without
    much transaction conflicts. We should be fine.
  - 4 bits set to 0x1. This is to represent the 'version' of the entity schema.
    Previous version had 0. Note that this value is XOR'ed in the DB so it's
    stored as 0xE. When the TaskRequest entity tree is modified in a breaking
    way that affects the packing and unpacking of task ids, this value should be
    bumped.

  The key id is this value XORed with task_pack.TASK_REQUEST_KEY_ID_MASK. The
  reason is that increasing key id values are in decreasing timestamp order.
  """
  # TODO(maruel): Use real randomness.
  suffix = random.getrandbits(16)
  return convert_to_request_key(utils.utcnow(), suffix)


def _put_request(request):
  """Puts the new TaskRequest in the DB.

  Returns:
    ndb.Key of the new entity. Returns None if failed, which should be surfaced
    to the user.
  """
  assert not request.key
  request.key = _new_request_key()
  return datastore_utils.insert(request, _new_request_key)


def _assert_keys(expected_keys, minimum_keys, actual_keys, name):
  """Raise an exception if expected keys are not present."""
  actual_keys = frozenset(actual_keys)
  superfluous = actual_keys - expected_keys
  missing = minimum_keys - actual_keys
  if superfluous or missing:
    msg_missing = (
        ('Missing: %s\n' % ', '.join(sorted(missing))) if missing else '')
    msg_superfluous = (
        ('Superfluous: %s\n' % ', '.join(sorted(superfluous)))
        if superfluous else '')
    message = 'Unexpected %s; did you make a typo?\n%s%s' % (
        name, msg_missing, msg_superfluous)
    raise ValueError(message)


### Public API.


def request_key_to_datetime(request_key):
  """Converts a TaskRequest.key to datetime.

  See _new_request_key() for more details.
  """
  if request_key.kind() != 'TaskRequest':
    raise ValueError('Expected key to TaskRequest, got %s' % request_key.kind())
  # Ignore lowest 20 bits.
  xored = request_key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
  offset_ms = (xored >> 20) / 1000.
  return _BEGINING_OF_THE_WORLD + datetime.timedelta(seconds=offset_ms)


def datetime_to_request_base_id(now):
  """Converts a datetime into a TaskRequest key base value.

  Used for query order().
  """
  if now < _BEGINING_OF_THE_WORLD:
    raise ValueError(
        'Time %s is set to before %s' % (now, _BEGINING_OF_THE_WORLD))
  delta = now - _BEGINING_OF_THE_WORLD
  return int(round(delta.total_seconds() * 1000.)) << 20


def convert_to_request_key(date, suffix=0):
  assert 0 <= suffix <= 0xffff
  request_id_base = datetime_to_request_base_id(date)
  return request_id_to_key(int(request_id_base | suffix << 4 | 0x1))


def request_id_to_key(request_id):
  """Converts a request id into a TaskRequest key.

  Note that this function does NOT accept a task id. This functions is primarily
  meant for limiting queries to a task creation range.
  """
  return ndb.Key(TaskRequest, request_id ^ task_pack.TASK_REQUEST_KEY_ID_MASK)


def validate_request_key(request_key):
  if request_key.kind() != 'TaskRequest':
    raise ValueError('Expected key to TaskRequest, got %s' % request_key.kind())
  task_id = request_key.integer_id()
  if not task_id:
    raise ValueError('Invalid null TaskRequest key')
  if (task_id & 0xF) == 0xE:
    # New style key.
    return

  # Check the shard.
  # TODO(maruel): Remove support 2015-02-01.
  request_shard_key = request_key.parent()
  if not request_shard_key:
    raise ValueError('Expected parent key for TaskRequest, got nothing')
  if request_shard_key.kind() != 'TaskRequestShard':
    raise ValueError(
        'Expected key to TaskRequestShard, got %s' % request_shard_key.kind())
  root_entity_shard_id = request_shard_key.string_id()
  if (not root_entity_shard_id or
      len(root_entity_shard_id) != task_pack.DEPRECATED_SHARDING_LEVEL):
    raise ValueError(
        'Expected root entity key (used for sharding) to be of length %d but '
        'length was only %d (key value %r)' % (
            task_pack.DEPRECATED_SHARDING_LEVEL,
            len(root_entity_shard_id or ''),
            root_entity_shard_id))


def make_request(data):
  """Constructs a TaskRequest out of a yet-to-be-specified API.

  Argument:
  - data: dict with:
    - name
    - parent_task_id*
    - properties
      - commands
      - data
      - dimensions
      - env
      - execution_timeout_secs
      - grace_period_secs*
      - idempotent*
      - io_timeout_secs
    - priority
    - scheduling_expiration_secs
    - tags
    - user

  * are optional.

  If parent_task_id is set, properties for the parent are used:
  - priority: defaults to parent.priority - 1
  - user: overriden by parent.user

  Returns:
    The newly created TaskRequest.
  """
  # Save ourself headaches with typos and refuses unexpected values.
  _assert_keys(_EXPECTED_DATA_KEYS, _REQUIRED_DATA_KEYS, data, 'request keys')
  data_properties = data['properties']
  _assert_keys(_EXPECTED_PROPERTIES_KEYS, _REQUIRED_PROPERTIES_KEYS,
               data_properties, 'request properties keys')

  parent_task_id = data.get('parent_task_id') or None
  if parent_task_id:
    data = data.copy()
    run_result_key = task_pack.unpack_run_result_key(parent_task_id)
    result_summary_key = task_pack.run_result_key_to_result_summary_key(
        run_result_key)
    request_key = task_pack.result_summary_key_to_request_key(
        result_summary_key)
    parent = request_key.get()
    if not parent:
      raise ValueError('parent_task_id is not a valid task')
    data['priority'] = max(min(data['priority'], parent.priority - 1), 0)
    # Drop the previous user.
    data['user'] = parent.user

  # Can't be a validator yet as we wouldn't be able to load previous task
  # requests.
  if len(data_properties.get('commands') or []) > 1:
    raise datastore_errors.BadValueError('Only one command is supported')

  # Class TaskProperties takes care of making everything deterministic.
  # TODO(cmassaro): New API must add support for run_isolate information.
  properties = TaskProperties(
      commands=data_properties['commands'],
      data=data_properties['data'],
      dimensions=data_properties['dimensions'],
      env=data_properties['env'],
      execution_timeout_secs=data_properties['execution_timeout_secs'],
      grace_period_secs=data_properties.get('grace_period_secs', 30),
      idempotent=data_properties.get('idempotent', False),
      io_timeout_secs=data_properties['io_timeout_secs'])

  now = utils.utcnow()
  expiration_ts = now + datetime.timedelta(
      seconds=data['scheduling_expiration_secs'])

  request = TaskRequest(
      authenticated=auth.get_current_identity(),
      created_ts=now,
      expiration_ts=expiration_ts,
      name=data['name'],
      parent_task_id=parent_task_id,
      priority=data['priority'],
      properties=properties,
      tags=data['tags'],
      user=data['user'] or '')
  _put_request(request)
  return request


def make_request_clone(original_request):
  """Makes a new TaskRequest from a previous one.

  Modifications:
  - Enforces idempotent=False.
  - Removes the parent_task_id if any.
  - Append suffix '(Retry #1)' to the task name, incrementing the number of
    followup retries.
  - Strip any tag starting with 'user:'.
  - Override request's user with the credentials of the currently logged in
    user.

  Returns:
    The newly created TaskRequest.
  """
  now = utils.utcnow()
  properties = TaskProperties(**original_request.properties.to_dict())
  properties.idempotent = False
  expiration_ts = (
      now + (original_request.expiration_ts - original_request.created_ts))
  name = original_request.name
  match = re.match(r'^(.*) \(Retry #(\d+)\)$', name)
  if match:
    name = '%s (Retry #%d)' % (match.group(1), int(match.group(2)) + 1)
  else:
    name += ' (Retry #1)'
  user = auth.get_current_identity()
  username = user.to_bytes()
  prefix = 'user:'
  if not username.startswith(prefix):
    raise ValueError('a request can only be cloned by a user, not a bot')
  username = username[len(prefix):]
  tags = set(t for t in original_request.tags if not t.startswith('user:'))
  request = TaskRequest(
      authenticated=user,
      created_ts=now,
      expiration_ts=expiration_ts,
      name=name,
      parent_task_id=None,
      priority=original_request.priority,
      properties=properties,
      tags=tags,
      user=username)
  _put_request(request)
  return request


def validate_priority(priority):
  """Throws ValueError if priority is not a valid value."""
  if 0 > priority or MAXIMUM_PRIORITY < priority:
    raise datastore_errors.BadValueError(
        'priority (%d) must be between 0 and %d (inclusive)' %
        (priority, MAXIMUM_PRIORITY))
