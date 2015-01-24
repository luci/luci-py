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


# Mask to TaskRequest key ids so they become decreasing numbers.
_TASK_REQUEST_KEY_ID_MASK = int(2L**63-1)


# Parameters for make_request().
# The content of the 'data' parameter. This relates to the context of the
# request, e.g. who wants to run a task.
_DATA_KEYS = frozenset(
    ['name', 'priority', 'properties', 'scheduling_expiration_secs', 'tags',
     'user'])
# The content of 'properties' inside the 'data' parameter. This relates to the
# task itself, e.g. what to run.
_REQUIRED_PROPERTIES_KEYS = frozenset(
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


def _validate_grace(prop, value):
  """Validates grace_period_secs in TaskProperties."""
  if not (0 <= value <= _ONE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between %ds and one day' % (prop._name, value, 0))


def _validate_timeout(prop, value):
  """Validates timeouts in seconds in TaskProperties."""
  if not (_MIN_TIMEOUT_SECS <= value <= _ONE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between %ds and one day' %
            (prop._name, value, _MIN_TIMEOUT_SECS))


def _validate_priority(_prop, value):
  """Validates TaskRequest.priority."""
  validate_priority(value)
  return value


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


def _validate_tags(prop, value):
  """Validates and sort TaskRequest.tags."""
  if not ':' in value:
    # pylint: disable=W0212
    raise ValueError('%s must be key:value form, not %s' % (prop._name, value))


### Models.


class TaskProperties(ndb.Model):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  This model is immutable.
  """
  # Hashing algorithm used to hash TaskProperties to create its key.
  HASHING_ALGO = hashlib.sha1

  # Commands to run. It is a list of lists. Each command is run one after the
  # other. Encoded json.
  commands = datastore_utils.DeterministicJsonProperty(
      validator=_validate_command, json_type=list, required=True)

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

  The key id is this value XORed with _TASK_REQUEST_KEY_ID_MASK. The reason is
  that increasing key id values are in decreasing timestamp order.
  """
  utcnow = utils.utcnow()
  if utcnow < _BEGINING_OF_THE_WORLD:
    raise ValueError(
        'Time %s is set to before %s' % (utcnow, _BEGINING_OF_THE_WORLD))
  delta = utcnow - _BEGINING_OF_THE_WORLD
  now = int(round(delta.total_seconds() * 1000.))
  # TODO(maruel): Use real randomness.
  suffix = random.getrandbits(16)
  task_id = int((now << 20) | (suffix << 4) | 0x1)
  return ndb.Key(TaskRequest, task_id ^ _TASK_REQUEST_KEY_ID_MASK)


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
    - user
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

  * are optional.

  Returns:
    The newly created TaskRequest.
  """
  # Save ourself headaches with typos and refuses unexpected values.
  _assert_keys(_DATA_KEYS, _DATA_KEYS, data, 'request keys')
  data_properties = data['properties']
  _assert_keys(
      _EXPECTED_PROPERTIES_KEYS, _REQUIRED_PROPERTIES_KEYS, data_properties,
      'request properties keys')

  # Class TaskProperties takes care of making everything deterministic.
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
      created_ts=now,
      authenticated=auth.get_current_identity(),
      name=data['name'],
      user=data['user'] or '',
      properties=properties,
      priority=data['priority'],
      expiration_ts=expiration_ts,
      tags=data['tags'])
  _put_request(request)
  return request


def validate_priority(priority):
  """Throws ValueError if priority is not a valid value."""
  if 0 > priority or MAXIMUM_PRIORITY < priority:
    raise datastore_errors.BadValueError(
        'priority (%d) must be between 0 and %d (inclusive)' %
        (priority, MAXIMUM_PRIORITY))
