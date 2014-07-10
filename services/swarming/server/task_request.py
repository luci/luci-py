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
      +------Root------+
      |TaskRequestShard|
      +----------------+
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
    <See task_to_run.py and task_result.py>

TaskProperties is embedded in TaskRequest. TaskProperties is still declared as a
separate entity to clearly declare the boundary for task request deduplication.
"""


import datetime
import hashlib
import random

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import task_common


# One day in seconds. Add 10s to account for small jitter.
_ONE_DAY_SECS = 24*60*60 + 10


# Minimum value for timeouts.
_MIN_TIMEOUT_SECS = 30


# Parameters for make_request().
# The content of the 'data' parameter. This relates to the context of the
# request, e.g. who wants to run a task.
_DATA_KEYS = frozenset(
    ['name', 'priority', 'properties', 'scheduling_expiration_secs', 'user'])
# The content of 'properties' inside the 'data' parameter. This relates to the
# task itself, e.g. what to run.
_PROPERTIES_KEYS = frozenset(
    ['commands', 'data', 'dimensions', 'env', 'execution_timeout_secs',
     'io_timeout_secs'])


# The production server must handle up to 1000 task requests per second. The
# number of root entities must be a few orders of magnitude higher. The goal is
# to almost completely get rid of transactions conflicts. This means that the
# probability of two transactions happening on the same shard must be very low.
# This relates to number of transactions per second * seconds per transaction /
# number of shard.
#
# Intentionally starve the canary server by using only 16³=4096 root entities.
# This will cause mild transaction conflicts during load tests. On the
# production server, use 16**6 (~16 million) root entities to reduce the number
# of transaction conflict.
_SHARDING_LEVEL = 3 if utils.is_canary() else 6


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


def _validate_timeout(prop, value):
  """Validates timeouts in seconds in TaskProperties."""
  if _MIN_TIMEOUT_SECS > value or _ONE_DAY_SECS < value:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between %ds and one day' %
            (prop._name, value, _MIN_TIMEOUT_SECS))


def _validate_priority(_prop, value):
  """Validates TaskRequest.priority."""
  task_common.validate_priority(value)
  return value


def _validate_expiration(prop, value):
  now = utils.utcnow()
  offset = int(round((value - now).total_seconds()))
  if _MIN_TIMEOUT_SECS > offset or _ONE_DAY_SECS < offset:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%s, %ds from now) must effectively be between %ds and one day '
        'from now (%s)' %
        (prop._name, value, offset, _MIN_TIMEOUT_SECS, now))


def _calculate_hash(request):
  """Calculates the hash for the embedded TaskProperties.

  Used to calculate the value of self.properties_hash.
  """
  return request.properties.HASHING_ALGO(
      utils.encode_to_json(request.properties)).digest()


### Models.


class TaskProperties(ndb.Model):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  TODO(maruel): Determine which of the below that are necessary.
  Things not caried over from TestCase:
  - TestObject.decorate_output
  - TestCase.verbose, use env var instead.
  - TestObject.hard_time_out, per command execution timeout, only global.
  - TestObject.io_time_out, I/O timeout, per command execution timeout, only
    global.

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

  # Maximum duration the bot can take to run this task.
  execution_timeout_secs = ndb.IntegerProperty(
      indexed=True, validator=_validate_timeout, required=True)

  # Bot controlled timeout for new bytes from the subprocess. If a subprocess
  # doesn't output new data to stdout for .io_timeout_secs, consider the command
  # timed out. Optional.
  io_timeout_secs = ndb.IntegerProperty(
      indexed=True, validator=_validate_timeout)


class TaskRequest(ndb.Model):
  """Contains a user request.

  The key is a increasing integer based on time since utils.EPOCH plus some
  randomness on 8 low order bits then lower 8 bits set to 0. See
  _new_request_key() for the complete gory details.

  This model is immutable.
  """
  # Time this request was registered. It is set manually instead of using
  # auto_now_add=True so that expiration_ts can be set very precisely relative
  # to this property.
  created_ts = ndb.DateTimeProperty(required=True)

  # The name for this test request.
  name = ndb.StringProperty(required=True)

  # User who requested this task.
  user = ndb.StringProperty(required=True)

  # TaskProperties value as generated by TaskProperties.HASHING_ALGO. It
  # uniquely identify the TaskProperties instance for an eventual deduplication
  # by the task scheduler.
  properties_hash = datastore_utils.BytesComputedProperty(_calculate_hash)

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

  @property
  def scheduling_expiration_secs(self):
    """Reconstructs this value from expiration_ts and created_ts."""
    return (self.expiration_ts - self.created_ts).total_seconds()

  def to_dict(self):
    """Converts properties_hash to hex so it is json serializable."""
    out = super(TaskRequest, self).to_dict()
    out['properties_hash'] = out['properties_hash'].encode('hex')
    return out


def _new_request_key():
  """Returns a valid ndb.Key for this entity.

  Key id is a 64 bit integer:
  - 1 bit highest order bit to detect overflow.
  - 47 bits is time since epoch at 1ms resolution is
    2**47 / 365 / 24 / 60 / 60 / 1000 = 4462 years.
  - 8 bits of randomness reduces to 2**-7 the probability of collision on exact
    same timestamp at 1ms resolution, so a maximum theoretical rate of 256000
    requests per second but an effective rate likely in the range of ~2560
    requests per second without too much transaction conflicts.
  - The last 8 bits are unused and set to 0.
  """
  now = task_common.milliseconds_since_epoch(None)
  assert 1 <= now < 2**47, now
  suffix = random.getrandbits(8)
  assert 0 <= suffix <= 0xFF
  value = (now << 16) | (suffix << 8)
  return id_to_request_key(value)


def _put_request(request):
  """Puts the new TaskRequest in the DB.

  Returns:
    ndb.Key of the new entity. Returns None if failed, which should be surfaced
    to the user.
  """
  assert not request.key
  request.key = _new_request_key()
  return datastore_utils.insert(request, _new_request_key)


### Public API.


def id_to_request_key(task_id):
  """Returns the ndb.Key for a TaskRequest id."""
  if task_id & 0xFF:
    raise ValueError('Unexpected low order byte is set')
  if not task_id:
    raise ValueError('Invalid null key')
  parent = datastore_utils.hashed_shard_key(
      str(task_id), _SHARDING_LEVEL, 'TaskRequestShard')
  return ndb.Key(TaskRequest, task_id, parent=parent)


def validate_request_key(request_key):
  if request_key.kind() != 'TaskRequest':
    raise ValueError('Expected key to TaskRequest, got %s' % request_key.kind())
  task_id = request_key.integer_id()
  if not task_id:
    raise ValueError('Invalid null TaskRequest key')
  if task_id & 0xFF:
    raise ValueError('Unexpected low order byte is set')

  # Check the shard.
  request_shard_key = request_key.parent()
  if not request_shard_key:
    raise ValueError('Expected parent key for TaskRequest, got nothing')
  if request_shard_key.kind() != 'TaskRequestShard':
    raise ValueError(
        'Expected key to TaskRequestShard, got %s' % request_shard_key.kind())
  root_entity_shard_id = request_shard_key.string_id()
  if not root_entity_shard_id or len(root_entity_shard_id) != _SHARDING_LEVEL:
    raise ValueError(
        'Expected root entity key (used for sharding) to be of length %d but '
        'length was only %d (key value %r)' % (
            _SHARDING_LEVEL,
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
      - io_timeout_secs
    - priority
    - scheduling_expiration_secs

  Returns:
    The newly created TaskRequest.
  """
  # Save ourself headaches with typos and refuses unexpected values.
  set_data = frozenset(data)
  if set_data != _DATA_KEYS:
    raise ValueError(
        'Unexpected parameters for make_request(): %s\nExpected: %s' % (
            ', '.join(sorted(_DATA_KEYS.symmetric_difference(set_data))),
            ', '.join(sorted(_DATA_KEYS))))
  data_properties = data['properties']
  set_data_properties = frozenset(data_properties)
  if set_data_properties != _PROPERTIES_KEYS:
    raise ValueError(
        'Unexpected properties for make_request(): %s\nExpected: %s' % (
            ', '.join(sorted(
                _PROPERTIES_KEYS.symmetric_difference(set_data_properties))),
            ', '.join(sorted(_PROPERTIES_KEYS))))

  # Class TaskProperties takes care of making everything deterministic.
  properties = TaskProperties(
      commands=data_properties['commands'],
      data=data_properties['data'],
      dimensions=data_properties['dimensions'],
      env=data_properties['env'],
      execution_timeout_secs=data_properties['execution_timeout_secs'],
      io_timeout_secs=data_properties['io_timeout_secs'])

  now = utils.utcnow()
  expiration_ts = now + datetime.timedelta(
      seconds=data['scheduling_expiration_secs'])
  request = TaskRequest(
      created_ts=now,
      name=data['name'],
      user=data['user'],
      properties=properties,
      priority=data['priority'],
      expiration_ts=expiration_ts)
  _put_request(request)
  return request
