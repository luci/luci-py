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
+----------------+
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
  <see task_scheduler.py>

TaskProperties is embedded in TaskRequest. TaskProperties is still declared as a
separate entity to clearly declare the boundary for task request deduplication.
"""


import datetime
import hashlib
import json
import math
import random

from google.appengine.ext import ndb

from common import test_request_message
from components import datastore_utils
from components import utils


# Intentionally starve the canary server by using only 16 root entities. This
# will force transaction conflicts. On the production server, use 16**5 (~1
# million) root entities to reduce the number of transaction conflict.
#
# The production server must handle up to 1000 task requests per second. The
# number of root entities must be a few orders of magnitude higher. The goal is
# to almost completely get rid of transactions conflicts. This means that the
# probability of two transactions happening on the same shard must be very low.
# This relates to number of transactions per second * seconds per transaction /
# number of shard.
#
# Each task request creation incurs 1 transaction:
# - TaskRequest creation.
#
# Each task request shard incurs transactions when executed, see
# task_scheduler.py.
_SHARDING_LEVEL = 1 if utils.is_canary() else 5


# The world started in 2014.
_EPOCH = datetime.datetime(2014, 1, 1)


# One day in seconds. Add 10s to account for small jitter.
_ONE_DAY_SECS = 24*60*60 + 10


# Minimum value for timeouts.
_MIN_TIMEOUT_SECS = 30


# Maximum acceptable priority value.
# TODO(maruel): Could be lowered eventually, 2**16 is probably more sensible.
_MAXIMUM_PRIORITY = 2**23


class TaskProperties(ndb.Model):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  TODO(maruel): Determine which of the below that are necessary.
  Things not caried over from TestCase:
  - TestObject.decorate_output
  - TestCase.cleanup, we always use the same value?
  - TestCase.encoding, enforce utf-8
  - TestCase.restart_on_failure, probably needed
  - TestCase.verbose, use env var instead.
  - TestCase.working_dir, not sure. this is cheezy.
  - TestObject.hard_time_out, per command execution timeout, only global.
  - TestObject.io_time_out, I/O timeout, per command execution timeout, only
    global.

  This model is immutable.
  """
  # Hashing algorithm used to hash TaskProperties to create its key.
  HASHING_ALGO = hashlib.sha1

  # Commands to run. It is a list of lists. Each command is run one after the
  # other. Encoded json.
  commands_json = ndb.StringProperty(indexed=False)

  # List of (URLs, local file) for the bot to download. Encoded as json. Must be
  # sorted by URLs. Optional.
  data_json = ndb.StringProperty(indexed=False)

  # Filter to use to determine the required properties on the bot to run on. For
  # example, Windows or hostname. Encoded as json. Optional but highly
  # recommended.
  dimensions_json = ndb.StringProperty(indexed=False)

  # Environment variables. Encoded as json. Optional.
  env_json = ndb.StringProperty(indexed=False)

  # Number of instances to split this task into.
  sharding = ndb.IntegerProperty(indexed=False)

  # Maximum duration the bot can take to run a shard of this task.
  execution_timeout_secs = ndb.IntegerProperty(indexed=True)

  # Bot controlled timeout for new bytes from the subprocess. If a subprocess
  # doesn't output new data to stdout for .io_timeout_secs, consider the command
  # timed out. Optional.
  io_timeout_secs = ndb.IntegerProperty(indexed=False)

  def commands(self):
    return json.loads(self.commands_json)

  def data(self):
    return json.loads(self.data_json)

  def dimensions(self):
    return json.loads(self.dimensions_json)

  def env(self):
    return json.loads(self.env_json)

  def to_dict(self):
    return {
      'commands': self.commands(),
      'data': self.data(),
      'dimensions': self.dimensions(),
      'env': self.env(),
      'execution_timeout_secs': self.execution_timeout_secs,
      'io_timeout_secs': self.io_timeout_secs,
      'sharding': self.sharding,
    }

  def _pre_put_hook(self):
    """Ensures internal consistency."""
    # TODO(maruel): Stop decoding and reencoding the json data here, it's not
    # efficient. Change the infrastructure to verify this at an higher level.

    # Validate commands_json.
    if not self.commands_json:
      raise ValueError('A command is required')
    commands = self.commands()
    if (not isinstance(commands, list) or
        not all(isinstance(c, list) for c in commands)):
      raise ValueError(
          'command must be a list of commands, each a list of arguments')
    expected = utils.encode_to_json(commands)
    if self.commands_json != expected:
      raise ValueError(
          'Command encoding is not what was expected. Got %r, expected %r' %
          (self.commands_json, expected))

    # Validate data_json.
    data = self.data()
    if (not isinstance(data, list) or
        not all(isinstance(d, list) and len(d) == 2 for d in data)):
      raise ValueError('data must be a list of (url, file)')
    expected = utils.encode_to_json(sorted(data))
    if self.data_json != expected:
      raise ValueError(
          'data encoding is not what was expected. Got %r, expected %r' %
          (self.data_json, expected))

    # Validate dimensions_json.
    dimensions = self.dimensions()
    if (not isinstance(dimensions, dict) or
        not all(
          isinstance(k, unicode) and isinstance(v, unicode)
          for k, v in dimensions.iteritems())):
      raise ValueError('dimensions must be a dict of strings')
    expected = utils.encode_to_json(dimensions)
    if self.dimensions_json != expected:
      raise ValueError(
          'dimensions encoding is not what was expected. Got %r, expected %r' %
          (self.dimensions_json, expected))

    # Validate env_json.
    env = self.env()
    if (not isinstance(env, dict) or
        not all(
          isinstance(k, unicode) and isinstance(v, unicode)
          for k, v in env.iteritems())):
      raise ValueError('env must be a dict of strings')
    expected = utils.encode_to_json(env)
    if self.env_json != expected:
      raise ValueError(
          'env encoding is not what was expected. Got %r, expected %r' %
          (self.env_json, expected))

    if 0 >= self.sharding or 50 < self.sharding:
      raise ValueError('sharding must be between 1 and 50')

    if (_MIN_TIMEOUT_SECS > self.execution_timeout_secs or
        _ONE_DAY_SECS < self.execution_timeout_secs):
      raise ValueError(
          'Execution timeout (%d) must be between %ds and one day' %
              (_MIN_TIMEOUT_SECS, self.execution_timeout_secs))

    if (self.io_timeout_secs and
        (_MIN_TIMEOUT_SECS > self.io_timeout_secs or
          _ONE_DAY_SECS < self.io_timeout_secs)):
      raise ValueError(
          'I/O timeout (%d) must be between 0 or %ds and one day' %
              (_MIN_TIMEOUT_SECS, self.io_timeout_secs))


class TaskRequest(ndb.Model):
  """Contains a user request.

  The key is a increasing integer based on time since _EPOCH plus some
  randomness on the low order bits.

  Timeouts are part of this entity, not TaskProperties. It is because a task
  execution failure will not be deduped, so if time outs are tweaked, the task
  should be rerun only if not successful, independent of the previous timeout
  parameters used.

  This model is immutable.
  """
  created_ts = ndb.DateTimeProperty(indexed=False)

  # The name for this test request.
  name = ndb.StringProperty()

  # User who requested this task.
  user = ndb.StringProperty()

  # TaskProperties value as generated by TaskProperties.HASHING_ALGO. It
  # uniquely identify the TaskProperties instance for an eventual deduplication
  # by the task scheduler.
  properties_hash = datastore_utils.BytesComputedProperty(
      lambda self: self._calculate_hash())

  # The actual properties are embedded in this model.
  properties = ndb.LocalStructuredProperty(TaskProperties, compressed=True)

  # Priority of the task to be run. A lower number is higher priority, thus will
  # preempt requests with lower priority (higher numbers).
  priority = ndb.IntegerProperty(indexed=False)

  # If the task request is not scheduled by this moment, it will be aborted by a
  # cron job.
  expiration_ts = ndb.DateTimeProperty(indexed=False)

  def to_dict(self):
    """Converts properties_hash to hex so it is json serializable."""
    out = super(TaskRequest, self).to_dict()
    out['properties_hash'] = out['properties_hash'].encode('hex')
    out['properties'] = self.properties.to_dict()
    return out

  def _calculate_hash(self):
    """Calculates the hash for the embedded TaskProperties.

    Used to calculate the value of self.properties_hash.
    """
    return self.properties.HASHING_ALGO(
        utils.encode_to_json(self.properties)).digest()

  def _pre_put_hook(self):
    """Ensures internal consistency."""
    validate_priority(self.priority)
    now = utcnow()
    offset = int(round((self.expiration_ts - now).total_seconds()))
    if _MIN_TIMEOUT_SECS > offset or _ONE_DAY_SECS < offset:
      raise ValueError(
          'Expiration (%d) must be between %ds and one day from now' %
          (offset, _MIN_TIMEOUT_SECS))
    self.properties._pre_put_hook()


def _new_task_request_key():
  """Returns a valid ndb.Key for this entity.

  It is a 63 bit integer, with 47 bits for the representation of epoch at 1ms
  resolution and 16 bits of pure randomness. Keep the highest order bit set to 0
  to keep the value positive.

  So every call should return a different value with a 2**-15 probability and
  gets us going for a thousand years.
  """
  now = int(round((utcnow() - _EPOCH).total_seconds() * 1000))
  assert 1 <= now < 2**47, now
  suffix = random.getrandbits(16)
  assert 0 <= suffix <= 0xFFFF
  value = (now << 16) | suffix
  return task_request_key(value)


def _put_task_request(request):
  """Puts the new TaskRequest in the DB.

  Returns:
    ndb.Key of the new entity. Returns None if failed, which should be surfaced
    to the user.
  """
  assert not request.key
  request.key = _new_task_request_key()
  return datastore_utils.insert(request, _new_task_request_key)


### Public API.


def utcnow():
  """To be mocked in tests."""
  return datetime.datetime.utcnow()


def validate_priority(priority):
  if 0 > priority or _MAXIMUM_PRIORITY <= priority:
    raise ValueError(
        'Priority (%d) must be between 0 and 2^%d' %
        (priority, int(round(math.log(_MAXIMUM_PRIORITY, 2)))))


def task_request_key(task_id):
  """Returns the ndb.Key for a TaskRequest id."""
  # Technically could use shard_key() since the bottom 16 bits is random.
  parent = datastore_utils.hashed_shard_key(
      str(task_id), _SHARDING_LEVEL, 'TaskRequestShard')
  return ndb.Key(TaskRequest, task_id, parent=parent)


def new_request(data):
  """Constructs a TaskRequest out of a yet-to-be-specified API.

  Argument:
  - data: dict with:
    - name
    - user
    - commands
    - data
    - dimensions
    - env
    - shards
    - priority
    - scheduling_expiration
    - execution_timeout
    - io_timeout

  Returns:
    The newly created TaskRequest.
  """

  # The following is very important to be deterministic. Keys must be sorted,
  # encoding must be deterministic.
  commands_json = utils.encode_to_json(data['commands'])
  data_json = utils.encode_to_json(sorted(data['data']))
  dimensions_json = utils.encode_to_json(data['dimensions'])
  env_json = utils.encode_to_json(data['env'])
  properties = TaskProperties(
      commands_json=commands_json,
      data_json=data_json,
      dimensions_json=dimensions_json,
      env_json=env_json,
      sharding=data['shards'],
      execution_timeout_secs=data['execution_timeout'],
      io_timeout_secs=data['io_timeout'])

  now = utcnow()
  deadline = now + datetime.timedelta(
      seconds=data['scheduling_expiration'])
  request = TaskRequest(
      created_ts=now,
      name=data['name'],
      user=data['user'],
      properties=properties,
      priority=data['priority'],
      expiration_ts=deadline)
  if _put_task_request(request):
    return request
  return None


def new_request_old_api(data):
  """Constructs a TaskProperties out of a test_request_message.TestCase.

  This code is kept for compatibility with the previous API. See new_request()
  for more details.
  """
  test_case = test_request_message.TestCase.FromJSON(data)
  # TODO(maruel): Add missing mapping and delete obsolete ones.
  assert len(test_case.configurations) == 1, test_case.configurations
  config = test_case.configurations[0]

  # Ignore all the settings that are deprecated.
  new_format = {
    'name': test_case.test_case_name,
    'user': test_case.requestor,
    'commands': [c.action for c in test_case.tests],
    'data': test_case.data,
    'dimensions': config.dimensions,
    'env': test_case.env_vars,
    'shards': config.num_instances,
    'priority': config.priority,
    'scheduling_expiration': config.deadline_to_run,
    'execution_timeout': int(round(test_case.tests[0].hard_time_out)),
    'io_timeout': int(round(test_case.tests[0].io_time_out)),
  }
  return new_request(new_format)
