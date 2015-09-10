# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines ProtoRPC types for the Swarming Server handlers."""

from protorpc import message_types
from protorpc import messages


### Enums


class TaskState(messages.Enum):
  (
    PENDING, RUNNING, PENDING_RUNNING, COMPLETED, COMPLETED_SUCCESS,
    COMPLETED_FAILURE, EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, ALL) = range(11)


class StateField(messages.Enum):
  RUNNING = 0x10    # 16
  PENDING = 0x20    # 32
  EXPIRED = 0x30    # 48
  TIMED_OUT = 0x40  # 64
  BOT_DIED = 0x50   # 80
  CANCELED = 0x60   # 96
  COMPLETED = 0x70  # 112


### Pretend Associative Array


class StringPair(messages.Message):
  """Represents a mapping of string to string."""
  key = messages.StringField(1)
  value = messages.StringField(2)


class StringListPair(messages.Message):
  """Represents a mapping of string to list of strings."""
  key = messages.StringField(1)
  value = messages.StringField(2, repeated=True)


### Task-Related Requests


class FilesRef(messages.Message):
  """Defines a data tree reference, normally a reference to a .isolated file."""
  # The hash of an isolated archive.
  isolated = messages.StringField(1)
  # The hostname of the isolated server to use.
  isolatedserver = messages.StringField(2)
  # Namespace on the isolate server.
  namespace = messages.StringField(3)


class TaskProperties(messages.Message):
  """Important metadata about a particular task."""
  command = messages.StringField(1, repeated=True)
  dimensions = messages.MessageField(StringPair, 2, repeated=True)
  env = messages.MessageField(StringPair, 3, repeated=True)
  execution_timeout_secs = messages.IntegerField(4)
  extra_args = messages.StringField(5, repeated=True)
  grace_period_secs = messages.IntegerField(6)
  idempotent = messages.BooleanField(7)
  inputs_ref = messages.MessageField(FilesRef, 8)
  io_timeout_secs = messages.IntegerField(9)


class NewTaskRequest(messages.Message):
  """Description of a new task request as described by the client."""
  expiration_secs = messages.IntegerField(1)
  name = messages.StringField(2)
  parent_task_id = messages.StringField(3)
  priority = messages.IntegerField(4)
  properties = messages.MessageField(TaskProperties, 5)
  tags = messages.StringField(6, repeated=True)
  user = messages.StringField(7)


class TaskRequest(messages.Message):
  """Description of a task request as registered by the server."""
  expiration_secs = messages.IntegerField(1)
  name = messages.StringField(2)
  parent_task_id = messages.StringField(3)
  priority = messages.IntegerField(4)
  properties = messages.MessageField(TaskProperties, 5)
  tags = messages.StringField(6, repeated=True)
  user = messages.StringField(7)
  authenticated = messages.StringField(8)
  created_ts = message_types.DateTimeField(9)


class TasksRequest(messages.Message):
  """Request to get some subset of available tasks."""
  limit = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)
  # These should be DateTimeField but endpoints + protorpc have trouble encoding
  # this message in a GET request, this is due to DateTimeField's special
  # encoding in protorpc-1.0/protorpc/message_types.py that is bypassed when
  # using endpoints-1.0/endpoints/protojson.py to add GET query parameter
  # support.
  end = messages.FloatField(3)
  start = messages.FloatField(4)
  state = messages.EnumField(TaskState, 5, default='ALL')
  tags = messages.StringField(6, repeated=True)


### Task-Related Responses


class CancelResponse(messages.Message):
  """Result of a request to cancel a task."""
  ok = messages.BooleanField(1)
  was_running = messages.BooleanField(2)


class ServerDetails(messages.Message):
  """Reports the server version."""
  server_version = messages.StringField(1)


class TaskOutput(messages.Message):
  """A task's output as a string."""
  output = messages.StringField(1)


class TaskResult(messages.Message):
  """Representation of the TaskResultSummary or TaskRunResult ndb model."""
  abandoned_ts = message_types.DateTimeField(1)
  bot_dimensions = messages.MessageField(StringListPair, 2, repeated=True)
  bot_id = messages.StringField(3)
  bot_version = messages.StringField(4)
  children_task_ids = messages.StringField(5, repeated=True)
  completed_ts = message_types.DateTimeField(6)
  cost_saved_usd = messages.FloatField(7)
  created_ts = message_types.DateTimeField(8)
  deduped_from = messages.StringField(9)
  duration = messages.FloatField(10)
  exit_code = messages.IntegerField(11)
  failure = messages.BooleanField(12)
  internal_failure = messages.BooleanField(13)
  modified_ts = message_types.DateTimeField(14)
  outputs_ref = messages.MessageField(FilesRef, 15)
  properties_hash = messages.StringField(16)
  server_versions = messages.StringField(17, repeated=True)
  started_ts = message_types.DateTimeField(18)
  state = messages.EnumField(StateField, 19)
  task_id = messages.StringField(20)
  try_number = messages.IntegerField(21)

  # Can be multiple values only in TaskResultSummary.
  costs_usd = messages.FloatField(22, repeated=True)
  # Only in TaskResultSummary.
  name = messages.StringField(23)
  # Only in TaskResultSummary.
  tags = messages.StringField(24, repeated=True)
  # Only in TaskResultSummary.
  user = messages.StringField(25)


class TaskList(messages.Message):
  """Wraps a list of TaskResult, along with request information."""
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResult, 2, repeated=True)


class TaskRequestMetadata(messages.Message):
  """Provides the ID of the requested TaskRequest."""
  request = messages.MessageField(TaskRequest, 1)
  task_id = messages.StringField(2)


### Bots


### Bot-Related Requests


class BotsRequest(messages.Message):
  """Information needed to request bot data."""
  limit = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)


class BotTasksRequest(messages.Message):
  """Request to get data about a bot's tasks."""
  limit = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)
  # These should be DateTimeField but endpoints + protorpc have trouble encoding
  # this message in a GET request, this is due to DateTimeField's special
  # encoding in protorpc-1.0/protorpc/message_types.py that is bypassed when
  # using endpoints-1.0/endpoints/protojson.py to add GET query parameter
  # support.
  end = messages.FloatField(3)
  start = messages.FloatField(4)


### Bot-Related Responses


class BotInfo(messages.Message):
  """Representation of the BotInfo ndb model."""
  bot_id = messages.StringField(1)
  dimensions = messages.MessageField(StringListPair, 2, repeated=True)
  external_ip = messages.StringField(3)
  first_seen_ts = message_types.DateTimeField(4)
  is_dead = messages.BooleanField(5)
  last_seen_ts = message_types.DateTimeField(6)
  quarantined = messages.BooleanField(7)
  task_id = messages.StringField(8)
  task_name = messages.StringField(9)
  version = messages.StringField(10)


class BotList(messages.Message):
  """Wraps a list of BotInfo, along with information about the request."""
  cursor = messages.StringField(1)
  items = messages.MessageField(BotInfo, 2, repeated=True)
  now = message_types.DateTimeField(3)
  death_timeout = messages.IntegerField(4)


class DeletedResponse(messages.Message):
  """Indicates whether a task was deleted."""
  deleted = messages.BooleanField(1)


class BotTasks(messages.Message):
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResult, 2, repeated=True)
  now = message_types.DateTimeField(3)
