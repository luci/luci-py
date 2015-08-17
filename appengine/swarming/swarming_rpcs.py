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


class TaskId(messages.Message):
  """Provides the task ID for task-related requests."""
  task_id = messages.StringField(1)


class TaskProperties(messages.Message):
  """Important metadata about a particular task."""
  command = messages.StringField(1, repeated=True)
  data = messages.MessageField(StringPair, 2, repeated=True)
  dimensions = messages.MessageField(StringPair, 3, repeated=True)
  env = messages.MessageField(StringPair, 4, repeated=True)
  execution_timeout_secs = messages.IntegerField(5)
  extra_args = messages.StringField(6, repeated=True)
  grace_period_secs = messages.IntegerField(7)
  idempotent = messages.BooleanField(8)
  # inputs_ref
  io_timeout_secs = messages.IntegerField(10)


class TaskRequest(messages.Message):
  """Representation of the TaskRequest ndb model."""
  authenticated = messages.StringField(1)
  created_ts = message_types.DateTimeField(2)
  expiration_secs = messages.IntegerField(3)
  name = messages.StringField(4, required=True)
  parent_task_id = messages.StringField(5)
  priority = messages.IntegerField(6)
  properties = messages.MessageField(TaskProperties, 7, required=True)
  tags = messages.StringField(8, repeated=True)
  user = messages.StringField(9)


class TasksRequest(messages.Message):
  """Request to get some subset of available tasks."""
  batch_size = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)
  end = message_types.DateTimeField(3)
  start = message_types.DateTimeField(4)
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
  children_task_ids = messages.StringField(4, repeated=True)
  completed_ts = message_types.DateTimeField(5)
  cost_saved_usd = messages.FloatField(6)
  costs_usd = messages.FloatField(7, repeated=True)
  created_ts = message_types.DateTimeField(8)
  deduped_from = messages.StringField(9)
  duration = messages.FloatField(10)
  exit_code = messages.IntegerField(11)
  failure = messages.BooleanField(12)
  internal_failure = messages.BooleanField(13)
  modified_ts = message_types.DateTimeField(14)
  name = messages.StringField(15)
  # outputs_ref
  started_ts = message_types.DateTimeField(17)
  state = messages.EnumField(StateField, 18)
  task_id = messages.StringField(19)
  try_number = messages.IntegerField(20)
  user = messages.StringField(21)


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


class BotId(messages.Message):
  """Provides the bot ID for Bot* requests."""
  bot_id = messages.StringField(1, required=True)


class BotsRequest(messages.Message):
  """Information needed to request bot data."""
  batch_size = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)


class BotTasksRequest(messages.Message):
  """Request to get data about a bot's tasks."""
  batch_size = messages.IntegerField(1, default=200)
  cursor = messages.StringField(3)
  end = message_types.DateTimeField(2)
  start = message_types.DateTimeField(4)
  bot_id = messages.StringField(5, required=True)


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
  items = messages.MessageField(BotInfo, 3, repeated=True)
  now = message_types.DateTimeField(4)
  death_timeout = messages.IntegerField(2)


class DeletedResponse(messages.Message):
  """Indicates whether a task was deleted."""
  deleted = messages.BooleanField(1)


class BotTasks(messages.Message):
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResult, 2, repeated=True)
  now = message_types.DateTimeField(3)
