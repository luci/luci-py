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


class TaskProperty(messages.Message):
  """Important metadata about a particular task."""
  command = messages.StringField(1, repeated=True)
  data = messages.MessageField(StringPair, 2, repeated=True)
  dimensions = messages.MessageField(StringPair, 3, repeated=True)
  env = messages.MessageField(StringPair, 4, repeated=True)
  execution_timeout_secs = messages.IntegerField(5)
  grace_period_secs = messages.IntegerField(6)
  idempotent = messages.BooleanField(7, default=False)
  io_timeout_secs = messages.IntegerField(8)


class TaskRequest(messages.Message):
  """Representation of the TaskRequest ndb model."""
  authenticated = messages.StringField(1)
  created_ts = message_types.DateTimeField(2)
  expiration_secs = messages.IntegerField(3, required=True)
  name = messages.StringField(4, required=True)
  parent_task_id = messages.StringField(5)
  priority = messages.IntegerField(6)
  properties = messages.MessageField(TaskProperty, 7, required=True)
  tags = messages.StringField(8, repeated=True)
  user = messages.StringField(9)


class TasksRequest(messages.Message):
  """Request to get some subset of available tasks."""
  cursor = messages.StringField(1)  # ?
  limit = messages.IntegerField(2, default=100)
  name = messages.StringField(3)
  sort = messages.StringField(4, default='created_ts')
  state = messages.EnumField(TaskState, 5, default='ALL')
  tag = messages.StringField(6, repeated=True)


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


class TaskResultSummary(messages.Message):
  """Representation of the TaskResultSummary ndb model."""
  abandoned_ts = message_types.DateTimeField(1)
  bot_id = messages.StringField(2)
  children_task_ids = messages.StringField(3, repeated=True)
  completed_ts = message_types.DateTimeField(4)
  cost_saved_usd = messages.FloatField(5)
  costs_usd = messages.FloatField(6, repeated=True)
  created_ts = message_types.DateTimeField(7)
  deduped_from = messages.StringField(8)
  duration = messages.FloatField(9)
  exit_code = messages.IntegerField(10)
  failure = messages.BooleanField(11, default=False)
  id = messages.StringField(12)
  internal_failure = messages.BooleanField(13, default=False)
  modified_ts = message_types.DateTimeField(14)
  name = messages.StringField(15)
  started_ts = message_types.DateTimeField(16)
  state = messages.EnumField(StateField, 17, default='PENDING')
  try_number = messages.IntegerField(18)
  user = messages.StringField(19)


class TaskList(messages.Message):
  """Wraps a list of TaskResultSummary, along with request information."""
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResultSummary, 2, repeated=True)
  limit = messages.IntegerField(3)
  sort = messages.StringField(4)
  state = messages.StringField(5)


class TaskRequestMetadata(messages.Message):
  """Provides the ID of the requested TaskRequest."""
  request = messages.MessageField(TaskRequest, 1)
  task_id = messages.StringField(2)


### Bots


### Bot-Related Requests


class BotId(messages.Message):
  """Provides the bot ID for Bot* requests."""
  bot_id = messages.StringField(1)


class BotsRequest(messages.Message):
  """Information needed to request bot data."""
  cursor = messages.StringField(1)
  limit = messages.IntegerField(2, default=1000)


class BotTasksRequest(messages.Message):
  """Request to get data about a bot's tasks."""
  bot_id = messages.StringField(1)
  cursor = messages.StringField(2)
  limit = messages.IntegerField(3)


### Bot-Related Responses


class BotInfo(messages.Message):
  """Representation of the BotInfo ndb model."""
  dimensions = messages.MessageField(StringListPair, 1, repeated=True)
  external_ip = messages.StringField(2)
  first_seen_ts = message_types.DateTimeField(3)
  id = messages.StringField(4)
  is_busy = messages.BooleanField(5)
  last_seen_ts = message_types.DateTimeField(6)
  quarantined = messages.BooleanField(7)
  task_id = messages.StringField(8)
  task_name = messages.StringField(9)
  version = messages.StringField(10)


class BotList(messages.Message):
  """Wraps a list of BotInfo, along with information about the request."""
  cursor = messages.StringField(1)
  death_timeout = messages.IntegerField(2)
  items = messages.MessageField(BotInfo, 3, repeated=True)
  limit = messages.IntegerField(4)
  now = message_types.DateTimeField(5)


class DeletedResponse(messages.Message):
  """Indicates whether a task was deleted."""
  deleted = messages.BooleanField(1)


class TaskRunResult(messages.Message):
  """Representation of the TaskRunResult ndb model."""
  abandoned_ts = message_types.DateTimeField(1)
  bot_id = messages.StringField(2)
  children_task_ids = messages.StringField(3, repeated=True)
  completed_ts = message_types.DateTimeField(4)
  cost_saved_usd = messages.FloatField(5, default=None)
  cost_usd = messages.FloatField(6)
  created_ts = message_types.DateTimeField(7)
  deduped_from = messages.StringField(8, default=None)
  duration = messages.FloatField(9)
  exit_code = messages.IntegerField(10)
  failure = messages.BooleanField(11, default=False)
  id = messages.StringField(12)
  internal_failure = messages.BooleanField(13, default=False)
  modified_ts = message_types.DateTimeField(14)
  started_ts = message_types.DateTimeField(15)
  state = messages.EnumField(StateField, 16, default='PENDING')
  try_number = messages.IntegerField(17)


class BotTask(messages.Message):
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskRunResult, 2, repeated=True)
  limit = messages.IntegerField(3)
  now = message_types.DateTimeField(4)
