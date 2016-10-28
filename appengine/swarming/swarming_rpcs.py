# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines ProtoRPC types for the Swarming Server handlers."""

from protorpc import message_types
from protorpc import messages


### Enums


class TaskState(messages.Enum):
  (
    PENDING, RUNNING, PENDING_RUNNING, COMPLETED, COMPLETED_SUCCESS,
    COMPLETED_FAILURE, EXPIRED, TIMED_OUT, BOT_DIED, CANCELED, ALL,
    DEDUPED) = range(12)


class StateField(messages.Enum):
  RUNNING = 0x10    # 16
  PENDING = 0x20    # 32
  EXPIRED = 0x30    # 48
  TIMED_OUT = 0x40  # 64
  BOT_DIED = 0x50   # 80
  CANCELED = 0x60   # 96
  COMPLETED = 0x70  # 112


class TaskSort(messages.Enum):
  """Flag to sort returned tasks. The natural sort is CREATED_TS."""
  CREATED_TS, MODIFIED_TS, COMPLETED_TS, ABANDONED_TS = range(4)


### Pretend Associative Array


class StringPair(messages.Message):
  """Represents a mapping of string to string."""
  key = messages.StringField(1)
  value = messages.StringField(2)


class StringListPair(messages.Message):
  """Represents a mapping of string to list of strings."""
  key = messages.StringField(1)
  value = messages.StringField(2, repeated=True)


class ThreeStateBool(messages.Enum):
  FALSE = 1
  TRUE = 2
  NONE = 3


def to_bool(three_state):
  if three_state in (None, True, False):
    return three_state
  if three_state == ThreeStateBool.FALSE:
    return False
  if three_state == ThreeStateBool.TRUE:
    return True


### Server related.


class ServerDetails(messages.Message):
  """Reports details about the server."""
  server_version = messages.StringField(1)
  bot_version = messages.StringField(2)
  machine_provider_template = messages.StringField(3)
  display_server_url_template = messages.StringField(4)


class BootstrapToken(messages.Message):
  """Returns a token to bootstrap a new bot."""
  bootstrap_token = messages.StringField(1)


class ClientPermissions(messages.Message):
  """Reports the client's permissions."""
  delete_bot = messages.BooleanField(1)
  terminate_bot = messages.BooleanField(2)
  get_configs = messages.BooleanField(3)
  put_configs = messages.BooleanField(4)
  cancel_task = messages.BooleanField(5)
  get_bootstrap_token = messages.BooleanField(6)


class FileContentRequest(messages.Message):
  """Content of a file."""
  content = messages.StringField(1)


class FileContent(messages.Message):
  """Content of a file."""
  content = messages.StringField(1)
  version = messages.IntegerField(2)
  who = messages.StringField(3)
  when = message_types.DateTimeField(4)


### Task-Related Requests


class FilesRef(messages.Message):
  """Defines a data tree reference, normally a reference to a .isolated file."""
  # The hash of an isolated archive.
  isolated = messages.StringField(1)
  # The hostname of the isolated server to use.
  isolatedserver = messages.StringField(2)
  # Namespace on the isolate server.
  namespace = messages.StringField(3)


class CipdPackage(messages.Message):
  """A CIPD package to install in $CIPD_PATH and $PATH before task execution."""
  # A template of a full CIPD package name, e.g.
  # "infra/tools/authutil/${platform}"
  # See also cipd.ALL_PARAMS.
  package_name = messages.StringField(1)
  # Valid package version for all packages matched by package name.
  version = messages.StringField(2)
  # Path to dir, relative to the root dir, where to install the package.
  # If empty, the package will be installed a the root of the mapped directory.
  # If file names in the package and in the isolate clash, it will cause a
  # failure.
  path = messages.StringField(3)


class CipdInput(messages.Message):
  """Defines CIPD packages to install in $CIPD_PATH.

  A command may use $CIPD_PATH in its arguments. It will be expanded to the path
  of the CIPD site root.
  """
  # URL of the CIPD server. Must start with "https://" or "http://".
  # This field or its subfields are optional if default cipd client is defined
  # in the server config.
  server = messages.StringField(1)

  # CIPD package of CIPD client to use.
  # client_package.version is required.
  # This field is optional is default value is defined in the server config.
  # client_package.path must be empty.
  client_package = messages.MessageField(CipdPackage, 2)

  # List of CIPD packages to install in $CIPD_PATH prior task execution.
  packages = messages.MessageField(CipdPackage, 3, repeated=True)


class CipdPins(messages.Message):
  """Defines pinned CIPD packages that were installed during the task."""

  # The pinned package + version of the CIPD client that was actually used.
  client_package = messages.MessageField(CipdPackage, 1)

  # List of CIPD packages that were installed in the task with fully resolved
  # package names and versions.
  packages = messages.MessageField(CipdPackage, 2, repeated=True)


class CacheEntry(messages.Message):
  """Describes a named cache that should be present on the bot.

  A CacheEntry in a task specified that the task prefers the cache to be present
  on the bot. A symlink to the cache directory is created at <run_dir>/|path|.
  If cache is not present on the machine, the directory is empty.
  If the tasks makes any changes to the contents of the cache directory, they
  are persisted on the machine. If another task runs on the same machine and
  requests the same named cache, even if mapped to a different path, it will see
  the changes.
  """

  # Unique name of the cache. Required. Length is limited to 4096.
  name = messages.StringField(1)
  # Relative path to the directory that will be linked to the named cache.
  # Required.
  # A path cannot be shared among multiple caches or CIPD installations.
  # A task will fail if a file/dir with the same name already exists.
  path = messages.StringField(2)


class TaskProperties(messages.Message):
  """Important metadata about a particular task."""
  caches = messages.MessageField(CacheEntry, 11, repeated=True)
  cipd_input = messages.MessageField(CipdInput, 10)
  command = messages.StringField(1, repeated=True)
  dimensions = messages.MessageField(StringPair, 2, repeated=True)
  env = messages.MessageField(StringPair, 3, repeated=True)
  execution_timeout_secs = messages.IntegerField(4)
  extra_args = messages.StringField(5, repeated=True)
  grace_period_secs = messages.IntegerField(6)
  idempotent = messages.BooleanField(7)
  inputs_ref = messages.MessageField(FilesRef, 8)
  io_timeout_secs = messages.IntegerField(9)
  outputs = messages.StringField(12, repeated=True)
  secret_bytes = messages.BytesField(13)  # only for rpc->ndb


class NewTaskRequest(messages.Message):
  """Description of a new task request as described by the client."""
  expiration_secs = messages.IntegerField(1)
  name = messages.StringField(2)
  parent_task_id = messages.StringField(3)
  priority = messages.IntegerField(4)
  properties = messages.MessageField(TaskProperties, 5)
  tags = messages.StringField(6, repeated=True)

  # Arbitrary user name associated with a task for UI and tags. Not validated.
  user = messages.StringField(7)

  # Defines what OAuth2 credentials the task uses when calling other services.
  #
  # Possible values are:
  #   - 'none': do not use task service accounts at all, this is default.
  #   - 'bot': use bot's own account, works only if bots authenticate with
  #       OAuth2.
  #   - <token>: a properly signed and scoped delegation token that asserts that
  #       the caller is allowed to use the specific service account (encoded in
  #       the token) on Swarming. This is not implemented yet.
  #
  # Note that the service account name is specified outside of task properties,
  # and thus it is possible to have two tasks with different service accounts,
  # but identical properties hash (so one can be deduped). If this is unsuitable
  # use 'idempotent=False' or include a service account name in properties
  # separately.
  service_account_token = messages.StringField(8)

  # Full topic name to post too, e.g. "projects/<id>/topics/<id>".
  pubsub_topic = messages.StringField(9)
  # Secret string to put into "auth_token" attribute of PubSub message.
  pubsub_auth_token = messages.StringField(10)
  # Will be but into "userdata" fields of PubSub message.
  pubsub_userdata = messages.StringField(11)


class TaskRequest(messages.Message):
  """Description of a task request as registered by the server."""
  expiration_secs = messages.IntegerField(1)
  name = messages.StringField(2)
  parent_task_id = messages.StringField(3)
  priority = messages.IntegerField(4)
  properties = messages.MessageField(TaskProperties, 5)
  tags = messages.StringField(6, repeated=True)
  created_ts = message_types.DateTimeField(7)

  # Arbitrary user name associated with a task for UI and tags. Not validated.
  user = messages.StringField(8)
  # User name of whoever posted this task, extracted from the credentials.
  authenticated = messages.StringField(9)
  # Indicates what OAuth2 credentials the task uses when calling other services.
  service_account = messages.StringField(10)

  pubsub_topic = messages.StringField(11)
  pubsub_userdata = messages.StringField(12)


class TasksRequest(messages.Message):
  """Request to get some subset of available tasks.

  This message can be used both to fetch the requests or the results.
  """
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
  sort = messages.EnumField(TaskSort, 7, default='CREATED_TS')
  # Only applicable when fetching results. This incurs more DB operations and
  # more data is returned so this is a bit slower.
  include_performance_stats = messages.BooleanField(8, default=False)


class TasksCountRequest(messages.Message):
  """Request to count some subset of tasks."""
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


class OperationStats(messages.Message):
  duration = messages.FloatField(1)
  initial_number_items = messages.IntegerField(2)
  initial_size = messages.IntegerField(3)
  # These buffers are compressed as deflate'd delta-encoded varints. They are
  # all the items for an isolated operation, which can scale in the 100k range.
  # So can be large! See //client/utils/large.py for the code to handle these.
  items_cold = messages.BytesField(4)
  items_hot = messages.BytesField(5)


class PerformanceStats(messages.Message):
  bot_overhead = messages.FloatField(1)
  isolated_download = messages.MessageField(OperationStats, 2)
  isolated_upload = messages.MessageField(OperationStats, 3)


class CancelResponse(messages.Message):
  """Result of a request to cancel a task."""
  ok = messages.BooleanField(1)
  was_running = messages.BooleanField(2)


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
  # Statistics about overhead for an isolated task. Only sent when requested.
  performance_stats = messages.MessageField(PerformanceStats, 26)

  # A listing of the ACTUAL pinned CipdPackages that the task used. These can
  # vary from the input packages if the inputs included non-identity versions
  # (e.g. a ref like "latest").
  cipd_pins = messages.MessageField(CipdPins, 27)


class TaskList(messages.Message):
  """Wraps a list of TaskResult."""
  # TODO(maruel): Rename to TaskResults.
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResult, 2, repeated=True)
  now = message_types.DateTimeField(3)


class TaskRequests(messages.Message):
  """Wraps a list of TaskRequest."""
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskRequest, 2, repeated=True)
  now = message_types.DateTimeField(3)


class TasksCount(messages.Message):
  """Returns the count, as requested."""
  count = messages.IntegerField(1)
  now = message_types.DateTimeField(2)


class TasksTags(messages.Message):
  """Returns all the tags and tag possibilities in the fleet."""
  tasks_tags = messages.MessageField(StringListPair, 1, repeated=True)
  # Time at which this summary was calculated.
  ts = message_types.DateTimeField(2)


class TaskRequestMetadata(messages.Message):
  """Provides the ID of the requested TaskRequest."""
  request = messages.MessageField(TaskRequest, 1)
  task_id = messages.StringField(2)
  # Set to finished task result in case task was deduplicated.
  task_result = messages.MessageField(TaskResult, 3)


### Bots


### Bot-Related Requests


class BotsRequest(messages.Message):
  """Information needed to request bot data."""
  limit = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)
  # Must be a list of 'key:value' strings to filter the returned list of bots
  # on.
  dimensions = messages.StringField(3, repeated=True)
  quarantined = messages.EnumField(ThreeStateBool, 4, default='NONE')
  is_dead = messages.EnumField(ThreeStateBool, 5, default='NONE')


class BotsCountRequest(messages.Message):
  """Information needed to request bot counts."""
  # Must be a list of 'key:value' strings to filter the returned list of bots
  # on.
  dimensions = messages.StringField(1, repeated=True)


class BotEventsRequest(messages.Message):
  """Request to get events for a bot."""
  limit = messages.IntegerField(1, default=200)
  cursor = messages.StringField(2)
  # These should be DateTimeField but endpoints + protorpc have trouble encoding
  # this message in a GET request, this is due to DateTimeField's special
  # encoding in protorpc-1.0/protorpc/message_types.py that is bypassed when
  # using endpoints-1.0/endpoints/protojson.py to add GET query parameter
  # support.
  end = messages.FloatField(3)
  start = messages.FloatField(4)


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
  state = messages.EnumField(TaskState, 5, default='ALL')
  # It is currently not possible to query by tag on a specific bot, this
  # information is not saved in TaskRunResult to reduce the entity size. It is
  # fixable if a need ever come up, but normally a bot doesn't run enough task
  # per day to have to bother with this, just enumerate all the tasks run on the
  # bot and filter on client side.
  sort = messages.EnumField(TaskSort, 7, default='CREATED_TS')
  # Only applicable when fetching results. This incurs more DB operations and
  # more data is returned so this is a bit slower.
  include_performance_stats = messages.BooleanField(8, default=False)


### Bot-Related Responses


class BotInfo(messages.Message):
  """Representation of the BotInfo ndb model."""
  bot_id = messages.StringField(1)
  dimensions = messages.MessageField(StringListPair, 2, repeated=True)
  external_ip = messages.StringField(3)
  authenticated_as = messages.StringField(4)
  first_seen_ts = message_types.DateTimeField(5)
  is_dead = messages.BooleanField(6)
  last_seen_ts = message_types.DateTimeField(7)
  quarantined = messages.BooleanField(8)
  task_id = messages.StringField(9)
  task_name = messages.StringField(10)
  version = messages.StringField(11)
  # Encoded as json since it's an arbitrary dict.
  state = messages.StringField(12)
  lease_id = messages.StringField(13)
  lease_expiration_ts = message_types.DateTimeField(14)


class BotList(messages.Message):
  """Wraps a list of BotInfo."""
  cursor = messages.StringField(1)
  items = messages.MessageField(BotInfo, 2, repeated=True)
  now = message_types.DateTimeField(3)
  death_timeout = messages.IntegerField(4)


class BotsCount(messages.Message):
  """Returns the count, as requested."""
  now = message_types.DateTimeField(1)
  count = messages.IntegerField(2)
  quarantined = messages.IntegerField(3)
  dead = messages.IntegerField(4)
  busy = messages.IntegerField(5)


class BotsDimensions(messages.Message):
  """Returns all the dimensions and dimension possibilities in the fleet."""
  bots_dimensions = messages.MessageField(StringListPair, 1, repeated=True)
  # Time at which this summary was calculated.
  ts = message_types.DateTimeField(2)


class BotEvent(messages.Message):
  # Timestamp of this event.
  ts = message_types.DateTimeField(1)
  # Type of event.
  event_type = messages.StringField(2)
  # Message included in the event.
  message = messages.StringField(3)
  # Bot dimensions at that moment.
  dimensions = messages.MessageField(StringListPair, 4, repeated=True)
  # Bot state at that moment, encoded as json.
  state = messages.StringField(5)
  # IP address as seen by the HTTP handler.
  external_ip = messages.StringField(6)
  # Bot identity as seen by the HTTP handler.
  authenticated_as = messages.StringField(7)
  # Version of swarming_bot.zip the bot is currently running.
  version = messages.StringField(8)
  # If True, the bot is not accepting task.
  quarantined = messages.BooleanField(9)
  # Affected by event_type == 'request_task', 'task_completed', 'task_error'.
  task_id = messages.StringField(10)


class BotEvents(messages.Message):
  cursor = messages.StringField(1)
  items = messages.MessageField(BotEvent, 2, repeated=True)
  now = message_types.DateTimeField(3)


class BotTasks(messages.Message):
  cursor = messages.StringField(1)
  items = messages.MessageField(TaskResult, 2, repeated=True)
  now = message_types.DateTimeField(3)


class DeletedResponse(messages.Message):
  """Indicates whether a bot was deleted."""
  deleted = messages.BooleanField(1)


class TerminateResponse(messages.Message):
  """Returns the pseudo taskid to wait for the bot to shut down."""
  task_id = messages.StringField(1)
