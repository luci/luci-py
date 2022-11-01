# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines ProtoRPC types for the Swarming Server handlers."""

from protorpc import message_types
from protorpc import messages


### Enums


class TaskStateQuery(messages.Enum):
  """Use one of the values in this enum to query for tasks in one of the
  specified state.

  Use 'ALL' to not use any filtering based on task state.

  As an example, this enum enables querying for all tasks with state COMPLETED
  but non-zero exit code via COMPLETED_FAILURE.

  Do not confuse TaskStateQuery and TaskState. TaskStateQuery is to query tasks
  via the API. TaskState is the current task state.
  """
  # Query for all tasks currently TaskState.PENDING.
  PENDING = 0
  # Query for all tasks currently TaskState.RUNNING. This includes tasks
  # currently in the overhead phase; mapping input files or archiving outputs
  # back to the server.
  RUNNING = 1
  # Query for all tasks currently TaskState.PENDING or TaskState.RUNNING. This
  # is the query for the 'active' tasks.
  PENDING_RUNNING = 2
  # Query for all tasks that completed normally as TaskState.COMPLETED,
  # independent of the process exit code.
  COMPLETED = 3
  # Query for all tasks that completed normally as TaskState.COMPLETED and that
  # had exit code 0.
  COMPLETED_SUCCESS = 4
  # Query for all tasks that completed normally as TaskState.COMPLETED and that
  # had exit code not 0.
  COMPLETED_FAILURE = 5
  # Query for all tasks that are TaskState.EXPIRED.
  EXPIRED = 6
  # Query for all tasks that are TaskState.TIMED_OUT.
  TIMED_OUT = 7
  # Query for all tasks that are TaskState.BOT_DIED.
  BOT_DIED = 8
  # Query for all tasks that are TaskState.CANCELED.
  CANCELED = 9
  # Query for all tasks, independent of the task state.
  #
  # In hindsight, this constant should have been the value 0. Sorry, the
  # original author was young and foolish.
  ALL = 10
  # Query for all tasks that are TaskState.COMPLETED but that actually didn't
  # run due to TaskProperties.idempotent being True *and* that a previous task
  # with the exact same TaskProperties had successfully run before, aka
  # COMPLETED_SUCCESS.
  DEDUPED = 11
  # Query for all tasks that are TaskState.KILLED.
  KILLED = 12
  # Query for all tasks that are TaskState.NO_RESOURCE.
  NO_RESOURCE = 13
  # Query for all tasks that are TaskState.CLIENT_ERROR.
  CLIENT_ERROR = 14


class TaskState(messages.Enum):
  """Represents the current task state.

  Some states are still mutable: PENDING and RUNNING. The others are final and
  will not change afterward.

  A task is guaranteed to be in exactly one state at any point of time.

  Do not confuse TaskStateQuery and TaskState. TaskStateQuery is to query tasks
  via the API. TaskState is the current task state.

  As you read the following constants, astute readers may wonder why these
  constants look like a bitmask. This is because of historical reasons and this
  is effectively an enum, not a bitmask.
  """
  # Invalid state, do not use.
  INVALID = 0x00

  # The task is currently running. This is in fact 3 phases: the initial
  # overhead to fetch input files, the actual task running, and the tear down
  # overhead to archive output files to the server.
  RUNNING = 0x10

  # The task is currently pending. This means that no bot reaped the task. It
  # will stay in this state until either a task reaps it or the expiration
  # elapsed. The task pending expiration is specified as
  # TaskSlice.expiration_secs, one per task slice.
  PENDING = 0x20

  # The task is not pending anymore, and never ran due to lack of capacity. This
  # means that other higher priority tasks ran instead and that not enough bots
  # were available to run this task for TaskSlice.expiration_secs seconds.
  EXPIRED = 0x30

  # The task ran for longer than the allowed time in
  # TaskProperties.execution_timeout_secs or TaskProperties.io_timeout_secs.
  # This means the bot forcefully killed the task process as described in the
  # graceful termination dance in the documentation.
  TIMED_OUT = 0x40

  # The task ran but the bot had an internal failure, unrelated to the task
  # itself. It can be due to the server being unavailable to get task update,
  # the host on which the bot is running crashing or rebooting, etc.
  BOT_DIED = 0x50

  # The task never ran, and was manually cancelled via the 'cancel' API before
  # it was reaped.
  CANCELED = 0x60

  # The task ran and completed normally. The task process exit code may be 0 or
  # another value.
  COMPLETED = 0x70

  # The task ran but was manually killed via the 'cancel' API. This means the
  # bot forcefully killed the task process as described in the graceful
  # termination dance in the documentation.
  KILLED = 0x80

  # The task was never set to PENDING and was immediately refused, as the server
  # determined that there is no bot capacity to run this task. This happens
  # because no bot exposes a superset of the requested task dimensions.
  #
  # Set TaskSlice.wait_for_capacity to True to force the server to keep the task
  # slice pending even in this case. Generally speaking, the task will
  # eventually switch to EXPIRED, as there's no bot to run it. That said, there
  # are situations where it is known that in some not-too-distant future a wild
  # bot will appear that will be able to run this task.
  NO_RESOURCE = 0x100

  # The task run into an issue that was caused by the client. It can be due to
  # a bad CIPD or CAS package. Retrying the task with the same parameters will
  # not change the result.
  CLIENT_ERROR = 0x200


class TaskSort(messages.Enum):
  """Flag to sort returned tasks. The natural sort is CREATED_TS."""
  CREATED_TS, MODIFIED_TS, COMPLETED_TS, ABANDONED_TS, STARTED_TS = range(5)


class PoolTaskTemplateField(messages.Enum):
  """Flag to control the application of the pool's TaskTemplate in a new
  TaskRequest.

  The non-endpoints counterpart is in task_request.
  """
  AUTO = 0
  CANARY_PREFER = 1
  CANARY_NEVER = 2
  SKIP = 3


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
  luci_config = messages.StringField(5)
  cas_viewer_server = messages.StringField(8)


class BootstrapToken(messages.Message):
  """Returns a token to bootstrap a new bot."""
  bootstrap_token = messages.StringField(1)


class ClientPermissions(messages.Message):
  """Reports the client's permissions."""
  delete_bot = messages.BooleanField(1)
  delete_bots = messages.BooleanField(10)
  terminate_bot = messages.BooleanField(2)
  get_configs = messages.BooleanField(3)
  put_configs = messages.BooleanField(4)
  # Cancel one single task
  cancel_task = messages.BooleanField(5)
  get_bootstrap_token = messages.BooleanField(6)
  # Cancel multiple tasks at once, usually in emergencies.
  cancel_tasks = messages.BooleanField(7)
  list_bots = messages.StringField(8, repeated=True)
  list_tasks = messages.StringField(9, repeated=True)


class FileContent(messages.Message):
  """Content of a file."""
  content = messages.StringField(1)
  version = messages.StringField(2)
  who = messages.StringField(3)
  when = message_types.DateTimeField(4)


### Task-Related Requests


class Digest(messages.Message):
  # This is a [Digest][build.bazel.remote.execution.v2.Digest] of a blob on
  # RBE-CAS. See the explanations at the original definition.
  # pylint: disable=line-too-long
  # https://github.com/bazelbuild/remote-apis/blob/77cfb44a88577a7ade5dd2400425f6d50469ec6d/build/bazel/remote/execution/v2/remote_execution.proto#L753-L791
  hash = messages.StringField(1)
  size_bytes = messages.IntegerField(2)


class CASReference(messages.Message):
  # Full name of RBE-CAS instance. `projects/{project_id}/instances/{instance}`.
  # e.g. projects/chromium-swarm/instances/default_instance
  cas_instance = messages.StringField(1)
  # CAS Digest consists of hash and size bytes.
  digest = messages.MessageField(Digest, 2)


class CipdPackage(messages.Message):
  """A CIPD package to install in the run dir before task execution."""
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
  """Defines CIPD packages to install in task run directory."""
  # URL of the CIPD server. Must start with "https://" or "http://".
  # This field or its subfields are optional if default cipd client is defined
  # in the server config.
  server = messages.StringField(1)

  # CIPD package of CIPD client to use.
  # client_package.version is required.
  # This field is optional is default value is defined in the server config.
  # client_package.path must be empty.
  client_package = messages.MessageField(CipdPackage, 2)

  # List of CIPD packages to install.
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


class ContainmentType(messages.Enum):
  """See proto/api/swarming.proto for description."""
  NOT_SPECIFIED = 0
  NONE = 1
  AUTO = 2
  JOB_OBJECT = 3


class Containment(messages.Message):
  """See proto/api/swarming.proto for description."""
  containment_type = messages.EnumField(ContainmentType, 2)
  # Deprecated: lower_priority, limit_processes, limit_total_committed_memory


class TaskProperties(messages.Message):
  """Important metadata about a particular task."""
  # Specifies named caches to map into the working directory. These caches
  # outlives the task, which can then be reused by tasks later used on this bot
  # that request the same named cache.
  caches = messages.MessageField(CacheEntry, 11, repeated=True)
  # CIPD packages to install. These packages are meant to be software that is
  # needed (a dependency) to the task being run. Unlike isolated files, the CIPD
  # packages do not expire from the server.
  cipd_input = messages.MessageField(CipdInput, 10)
  # Command to run. This has priority over a command specified in the isolated
  # files.
  command = messages.StringField(1, repeated=True)
  # Relative working directory to start the 'command' in, defaults to the root
  # mapped directory or what is provided in the isolated file, if any.
  relative_cwd = messages.StringField(15)
  # Dimensions are what is used to determine which bot can run the task. The
  # bot must have all the matching dimensions, even for repeated keys with
  # multiple different values. It is a logical AND, all values must match.
  #
  # It should have been a StringListPair but this would be a breaking change.
  dimensions = messages.MessageField(StringPair, 2, repeated=True)
  # Environment variables to set when running the task.
  env = messages.MessageField(StringPair, 3, repeated=True)
  # Swarming-root relative paths to prepend to a given environment variable.
  #
  # These allow you to put certain subdirectories of the task into PATH,
  # PYTHONPATH, or other PATH-like environment variables. The order of
  # operations is:
  #   * Turn slashes into native-platform slashes.
  #   * Make the path absolute
  #   * Prepend it to the current value of the envvar using the os-native list
  #     separator (i.e. `;` on windows, `:` on POSIX).
  #
  # Each envvar can have multiple paths to prepend. They will be prepended in
  # the order seen here.
  #
  # For example, if env_prefixes was:
  #   [("PATH", ["foo", "bar"]),
  #    ("CUSTOMPATH", ["custom"])]
  #
  # The task would see:
  #   PATH=/path/to/swarming/rundir/foo:/path/to/swarming/rundir/bar:$PATH
  #   CUSTOMPATH=/path/to/swarming/rundir/custom
  #
  # The path should always be specified here with forward-slashes, and it must
  # not attempt to escape the swarming root (i.e. must not contain `..`).
  #
  # These are applied AFTER evaluating `env` entries.
  env_prefixes = messages.MessageField(StringListPair, 14, repeated=True)
  # Maximum number of seconds the task can run before its process is forcibly
  # terminated and the task results in TIMED_OUT.
  execution_timeout_secs = messages.IntegerField(4)
  # Number of second to give the child process after a SIGTERM before sending a
  # SIGKILL. See doc/Bot.md#timeout-handling
  grace_period_secs = messages.IntegerField(6)
  # True if the task does not access any service through the network and is
  # believed to be 100% reproducible with the same outcome. In the case of a
  # successful task, previous results will be reused if possible.
  idempotent = messages.BooleanField(7)
  # Digest of the input root uploaded to RBE-CAS.
  # This MUST be digest of [build.bazel.remote.execution.v2.Directory].
  cas_input_root = messages.MessageField(CASReference, 17)
  # Maximum number of seconds the task may be silent (no output to stdout nor
  # stderr) before it is considered hung and it forcibly terminated early and
  # the task results in TIMED_OUT.
  io_timeout_secs = messages.IntegerField(9)
  # Paths in the working directory to archive back.
  outputs = messages.StringField(12, repeated=True)
  # Secret bytes to provide to the task. Cannot be retrieved back.
  secret_bytes = messages.BytesField(13)
  # Containment of the task processes.
  containment = messages.MessageField(Containment, 16)


class TaskSlice(messages.Message):
  """Defines a possible task execution for a task request to be run on the
  Swarming infrastructure.

  This is one of the possible fallback on a task request.
  """
  # The property of the task to try to run.
  #
  # If there is no bot that can serve this properties.dimensions when this task
  # slice is enqueued, it is immediately denied. This can trigger if:
  # - There is no bot with these dimensions currently known.
  # - Bots that could run this task are either all dead or quarantined.
  # Swarming considers a bot dead if it hasn't pinged in the last N minutes
  # (currently 10 minutes).
  properties = messages.MessageField(TaskProperties, 1)
  # Maximum of seconds the task slice may stay PENDING.
  #
  # If this task request slice is not scheduled after waiting this long, the
  # next one will be processed. If this slice is the last one, the task state
  # will be set to EXPIRED.
  expiration_secs = messages.IntegerField(2)
  # When a task is scheduled and there are currently no bots available to run
  # the task, the TaskSlice can either be PENDING, or be denied immediately.
  # When denied, the next TaskSlice is enqueued, and if there's no following
  # TaskSlice, the task state is set to NO_RESOURCE. This should normally be
  # set to False to avoid unnecessary waiting.
  wait_for_capacity = messages.BooleanField(3)


class ResultDBCfg(messages.Message):
  """Swarming:ResultDB integration configuration for a task.

  See NewTaskRequest.resultdb for more details.
  """

  # If True and this task is not deduplicated, create
  # "task-{swarming_hostname}-{run_id}" invocation for this task,
  # provide its update token to the task subprocess via LUCI_CONTEXT
  # and finalize the invocation when the task is done.
  # If the task is deduplicated, then TaskResult.invocation_name will be the
  # invocation name of the original task.
  # Swarming:ResultDB integration is off by default, but it may change in the
  # future.
  enable = messages.BooleanField(1)


class NewTaskRequest(messages.Message):
  """Description of a new task request as described by the client.

  This message is used to create a new task.
  """
  # DEPRECATED. Use task_slices[0].expiration_secs.
  expiration_secs = messages.IntegerField(1)
  # Task name for display purpose.
  name = messages.StringField(2)
  # Parent Swarming run ID of the process requesting this task. This is to tell
  # the server about reentrancy: when a task creates children Swarming tasks, so
  # that the tree of tasks can be presented in the UI; the parent task will list
  # all the children tasks that were triggered.
  parent_task_id = messages.StringField(3)
  # Task priority, the lower the more important.
  priority = messages.IntegerField(4)
  # DEPRECATED. Use task_slices[0].properties.
  properties = messages.MessageField(TaskProperties, 5)
  # Slice of TaskSlice, along their scheduling parameters. Cannot be used at the
  # same time as properties and expiration_secs.
  #
  # This defines all the various possible task execution for a task request to
  # be run on the Swarming infrastructure. They are processed in order, and it
  # is guaranteed that at most one of these will be processed.
  task_slices = messages.MessageField(TaskSlice, 12, repeated=True)
  # Tags are 'key:value' strings that describes what the task is about. This can
  # later be leveraged to search for kinds of tasks per tag.
  tags = messages.StringField(6, repeated=True)
  # User on which behalf this task is run, if relevant. Not validated.
  user = messages.StringField(7)

  # Defines what OAuth2 credentials the task uses when calling other services.
  #
  # Possible values are:
  #   - 'none': do not use a task service account at all, this is the default.
  #   - 'bot': use bot's own account, works only if bots authenticate with
  #       OAuth2.
  #   - <some email>: use this specific service account if it is allowed in the
  #       pool (via 'allowed_service_account' pools.cfg setting) and configured
  #       in the token server's service_accounts.cfg.
  #
  # Note that the service account name is specified outside of task properties,
  # and thus it is possible to have two tasks with different service accounts,
  # but identical properties hash (so one can be deduped). If this is unsuitable
  # use 'idempotent=False' or include a service account name in properties
  # separately.
  service_account = messages.StringField(8)

  # Full topic name to post task state updates to, e.g.
  # "projects/<id>/topics/<id>".
  pubsub_topic = messages.StringField(9)
  # Secret string to put into "auth_token" attribute of PubSub message.
  pubsub_auth_token = messages.StringField(10)
  # Will be but into "userdata" fields of PubSub message.
  pubsub_userdata = messages.StringField(11)

  # Only evaluate the task, as if we were going to schedule it, but don't
  # actually schedule it. This will return the TaskRequest, but without
  # a task_id.
  evaluate_only = messages.BooleanField(13)

  # Controls the application of the pool's TaskTemplate to the creation of this
  # task. By default this will automatically select the pool's preference for
  # template, but you can also instruct swarming to prefer/prevent the
  # application of canary templates, as well as skipping the template
  # altogether.
  pool_task_template = messages.EnumField(
      PoolTaskTemplateField, 14, default='AUTO')

  # Maximum delay between bot pings before the bot is considered dead
  # while running a task.
  bot_ping_tolerance_secs = messages.IntegerField(15)

  # This is used to make new task request idempotent in best effort.
  # If new request has request_uuid field, it checks memcache before scheduling
  # actual task to check there is already the task triggered by same request
  # previously.
  request_uuid = messages.StringField(16)

  # Configuration of Swarming:ResultDB integration.
  resultdb = messages.MessageField(ResultDBCfg, 17)

  # Task realm.
  # See api/swarming.proto for more details.
  realm = messages.StringField(18)

class TaskRequest(messages.Message):
  """Description of a task request as registered by the server.

  This message is used when retrieving information about an existing task.

  See NewTaskRequest for more details.
  """
  expiration_secs = messages.IntegerField(1)
  name = messages.StringField(2)
  task_id = messages.StringField(15)
  parent_task_id = messages.StringField(3)
  priority = messages.IntegerField(4)
  # For some amount of time, the properties will be copied into the
  # task_slices and vice-versa, to give time to the clients to update.
  # Eventually, only task_slices will be supported.
  properties = messages.MessageField(TaskProperties, 5)
  tags = messages.StringField(6, repeated=True)
  created_ts = message_types.DateTimeField(7)
  user = messages.StringField(8)
  # User name of whoever posted this task, extracted from the credentials.
  authenticated = messages.StringField(9)
  task_slices = messages.MessageField(TaskSlice, 13, repeated=True)
  # Indicates what OAuth2 credentials the task uses when calling other services.
  service_account = messages.StringField(10)
  realm = messages.StringField(16)

  # Configuration of Swarming:ResultDB integration.
  resultdb = messages.MessageField(ResultDBCfg, 17)

  pubsub_topic = messages.StringField(11)
  pubsub_userdata = messages.StringField(12)
  bot_ping_tolerance_secs = messages.IntegerField(14)


class TaskCancelRequest(messages.Message):
  """Request to cancel one task."""
  kill_running = messages.BooleanField(1)


class TasksCancelRequest(messages.Message):
  """Request to cancel some subset of pending/running tasks."""
  tags = messages.StringField(1, repeated=True)
  cursor = messages.StringField(2)
  limit = messages.IntegerField(3, default=100)
  kill_running = messages.BooleanField(4)
  end = messages.FloatField(5)
  start = messages.FloatField(6)


### Task-Related Responses


class OperationStats(messages.Message):
  duration = messages.FloatField(1)


class CASOperationStats(messages.Message):
  duration = messages.FloatField(1)
  initial_number_items = messages.IntegerField(2)
  initial_size = messages.IntegerField(3)
  # These buffers are compressed as deflate'd delta-encoded varints. They are
  # all the items for an isolated operation, which can scale in the 100k range.
  # So can be large! See //client/utils/large.py for the code to handle these.
  items_cold = messages.BytesField(4)
  items_hot = messages.BytesField(5)
  # Corresponding summaries; for each list above, sum of the number of files
  # and the sum bytes of the files.
  num_items_cold = messages.IntegerField(6)
  total_bytes_items_cold = messages.IntegerField(7)
  num_items_hot = messages.IntegerField(8)
  total_bytes_items_hot = messages.IntegerField(9)


class PerformanceStats(messages.Message):
  """Performance stats of task execution.

  See task_result.PerformanceStats for details.
  """
  bot_overhead = messages.FloatField(1)
  isolated_download = messages.MessageField(CASOperationStats, 2)
  isolated_upload = messages.MessageField(CASOperationStats, 3)
  package_installation = messages.MessageField(OperationStats, 4)
  cache_trim = messages.MessageField(OperationStats, 5)
  named_caches_install = messages.MessageField(OperationStats, 6)
  named_caches_uninstall = messages.MessageField(OperationStats, 7)
  cleanup = messages.MessageField(OperationStats, 8)


class CancelResponse(messages.Message):
  """Result of a request to cancel a task."""
  ok = messages.BooleanField(1)
  was_running = messages.BooleanField(2)


class TasksCancelResponse(messages.Message):
  """Result of canceling some subset of pending tasks.
  """
  cursor = messages.StringField(1)
  now = message_types.DateTimeField(2)
  matched = messages.IntegerField(3)


class TaskOutput(messages.Message):
  """A task's output as a string."""
  output = messages.StringField(1)
  # Current state of the task (e.g. PENDING, RUNNING, COMPLETED, EXPIRED, etc).
  state = messages.EnumField(TaskState, 2)


class ResultDBInfo(messages.Message):
  """ResultDB related properties."""
  # ResultDB hostname, e.g. "results.api.cr.dev"
  hostname = messages.StringField(1)

  # e.g. "invocations/task-chromium-swarm.appspot.com-deadbeef1"
  #
  # If the task was deduplicated, this equals invocation name of the original
  # task.
  invocation = messages.StringField(2)


class TaskResult(messages.Message):
  """Representation of the TaskResultSummary or TaskRunResult ndb model."""
  # Time when the task was abandoned instead of normal completion (e.g.
  # EXPIRED, BOT_DIED, KILLED).
  #
  # In the case of KILLED, this records the time the user requested the task to
  # stop.
  abandoned_ts = message_types.DateTimeField(1)
  # The same key cannot be repeated.
  bot_dimensions = messages.MessageField(StringListPair, 2, repeated=True)
  # Unique ID of the bot.
  bot_id = messages.StringField(3)
  # Time the bot became ready for a next task.
  bot_idle_since_ts = message_types.DateTimeField(32)
  # Hash of the bot code which ran the task.
  bot_version = messages.StringField(4)
  # The cloud project id where the bot saves its logs.
  bot_logs_cloud_project = messages.StringField(35)
  # List of task IDs that this task triggered, if any.
  children_task_ids = messages.StringField(5, repeated=True)
  # Time the task completed normally. Only one of abandoned_ts or completed_ts
  # can be set except for state == KILLED.
  #
  # In case of KILLED, completed_ts is the time the task completed.
  completed_ts = message_types.DateTimeField(6)
  # $ saved for task with state DEDUPED.
  cost_saved_usd = messages.FloatField(7)
  # Time the task was requested.
  created_ts = message_types.DateTimeField(8)
  # Task ID which results was reused for state DEDUPED.
  deduped_from = messages.StringField(9)
  # Duration of the task in seconds. This excludes overheads.
  duration = messages.FloatField(10)
  # Process exit code if relevant. May be forcibly set to -1 in exceptional
  # cases.
  exit_code = messages.IntegerField(11)
  # True if exit_code != 0.
  failure = messages.BooleanField(12)
  # True if state is BOT_DIED.
  internal_failure = messages.BooleanField(13)
  # Time the results was last updated in the DB.
  modified_ts = message_types.DateTimeField(14)
  # CAS Digest of the output root uploaded to RBE-CAS.
  # This MUST be digest of [build.bazel.remote.execution.v2.Directory].
  cas_output_root = messages.MessageField(CASReference, 31)
  # Server versions that touched this task.
  server_versions = messages.StringField(17, repeated=True)
  # Time the task started being run by a bot.
  started_ts = message_types.DateTimeField(18)
  # Current state of the task (e.g. PENDING, RUNNING, COMPLETED, EXPIRED, etc).
  state = messages.EnumField(TaskState, 19)
  # Summary task ID (ending with '0') when creating a new task.
  task_id = messages.StringField(20)

  # Can be multiple values only in TaskResultSummary.
  costs_usd = messages.FloatField(22, repeated=True)
  # Name of the task. Only set when requesting task ID summary, ending with '0'.
  name = messages.StringField(23)
  # Tags associated with the task when it was requested. Only set when
  # requesting task ID summary, ending with '0'.
  tags = messages.StringField(24, repeated=True)
  # User on behalf this task was requested. Only set when requesting task ID
  # summary, ending with '0'.
  user = messages.StringField(25)
  # Statistics about overhead for an isolated task. Only sent when requested.
  performance_stats = messages.MessageField(PerformanceStats, 26)

  # Listing of the ACTUAL pinned CipdPackages that the task used. These can vary
  # from the input packages if the inputs included non-identity versions (e.g. a
  # ref like "latest").
  cipd_pins = messages.MessageField(CipdPins, 27)
  # Actual executed task id that this task represents. For deduped tasks, it is
  # the same value as deduped_from. This value can be empty if there is no
  # execution, for example the task was cancelled.
  run_id = messages.StringField(28)

  # Index in the TaskRequest.task_slices (TaskSlice instance) that this result
  # represents. This is updated when a TaskSlice is enqueued to run.
  #
  # The TaskSlice contains a TaskProperties, which defines what is run.
  current_task_slice = messages.IntegerField(29)

  # ResultDB related information.
  # None if the integration was not enabled for this task.
  resultdb_info = messages.MessageField(ResultDBInfo, 30)

  # Reported missing CAS packages on CLIENT_ERROR state
  missing_cas = messages.MessageField(CASReference, 33, repeated=True)

  # Reported missing CIPD packages on CLIENT_ERROR state
  missing_cipd = messages.MessageField(CipdPackage, 34, repeated=True)


class TaskStates(messages.Message):
  """Only holds states. Used in the 'get_states' RPC."""
  states = messages.EnumField(TaskState, 1, repeated=True)


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


class TaskRequestMetadata(messages.Message):
  """Provides the ID of the requested TaskRequest."""
  request = messages.MessageField(TaskRequest, 1)
  task_id = messages.StringField(2)
  # Set to finished task result in case task was deduplicated.
  task_result = messages.MessageField(TaskResult, 3)


### Task queues


class TaskQueue(messages.Message):
  # Must be a list of 'key:value' strings to filter the returned list of bots
  # on.
  dimensions = messages.StringField(1, repeated=True)
  valid_until_ts = message_types.DateTimeField(2)


class TaskQueueList(messages.Message):
  cursor = messages.StringField(1)
  # Note that it's possible that the RPC returns a tad more or less items than
  # requested limit.
  items = messages.MessageField(TaskQueue, 2, repeated=True)
  now = message_types.DateTimeField(3)


### Bots


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
  maintenance_msg = messages.StringField(18)
  task_id = messages.StringField(9)
  task_name = messages.StringField(10)
  version = messages.StringField(11)
  # Encoded as json since it's an arbitrary dict.
  state = messages.StringField(12)
  deleted = messages.BooleanField(15)

  # Deprecated. TODO(crbug/897355): Remove.
  lease_id = messages.StringField(13)
  lease_expiration_ts = message_types.DateTimeField(14)
  machine_type = messages.StringField(16)
  machine_lease = messages.StringField(17)
  leased_indefinitely = messages.BooleanField(19)


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
  maintenance = messages.IntegerField(6)
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
  # If True, the bot is not accepting task due to being quarantined.
  quarantined = messages.BooleanField(9)
  # If set, the bot is rejecting tasks due to maintenance.
  maintenance_msg = messages.StringField(11)
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
