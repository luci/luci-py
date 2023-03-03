# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Contains code which is common between bot the prpc API and the protorpc api.
"""
import logging
from collections import namedtuple

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

import api_helpers
import handlers_exceptions
from components import auth
from components import datastore_utils
from server import task_queues
from server import bot_management
from server import realms
from server import task_scheduler
from server import task_request
from server import task_pack
from server import task_result


def _get_or_raise(key):
  """Checks if ndb entity exists for key exists or else throws
  handlers_exceptions.NotFoundException.
  """
  result = key.get()
  if not result:
    raise handlers_exceptions.NotFoundException('%s not found.' % key.id())
  return result


def get_bot(bot_id):
  """Retrieves a bot for a given bot_id.

  Returns:
    (bot_management.BotInfo, deleted) Deleted is true if a bot with this bot_id
      has existed at any point and has a trace event history.

  Raises:
    handlers_exceptions.NotFoundException if the bot_id has never existed.
    auth.AuthorizationError if caller fails realm authorization test.
  """
  realms.check_bot_get_acl(bot_id)
  bot = bot_management.get_info_key(bot_id).get()
  deleted = False

  if not bot:
    # If there is not BotInfo, look if there are BotEvent child of this
    # entity. If this is the case, it means the bot was deleted but it's
    # useful to show information about it to the user even if the bot was
    # deleted.
    events = bot_management.get_events_query(bot_id).fetch(1)
    if events:
      # Between the first call to get_info_key and get_events_query a bot may
      # have handshaked, meaning that it is online and not deleted.
      # Here, another call is made to get_info_key to make sure the bot is still
      # deleted.
      # See - https://crbug.com/1407381 for more information.
      bot = bot_management.get_info_key(bot_id).get(use_cache=False)
      if not bot:
        bot = bot_management.BotInfo(
            key=bot_management.get_info_key(bot_id),
            dimensions_flat=task_queues.bot_dimensions_to_flat(
                events[0].dimensions),
            state=events[0].state,
            external_ip=events[0].external_ip,
            authenticated_as=events[0].authenticated_as,
            version=events[0].version,
            quarantined=events[0].quarantined,
            maintenance_msg=events[0].maintenance_msg,
            task_id=events[0].task_id,
            last_seen_ts=events[0].ts)
        # message_conversion.bot_info_to_rpc calls `is_dead` and this property
        # require `composite` to be calculated. The calculation is done in
        # _pre_put_hook usually. But the BotInfo shouldn't be stored in this
        # case, as it's already deleted.
        bot.composite = bot.calc_composite()
        deleted = True
  if not bot:
    raise handlers_exceptions.NotFoundException("%s not found." % bot_id)
  return (bot, deleted)


def delete_bot(bot_id):
  """Deletes the bot corresponding to a provided bot_id.

  The bot will be considered "deleted" by swarming but information about it
  will still be available to calls of `get_bot`.

  It is meant to remove from the DB the presence of a bot that was retired,
  e.g. the VM was shut down already. Use 'terminate' instead of the bot is
  still alive.

  Raises:
    handlers_exceptions.NotFoundException if there is no such bot.
    auth.AuthorizationError if bot fails realm authorization test.
  """
  realms.check_bot_delete_acl(bot_id)
  bot_info_key = bot_management.get_info_key(bot_id)
  _get_or_raise(bot_info_key)  # raises 404 if there is no such bot
  # It is important to note that the bot is not there anymore, so it is not
  # a member of any task queue.
  task_queues.cleanup_after_bot(bot_id)
  bot_info_key.delete()


def get_bot_events(bot_id, start, end, limit, cursor):
  """Retrieves a list of bot_management.BotEvent within a specific time range.

  Returns:
    (items, cursor) where items is a list of BotEvent entities and a cursor to
    next group of results."""
  realms.check_bot_get_acl(bot_id)
  q = bot_management.get_events_query(bot_id)
  if start:
    q = q.filter(bot_management.BotEvent.ts >= start)
  if end:
    q = q.filter(bot_management.BotEvent.ts < end)
  return datastore_utils.fetch_page(q, limit, cursor)


def terminate_bot(bot_id):
  """Terminates a bot with a given bot_id.

  Returns:
    task_id of the task to terminate the bot.

  Raises:
    handlers_exceptions.BadRequestException if error occurs when creating the
      termination task.
    handlers_exceptions.NotFoundException if there is no such bot.
    auth.AuthorizationError if bot fails realm authorization test.
  """
  realms.check_bot_terminate_acl(bot_id)
  bot_key = bot_management.get_info_key(bot_id)
  _get_or_raise(bot_key)  # raises 404 if there is no such bot
  try:
    # Craft a special priority 0 task to tell the bot to shutdown.
    request = task_request.create_termination_task(bot_id,
                                                   wait_for_capacity=True)
  except (datastore_errors.BadValueError, TypeError, ValueError) as e:
    raise handlers_exceptions.BadRequestException(e.message)

  result_summary = task_scheduler.schedule_request(request,
                                                   enable_resultdb=False)
  return task_pack.pack_result_summary_key(result_summary.key)


def list_bot_tasks(bot_id, start, end, sort, state, cursor, limit):
  """Lists all tasks which have been executed by a given bot which match the
  filters.

  Arguments:
    bot_id: bot_id to filter tasks.
    start: datetime.datetime object or None. If not None, only tasks where the
      datetime field specified by `sort` is greater than `start` will be shown.
    end: Datetime.datetime object or None. If not None, only tasks where the
      datetime field specified by `sort` is less than `end` will be shown.
    sort: May be either 'created_ts', 'started_ts' or 'completed_ts'. Specifies
      which which datetime field in the task to apply [start, end] filter.
    state: A string representation of possible task_state_query State.
    cursor: Cursor returned by previous invocation of this request.
    limit: Number of items to return per request.

  Returns:
    List of tasks results with filters applied.

  Raises:
    handlers_exceptions.BadRequestException if a filter or sort is invalid.
    auth.AuthorizationError if bot fails realm authorization test.
  """
  try:
    realms.check_bot_tasks_acl(bot_id)
    q = task_result.get_run_results_query(start, end, sort, state, bot_id)
    return datastore_utils.fetch_page(q, limit, cursor)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(
        'Inappropriate filter for bot.tasks: %s' % e)


def to_keys(task_id):
  """Converts task_id into task request and task_result keys.

  Arguments:
    task_id: a string task_id.

  Returns:
    TaskRequest and TaskResultSummary ndb keys.

  Raises:
    handlers_exceptions.BadRequestException if the key is invalid.
  """
  try:
    return task_pack.get_request_and_result_keys(task_id)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException('invalid task_id %s: %s' %
                                                  (task_id, e))


# Used by get_task_request_async(), clearer than using True/False and important
# as this is part of the security boundary.
CANCEL = object()
VIEW = object()


@ndb.tasklet
def get_task_request_async(task_id, request_key, permission):
  """Returns the TaskRequest corresponding to a task ID.

  Enforces the ACL for users. Allows bots all access for the moment.

  Arguments:
    task_id: task_id of TaskRequest to search for. Caller must ensure that
      request_key is generated from this task_id.
    request_key: request_key generated from task_id to search for.
    permission: Can be either CANCEL or VIEW: determines whether task_cancel_acl
      or get_task_acl realm checks should be used.

  Returns:
    TaskRequest ndb entity.

  Raises:
    auth.AuthorizationError if bot fails realm authorization test.
  """
  try:
    request = yield request_key.get_async()
  except ValueError as e:
    raise handlers_exceptions.BadRequestException("invalid task_id %s: %s" %
                                                  (task_id, e))
  if not request:
    raise handlers_exceptions.NotFoundException('%s not found.' % task_id)
  if permission == VIEW:
    realms.check_task_get_acl(request)
  elif permission == CANCEL:
    realms.check_task_cancel_acl(request)
  else:
    raise handlers_exceptions.InternalException('get_task_request_async()')
  raise ndb.Return(request)


def get_request_and_result(task_id, permission, trust_memcache):
  """Returns the task request and task result corresponding to a task ID.

  For the task result, first do an explict lookup of the caches, and then decide
  if it is necessary to fetch from the DB.

  Arguments:
    task_id: task ID as provided by the user.
    permission: Can be either CANCEL or VIEW: determines whether task_cancel_acl
      or get_task_acl realm checks should be used.
    trust_memcache: bool to state if memcache should be trusted for running
        task. If False, when a task is still pending/running, do a DB fetch.

  Returns:
    tuple(TaskRequest, result): result can be either for a TaskRunResult or a
                                TaskResultSummay.

  Raises:
    handlers_exceptions.BadRequestException: if task_id is invalid.
    handlers_exceptions.NotFoundException: if no task is found for task_id.
  """
  request_key, result_key = to_keys(task_id)
  try:
    # The task result has a very high odd of taking much more time to fetch than
    # the TaskRequest, albeit it is the TaskRequest that enforces ACL. Do the
    # task result fetch first, the worst that will happen is unnecessarily
    # fetching the task result.
    result_future = result_key.get_async(use_cache=True,
                                         use_memcache=True,
                                         use_datastore=False)

    # The TaskRequest has P(99.9%) chance of being fetched from memcache since
    # it is immutable.
    request_future = get_task_request_async(task_id, request_key, permission)

    result = result_future.get_result()
    if (not result or (result.state in task_result.State.STATES_RUNNING
                       and not trust_memcache)):
      # Either the entity is not in cache, or we don't trust memcache for a
      # running task result. Do the DB fetch, which is slow.
      result = result_key.get(use_cache=False,
                              use_memcache=False,
                              use_datastore=True)

    request = request_future.get_result()
  except ValueError as e:
    raise handlers_exceptions.BadRequestException('invalid task_id %s: %s' %
                                                  (task_id, e))
  if not result:
    raise handlers_exceptions.NotFoundException('%s not found.' % task_id)
  return request, result


def cancel_task(task_id, kill_running):
  """Initiates the cancellation of a swarming task.

  Arguments:
    task_id: task_id to be cancelled.
    kill_running: if False, this will not kill tasks in a state of RUNNING. If
      True then running tasks and their "child tasks" will be killed.

  Returns:
    tuple(cancelled, was_running): cancelled=True implies that the operation
      completed successfully, which has a different meaning depending on
      whether the task was in RUNNING state. If the task was not in RUNNING
      state then the state of the task changes was changed to CANCELLED with no
      further work needed. If kill_running=True and the task is in RUNNING state
      (implying that it is being executed on a bot) then the bot still needs to
      cancel the actual task process. The state of the task will be set to
      CANCELLED after the bot has killed the process and reported this to
      swarming service.
      was_running=True implies that the task was running when cancellation was
      initiated and further work needs to take place on the bot before this
      the task changes state state to CANCELLED.

  Raises:
    auth.AuthorizationError if realm check fails.
    handlers_exceptions.BadRequestException: if task_id is invalid.
    handlers_exceptions.NotFoundException: if no task is found for task_id.
  """
  request_key, result_key = to_keys(task_id)
  request_obj = get_task_request_async(task_id, request_key,
                                       CANCEL).get_result()
  return task_scheduler.cancel_task(request_obj, result_key, kill_running
                                    or False, None)

# Maximum content fetched at once, mostly for compatibility with previous
# behavior. pRPC implementation should limit to a multiple of CHUNK_SIZE
# (one or two?) for efficiency.
# Technically this is 160 CHUNKS - see task_result.TaskOutput.CHUNK_SIZE.
RECOMMENDED_OUTPUT_LENGTH = 16 * 1000 * 1024


def get_output(task_id, offset, length):
  """Returns the output of the task corresponding to a task ID.

  Arguments:
    task_id: ID of task to return output of.
    offset: byte offset to start fetching.
    length: number of bytes from offset to fetch.

  Returns:
    tuple(output, state): output is a bytearray ('str') of task output. state is
      the current TaskState of the task.
  """
  _, result = get_request_and_result(task_id, VIEW, True)
  output = result.get_output(offset or 0, length or RECOMMENDED_OUTPUT_LENGTH)
  return output, result.state


NewTaskResult = namedtuple('NewTaskResult',
                           ['request', 'task_id', 'task_result'])


def new_task(request, secret_bytes, template_apply, evaluate_only,
             request_uuid):
  """Schedules a new task for a bot with given dimensions.

  Arguments:
    request: ndb TaskRequest entity representing the new task.
    secret_bytes: bytestring representing the secret bytes.
    template_apply: swarming_rpcs.PoolTaskTemplateField which determines how
      to apply templates to the new task.
    evaluate_only: evaluate whether task can be scheduled but don't actually
      schedule it. Basically a dry run.
    request_uuid: a str uuid used to make the request idempotent

  Returns:
    NewTaskResult(task_request, task_id, task_result) where
      task_request is the TaskRequest object which would result from this
        routine. Only stored in datastore if evaluate_only=False.
      task_id identifies the new task, will be None if evaluate_only=True.
      task_result initial TaskResultSummary entity in datastore. Will only be
        created if evaluate_only=False.

  Raises:
    auth.AuthorizationError if acl checks fail.
    handlers_exceptions.BadRequestException if creating the task fails for an
      expected reason.
  """
  api_helpers.process_task_request(request, template_apply)

  # If the user only wanted to evaluate scheduling the task, but not actually
  # schedule it, return early without a task_id.
  if evaluate_only:
    request._pre_put_hook()
    return NewTaskResult(request=request, task_id=None, task_result=None)

  # This check is for idempotency when creating new tasks.
  # TODO(crbug.com/997221): Make idempotency robust.
  # There is still possibility of duplicate task creation if requests with
  # the same uuid are sent in a short period of time.
  def _schedule_request():
    try:
      result_summary = task_scheduler.schedule_request(
          request,
          enable_resultdb=(request.resultdb and request.resultdb.enable),
          secret_bytes=secret_bytes)
    except (datastore_errors.BadValueError, TypeError, ValueError) as e:
      logging.exception("got exception around task_scheduler.schedule_request")
      raise handlers_exceptions.BadRequestException(e.message)

    return NewTaskResult(request=request,
                         task_id=task_pack.pack_result_summary_key(
                             result_summary.key),
                         task_result=result_summary)

  new_task_result, cache_hit = api_helpers.cache_request(
      'task_new', request_uuid, _schedule_request)

  if cache_hit:
    # The returned metadata may have been fetched from cache.
    # Compare it with original request
    # Since request does not have key yet, it needs to be excluded for
    # validation.
    original_key = new_task_result.request.key
    new_task_result.request.key = None
    if new_task_result.request != request:
      logging.warning(
          'the same request_uuid value was reused for different task '
          'requests')
    new_task_result.request.key = original_key
    logging.info('Reusing task %s with uuid %s',
                 new_task_result.request.task_id, request_uuid)

  return new_task_result
