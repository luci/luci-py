# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Contains code which is common between bot the prpc API and the protorpc api.
"""
import logging
import os
from collections import namedtuple

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.api import memcache

import api_helpers
import handlers_exceptions
from components import auth
from components import datastore_utils
from components import utils
from server import acl
from server import bot_management
from server import bot_code
from server import config
from server import pools_config
from server import task_queues
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



# Stores a list of filters for the function task_result.get_run_results_query
TaskFilters = namedtuple(
    'TaskFilters',
    [
        # datetime.datetime object or None. If not None, only tasks where the
        # datetime field specified by `sort` is greater than `start` will be
        # shown.
        'start',
        # Datetime.datetime object or None. If not None, only tasks where the
        # datetime field specified by `sort` is less than `end` will be shown.
        'end',
        # May be either 'created_ts', 'started_ts' or 'completed_ts'. Specifies
        # which which datetime field in the task to apply [start, end] filter.
        'sort',
        # A string representation of possible task_state_query State.
        'state',
        # list of key:value pair strings.
        'tags',
    ])


def list_bot_tasks(bot_id, filters, cursor, limit):
  """Lists all tasks which have been executed by a given bot which match the
  filters.

  Arguments:
    bot_id: bot_id to filter tasks.
    filters: A TaskFilters object to generate query for
      task_result.TaskResult. Does not make use of the tags field.
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
    q = task_result.get_run_results_query(filters.start, filters.end,
                                          filters.sort, filters.state, bot_id)
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


TasksCancelResult = namedtuple('TasksCancelResponse',
                               ['cursor', 'matched', 'now'])


def cancel_tasks(tags, start, end, limit, cursor, kill_running):
  """Mass cancels tasks which match the filter.

  Args:
    tags: list of 'key:value' strings. Tasks matching these tags will be queued
      for cancellation.
    start: datetime.datetime. only tasks created after start will be cancelled.
    end: datetime.datetime. only tasks created before end will be cancelled.
    limit: number of tasks to cancel per request.
    cursor: str representing cursor from previous request.
    kill_running: if True, tasks which are running at time of request will be
      scheduled for cancellation. If False they are ignored.

  Returns:
    TasksCancelResponse(cursor, matched, now): matched is the number of tasks
      queued for cancellation. now is time before cancellation was scheduled.
      cursor is used to continue query for future requests.

  Raises:
    auth.AuthorizationError if acl checks fail.
    handlers_exceptions.BadRequestException if limit is not in [1, 1000]
  """
  if not tags:
    # Prevent accidental cancellation of everything.
    raise handlers_exceptions.BadRequestException(
        'You must specify tags when cancelling multiple tasks.')

  # Check permission.
  # If the caller has global permission, it can access all tasks.
  # Otherwise, it requires a pool tag to check ACL.
  pools = bot_management.get_pools_from_dimensions_flat(tags)
  realms.check_tasks_cancel_acl(pools)

  now = utils.utcnow()

  query = task_result.get_result_summaries_query(
      start, end, 'created_ts',
      'pending_running' if kill_running else 'pending', tags)
  try:
    cursor, results = task_scheduler.cancel_tasks(limit,
                                                  query=query,
                                                  cursor=cursor)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(str(e))

  return TasksCancelResult(cursor=cursor, matched=len(results), now=now)


DimensionsResponse = namedtuple('DimensionsResponse', ['bots_dimensions', 'ts'])


def get_dimensions(pool):
  """Returns an aggregated list of all dimensions for a specific pool.

  Args:
    pool: pool to find dimensions for.

  Returns: DimensionsResponse(bots_dimensions, ts) where ts is the time
    the query was made and bot_dimensions is a dictionary with they type
    dict[str, list[str]] with the key being a dimension and the values being
    a list of values associated with that dimension.

  Raises:
    auth.AuthorizationError if acl checks fail.
    handlers_exceptions.NotFoundException if a DimensionsAggregation is not
      found for pool.

  """
  realms.check_bots_list_acl([pool] if pool else None)

  # TODO(jwata): change 'current' to 'all' once the entity is ready.
  agg = bot_management.get_aggregation_key(pool or 'current').get()
  if not agg:
    raise handlers_exceptions.NotFoundException(
        'Dimension aggregation for pool %s does not exit' % pool)
  return DimensionsResponse(bots_dimensions=agg.dimensions, ts=agg.ts)


def list_task_results(filters, cursor, limit):
  """Returns a list of task results which match filters mentioned in
  ResultSummaryFilters.

  Args:
    filters: A TaskFilters namedtuple.
    cursor: str representing cursor from previous request.
    limit: max number of entities to fetch per request.

  Returns:
    List of TaskResultSummary entities.
  """
  pools = bot_management.get_pools_from_dimensions_flat(filters.tags)
  realms.check_tasks_list_acl(pools)

  try:
    return datastore_utils.fetch_page(
        task_result.get_result_summaries_query(filters.start, filters.end,
                                               filters.sort, filters.state,
                                               filters.tags), limit, cursor)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(
        'Inappropriate filter for tasks/list: %s' % e)
  except datastore_errors.NeedIndexError as e:
    raise handlers_exceptions.BadRequestException(
        'Requires new index, ask admin to create one.')
  except datastore_errors.BadArgumentError as e:
    raise handlers_exceptions.BadRequestException(
        'This combination is unsupported, sorry.')


BotFilters = namedtuple(
    'BotFilters',
    [
        # List of 'key:value' strings representing the dimensions of the bot.
        'dimensions',
        # Bool, if true bots will be filtered if not quarantined.
        'quarantined',
        # Bool, if true bots will be filtered if not in maintenance mode.
        'in_maintenance',
        # Bool, if true, bots will be filtered if not dead.
        'is_dead',
        # Bool, if true, bots will be filtered if idle.
        'is_busy',
    ])


def list_bots(filters, limit, cursor):
  """Returns a list of bots which match the filters.

  Args:
    filters: BotFilters namedtuple.
    limit: number of items to fetch.
    cursor: cursor from previous request or None.

  Returns:
    A list of `BotInfo` ndb entities.
  """
  # Check permission.
  # If the caller has global permission, it can access all bots.
  # Otherwise, it requires pool dimension to check ACL.
  pools = bot_management.get_pools_from_dimensions_flat(filters.dimensions)
  realms.check_bots_list_acl(pools)

  # Disable the in-process local cache. This is important, as there can be up
  # to a thousand entities loaded in memory, and this is a pure memory leak,
  # as there's no chance this specific instance will need these again,
  # therefore this leads to 'Exceeded soft memory limit' AppEngine errors.
  q = bot_management.BotInfo.query(default_options=ndb.QueryOptions(
      use_cache=False))
  try:
    q = bot_management.filter_dimensions(q, filters.dimensions)
    q = bot_management.filter_availability(q, filters.quarantined,
                                           filters.in_maintenance,
                                           filters.is_dead, filters.is_busy)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(str(e))
  # this is required to request MultiQuery for OR dimension support.
  q = q.order(bot_management.BotInfo._key)

  try:
    return datastore_utils.fetch_page(q, limit, cursor)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(
        'Inappropriate filter for tasks/list: %s' % e)


BotsCount = namedtuple(
    'BotCount',
    [
        # total number of bots.
        'count',
        # number of dead bots.
        'dead',
        # number of quarantined bots.
        'quarantined',
        # number of bots in maintenance.
        'maintenance',
        # number of non-idle bots.
        'busy',
    ])


def count_bots(dimensions):
  """Counts the number of bots in various states which match given dimensions.

  Args:
    dimensions: list of "key:value" strings.

  Returns:
    BotsCount named tuple.

  Raises:
    handlers_exceptions.BadRequestException if dimensions are invalid.
  """
  # Check permission.
  # If the caller has global permission, it can access all bots.
  # Otherwise, it requires pool dimension to check ACL.
  pools = bot_management.get_pools_from_dimensions_flat(dimensions)
  realms.check_bots_list_acl(pools)

  q = bot_management.BotInfo.query()
  try:
    q = bot_management.filter_dimensions(q, dimensions)
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(str(e))

  f_count = q.count_async()
  f_dead = bot_management.filter_availability(q, None, None, True,
                                              None).count_async()
  f_quarantined = bot_management.filter_availability(q, True, None, None,
                                                     None).count_async()
  f_maintenance = bot_management.filter_availability(q, None, True, None,
                                                     None).count_async()
  f_busy = bot_management.filter_availability(q, None, None, None,
                                              True).count_async()
  return BotsCount(count=f_count.get_result(),
                   dead=f_dead.get_result(),
                   quarantined=f_quarantined.get_result(),
                   maintenance=f_maintenance.get_result(),
                   busy=f_busy.get_result())


def _memcache_key(filters, now):
  # Floor now to minute to account for empty "end"
  end = filters.end or now.replace(second=0, microsecond=0)
  filters.tags.sort()
  return '%s|%s|%s|%s' % (filters.tags, filters.state, filters.start, end)


def count_tasks(filters, now):
  """Counts number of tasks which match the filter.
  Operation creates a cache of the count matching the filters.
  Args:
    filters: A TaskFilters object.
    now: datetime.datetime object representing current time. Used
      to create cache key.

  Returns:
    int matching the number of filters.

  Raises:
    handlers_exceptions.BadRequestException if invalid filter is provided.
  """
  # Check permission.
  # If the caller has global permission, it can access all tasks.
  # Otherwise, it requires pool dimension to check ACL.
  pools = bot_management.get_pools_from_dimensions_flat(filters.tags)
  realms.check_tasks_list_acl(pools)

  if not filters.start:
    raise handlers_exceptions.BadRequestException(
        'start (as epoch) is required')
  now = utils.utcnow()
  mem_key = _memcache_key(filters, now)
  count = memcache.get(mem_key, namespace='tasks_count')
  if count is not None:
    return count

  try:
    count = task_result.get_result_summaries_query(filters.start, filters.end,
                                                   filters.sort, filters.state,
                                                   filters.tags).count()
    memcache.add(mem_key, count, 24 * 60 * 60, namespace='tasks_count')
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(
        'Inappropriate filter for tasks/count: %s' % e)
  return count


def list_task_requests_no_realm_check(filters, limit, cursor):
  """Returns a list of task_request.TaskRequest items.

  This method does NOT perform a realm check. The original endpoints method
  allows consumers of the api to query for TaskRequests accross all realms
  as long as they are part a more restrictive `can_view_all_tasks` acl.

  Args:
    filters: TaskFilters named tuple.
    limit: number of items to return.
    cursor: cursoro from previous request.

  Returns:
    list of task requests.

  Raises:
    handlers_exceptions.BadRequestException if invalid filter is provided.
  """
  try:
    # Get the TaskResultSummary keys, then fetch the corresponding
    # TaskRequest entities.
    query = task_result.get_result_summaries_query(filters.start, filters.end,
                                                   filters.sort, filters.state,
                                                   filters.tags)
    keys, cursor = datastore_utils.fetch_page(query,
                                              limit,
                                              cursor,
                                              keys_only=True)
    items = ndb.get_multi(
        task_pack.result_summary_key_to_request_key(k) for k in keys)
    return items, cursor
  except ValueError as e:
    raise handlers_exceptions.BadRequestException(
        'Inappropriate filter for tasks/requests: %s' % e)
  except datastore_errors.NeedIndexError as e:
    logging.error('%s', e)
    raise handlers_exceptions.BadRequestException(
        'Requires new index, ask admin to create one.')
  except datastore_errors.BadArgumentError as e:
    logging.error('%s', e)
    raise handlers_exceptions.BadRequestException(
        'This combination is unsupported, sorry.')


def get_states(task_ids):
  """Gets a list of task states for the given task_ids.

  Will return PENDING for any task_id which is not associated with any task.

  Args:
    task_ids: list of str task_ids.

  Returns:
    list of task_result.TaskState for each task_id.

  Raises:
    handlers_exceptions.BadRequestException if any task_id is invalid.
  """

  try:
    task_results = task_result.fetch_task_results(list(task_ids))
  except ValueError as e:
    raise handlers_exceptions.BadRequestException('Invalid task_id: %s' %
                                                  str(e))
  return [
      result.state if result else task_result.State.PENDING
      for result in task_results
  ]


ServerDetails = namedtuple(
    'ServerDetails',
    [
        # str of commit hash of currently deployed swarming_bot.
        'bot_version',
        # str of commit hash of currently deployed swarming_server.
        'server_version',
        # A url to a task display server (e.g. milo).  This should have a %s
        # where a task id can go.
        'display_server_url_template',
        # str hostname of luci_config server.
        'luci_config',
        # Host of RBE-CAS viewer server.
        'cas_viewer_server',
    ])


def get_server_details():
  """Returns a ServerDetails namedtuple describing information on the swarming
  server environment"""
  host = 'https://' + os.environ['HTTP_HOST']

  cfg = config.settings()
  server_version = utils.get_app_version()

  return ServerDetails(
      bot_version=bot_code.get_bot_version(host)[0],
      server_version=server_version,
      display_server_url_template=cfg.display_server_url_template,
      luci_config=config.config.config_service_hostname(),
      cas_viewer_server=cfg.cas.viewer_server)


ClientPermissions = namedtuple(
    'ClientPermissions',
    [
        # Client delete a bot.
        'delete_bot',
        # Clients mass delete bots.
        'delete_bots',
        # Client can call terminate bot.
        'terminate_bot',
        # Client can get bot_config.
        'get_configs',
        'put_configs',
        # Client can cancel a single task with given task_id.
        'cancel_task',
        # Client may mass cancel many tasks.
        'cancel_tasks',
        # Client may access the bootstrap token for a given bot.
        'get_bootstrap_token',
        # List of pools which client may list bots.
        'list_bots',
        # List of pools for which a client may list tasks.
        'list_tasks',
    ])


def _can_cancel_task(task_id):
  if not task_id:
    # TODO(crbug.com/1066839):
    # This is for the compatibility until Web clients send task_id.
    # return False if task_id is not given.
    return acl._is_privileged_user()
  task_key, _ = to_keys(task_id)
  task = task_key.get()
  if not task:
    raise handlers_exceptions.NotFoundException('%s not found.' % task_id)
  return realms.can_cancel_task(task)


def get_permissions(bot_id, task_id, tags):
  """Checks what permissions are available for the client.

  Args:
    bot_id: str or None bot identifier.
    task_id: str or None task identifier.
    tags: list of str in the form of "key:value" describing tags.

  Returns:
    ClientPermissions named tuple.
  """
  pools_list_bots = [p for p in pools_config.known() if realms.can_list_bots(p)]
  pools_list_tasks = [
      p for p in pools_config.known() if realms.can_list_tasks(p)
  ]
  pool_tags = bot_management.get_pools_from_dimensions_flat(tags)
  return ClientPermissions(delete_bot=realms.can_delete_bot(bot_id),
                           delete_bots=realms.can_delete_bots(pool_tags),
                           terminate_bot=realms.can_terminate_bot(bot_id),
                           get_configs=acl.can_view_config(),
                           put_configs=acl.can_edit_config(),
                           cancel_task=_can_cancel_task(task_id),
                           cancel_tasks=realms.can_cancel_tasks(pool_tags),
                           get_bootstrap_token=acl.can_create_bot(),
                           list_bots=pools_list_bots,
                           list_tasks=pools_list_tasks)
