# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Contains code which is common between bot the prpc API and the protorpc api.
"""
from google.appengine.api import datastore_errors

import handlers_exceptions

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
    auth.AuthorizationError if bot fails realm authorization test.
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
