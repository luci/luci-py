# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""High level tasks execution scheduling API.

This is the interface closest to the HTTP handlers.

It supports retries* and deduping*.
TODO(maruel): * is not implemented and gratitious vaporware.

Overview of transactions:
- bot_reap_task() mutate a TaskToRun in a transaction.
- cron_abort_stale() uses transactions when aborting tasks.
"""

import contextlib
import datetime
import logging
import math
import random

from google.appengine.api import datastore_errors
from google.appengine.api import search
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import utils
from server import bot_management
from server import stats
from server import task_common
from server import task_request
from server import task_result
from server import task_to_run


### Private stuff.


_PROBABILITY_OF_QUICK_COMEBACK = 0.05


def _secs_to_ms(value):
  """Converts a seconds value in float to the number of ms as an integer."""
  return int(round(value * 1000.))


def _update_task(task_key, request, bot_id):
  """Reaps a task and insert the results entity.

  If bot_id is None, the task is declared EXPIRED.

  Returns:
    TaskRunResult if successful, None otherwise.
  """
  assert request.key == task_to_run.task_to_run_key_to_request_key(task_key)
  result_summary_future = task_result.request_key_to_result_summary_key(
      request.key).get_async()

  # Look if the TaskToRun is reapable once before doing the check inside the
  # transaction. This reduces the likelihood of failing this check inside
  # the transaction, which is an order of magnitude more costly.
  if not task_to_run.is_task_reapable(task_key, None):
    result_summary_future.wait()
    return None

  result_summary = result_summary_future.get_result()

  def run():
    task = task_to_run.is_task_reapable(task_key, None)
    if not task:
      return None, None
    task.queue_number = None
    # TODO(maruel): Use datastore_util.insert() to create the new try_number.
    run_result = task_result.new_run_result(request, 1, bot_id)
    if not bot_id:
      run_result.state = task_result.State.EXPIRED
      run_result.abandoned_ts = utils.utcnow()
      run_result.internal_failure = True
    result_summary.set_from_run_result(run_result)
    ndb.put_multi([task, run_result, result_summary])
    return run_result, task

  try:
    run_result, task = ndb.transaction(run, retries=0)
    if task:
      task_to_run.set_lookup_cache(task.key, False)
    return run_result
  except (
      apiproxy_errors.CancelledError,
      datastore_errors.BadRequestError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      RuntimeError):
    return None


def _fetch_for_update(run_result_key, bot_id):
  """Fetches all the data for a bot task update."""
  bot_key = bot_management.get_bot_key(bot_id)
  request_key = task_result.result_summary_key_to_request_key(
      task_result.run_result_key_to_result_summary_key(run_result_key))

  run_result, request, bot = ndb.get_multi(
      [run_result_key, request_key, bot_key])
  if not run_result:
    logging.error('No result found for %s', run_result_key)
    return None, None, None
  if run_result.bot_id != bot_id:
    logging.error(
        'Bot %s sent update for task %s owned by bot %s',
        bot_id, run_result.bot_id, run_result.key)
    return None, None, None
  return run_result, request, bot


def _update_stats(run_result, bot_id, request, completed):
  """Updates stats after a bot task update notification."""
  if completed:
    stats.add_run_entry(
        'run_completed', run_result.key,
        bot_id=bot_id,
        dimensions=request.properties.dimensions,
        runtime_ms=_secs_to_ms(run_result.duration.total_seconds()),
        user=request.user)
    stats.add_task_entry(
        'task_completed',
        task_result.request_key_to_result_summary_key(request.key),
        dimensions=request.properties.dimensions,
        pending_ms=_secs_to_ms(
            (run_result.completed_ts - request.created_ts).total_seconds()),
        user=request.user)
  else:
    stats.add_run_entry(
        'run_updated', run_result.key, bot_id=bot_id,
        dimensions=request.properties.dimensions)


### Public API.


def exponential_backoff(attempt_num):
  """Returns an exponential backoff value in seconds."""
  assert attempt_num >= 0
  if random.random() < _PROBABILITY_OF_QUICK_COMEBACK:
    # Randomly ask the bot to return quickly.
    return 1.0
  return min(60.0, math.pow(1.5, min(attempt_num, 10) + 1))


def make_request(data):
  """Creates and stores all the entities for a new task request.

  The number of entities created is 3: TaskRequest, TaskResultSummary and
  TaskToRun.

  The TaskRequest is saved first as a DB transaction, then TaskResultSummary and
  TaskToRun are saved as a single DB RPC. The Search index is also updated
  in-between.

  Arguments:
  - data: is in the format expected by task_request.make_request().

  Returns:
    tuple(TaskRequest, TaskResultSummary). TaskToRun is not returned.
  """
  request = task_request.make_request(data)

  # At this point, the request is now in the DB but not yet in a mode where it
  # can be triggered or visible. Index it right away so it is searchable. If any
  # of remaining calls in this function fail, the TaskRequest and Search
  # Document will simply point to an incomplete task, which will be ignored.
  #
  # Creates the entities TaskToRun and TaskResultSummary but do not save them
  # yet. TaskRunResult will be created once a bot starts it.
  task = task_to_run.new_task_to_run(request)
  result_summary = task_result.new_result_summary(request)

  # Do not specify a doc_id, as they are guaranteed to be monotonically
  # increasing and searches are done in reverse order, which fits exactly the
  # created_ts ordering. This is useful because DateField is precise to the date
  # (!) and NumberField is signed 32 bits so the best it could do with EPOCH is
  # second resolution up to year 2038.
  index = search.Index(name='requests')
  packed = task_common.pack_result_summary_key(result_summary.key)
  doc = search.Document(
      fields=[
        search.TextField(name='name', value=request.name),
        search.AtomField(name='id', value=packed),
      ])
  # Even if it fails here, we're still fine, as the task is not "alive" yet.
  index.put([doc])

  # Storing these entities makes this task live. It is important at this point
  # that the HTTP handler returns as fast as possible, otherwise the task will
  # be run but the client will not know about it.
  ndb.put_multi([result_summary, task])
  stats.add_task_entry(
      'task_enqueued', result_summary.key,
      dimensions=request.properties.dimensions,
      user=request.user)
  return request, result_summary


def unpack_result_summary_key(packed_key):
  """Returns the TaskResultSummary ndb.Key from a packed key.

  The expected format of |packed_key| is %x.
  """
  key_id = int(packed_key, 16)
  if key_id & 0xff:
    raise ValueError('Can\'t reference to a specific try result.')
  request_key = task_request.id_to_request_key(key_id)
  return task_result.request_key_to_result_summary_key(request_key)


def unpack_run_result_key(packed_key):
  """Returns the TaskRunResult ndb.Key from a packed key.

  The expected format of |packed_key| is %x.
  """
  key_id = int(packed_key, 16)
  run_id = key_id & 0xff
  if not run_id:
    raise ValueError('Can\'t reference to the overall task result.')
  result_summary_key = unpack_result_summary_key('%x' % (key_id & ~0xff))
  return task_result.result_summary_key_to_run_result_key(
      result_summary_key, run_id)


def bot_reap_task(dimensions, bot_id):
  """Reaps a TaskToRun if one is available.

  The process is to find a TaskToRun where its .queue_number is set, then
  create a TaskRunResult for it.

  Returns:
    tuple of (TaskRequest, TaskRunResult) for the task that was reaped.
    The TaskToRun involved is not returned.
  """
  assert bot_id
  bot = bot_management.get_bot_key(bot_id).get()
  if bot and bot.quarantined:
    return None, None

  q = task_to_run.yield_next_available_task_to_dispatch(dimensions)
  # When a large number of bots try to reap hundreds of tasks simultaneously,
  # they'll constantly fail to call reap_task_to_run() as they'll get preempted
  # by other bots. So randomly jump farther in the queue when the number of
  # failures is too large.
  failures = 0
  to_skip = 0
  total_skipped = 0
  for request, task in q:
    if to_skip:
      to_skip -= 1
      total_skipped += 1
      continue

    run_result = _update_task(task.key, request, bot_id)
    if not run_result:
      failures += 1
      #logging.warning('Failed to reap %d', task.key.integer_id())
      # TODO(maruel): Add unit test!
      # Every 3 failures starting on the very first one, jump randomly ahead of
      # the pack. This reduces the contention where hundreds of bots fight for
      # exactly the same task while there's many ready to be run waiting in the
      # queue.
      if (failures % 3) == 1:
        # TODO(maruel): Choose curve that makes the most sense. The tricky part
        # is finding a good heuristic to guess the load without much information
        # available in this content. When 'failures' is high, this means a lot
        # of bots are reaping tasks like crazy, which means there is a good flow
        # of tasks going on. On the other hand, skipping too much is useless. So
        # it should have an initial bump but then slow down on skipping.
        to_skip = min(int(round(random.gammavariate(3, 1))), 30)
      continue

    # Try to optimize these values but do not add as formal stats (yet).
    logging.info('failed %d, skipped %d', failures, total_skipped)

    pending_time = run_result.started_ts - request.created_ts
    stats.add_run_entry(
        'run_started', run_result.key,
        bot_id=bot_id,
        dimensions=request.properties.dimensions,
        pending_ms=_secs_to_ms(pending_time.total_seconds()),
        user=request.user)
    return request, run_result
  if failures:
    logging.info(
        'Chose nothing (failed %d, skipped %d)', failures, total_skipped)
  return None, None


@contextlib.contextmanager
def bot_update_task(
    run_result_key, bot_id, command_index, output, output_chunk_start,
    exit_code, duration):
  """Updates a TaskRunResult, returns the packets to save in a context.

  Arguments:
  - run_result_key: ndb.Key to TaskRunResult.
  - bot_id: Self advertised bot id to ensure it's the one expected.
  - command_index: index in TaskRequest.properties.commands.
  - output: Data to append to this command output.
  - output_chunk_start: Index of output in the stdout stream.
  - exit_code: Mark that this command, as specified by command_index, is
      terminated.
  - duration: Time spent in seconds for this command.

  Invalid states, these are flat out refused:
  - A command is updated after it had an exit code assigned to.
  - Out of order processing of command_index.

  The trickiest part of this function is that partial updates must be
  specifically handled, in particular:
  - TaskRunResult was updated but not TaskResultSummary.
  - TaskChunkOutput was partially writen, with Result entities updated or not.
  """
  assert output_chunk_start is None or isinstance(output_chunk_start, int)
  assert output is None or isinstance(output, str)

  run_result, request, bot = _fetch_for_update(run_result_key, bot_id)
  if not run_result:
    yield None
    return

  now = utils.utcnow()
  UPDATE_RATE_DELTA = datetime.timedelta(seconds=30)
  to_put = []
  futures = []
  if (bot and
      (bot.task != run_result_key or
       now - bot.last_seen_ts > UPDATE_RATE_DELTA)):
    bot.last_seen_ts = now
    bot.task = run_result_key
    # Because Bot is not in the same root entity, save it independently.
    # Otherwise it would result in requiring an xg=True transaction. It's not a
    # big deal even if this entity is not strongly consistent with the others.
    futures.append(bot.put_async())

  if len(run_result.exit_codes) not in (command_index, command_index+1):
    raise ValueError('Unexpected ordering')

  if (duration is None) != (exit_code is None):
    raise ValueError('duration is expected if and only if a command completes')

  if exit_code is not None:
    # The command |command_index| completed.
    run_result.durations.append(duration)
    run_result.exit_codes.append(exit_code)

  task_completed = (
      len(run_result.exit_codes) == len(request.properties.commands))
  if task_completed:
    run_result.state = task_result.State.COMPLETED
    run_result.completed_ts = now

  if output:
    to_put.extend(
        run_result.append_output(
            command_index, output, output_chunk_start or 0))

  to_put.extend(task_result.prepare_put_run_result(run_result))

  yield to_put

  _update_stats(run_result, bot_id, request, task_completed)
  ndb.Future.wait_all(futures)


def bot_kill_task(run_result):
  """Terminates a task that is currently running as an internal failure."""
  request_future = run_result.request_key.get_async()
  run_result.state = task_result.State.BOT_DIED
  run_result.internal_failure = True
  run_result.abandoned_ts = utils.utcnow()
  ndb.put_multi(task_result.prepare_put_run_result(run_result))
  request = request_future.get_result()
  stats.add_run_entry(
      'run_bot_died', run_result.key,
      bot_id=run_result.bot_id,
      dimensions=request.properties.dimensions,
      user=request.user)


def search_by_name(word, cursor_str, limit):
  """Returns TaskResultSummary in -created_ts order containing the word."""
  cursor = search.Cursor(web_safe_string=cursor_str, per_result=True)
  index = search.Index(name='requests')

  def item_to_id(item):
    for field in item.fields:
      if field.name == 'id':
        return field.value

  # The code is structured to handle incomplete entities but still return
  # 'limit' items. This is done by fetching a few more entities than necessary,
  # then keeping track of the cursor per item so the right cursor can be
  # returned.
  opts = search.QueryOptions(limit=limit + 5, cursor=cursor)
  results = index.search(search.Query('name:%s' % word, options=opts))
  result_summary_keys = []
  cursors = []
  for item in results.results:
    value = item_to_id(item)
    if value:
      result_summary_keys.append(unpack_result_summary_key(value))
      cursors.append(item.cursor)

  # Handle None result value. See make_request() for details about how this can
  # happen.
  tasks = []
  cursor = None
  for task, c in zip(ndb.get_multi(result_summary_keys), cursors):
    if task:
      cursor = c
      tasks.append(task)
      if len(tasks) == limit:
        # Drop the rest.
        break
  else:
    if len(cursors) == limit + 5:
      while len(tasks) < limit:
        # Go into the slow path, seems like we got a lot of corrupted items.
        opts = search.QueryOptions(limit=limit-len(tasks) + 5, cursor=cursor)
        results = index.search(search.Query('name:%s' % word, options=opts))
        if not results.results:
          # Nothing else.
          cursor = None
          break
        for item in results.results:
          value = item_to_id(item)
          if value:
            cursor = item.cursor
            task = unpack_result_summary_key(value).get()
            if task:
              tasks.append(task)
              if len(tasks) == limit:
                break

  cursor_str = cursor.web_safe_string if cursor else None
  return tasks, cursor_str


### Cron job.


def cron_abort_expired_task_to_run():
  """Aborts expired TaskToRun requests to execute a TaskRequest on a bot.

  Three reasons can cause this situation:
  - Higher throughput of task requests incoming than the rate task requests
    being completed, e.g. there's not enough bots to run all the tasks that gets
    in at the current rate. That's normal overflow and must be handled
    accordingly.
  - No bot connected that satisfies the requested dimensions. This is trickier,
    it is either a typo in the dimensions or bots all died and the admins must
    reconnect them.
  - Server has internal failures causing it to fail to either distribute the
    tasks or properly receive results from the bots.
  """
  killed = 0
  skipped = 0
  try:
    for task in task_to_run.yield_expired_task_to_run():
      # Create the TaskRunResult and kill it immediately.
      request = task.request_key.get()
      if _update_task(task.key, request, None):
        killed += 1
        stats.add_task_entry(
            'task_request_expired',
            task_result.request_key_to_result_summary_key(
                request.key),
            dimensions=request.properties.dimensions,
            user=request.user)
      else:
        # It's not a big deal, the bot will continue running.
        skipped += 1
  finally:
    # TODO(maruel): Use stats_framework.
    logging.info('Killed %d task, skipped %d', killed, skipped)
  return killed


def cron_abort_bot_died():
  """Aborts stale TaskRunResult where the bot stopped sending updates.

  Basically, sets the task result to State.BOT_DIED in this case.
  """
  total = 0
  try:
    for run_result in task_result.yield_run_results_with_dead_bot():
      # Implement automatic retry;
      # https://code.google.com/p/swarming/issues/detail?id=108
      bot_kill_task(run_result)
      total += 1
  finally:
    # TODO(maruel): Use stats_framework.
    logging.info('Killed %d task', total)
  return total
