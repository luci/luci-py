# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""High level tasks execution scheduling API.

This is the interface closest to the HTTP handlers.
"""

import contextlib
import logging
import math
import random

from google.appengine.api import datastore_errors
from google.appengine.api import search
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import utils
from server import stats
from server import task_request
from server import task_result
from server import task_to_run


### Private stuff.


_PROBABILITY_OF_QUICK_COMEBACK = 0.05


def _secs_to_ms(value):
  """Converts a seconds value in float to the number of ms as an integer."""
  return int(round(value * 1000.))


def _expire_task(to_run_key, request):
  """Expires a TaskResultSummary and unschedules the TaskToRun.

  Returns:
    True on success.
  """
  # Look if the TaskToRun is reapable once before doing the check inside the
  # transaction. This reduces the likelihood of failing this check inside the
  # transaction, which is an order of magnitude more costly.
  if not to_run_key.get().is_reapable:
    logging.info('Not reapable anymore')
    return None

  result_summary_key = task_result.request_key_to_result_summary_key(
      request.key)

  def run():
    # 2 concurrent GET, one PUT. Optionally with an additional serialized GET.
    to_run_future = to_run_key.get_async()
    result_summary_future = result_summary_key.get_async()
    to_run = to_run_future.get_result()
    if not to_run or not to_run.is_reapable:
      result_summary_future.wait()
      return False

    to_run.queue_number = None
    result_summary = result_summary_future.get_result()
    if result_summary.try_number:
      # It's a retry that is being expired. Keep the old state. That requires an
      # additional pipelined GET but that shouldn't be the common case.
      run_result = result_summary.run_result_key.get()
      result_summary.set_from_run_result(run_result, request)
    else:
      result_summary.state = task_result.State.EXPIRED
    result_summary.abandoned_ts = utils.utcnow()
    ndb.put_multi([to_run, result_summary])
    return True

  try:
    success = ndb.transaction(run)
    if success:
      task_to_run.set_lookup_cache(to_run_key, False)
      logging.info(
          'Expired %s',
          task_result.pack_result_summary_key(result_summary_key))
    return success
  except (
      apiproxy_errors.CancelledError,
      datastore_errors.BadRequestError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      RuntimeError) as e:
    packed = task_result.pack_result_summary_key(result_summary_key)
    logging.info('Failed to reap %s: %s', packed, e)
    return False


def _reap_task(to_run_key, request, bot_id, bot_version):
  """Reaps a task and insert the results entity.

  Returns:
    TaskRunResult if successful, None otherwise.
  """
  assert bot_id, bot_id
  assert request.key == task_to_run.task_to_run_key_to_request_key(to_run_key)
  result_summary_key = task_result.request_key_to_result_summary_key(
      request.key)

  def run():
    # 2 GET, 1 PUT at the end.
    to_run_future = to_run_key.get_async()
    result_summary_future = result_summary_key.get_async()
    to_run = to_run_future.get_result()
    if not to_run or not to_run.is_reapable:
      result_summary_future.wait()
      return None
    to_run.queue_number = None
    result_summary = result_summary_future.get_result()
    run_result = task_result.new_run_result(
        request, (result_summary.try_number or 0) + 1, bot_id, bot_version)
    result_summary.set_from_run_result(run_result, request)
    ndb.put_multi([to_run, run_result, result_summary])
    return run_result

  try:
    run_result = ndb.transaction(run, retries=0)
    if run_result:
      task_to_run.set_lookup_cache(to_run_key, False)
    return run_result
  except (
      apiproxy_errors.CancelledError,
      datastore_errors.BadRequestError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      RuntimeError) as e:
    logging.warning('Failed to reap: %s', e)
    return None


def _fetch_for_update(run_result_key, bot_id):
  """Fetches all the data for a bot task update."""
  request_key = task_result.result_summary_key_to_request_key(
      task_result.run_result_key_to_result_summary_key(run_result_key))
  run_result, request = ndb.get_multi([run_result_key, request_key])
  if not run_result:
    logging.error('No result found for %s', run_result_key)
    return None, None
  if run_result.bot_id != bot_id:
    logging.error(
        'Bot %s sent update for task %s owned by bot %s',
        bot_id, run_result.bot_id, run_result.key)
    return None, None
  return run_result, request


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


def _handle_dead_bot(run_result_key):
  """Handles TaskRunResult where its bot has stopped showing sign of life.

  Transactionally updates the entities depending on the state of this task. The
  task may be retried automatically, canceled or left alone.

  Returns:
    True if the task was retried, False if the task was killed, None if no
    action was done.
  """
  result_summary_key = task_result.run_result_key_to_result_summary_key(
      run_result_key)
  request_key = task_result.result_summary_key_to_request_key(
      result_summary_key)
  request_future = request_key.get_async()
  now = utils.utcnow()
  server_version = utils.get_app_version()
  packed = task_result.pack_run_result_key(run_result_key)
  logging.info('Handling dead bot task %s', packed)
  request = request_future.get_result()
  to_run_key = task_to_run.request_to_task_to_run_key(request)

  def run():
    """Returns tuple(Result, bot_id)."""
    # Do one GET, one PUT at the end.
    run_result, result_summary, to_run = ndb.get_multi(
        (run_result_key, result_summary_key, to_run_key))
    if run_result.state != task_result.State.RUNNING:
      # It was updated already or not updating last. Likely DB index was stale.
      return None, run_result.bot_id

    if (not run_result.server_versions or
        run_result.server_versions[-1] != server_version):
      run_result.server_versions.append(server_version)

    if result_summary.try_number != run_result.try_number:
      # Not updating correct run_result, cancel it without touching
      # result_summary.
      to_put = (run_result,)
      run_result.state = task_result.State.BOT_DIED
      run_result.internal_failure = True
      run_result.abandoned_ts = now
      result = False
    elif result_summary.try_number == 1:
      # Retry it.
      to_put = (run_result, result_summary, to_run)
      to_run.queue_number = task_to_run.gen_queue_number_key(request)
      run_result.state = task_result.State.BOT_DIED
      run_result.internal_failure = True
      run_result.abandoned_ts = now
      # Do not sync data from run_result to result_summary, since the task is
      # being retried.
      result_summary.reset_to_pending()
      result = True
    else:
      # Cancel it, there was more than one try.
      to_put = (run_result, result_summary)
      run_result.state = task_result.State.BOT_DIED
      run_result.internal_failure = True
      run_result.abandoned_ts = now
      result_summary.set_from_run_result(run_result, request)
      result = False
    ndb.put_multi(to_put)
    return result, run_result.bot_id

  try:
    success, bot_id = ndb.transaction(run)
    if success is not None:
      task_to_run.set_lookup_cache(to_run_key, success)
      if not success:
        stats.add_run_entry(
            'run_bot_died', run_result_key,
            bot_id=bot_id[0],
            dimensions=request.properties.dimensions,
            user=request.user)
      else:
        logging.info('Retried')
    else:
      logging.info('Ignored')
    return success
  except (
      apiproxy_errors.CancelledError,
      datastore_errors.BadRequestError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      RuntimeError) as e:
    logging.info('Failed retrying task\n%s\n%s', packed, e)
    return None


def _copy_entity(src, dst, skip_list):
  """Copies the attributes of entity src into dst.

  It doesn't copy the key nor any member in skip_list.
  """
  assert type(src) == type(dst), '%s!=%s' % (src.__class__, dst.__class__)
  # Access to a protected member _XX of a client class - pylint: disable=W0212
  kwargs = {
    k: getattr(src, k) for k, v in src.__class__._properties.iteritems()
    if not isinstance(v, ndb.ComputedProperty) and k not in skip_list
  }
  dst.populate(**kwargs)


### Public API.


def exponential_backoff(attempt_num):
  """Returns an exponential backoff value in seconds."""
  assert attempt_num >= 0
  if random.random() < _PROBABILITY_OF_QUICK_COMEBACK:
    # Randomly ask the bot to return quickly.
    return 1.0

  # Enforces more frequent polls on canary.
  max_wait = 3. if utils.is_canary() else 60.
  return min(max_wait, math.pow(1.5, min(attempt_num, 10) + 1))


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

  dupe_future = None
  if request.properties.idempotent:
    # Find a previously run task that is also idempotent and completed. Start a
    # query to fetch items that can be used to dedupe the task. See the comment
    # for this property for more details.
    dupe_future = task_result.TaskResultSummary.query(
        task_result.TaskResultSummary.properties_hash==
            request.properties.properties_hash).get_async()

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
  packed = task_result.pack_result_summary_key(result_summary.key)
  doc = search.Document(
      fields=[
        search.TextField(name='name', value=request.name),
        search.AtomField(name='id', value=packed),
      ])
  # Even if it fails here, we're still fine, as the task is not "alive" yet.
  index.put([doc])

  if dupe_future:
    # Reuse the results!
    dupe_summary = dupe_future.get_result()
    if dupe_summary:
      # If there's a bug, commenting out this block is sufficient to disable the
      # functionality.
      # Setting task.queue_number to None removes it from the scheduling.
      task.queue_number = None
      _copy_entity(dupe_summary, result_summary, ('created_ts', 'name', 'user'))
      result_summary.try_number = 0
      result_summary.deduped_from = task_result.pack_run_result_key(
          dupe_summary.run_result_key)

  # Storing these entities makes this task live. It is important at this point
  # that the HTTP handler returns as fast as possible, otherwise the task will
  # be run but the client will not know about it.
  ndb.put_multi([result_summary, task])
  stats.add_task_entry(
      'task_enqueued', result_summary.key,
      dimensions=request.properties.dimensions,
      user=request.user)
  return request, result_summary


def bot_reap_task(dimensions, bot_id, bot_version):
  """Reaps a TaskToRun if one is available.

  The process is to find a TaskToRun where its .queue_number is set, then
  create a TaskRunResult for it.

  Returns:
    tuple of (TaskRequest, TaskRunResult) for the task that was reaped.
    The TaskToRun involved is not returned.
  """
  assert bot_id
  q = task_to_run.yield_next_available_task_to_dispatch(dimensions)
  # When a large number of bots try to reap hundreds of tasks simultaneously,
  # they'll constantly fail to call reap_task_to_run() as they'll get preempted
  # by other bots. So randomly jump farther in the queue when the number of
  # failures is too large.
  failures = 0
  to_skip = 0
  total_skipped = 0
  for request, to_run in q:
    if to_skip:
      to_skip -= 1
      total_skipped += 1
      continue

    run_result = _reap_task(to_run.key, request, bot_id, bot_version)
    if not run_result:
      failures += 1
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

  Yields:
  - tuple(list(entities), bool(completed).
  """
  assert output_chunk_start is None or isinstance(output_chunk_start, int)
  assert output is None or isinstance(output, str)

  run_result, request = _fetch_for_update(run_result_key, bot_id)
  if not run_result:
    yield None, False
    return

  now = utils.utcnow()

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

  to_put = []
  if output:
    to_put.extend(
        run_result.append_output(
            command_index, output, output_chunk_start or 0))

  to_put.extend(task_result.prepare_put_run_result(run_result, request))

  yield to_put, task_completed

  _update_stats(run_result, bot_id, request, task_completed)


def bot_kill_task(run_result_key, bot_id):
  """Terminates a task that is currently running as an internal failure.

  Returns:
    str if an error message.
  """
  result_summary_key = task_result.run_result_key_to_result_summary_key(
      run_result_key)
  request_key = task_result.result_summary_key_to_request_key(
      result_summary_key)
  request_future = request_key.get_async()
  server_version = utils.get_app_version()
  now = utils.utcnow()
  packed = task_result.pack_run_result_key(run_result_key)

  def run():
    run_result, result_summary = ndb.get_multi(
        (run_result_key, result_summary_key))
    if (not run_result.server_versions or
        run_result.server_versions[-1] != server_version):
      run_result.server_versions.append(server_version)

    if bot_id and run_result.bot_id != bot_id:
      return None, 'Bot %s sent task kill for task %s owned by bot %s' % (
          bot_id, packed, run_result.bot_id)

    if run_result.state == task_result.State.BOT_DIED:
      return None, 'Task %s was already killed' % packed

    run_result.state = task_result.State.BOT_DIED
    run_result.internal_failure = True
    run_result.abandoned_ts = now
    result_summary.set_from_run_result(run_result, None)
    ndb.put_multi((run_result, result_summary))
    return run_result, None

  try:
    run_result, msg = ndb.transaction(run)
    request = request_future.get_result()
    if run_result:
      stats.add_run_entry(
          'run_bot_died', run_result.key,
          bot_id=run_result.bot_id,
          dimensions=request.properties.dimensions,
          user=request.user)
    return msg
  except (
      apiproxy_errors.CancelledError,
      datastore_errors.BadRequestError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      RuntimeError) as e:
    return 'Failed killing task %s: %s' % (packed, e)


def cancel_task(summary_key):
  """Cancels a task if possible."""
  request_key = task_result.result_summary_key_to_request_key(summary_key)
  task_key = task_to_run.request_to_task_to_run_key(request_key.get())
  task_to_run.abort_task_to_run(task_key.get())
  result_summary = summary_key.get()
  ok = False
  was_running = False
  if result_summary.can_be_canceled:
    ok = True
    was_running = result_summary.state == task_result.State.RUNNING
    result_summary.state = task_result.State.CANCELED
    result_summary.abandoned_ts = utils.utcnow()
    result_summary.put()
  return ok, was_running


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
    for to_run in task_to_run.yield_expired_task_to_run():
      request = to_run.request_key.get()
      if _expire_task(to_run.key, request):
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


def cron_handle_bot_died():
  """Aborts or retry stale TaskRunResult where the bot stopped sending updates.

  If the task was at its first try, it'll be retried. Otherwise the task will be
  canceled.
  """
  ignored = 0
  killed = 0
  retried = 0
  try:
    for run_result_key in task_result.yield_run_result_keys_with_dead_bot():
      result = _handle_dead_bot(run_result_key)
      if result is True:
        retried += 1
      elif result is False:
        killed += 1
      else:
        ignored += 1
  finally:
    # TODO(maruel): Use stats_framework.
    logging.info('Killed %d; retried %d; ignored: %d', killed, retried, ignored)
  return killed, retried, ignored
