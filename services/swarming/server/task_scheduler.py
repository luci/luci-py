# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""High level tasks execution scheduling API.

This is the interface closest to the HTTP handlers.

It supports sharding, retries* and deduping*.
TODO(maruel): * is not implemented and gratitious vaporware.

Overview of transactions:
- bot_reap_task() mutate a TaskShardToRun in a transaction.
- cron_abort_stale() uses transactions when aborting tasks.
"""

import logging
import math
import random

from google.appengine.ext import ndb

from server import stats_new as stats
from server import task_common
from server import task_request
from server import task_result
from server import task_shard_to_run


_PROBABILITY_OF_QUICK_COMEBACK = 0.05


### Public API.


def exponential_backoff(attempt_num):
  """Returns an exponential backoff value in seconds."""
  assert attempt_num >= 0
  if random.random() < _PROBABILITY_OF_QUICK_COMEBACK:
    # Randomly ask the bot to return quickly.
    return 1.0
  return min(60.0, math.pow(1.5, min(attempt_num + 1, 10) + 1))


def new_request(data):
  """Creates and store all the entities for a new task request.

  The number of entities created is N+2 where N is the number of shards, one
  TaskRequest, one TaskResultSummary and N TaskShardToRun.

  Arguments:
  - data: is in the format expected by task_request.new_request().
  """
  request = task_request.new_request(data)
  # Creates the entities TaskShardToRun and TaskResultSummary.
  # TaskShardResult will be created once a bot starts it.
  shard_runs = task_shard_to_run.new_shards_to_run_for_request(request)
  items = [task_result.new_result_summary(request)] + shard_runs
  ndb.put_multi(items)
  stats.add_entry('shard_enqueued', request.properties.number_shards)
  return request, shard_runs


def pack_shard_result_key(shard_result_key):
  """Returns the encoded key that is safe to use in HTTP requests."""
  shard_to_run_key = shard_result_key.parent()
  return '%x-%d' % (
      shard_to_run_key.integer_id(), shard_result_key.integer_id())


def unpack_shard_result_key(packed_key):
  """Returns the TaskShardResult ndb.Key from a packed key."""
  if '-' in packed_key:
    shard_key_id, try_number = packed_key.split('-', 1)
    shard_key_id = int(shard_key_id, 16)
    try_number = int(try_number)
  else:
    shard_key_id = int(packed_key, 16)
    # TODO(maruel): Fetch it from TaskResultSummary.
    try_number = 1
  shard_to_run_key = task_shard_to_run.shard_id_to_key(shard_key_id)
  return task_result.shard_to_run_key_to_shard_result_key(
      shard_to_run_key, try_number)


def bot_reap_task(dimensions, bot_id):
  """Reaps a TaskShardToRun if one is available.

  The process is to find a TaskShardToRun where its .queue_number is set, then
  create a TaskShardResult for it.

  Returns:
    tuple of (TaskShardToRun, TaskShardResult) for the shard that was reaped.
  """
  bot_id = bot_id or dimensions['hostname']
  q = task_shard_to_run.yield_next_available_shard_to_dispatch(dimensions)
  # When a large number of bots try to reap hundreds of tasks simultaneously,
  # they'll constantly fail to call reap_shard_to_run() as they'll get preempted
  # by other bots. So randomly jump farther in the queue when the number of
  # failures is too large.
  failures = 0
  to_skip = 0
  total_skipped = 0
  for request, shard_to_run in q:
    if to_skip:
      to_skip -= 1
      total_skipped += 1
      continue

    if not task_shard_to_run.reap_shard_to_run(shard_to_run.key):
      failures += 1
      #logging.warning('Failed to reap %d', shard_to_run.key.integer_id())
      # TODO(maruel): Add unit test!
      # Every 3 failures starting on the very first one, jump randomly ahead of
      # the pack. This reduces the contention where hundreds of bots fight for
      # exactly the same shards while there's many ready to be run waiting in
      # the queue.
      if (failures % 3) == 1:
        # TODO(maruel): Choose curve that makes the most sense. The tricky part
        # is finding a good heuristic to guess the load without much information
        # available in this content. When 'failures' is high, this means a lot
        # of bots are reaping tasks like crazy, which means there is a good flow
        # of tasks going on. On the other hand, skipping too much is useless. So
        # it should have an initial bump but then slow down on skipping.
        to_skip = min(int(round(random.gammavariate(3, 1))), 30)
      continue
    try:
      # TODO(maruel): Use datastore_util.insert() to create the new try_number.
      shard_result = task_result.new_shard_result(shard_to_run.key, 1, bot_id)
      task_result.put_shard_result(shard_result)
      wait_time = (task_common.utcnow() - request.created_ts).total_seconds()
      logging.info(
          'Chose %s (failed %d, skipped %d) waited %.2fs',
          pack_shard_result_key(shard_result.key),
          failures,
          total_skipped,
          wait_time)
      stats.add_entry('shard_assigned', int(round(wait_time * 1000.)))
      return request, shard_result
    except:
      logging.error('Lost TaskShardToRun %s', shard_to_run.key)
      raise
  if failures:
    logging.info(
        'Chose nothing (failed %d, skipped %d)', failures, total_skipped)
  return None, None


def bot_update_task(shard_result_key, data, bot_id):
  """Updates a TaskShardResult entity with the latest info from the bot."""
  now = task_common.utcnow()
  shard_result = shard_result_key.get()
  if not shard_result:
    logging.error('No shard result found for %s', shard_result_key)
    return False
  if shard_result.task_state not in task_result.State.STATES_RUNNING:
    logging.error(
        'A zombie bot reappeared after the time out.\n%s; %s',
        shard_result.bot_id, shard_result.task_state)
    return False
  if shard_result.bot_id != bot_id:
    logging.error(
        'Bot %s sent updated for task %s owned by bot %s',
        bot_id, shard_result.bot_id, shard_result.key)
    return False

  # TODO(maruel): Wrong but that's the current behavior of the swarming bots.
  # Eventually change the bot protocol to be able to send more details.
  completed = 'exit_codes' in data
  if completed:
    shard_result.task_state = task_result.State.COMPLETED
    shard_result.completed_ts = now
    shard_result.exit_codes.extend(data['exit_codes'])
    duration = (now - shard_result.started_ts).total_seconds()
  if 'outputs' in data:
    shard_result.outputs.extend(data['outputs'])
  task_result.put_shard_result(shard_result)
  if completed:
    stats.add_entry('shard_completed', int(round(duration * 1000.)))
  else:
    stats.add_entry('shard_updated', pack_shard_result_key(shard_result.key))
  return True


### Cron job.


def cron_abort_expired_shard_to_run():
  """Aborts expired TaskShardToRun requests to execute a TaskRequest on a bot.

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
    for shard_to_run in task_shard_to_run.yield_expired_shard_to_run():
      # TODO(maruel): Run all this inside a single transaction.
      if task_shard_to_run.reap_shard_to_run(shard_to_run.key):
        # Create the TaskShardResult and kill it immediately.
        killed += 1
        shard_result = task_result.new_shard_result(shard_to_run.key, 1, None)
        task_result.terminate_shard_result(
            shard_result, task_result.State.EXPIRED)
        stats.add_entry(
            'shard_request_expired', pack_shard_result_key(shard_result.key))
      else:
        # It's not a big deal, the bot will continue running.
        skipped += 1
  finally:
    # TODO(maruel): Use stats_framework.
    logging.info('Killed %d task, skipped %d', killed, skipped)
  return killed


def cron_abort_bot_died():
  """Aborts stale TaskShardResult where the bot stopped sending updates.

  Basically, sets the task result to Stae.BOT_DIED in this case.
  """
  total = 0
  try:
    for shard_result in task_result.yield_shard_results_without_update():
      task_result.terminate_shard_result(
          shard_result, task_result.State.BOT_DIED)
      stats.add_entry('shard_bot_died', pack_shard_result_key(shard_result.key))
      total += 1
  finally:
    # TODO(maruel): Use stats_framework.
    logging.info('Killed %d task', total)
  return total


def cron_sync_all_result_summary():
  """Ensures consistency between TaskShardResult and TaskResultSummary."""
  return task_result.sync_all_result_summary()
