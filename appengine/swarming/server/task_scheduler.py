# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""High level tasks execution scheduling API.

This is the interface closest to the HTTP handlers.
"""

import collections
import datetime
import json
import logging
import math
import random
import time
import urlparse
import uuid

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.protobuf import timestamp_pb2

from components import auth
from components import datastore_utils
from components import pubsub
from components import utils

import backend_conversions
import handlers_exceptions
import ts_mon_metrics

from proto.api_v2.swarming_pb2 import TaskState
from server import bot_management
from server import config
from server import external_scheduler
from server import pools_config
from server import rbe
from server import resultdb
from server import service_accounts_utils
from server import task_pack
from server import task_queues
from server import task_request
from server import task_result
from server import task_to_run

from bb.go.chromium.org.luci.buildbucket.proto import task_pb2


### Private stuff.


_PROBABILITY_OF_QUICK_COMEBACK = 0.05

# When falling back from external scheduler, requests that belong to any
# external scheduler are ignored for this duration at the beginning of their
# life. This number should be larger than the bot polling period.
_ES_FALLBACK_SLACK = datetime.timedelta(minutes=6)


# Non-essential bot information for reaping a task
BotDetails = collections.namedtuple('BotDetails',
    ['bot_version', 'logs_cloud_project'])


class Error(Exception):
  pass


class ClaimError(Error):
  pass


class TaskExistsException(Error):
  pass


def _expire_slice_tx(request, to_run_key, terminal_state, capacity, es_cfg):
  """Expires a TaskToRunShardXXX and enqueues the next one, if necessary.

  Called as a ndb transaction by _expire_slice().

  Arguments:
    request: the TaskRequest instance with all slices.
    to_run_key: the TaskToRunShard to expire.
    terminal_state: the task state to set if this slice is the last one.
    capacity: dict {slice index => True if can run it}, None to skip the check.
    es_cfg: ExternalSchedulerConfig for this task.

  Returns:
    (
        TaskResultSummary if updated it,
        TaskToRunShardXXX matching to_run_key if expired it,
        TaskToRunShardXXX with new enqueued slice if created it,
        True if TaskResultSummary.state has changed by these actions,
    )
  """
  assert ndb.in_transaction()
  assert to_run_key.parent() == request.key

  now = utils.utcnow()
  slice_index = task_to_run.task_to_run_key_slice_index(to_run_key)
  result_summary_key = task_pack.request_key_to_result_summary_key(request.key)

  # Check the TaskToRunShardXXX is pending. Fetch TaskResultSummary while at it.
  to_run, result_summary = ndb.get_multi([to_run_key, result_summary_key],
                                         use_cache=False,
                                         use_memcache=False)
  if not to_run or not result_summary:
    logging.warning('%s/%s: already gone', request.task_id, slice_index)
    return None, None, None, False
  if not to_run.is_reapable:
    logging.info('%s/%s: already processed', request.task_id, slice_index)
    return None, None, None, False

  # TaskResultSummary always "tracks" the current slice and there can be only
  # one current slice (i.e. with is_reapable==True), and it is `to_run`.
  #
  # TODO(vadimsh): Unfortunately these invariants are violated by
  # _ensure_active_slice used with External Scheduler flow. Not sure if this is
  # intentional or it is a bug. Either way, these asserts will fire for tasks
  # scheduled through the external scheduler, so skip them. The rest should be
  # fine.
  if not es_cfg:
    assert to_run.task_slice_index == slice_index, to_run
    assert result_summary.current_task_slice == slice_index, result_summary
    assert not result_summary.try_number, result_summary

  # Will accumulate all entities that need to be stored at the end.
  to_put = []

  # Record the expiration delay if the slice expired by reaching its deadline.
  # It may end up negative if there's a clock drift between the process that ran
  # yield_expired_task_to_run() and the process that runs this transaction. This
  # should be rare.
  if terminal_state == task_result.State.EXPIRED:
    delay = (now - to_run.expiration_ts).total_seconds()
    if delay < 0:
      logging.warning(
          '_expire_slice_tx: the task is not expired. task_id=%s slice=%d '
          'expiration_ts=%s, delay=%f', to_run.task_id, to_run.task_slice_index,
          to_run.expiration_ts, delay)
      delay = 0.0
    to_run.expiration_delay = delay

  # Mark the current TaskToRunShardXXX as consumed.
  to_run.consume(None)
  to_put.append(to_run)

  # Check if there's a fallback slice we have capacity to run.
  new_to_run = None
  for idx in range(slice_index + 1, request.num_task_slices):
    if capacity is None or capacity[idx]:
      new_to_run = task_to_run.new_task_to_run(request, idx)
      to_put.append(new_to_run)
      break

  orig_summary_state = result_summary.state
  to_put.append(result_summary)

  if new_to_run:
    # Start "tracking" the new slice.
    result_summary.current_task_slice = new_to_run.task_slice_index
    result_summary.modified_ts = now
  else:
    # There's no fallback, giving up.
    result_summary.state = terminal_state
    result_summary.internal_failure = (
        terminal_state == task_result.State.BOT_DIED)
    result_summary.modified_ts = now
    result_summary.abandoned_ts = now
    result_summary.completed_ts = now
    if terminal_state == task_result.State.EXPIRED:
      delay = (now - request.expiration_ts).total_seconds()
      result_summary.expiration_delay = max(0.0, delay)

  futures = ndb.put_multi_async(to_put)
  _maybe_taskupdate_notify_via_tq(result_summary,
                                  request,
                                  es_cfg,
                                  transactional=True)
  if new_to_run and request.rbe_instance:
    rbe.enqueue_rbe_task(request, new_to_run)
  for f in futures:
    f.check_success()

  state_changed = result_summary.state != orig_summary_state
  return result_summary, to_run, new_to_run, state_changed


def _expire_slice(request, to_run_key, terminal_state, claim, txn_retries,
                  txn_catch_errors, reason):
  """Expires a single TaskToRunShard if it is still pending.

  If the task has more slices, enqueues the next slice. Otherwise marks the
  whole task as expired by updating TaskResultSummary.

  Arguments:
    request: the TaskRequest instance with all slices.
    to_run_key: the TaskToRunShard to expire.
    terminal_state: the task state to set if this slice is the last one.
    claim: if True, obtain task_to_run.Claim before touching the task.
    txn_retries: how many times to retry the transaction on collisions.
    txn_catch_errors: if True, ignore datastore_utils.CommitError.
    reason: a string tsmon label used to identify how expiration happened.

  Returns:
    (TaskResultSummary if updated it, new TaskToRunShard if enqueued a slice).
  """
  assert to_run_key.parent() == request.key
  slice_index = task_to_run.task_to_run_key_slice_index(to_run_key)

  # Obtain a claim if asked. This prevents other concurrent calls from touching
  # this task. This is a best effort mechanism to reduce datastore contention.
  if claim and not task_to_run.Claim.obtain(to_run_key):
    logging.warning('%s/%s is marked as claimed, proceeding anyway',
                    request.task_id, slice_index)

  # Look if the TaskToRunShard is reapable once before doing the check inside
  # the transaction. This allows to skip the transaction completely if the
  # entity was already reaped.
  to_run = to_run_key.get()
  if not to_run:
    logging.warning('%s/%s: already gone', request.task_id, slice_index)
    return None, None
  if not to_run.is_reapable:
    logging.info('%s/%s: already processed', request.task_id, slice_index)
    return None, None

  # None means "skip the check and always try to schedule the task".
  capacity = None

  # For tasks scheduled through native Swarming scheduler do a check for
  # capacity for the remaining slices (if any) before the
  # transaction, as has_capacity() cannot be called within a transaction.
  if not request.rbe_instance:
    capacity = {}
    for idx in range(slice_index + 1, request.num_task_slices):
      ts = request.task_slice(idx)
      capacity[idx] = ts.wait_for_capacity or bot_management.has_capacity(
          ts.properties.dimensions)

  # RBE tasks don't use the external scheduler, don't need to check the config.
  es_cfg = None
  if not request.rbe_instance:
    es_cfg = external_scheduler.config_for_task(request)

  try:
    summary, old_ttr, new_ttr, state_changed = datastore_utils.transaction(
        lambda: _expire_slice_tx(request, to_run_key, terminal_state, capacity,
                                 es_cfg),
        retries=txn_retries)
  except datastore_utils.CommitError as exc:
    if not txn_catch_errors:
      raise
    logging.warning('_expire_slice_tx failed: %s', exc)
    return None, None

  if summary:
    logging.info('Expired %s/%s', request.task_id, slice_index)
    ts_mon_metrics.on_task_expired(summary, old_ttr, reason)
  if state_changed:
    ts_mon_metrics.on_task_status_change_scheduler_latency(summary)

  return summary, new_ttr


def _reap_task(bot_dimensions,
               bot_details,
               to_run_key,
               request,
               claim_id=None,
               txn_retries=0,
               txn_catch_errors=True):
  """Reaps a task and insert the results entity.

  Returns:
    (TaskRunResult, SecretBytes) if successful, (None, None) otherwise.
  """
  assert request.key == task_to_run.task_to_run_key_to_request_key(to_run_key)
  result_summary_key = task_pack.request_key_to_result_summary_key(request.key)
  bot_id = bot_dimensions[u'id'][0]
  bot_info = bot_management.get_info_key(bot_id).get(use_cache=False,
                                                     use_memcache=False)
  if not bot_info:
    raise ClaimError('Bot %s doesn\'t exist.' % bot_id)

  now = utils.utcnow()
  # Log before the task id in case the function fails in a bad state where the
  # DB TX ran but the reply never comes to the bot. This is the worst case as
  # this leads to a task that results in BOT_DIED without ever starting. This
  # case is specifically handled in cron_handle_bot_died().
  logging.info('_reap_task(%s, %s)',
               task_pack.pack_result_summary_key(result_summary_key), claim_id)

  es_cfg = external_scheduler.config_for_task(request)

  keys_to_fetch = [to_run_key, result_summary_key]

  # Fetch SecretBytes as well if the slice uses secrets.
  slice_index = task_to_run.task_to_run_key_slice_index(to_run_key)
  if request.task_slice(slice_index).properties.has_secret_bytes:
    keys_to_fetch.append(request.secret_bytes_key)

  def run():
    entities = ndb.get_multi(keys_to_fetch, use_cache=False, use_memcache=False)
    to_run, result_summary = entities[0], entities[1]
    secret_bytes = entities[2] if len(entities) == 3 else None
    orig_summary_state = result_summary.state

    if not to_run:
      logging.error('Missing TaskToRunShard?\n%s', result_summary.task_id)
      if claim_id:
        raise ClaimError('No task slice')
      return None, None, None, None, False

    if not to_run.is_reapable:
      if claim_id:
        if to_run.claim_id != claim_id:
          raise ClaimError('No longer available')
        # The caller already holds the claim and this is a retry. Just fetch all
        # entities that should already exist.
        run_result = task_pack.result_summary_key_to_run_result_key(
            result_summary_key).get(use_cache=False, use_memcache=False)
        if not run_result:
          raise Error('TaskRunResult unexpectedly missing on retry')
        if run_result.current_task_slice != to_run.task_slice_index:
          raise ClaimError('Obsolete')
        return run_result, secret_bytes, result_summary, None, False
      logging.info('%s is not reapable', result_summary.task_id)
      return None, None, None, None, False

    if result_summary.bot_id == bot_id:
      # This means two things, first it's a retry, second it's that the first
      # try failed and the retry is being reaped by the same bot. Deny that, as
      # the bot may be deeply broken and could be in a killing spree.
      # TODO(maruel): Allow retry for bot locked task using 'id' dimension.
      # TODO(vadimsh): This should not be possible, retries were removed.
      logging.warning('%s can\'t retry its own internal failure task',
                      result_summary.task_id)
      return None, None, None, None, False

    to_run.consume(claim_id)
    run_result = task_result.new_run_result(request, to_run, bot_id,
                                            bot_details, bot_dimensions,
                                            result_summary.resultdb_info)
    # Upon bot reap, both .started_ts and .modified_ts matches. They differ on
    # the first ping.
    run_result.started_ts = now
    run_result.modified_ts = now
    # The bot may became available at this request. Use current time in that
    # case.
    run_result.bot_idle_since_ts = bot_info.idle_since_ts or now
    # Upon bot reap, set .dead_after_ts taking into consideration the
    # user-provided keep-alive value. This is updated after each ping
    # from the bot."
    run_result.dead_after_ts = now + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    result_summary.set_from_run_result(run_result, request)
    ndb.put_multi([to_run, run_result, result_summary])
    state_changed = result_summary.state != orig_summary_state
    if result_summary.state != orig_summary_state:
      _maybe_taskupdate_notify_via_tq(result_summary,
                                      request,
                                      es_cfg,
                                      transactional=True)
      state_changed = True
    return run_result, secret_bytes, result_summary, to_run, state_changed

  run_result = None
  secret_bytes = None
  summary = None
  to_run = None
  state_changed = False
  try:
    run_result, secret_bytes, summary, to_run, state_changed = \
      datastore_utils.transaction(run, retries=txn_retries, deadline=30)
  except datastore_utils.CommitError:
    if not txn_catch_errors:
      raise
    # The challenge here is that the transaction may have failed because:
    # - The DB had an hickup and the TaskToRunShard, TaskRunResult and
    #   TaskResultSummary haven't been updated.
    # - The entities had been updated by a concurrent transaction on another
    #   handler so it was not reapable anyway. This does cause exceptions as
    #   both GET returns the TaskToRunShard.queue_number != None but only one
    #   succeed at the PUT.
    #
    # In the first case, we may want to release the claim, while we don't
    # want to in the later case. The trade off are one of:
    # - the claim is not released, so the task is not reapable for 15s
    # - releasing the claim would cause even more contention
    #
    # We chose the first one here for now, as the when the DB starts misbehaving
    # and the index becomes stale, it means the DB is *already* not in good
    # shape, so it is preferable to not put more stress on it, and skipping a
    # few tasks for 15s may even actively help the DB to stabilize.
    #
    # The bot will reap the next available task in case of failure, no big deal.
    logging.info('CommitError; reaping failed')
  if state_changed:
    ts_mon_metrics.on_task_status_change_scheduler_latency(summary)
  if to_run:
    ts_mon_metrics.on_task_to_run_consumed(summary, to_run)
  return run_result, secret_bytes


def _detect_dead_task_async(run_result_key):
  """Checks if the bot has stopped working on the task.

  Transactionally updates the entities depending on the state of this task. The
  task may be retried automatically, canceled or left alone.

  Returns:
    ndb.Future that returns tuple(ndb.Key, bool, datetime.timedelta, List[str])
      TaskRunResult key if the task was killed, otherwise None
      True if the task state has changed
      latency of task dead detection (state transition)
      tags of task result summary
    None if no action was done.
  """
  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request = task_pack.result_summary_key_to_request_key(
      result_summary_key).get()
  if not request:
    # That's a particularly broken task, there's no TaskRequest in the DB!
    #
    # The cleanest thing to do would be to delete the whole entity, but that's
    # risky. Let's say there's a bug or a runtime issue that makes the DB GET
    # fail spuriously, we don't want to delete a whole task due to a transient
    # RPC failure.
    #
    # An other option is to create a temporary in-memory TaskRequest() entity,
    # but it's more trouble than it look like, as we need to populate one that
    # is valid, and the code in task_result really assumes it is in the DB.
    #
    # So for now, just skip it to unblock the cron job.
    return None

  now = utils.utcnow()
  es_cfg = external_scheduler.config_for_task(request)

  @ndb.tasklet
  def run():
    """Obtain the result and update task state to either KILLED or BOT_DIED.
    1x GET, 1x GETs 2~3x PUT.
    """
    run_result = run_result_key.get(use_cache=False, use_memcache=False)
    run_result._request_cache = request

    if run_result.state != task_result.State.RUNNING:
      # It was updated already or not updating last. Likely DB index was stale.
      raise ndb.Return(None, False, None, None)

    if not run_result.dead_after_ts or run_result.dead_after_ts > now:
      raise ndb.Return(None, False, None, None)

    old_abandoned_ts = run_result.abandoned_ts
    old_dead_after_ts = run_result.dead_after_ts

    run_result.signal_server_version()
    run_result.modified_ts = now
    run_result.completed_ts = now
    if not run_result.abandoned_ts:
      run_result.abandoned_ts = now
    # set .dead_after_ts to None since the task is terminated.
    run_result.dead_after_ts = None
    # mark as internal failure as the task doesn't get completed normally.
    run_result.internal_failure = True

    result_summary = result_summary_key.get(use_cache=False, use_memcache=False)
    result_summary._request_cache = request
    result_summary.modified_ts = now
    result_summary.completed_ts = now
    if not result_summary.abandoned_ts:
      result_summary.abandoned_ts = now
    orig_summary_state = result_summary.state

    # Mark it as KILLED if run_result is in killing state.
    # Otherwise, mark it BOT_DIED. the bot hasn't been sending for the task.
    to_put = (run_result, result_summary)
    if run_result.killing:
      run_result.killing = False
      run_result.state = task_result.State.KILLED
      run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
      # run_result.abandoned_ts is set when run_result.killing == True
      actual_state_change_time = old_abandoned_ts
    else:
      run_result.state = task_result.State.BOT_DIED
      actual_state_change_time = old_dead_after_ts
    result_summary.set_from_run_result(run_result, request)

    logging.warning(
        'Updating task state for bot missing. task:%s, state:%s, bot:%s',
        run_result.task_id, task_result.State.to_string(run_result.state),
        run_result.bot_id)
    futures = ndb.put_multi_async(to_put)

    state_changed = orig_summary_state != result_summary.state
    if state_changed:
      _maybe_taskupdate_notify_via_tq(result_summary,
                                      request,
                                      es_cfg,
                                      transactional=True)
    yield futures
    latency = utils.utcnow() - actual_state_change_time
    logging.warning('Task state was successfully updated. task: %s',
                    run_result.task_id)
    raise ndb.Return(run_result_key, state_changed, latency,
                     result_summary.tags)

  return datastore_utils.transaction_async(run)


def _maybe_pubsub_notify_now(result_summary, request):
  """Examines result_summary and sends task completion PubSub message.

  Does it only if result_summary indicates a task in some finished state and
  the request is specifying pubsub topic.

  Returns False to trigger the retry (on transient errors), or True if retry is
  not needed (e.g. messages was sent successfully or fatal error happened).
  """
  assert not ndb.in_transaction()
  assert isinstance(result_summary,
                    task_result.TaskResultSummary), result_summary
  assert isinstance(request, task_request.TaskRequest), request
  if (result_summary.state not in task_result.State.STATES_RUNNING and
      request.pubsub_topic):
    task_id = task_pack.pack_result_summary_key(result_summary.key)
    start_time = utils.milliseconds_since_epoch()
    try:
      _pubsub_notify(task_id, request.pubsub_topic, request.pubsub_auth_token,
                     request.pubsub_userdata, result_summary.tags,
                     result_summary.state, start_time)
    except pubsub.TransientError:
      return False
    except pubsub.Error:
      return True # do not retry it
  return True


def _maybe_pubsub_send_build_task_update(bb_task, build_id, pubsub_topic):
  """Sends an update message to buildbucket about the task's current status.

  Arguments:
    bb_task task_pb2.Task: Created by caller of this funciton to send to
      Buildbucket.
    build_id string: buildbucket build id provided by buildbucket.
    pubsub_topic string: pubsub topic to publish to. Provided by buildbucket.

  Returns:
    bool: False if there was a pubsub.TransientError, True otherwise. If False,
      then the function may be retried.
  """
  assert not ndb.in_transaction()
  assert isinstance(bb_task, task_pb2.Task), bb_task
  msg = task_pb2.BuildTaskUpdate(build_id=build_id, task=bb_task)
  try:
    pubsub.publish(topic=pubsub_topic,
                   message=msg.SerializeToString(),
                   attributes=None)
  except pubsub.TransientError as e:
    http_status_code = e.inner.status_code
    logging.exception(
        'Transient error (status_code=%s) when sending PubSub notification',
        http_status_code)
    return False
  except pubsub.Error as e:
    http_status_code = e.inner.status_code
    logging.exception(
        'Fatal error (status_code=%s) when sending PubSub notification',
        http_status_code)
    return True  # do not retry it
  except Exception as e:
    logging.exception("Unknown exception (%s) not handled by " \
                      "_maybe_pubsub_send_build_task_update", e)
    raise e  # raise the error if it is unknown
  return True


def _route_to_go(prod_pct=0, dev_pct=0):
  """Returns True if it should route to Go.

  Arguments:
    prod_pct: percentage of traffic in Prod that should route to Go.
    dev_pct est: percentage of traffic on Prod that should route to Go.
  """

  pct = prod_pct
  if utils.is_dev():
    pct = dev_pct
  return random.randint(0, 99) < pct


def _maybe_taskupdate_notify_via_tq(result_summary, request, es_cfg,
                                    transactional):
  """Enqueues tasks to send PubSub, es, and bb notifications for given request.

  Arguments:
    result_summary: a task_result.TaskResultSummary instance.
    request: a task_request.TaskRequest instance.
    es_cfg: a pool_config.ExternalSchedulerConfig instance if one exists
            for this task, or None otherwise.
    transactional: if runs as part of a db transaction.

  Raises CommitError on errors (to abort the transaction).
  """
  assert transactional == ndb.in_transaction()
  assert isinstance(result_summary,
                    task_result.TaskResultSummary), result_summary
  assert isinstance(request, task_request.TaskRequest), request
  task_id = task_pack.pack_result_summary_key(result_summary.key)
  if request.pubsub_topic:
    if ('projects/cr-buildbucket' in request.pubsub_topic
        or _route_to_go(prod_pct=100, dev_pct=100)):
      now = timestamp_pb2.Timestamp()
      now.FromDatetime(utils.utcnow())
      payload = {
          'class': 'pubsub-go',
          'body': {
              'taskId': task_id,
              'topic': request.pubsub_topic,
              'authToken': request.pubsub_auth_token,
              'userdata': request.pubsub_userdata,
              'tags': result_summary.tags,
              'state':
              TaskState.DESCRIPTOR.values_by_number[result_summary.state].name,
              'startTime': now.ToJsonString(),
          }
      }
      ok = utils.enqueue_task('/internal/tasks/t/pubsub-go/%s' % task_id,
                              'pubsub-v2',
                              transactional=transactional,
                              payload=utils.encode_to_json(payload))
    else:
      payload = {
          'task_id': task_id,
          'topic': request.pubsub_topic,
          'auth_token': request.pubsub_auth_token,
          'userdata': request.pubsub_userdata,
          'tags': result_summary.tags,
          'state': result_summary.state,
          'start_time': utils.milliseconds_since_epoch()
      }
      ok = utils.enqueue_task(
          '/internal/taskqueue/important/pubsub/notify-task/%s' % task_id,
          'pubsub',
          transactional=transactional,
          payload=utils.encode_to_json(payload))
    logging.debug("Payload of PubSub msg that was tried to enqueued: %s",
                  str(payload))
    if not ok:
      raise datastore_utils.CommitError('Failed to enqueue pubsub notify task')

  if es_cfg:
    external_scheduler.notify_requests(
        es_cfg, [(request, result_summary)], True, False)

  if request.has_build_task:
    if _route_to_go(prod_pct=100, dev_pct=100):
      go_payload = {
          'class': 'buildbucket-notify-go',
          'body': {
              "taskId": task_id,
              "state":
              TaskState.DESCRIPTOR.values_by_number[result_summary.state].name,
              "updateId": utils.time_time_ns(),
          }
      }
      ok = utils.enqueue_task('/internal/tasks/t/buildbucket-notify-go/%s' %
                              task_id,
                              'buildbucket-notify-go',
                              transactional=transactional,
                              payload=utils.encode_to_json(go_payload))
    else:
      payload = {
          'task_id': task_id,
          'state': result_summary.state,
          'update_id': utils.time_time_ns(),
      }
      ok = utils.enqueue_task(
          '/internal/taskqueue/important/buildbucket/notify-task/%s' % task_id,
          'buildbucket-notify',
          transactional=transactional,
          payload=utils.encode_to_json(payload))
    if not ok:
      raise datastore_utils.CommitError(
          'Failed to enqueue buildbucket notify task')


def _buildbucket_update(request_key, run_result_state, update_id):
  """Handles sending a pubsub update to buildbucket or not.
  Arguments:
    request_key: ndb.Key for a task_request.TaskRequest object.
    run_result_state: task_result.State enum.
    update_id: int timestamp for when the update was made. Used by buildbucket
      to only accept the most up to date updates.
  Returns:
    bool. False if there was a transient error. This will allow the caller of
      the function to be able to retry. True otherwise.
  """
  build_task = task_pack.request_key_to_build_task_key(request_key).get()
  # Returning early if we shouldn't make the update.
  if build_task.update_id > update_id:
    return True
  if run_result_state == build_task.latest_task_status:
    return True

  result_summary = task_pack.request_key_to_result_summary_key(request_key).get(
      use_cache=False, use_memcache=False)
  # Need to try to get bot_dimensions from build_task first, if not get it
  # from result_summary.
  if build_task.bot_dimensions:
    bot_dimensions = build_task.bot_dimensions
  else:
    bot_dimensions = result_summary.bot_dimensions

  task_id = task_pack.pack_result_summary_key(
      task_pack.request_key_to_result_summary_key(request_key))
  bb_task = task_pb2.Task(
      id=task_pb2.TaskID(id=task_id,
                         target="swarming://%s" %
                         app_identity.get_application_id()),
      update_id=update_id,
      details=backend_conversions.convert_backend_task_details(bot_dimensions),
  )
  backend_conversions.convert_task_state_to_status(run_result_state,
                                                   result_summary.failure,
                                                   bb_task)
  update_buildbucket_pubsub_success = _maybe_pubsub_send_build_task_update(
      bb_task, build_task.build_id, build_task.pubsub_topic)
  # Caller must retry if PubSub enqueue fails with a transient error.
  if not update_buildbucket_pubsub_success:
    return False
  build_task.latest_task_status = run_result_state
  build_task.update_id = update_id
  # If build_task doesn't have bot_dimensions, add it.
  if not build_task.bot_dimensions:
    build_task.bot_dimensions = bot_dimensions
  build_task.put()
  return True


def _pubsub_notify(task_id, topic, auth_token, userdata, tags, state,
                   start_time):
  """Sends PubSub notification about task completion.

  Raises pubsub.TransientError on transient errors otherwise raises pubsub.Error
  for fatal errors.
  """
  logging.debug(
      'Sending PubSub notify to "%s" (with userdata="%s", tags="%s",'
      'state="%s") about completion of "%s"', topic, userdata, tags, state,
      task_id)
  msg = {'task_id': task_id}
  if userdata:
    msg['userdata'] = userdata
  http_status_code = 0
  try:
    pubsub.publish(
        topic=topic,
        message=utils.encode_to_json(msg),
        attributes={'auth_token': auth_token} if auth_token else None)
    http_status_code = 200
  except pubsub.TransientError as e:
    http_status_code = e.inner.status_code
    logging.exception(
        'Transient error (status_code=%s) when sending PubSub notification',
        http_status_code)
    raise e
  except pubsub.Error as e:
    http_status_code = e.inner.status_code
    logging.exception(
        'Fatal error (status_code=%s) when sending PubSub notification',
        http_status_code)
    raise e
  except Exception as e:
    logging.exception("Unknown exception (%s) not handled by _pubsub_notify", e)
    raise e
  finally:
    if start_time is not None:
      now = utils.milliseconds_since_epoch()
      latency = now - start_time
      if latency < 0:
        logging.warning(
            'ts_mon_metric pubsub latency %dms (%d - %d) is negative. '
            'Setting latency to 0', latency, now, start_time)
        latency = 0
      logging.debug(
          'Updating ts_mon_metric pubsub with latency: %dms (%d - %d)', latency,
          now, start_time)
      ts_mon_metrics.on_task_status_change_pubsub_latency(
          tags, state, http_status_code, latency)


def _find_dupe_task(now, h):
  """Finds a previously run task that is also idempotent and completed.

  Fetch items that can be used to dedupe the task. See the comment for this
  property for more details.

  Do not use "task_result.TaskResultSummary.created_ts > oldest" here because
  this would require an index. It's unnecessary because TaskRequest.key is
  equivalent to decreasing TaskRequest.created_ts, ordering by key works as
  well and doesn't require an index.
  """
  logging.info("_find_dupe_task for properties_hash: %s", h.encode('hex'))
  # TODO(maruel): Make a reverse map on successful task completion so this
  # becomes a simple ndb.get().
  cls = task_result.TaskResultSummary
  q = cls.query(cls.properties_hash==h).order(cls.key)
  for i, dupe_summary in enumerate(q.iter(batch_size=1)):
    # It is possible for the query to return stale items.
    if (dupe_summary.state != task_result.State.COMPLETED or
        dupe_summary.failure):
      if i == 2:
        logging.info("indexes are very inconsistent, give up.")
        return None
      continue

    # Refuse tasks older than X days. This is due to the isolate server
    # dropping files.
    # TODO(maruel): The value should be calculated from the isolate server
    # setting and be unbounded when no isolated input was used.
    oldest = now - datetime.timedelta(
        seconds=config.settings().reusable_task_age_secs)
    if dupe_summary.created_ts <= oldest:
      logging.info("found result (%s) is older than threshold (%s)",
                   dupe_summary.created_ts, oldest)
      return None
    logging.info("_find_dupe_task: dupped with %s", dupe_summary.task_id)
    return dupe_summary
  return None


def _copy_summary(src, dst, skip_list):
  """Copies the attributes of entity src into dst.

  It doesn't copy the key nor any member in skip_list.
  """
  # pylint: disable=unidiomatic-typecheck
  assert type(src) == type(dst), '%s!=%s' % (src.__class__, dst.__class__)
  # Access to a protected member _XX of a client class - pylint: disable=W0212
  kwargs = {
    k: getattr(src, k) for k in src._properties_fixed() if k not in skip_list
  }
  dst.populate(**kwargs)


def _dedupe_result_summary(dupe_summary, result_summary, task_slice_index):
  """Copies the results from dupe_summary into result_summary."""
  # PerformanceStats is not copied over, since it's not relevant, nothing
  # ran.
  _copy_summary(
      dupe_summary, result_summary,
      ('created_ts', 'modified_ts', 'name', 'user', 'tags'))
  # Copy properties not covered by _properties_fixed().
  result_summary.bot_id = dupe_summary.bot_id
  result_summary.missing_cas = dupe_summary.missing_cas
  result_summary.missing_cipd = dupe_summary.missing_cipd
  # Zap irrelevant properties.
  result_summary.cost_saved_usd = dupe_summary.cost_usd
  result_summary.costs_usd = []
  result_summary.current_task_slice = task_slice_index
  result_summary.deduped_from = task_pack.pack_run_result_key(
      dupe_summary.run_result_key)
  # It is not possible to dedupe against a deduped task, so zap properties_hash.
  result_summary.properties_hash = None
  result_summary.try_number = 0


def _is_allowed_to_schedule(pool_cfg):
  """True if the current caller is allowed to schedule tasks in the pool."""
  caller_id = auth.get_current_identity()

  # Listed directly?
  if caller_id in pool_cfg.scheduling_users:
    logging.info(
        'Caller "%s" is allowed to schedule tasks in the pool "%s" by being '
        'specified directly in the pool config', caller_id.to_bytes(),
        pool_cfg.name)
    return True

  # Listed through a group?
  for group in pool_cfg.scheduling_groups:
    if auth.is_group_member(group, caller_id):
      logging.info(
          'Caller "%s" is allowed to schedule tasks in the pool "%s" by being '
          'referenced via the group "%s" in the pool config',
          caller_id.to_bytes(), pool_cfg.name, group)
      return True

  # Using delegation?
  delegation_token = auth.get_delegation_token()
  if not delegation_token:
    logging.info('No delegation token')
    return False

  # Log relevant info about the delegation to simplify debugging.
  peer_id = auth.get_peer_identity()
  token_tags = set(delegation_token.tags or [])
  logging.info(
      'Using delegation, delegatee is "%s", delegation tags are %s',
      peer_id.to_bytes(), sorted(map(str, token_tags)))

  # Is the delegatee listed in the config?
  trusted_delegatee = pool_cfg.trusted_delegatees.get(peer_id)
  if not trusted_delegatee:
    logging.warning('The delegatee "%s" is unknown', peer_id.to_bytes())
    return False

  # Are any of the required delegation tags present in the token?
  cross = token_tags & trusted_delegatee.required_delegation_tags
  if not cross:
    cross = set()
    wildcard_delegation_tags = filter(
        lambda t: t.endswith('/*'), trusted_delegatee.required_delegation_tags)
    for t in wildcard_delegation_tags:
      t = t[:-1]
      cross |= set(filter(lambda tt: tt.startswith(t), token_tags))
  if cross:
    logging.info(
        'Caller "%s" is allowed to schedule tasks in the pool "%s" by acting '
        'through a trusted delegatee "%s" that set the delegation tags %s',
        caller_id.to_bytes(), pool_cfg.name, peer_id.to_bytes(),
        sorted(map(str, cross)))
    return True

  logging.warning(
      'Expecting any of %s tags, got %s, forbidding the call',
      sorted(map(str, trusted_delegatee.required_delegation_tags)),
      sorted(map(str, token_tags)))
  return False


def _bot_update_tx(run_result_key, bot_id, output, output_chunk_start,
                   exit_code, duration, hard_timeout, io_timeout, cost_usd,
                   cas_output_root, cipd_pins, need_cancel, performance_stats,
                   now, result_summary_key, request, es_cfg, canceled):
  """Runs the transaction for bot_update_task().

  es_cfg is only required when need_cancel is True.

  Returns tuple(TaskRunResult, TaskResultSummary, str(error)).

  Any error is returned as a string to be passed to logging.error() instead of
  logging inside the transaction for performance.
  """
  # 2 or 3 consecutive GETs, one PUT.
  #
  # Assumptions:
  # - duration and exit_code are both set or not set. That's not always true for
  #   TIMED_OUT/KILLED.
  # - same for run_result.
  # - if one of hard_timeout or io_timeout is set, duration is also set.
  # - hard_timeout or io_timeout can still happen in the case of killing. This
  #   still needs to result in KILLED, not TIMED_OUT.
  logging.info('Starting transaction attempt')

  run_result, result_summary = ndb.get_multi(
      [run_result_key, result_summary_key], use_cache=False, use_memcache=False)
  if not run_result:
    return None, None, 'is missing'

  if run_result.bot_id != bot_id:
    return None, None, (
        'expected bot (%s) but had update from bot %s' % (
        run_result.bot_id, bot_id))

  if not run_result.started_ts:
    return None, None, 'TaskRunResult is broken; %s' % (
        run_result.to_dict())

  if exit_code is not None:
    if run_result.exit_code is not None:
      # This happens as an HTTP request is retried when the DB write succeeded
      # but it still returned HTTP 500.
      if run_result.exit_code != exit_code:
        return None, None, 'got 2 different exit_code; %s then %s' % (
            run_result.exit_code, exit_code)
      if run_result.duration != duration:
        return None, None, 'got 2 different durations; %s then %s' % (
            run_result.duration, duration)
    else:
      run_result.duration = duration
      run_result.exit_code = exit_code

  if cas_output_root:
    run_result.cas_output_root = cas_output_root

  if cipd_pins:
    run_result.cipd_pins = cipd_pins

  if run_result.state in task_result.State.STATES_RUNNING:
    # Task was still registered as running. Look if it should be terminated now.
    if run_result.killing:
      if canceled:
        # A user requested to cancel the task while setting up the task.
        # Since the task hasn't started running yet, we can mark the state as
        # CANCELED.
        run_result.killing = False
        run_result.state = task_result.State.CANCELED
        # reset duration, exit_code since not allowed
        run_result.duration = None
        run_result.exit_code = None
        run_result.completed_ts = now
      elif duration is not None:
        # A user requested to cancel the task while it was running. Since the
        # task is now stopped, we can tag the task result as KILLED.
        run_result.killing = False
        run_result.state = task_result.State.KILLED
        run_result.completed_ts = now
      else:
        # The bot is still executing the task in this path. The server should
        # return killing signal to the bot. After the bot stopped the task,
        # the task update will include `duration` and go to the above path to
        # mark TaskRunResult completed finally.
        pass
    else:
      if hard_timeout or io_timeout:
        # This needs to be changed with new state TERMINATING;
        # https://crbug.com/916560
        run_result.state = task_result.State.TIMED_OUT
        run_result.completed_ts = now
        run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
      elif run_result.exit_code is not None:
        run_result.state = task_result.State.COMPLETED
        run_result.completed_ts = now

  run_result.signal_server_version()
  to_put = [run_result]
  if output:
    # This does 1 multi GETs. This also modifies run_result in place.
    to_put.extend(run_result.append_output(output, output_chunk_start or 0))
  if performance_stats:
    performance_stats.key = task_pack.run_result_key_to_performance_stats_key(
        run_result.key)
    to_put.append(performance_stats)

  run_result.cost_usd = max(cost_usd, run_result.cost_usd or 0.)
  run_result.modified_ts = now
  if run_result.state in task_result.State.STATES_RUNNING:
    run_result.dead_after_ts = now + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
  else:
    run_result.dead_after_ts = None

  result_summary.set_from_run_result(run_result, request)
  to_put.append(result_summary)

  if need_cancel and run_result.state in task_result.State.STATES_RUNNING:
    logging.info('Calling _cancel_task_tx')
    _cancel_task_tx(request, result_summary, True, bot_id, now, es_cfg,
                    run_result)

  logging.info('Storing %d entities: %s', len(to_put), [e.key for e in to_put])
  ndb.put_multi(to_put)

  logging.info('Committing transaction')
  return result_summary, run_result, None


def _set_fallbacks_to_exit_code_and_duration(run_result, now):
  """Sets fallback values to exit_code and duration"""
  if run_result.exit_code is None:
    run_result.exit_code = -1
  if not run_result.duration:
    # Calculate an approximate time.
    run_result.duration = (now - run_result.started_ts).total_seconds()
  return run_result


def _cancel_task_tx(request,
                    result_summary,
                    kill_running,
                    bot_id,
                    now,
                    es_cfg,
                    run_result=None):
  """Runs the transaction for cancel_task().

  Arguments:
    request: TaskRequest instance to cancel.
    result_summary: result summary for request to cancel. Will be mutated in
        place and then stored into Datastore.
    kill_running: if true, allow cancelling a task in RUNNING state.
    bot_id: if specified, only cancel task if it is RUNNING on this bot. Cannot
        be specified if kill_running is False.
    now: timestamp used to update result_summary and run_result.
    es_cfg: pools_config.ExternalSchedulerConfig for external scheduler.
    run_result: Used when this is given, otherwise took from result_summary.

  Returns:
    tuple(bool, bool)
    - True if the cancellation succeeded. Either the task atomically changed
      from PENDING to CANCELED or it was RUNNING and killing bit has been set.
    - True if the task was running while it was canceled.
  """
  was_running = result_summary.state == task_result.State.RUNNING

  # Finished tasks can't be canceled.
  if not result_summary.can_be_canceled:
    return False, was_running
  # Deny cancelling a non-running task if bot_id was specified.
  if not was_running and bot_id:
    return False, was_running
  # Deny canceling a task that started.
  if was_running and not kill_running:
    return False, was_running
  # Deny cancelling a task if it runs on unexpected bot.
  if was_running and bot_id and bot_id != result_summary.bot_id:
    return False, was_running

  to_put = [result_summary]
  result_summary.abandoned_ts = now
  result_summary.modified_ts = now

  to_runs = []
  if not was_running:
    # The task is in PENDING state now and can be canceled right away.
    result_summary.state = task_result.State.CANCELED
    result_summary.completed_ts = now

    # This should return 1 entity (the pending TaskToRunShard).
    to_runs = task_to_run.get_task_to_runs(
        request, result_summary.current_task_slice or 0)
    for to_run in to_runs:
      to_run.consume(None)
      to_put.append(to_run)
  else:
    # The task in RUNNING state now. Do not change state to KILLED yet. Instead,
    # use a two step protocol:
    # - set killing to True
    # - on next bot report, tell the bot to kill the task
    # - once the bot reports the task as terminated, set state to KILLED
    run_result = run_result or result_summary.run_result_key.get(
        use_cache=False, use_memcache=False)
    run_result.killing = True
    run_result.abandoned_ts = now
    run_result.modified_ts = now
    to_put.append(run_result)

  futures = ndb.put_multi_async(to_put)
  _maybe_taskupdate_notify_via_tq(result_summary,
                                  request,
                                  es_cfg,
                                  transactional=True)

  # Enqueue TQ tasks to cancel RBE reservations if in the RBE mode.
  if request.rbe_instance:
    for ttr in to_runs:
      if ttr.rbe_reservation:
        rbe.enqueue_rbe_cancel(request, ttr)

  for f in futures:
    f.check_success()

  return True, was_running


def _get_task_from_external_scheduler(es_cfg, bot_dimensions):
  """Gets a task to run from external scheduler.

  Arguments:
    es_cfg: pool_config.ExternalSchedulerConfig instance.
    bot_dimensions: dimensions {string key: list of string values}

  Returns: (TaskRequest, TaskToRunShard) if a task was available,
           or (None, None) otherwise.
  """
  task_id, slice_number = external_scheduler.assign_task(es_cfg, bot_dimensions)
  if not task_id:
    return None, None

  logging.info('Got task id %s', task_id)
  request_key, result_key = task_pack.get_request_and_result_keys(task_id)
  logging.info('Determined request_key, result_key %s, %s', request_key,
               result_key)
  request = request_key.get()
  result_summary = result_key.get(use_cache=False, use_memcache=False)

  logging.info('Determined slice_number %s', slice_number)

  to_run, raise_exception = _ensure_active_slice(request, slice_number)
  if not to_run:
    # We were unable to ensure the given request was at the desired slice. This
    # means the external scheduler must have stale state about this request, so
    # notify it of the newest state.
    external_scheduler.notify_requests(
        es_cfg, [(request, result_summary)], True, False)
    if raise_exception:
      raise external_scheduler.ExternalSchedulerException(
          'unable to ensure active slice for task %s' % task_id)

  return request, to_run


def _ensure_active_slice(request, task_slice_index):
  """Ensures the existence of a TaskToRunShard for the given request, slice.

  Ensure that the given request is currently active at a given task_slice_index
  (modifying the current try or slice if necessary), and that
  no other TaskToRunShard is pending.

  This is intended for use as part of the external scheduler flow.

  Internally, this runs up to 2 GETs and 1 PUT in a transaction.

  Arguments:
    request: TaskRequest instance
    task_slice_index: slice index to ensure is active.

  Returns:
    TaskToRunShard: A saved TaskToRunShard instance corresponding to the given
                    request, and slice, if exists, or None otherwise.
    Boolean: Whether or not it should raise exception
  """
  # External scheduler and RBE scheduler do not mix.
  assert not request.rbe_instance

  def run():
    logging.debug('_ensure_active_slice(%s, %d)', request.task_id,
                  task_slice_index)
    to_runs = task_to_run.get_task_to_runs(request, request.num_task_slices - 1)
    to_runs = [r for r in to_runs if r.queue_number]
    if to_runs:
      if len(to_runs) != 1:
        logging.warning('_ensure_active_slice: %s != 1 TaskToRunShards',
                        len(to_runs))
        return None, True
      assert len(to_runs) == 1, 'Too many pending TaskToRunShards.'

    to_run = to_runs[0] if to_runs else None

    if to_run:
      if to_run.task_slice_index == task_slice_index:
        logging.debug('_ensure_active_slice: already active')
        return to_run, False

      # Deactivate old TaskToRunShard, create new one.
      to_run.consume(None)
      new_to_run = task_to_run.new_task_to_run(request, task_slice_index)
      ndb.put_multi([to_run, new_to_run])
      logging.debug('_ensure_active_slice: added new TaskToRunShard')
      return new_to_run, False

    result_summary = task_pack.request_key_to_result_summary_key(
        request.key).get(use_cache=False, use_memcache=False)
    if not result_summary:
      logging.warning(
          '_ensure_active_slice: no TaskToRunShard or TaskResultSummary')
      return None, True

    if not result_summary.is_pending:
      logging.debug(
          '_ensure_active_slice: request is not PENDING.'
          ' state: "%s", task_id: "%s"',
          result_summary.to_string(), result_summary.task_id)
      # just notify to external scheudler without exception
      return None, False

    new_to_run = task_to_run.new_task_to_run(request, task_slice_index)
    new_to_run.put()
    logging.debug(
        'ensure_active_slice: added new TaskToRunShard (no previous one)')
    return new_to_run, False

  return datastore_utils.transaction(run)


def _bot_reap_task_external_scheduler(bot_dimensions, bot_details, es_cfg):
  """Reaps a TaskToRunShard (chosen by external scheduler) if available.

  This is a simpler version of bot_reap_task that skips a lot of the steps
  normally taken by the native scheduler.

  Arguments:
    - bot_dimensions: The dimensions of the bot as a dictionary in
          {string key: list of string values} format.
    - bot_details: a BotDetails tuple, non-essential but would be propagated to
          the task.
    - es_cfg: ExternalSchedulerConfig for this bot.
  """
  request, to_run = _get_task_from_external_scheduler(es_cfg, bot_dimensions)
  if not request or not to_run:
    return None, None, None

  run_result, secret_bytes = _reap_task(bot_dimensions, bot_details, to_run.key,
                                        request)
  if not run_result:
    raise external_scheduler.ExternalSchedulerException(
        'failed to reap %s' % task_pack.pack_request_key(to_run.request_key))

  logging.info('Reaped (external scheduler): %s', run_result.task_id)
  return request, secret_bytes, run_result


def _should_allow_es_fallback(es_cfg, request):
  """Determines whether to allow external scheduler fallback to the given task.

  Arguments:
    - es_cfg: ExternalSchedulerConfig to potentially fallback from.
    - request: TaskRequest in question to fallback to.
  """
  task_es_cfg = external_scheduler.config_for_task(request)
  if not task_es_cfg:
    # Other task is not es-owned. Allow fallback.
    return True

  if not es_cfg.allow_es_fallback:
    # Fallback to es-owned task is disabled.
    return False

  # Allow fallback if task uses precisely the same external scheduler as
  # the one we are falling back from.
  return task_es_cfg == es_cfg


### Public API.


def exponential_backoff(attempt_num):
  """Returns an exponential backoff value in seconds."""
  assert attempt_num >= 0
  if random.random() < _PROBABILITY_OF_QUICK_COMEBACK:
    # Randomly ask the bot to return quickly.
    return 1.0

  # If the user provided a max then use it, otherwise use default 60s.
  max_wait = config.settings().max_bot_sleep_time or 60.
  return min(max_wait, math.pow(1.5, min(attempt_num, 10) + 1))


def check_schedule_request_acl_caller(pool_cfg):
  if not _is_allowed_to_schedule(pool_cfg):
    raise auth.AuthorizationError(
        'User "%s" is not allowed to schedule tasks in the pool "%s", '
        'see pools.cfg' %
        (auth.get_current_identity().to_bytes(), pool_cfg.name))


def check_schedule_request_acl_service_account(request):
  # request.service_account can be 'bot' or 'none'. We don't care about these,
  # they are always allowed. We care when the service account is a real email.
  has_service_account = service_accounts_utils.is_service_account(
      request.service_account)
  if has_service_account:
    raise auth.AuthorizationError(
        'Task service account "%s" as specified in the task request is not '
        'allowed to be used in the pool "%s".' %
        (request.service_account, request.pool))


def schedule_request(request,
                     request_id=None,
                     enable_resultdb=False,
                     secret_bytes=None,
                     build_task=None):
  """Creates and stores all the entities to schedule a new task request.

  Assumes the request was already processed by api_helpers.process_task_request
  and the ACL check already happened there.

  Uses the scheduling algorithm specified by request.scheduling_algorithm field.

  The number of entities created is ~4: TaskRequest, TaskToRunShard and
  TaskResultSummary and (optionally) SecretBytes. They are in single entity
  group and saved in a single transaction.

  Arguments:
  - request: TaskRequest entity to be saved in the DB. It's key must not be set
             and the entity must not be saved in the DB yet. It will be mutated
             to have the correct key and have `properties_hash` populated.
  - request_id: Optional string. Used to pull TaskRequestID from datastore.
             If request_id is not provided, there is no
             guarantee of idempotency.
  - enable_resultdb: Optional Boolean (default False) of whether we use resultdb
             or not for this task.
  - secret_bytes: Optional SecretBytes entity to be saved in the DB. It's key
             will be set and the entity will be stored by this function.
  - build_task: Optional BuildTask entity to be saved in the DB. It's key will
             be set and the entity will be stored by this function.
  Returns:
    TaskResultSummary. TaskToRunShard is not returned.
  """
  assert isinstance(request, task_request.TaskRequest), request
  assert not request.key, request.key

  def _get_task_result_summary_from_task_id(task_id, request):
    """Given a task_id, the corresponding TaskResultSummary is pulled.
    The request object is also modified in place to associate the key
    with the already existing key.

    Arguments:
    - task_id: string id to pull the TaskResultSummary
    - request: TaskRequest to be modified
    Returns:
      TaskResultSummary.
    """
    req_key, result_summary_key = task_pack.get_request_and_result_keys(task_id)
    request.key = req_key
    return result_summary_key.get(use_cache=False, use_memcache=False)

  task_req_to_id_key = None
  if request_id:
    task_req_to_id_key = task_request.TaskRequestID.create_key(request_id)
    task_req_to_id = task_req_to_id_key.get()
    if task_req_to_id:
      return _get_task_result_summary_from_task_id(task_req_to_id.task_id,
                                                   request)

  # Register the dimension set in the native scheduler only when not on RBE.
  task_asserted_future = None
  if not request.rbe_instance:
    task_asserted_future = task_queues.assert_task_async(request)

  now = utils.utcnow()

  # Note: this key is not final. We may need to change it in the transaction
  # below if it is already occupied.
  request.key = task_request.new_request_key()
  result_summary = task_result.new_result_summary(request)
  result_summary.modified_ts = now

  # Precalculate all property hashes in advance. That way even if we end up
  # using e.g. first task slice, all hashes will still be populated (for BQ
  # export).
  for i in range(request.num_task_slices):
    request.task_slice(i).precalculate_properties_hash(secret_bytes)

  # If have results for any idempotent slice, reuse it. No need to run anything.
  dupe_summary = None
  for i in range(request.num_task_slices):
    t = request.task_slice(i)
    if t.properties.idempotent:
      dupe_summary = _find_dupe_task(now, t.properties_hash)
      if not dupe_summary:
        # Try the new hash method.
        dupe_summary = _find_dupe_task(
            now, t.calculate_properties_hash_v2(secret_bytes))
        if dupe_summary:
          logging.info(
              "found dupe task using properties hash calculated in the new way")
      if dupe_summary:
        _dedupe_result_summary(dupe_summary, result_summary, i)
        # In this code path, there's not much to do as the task will not be run,
        # previous results are returned. We still need to store the TaskRequest
        # and TaskResultSummary.
        # Since the task is never scheduled, TaskToRunShard is not stored.
        # Since the has_secret_bytes/has_build_task property is already set
        # for UI purposes, and the task itself will never be run, we skip
        # storing the SecretBytes/BuildTask, as they would never be read and
        # will just consume space in the datastore (and the task we deduplicated
        # with will have them stored anyway, if we really want to get them
        # again).
        secret_bytes = None
        build_task = None
        break

  to_run = None
  resultdb_update_token_future = None

  if not dupe_summary:
    # The task has to run. Find a slice index that should be launched based
    # on available capacity. For RBE tasks always start with zeroth slice, we
    # have no visibility into RBE capacity here. If there are slices that can't
    # execute due to missing bots, there will be a ping pong game between
    # Swarming and RBE skipping them.
    for index in range(request.num_task_slices):
      t = request.task_slice(index)
      if (request.rbe_instance or t.wait_for_capacity
          or bot_management.has_capacity(t.properties.dimensions)):
        # Pick this slice as the current.
        result_summary.current_task_slice = index
        to_run = task_to_run.new_task_to_run(request, index)
        if enable_resultdb:
          # TODO(vadimsh): The invocation may end up associated with wrong
          # task ID if we end up changing `request.key` in the transaction
          # below due to a collision.
          resultdb_update_token_future = resultdb.create_invocation_async(
              task_pack.pack_run_result_key(to_run.run_result_key),
              request.realm, request.execution_deadline)
        break
    else:
      # No available capacity for any slice. Fail the task right away.
      to_run = None
      secret_bytes = None
      result_summary.abandoned_ts = result_summary.created_ts
      result_summary.completed_ts = result_summary.created_ts
      result_summary.state = task_result.State.NO_RESOURCE

  # Determine external scheduler (if relevant) prior to making task live, to
  # make HTTP handler return as fast as possible after making task live.
  es_cfg = None
  if not request.rbe_instance:
    es_cfg = external_scheduler.config_for_task(request)

  # This occasionally triggers a task queue. May throw, which is surfaced to the
  # user but it is safe as the task request wasn't stored yet.
  if task_asserted_future:
    task_asserted_future.get_result()

  # Wait until ResultDB invocation is created to get its update token.
  if resultdb_update_token_future:
    request.resultdb_update_token = resultdb_update_token_future.get_result()
    result_summary.resultdb_info = task_result.ResultDBInfo(
        hostname=urlparse.urlparse(config.settings().resultdb.server).hostname,
        invocation=resultdb.get_invocation_name(
            task_pack.pack_run_result_key(to_run.run_result_key)),
    )

  def reparent(key):
    """Changes entity group key for all entities being stored."""
    request.key = key
    result_summary.key = task_pack.request_key_to_result_summary_key(key)
    if to_run:
      to_run.key = ndb.Key(to_run.key.kind(), to_run.key.id(), parent=key)
      if request.rbe_instance:
        to_run.populate_rbe_reservation()
    if secret_bytes:
      assert request.secret_bytes_key
      secret_bytes.key = request.secret_bytes_key
    if build_task:
      build_task.key = request.build_task_key

  # Populate all initial keys.
  reparent(request.key)

  # This is used to detect if we actually already stored entities in `txn`
  # on a retry after a transient error.
  request.txn_uuid = str(uuid.uuid4())

  def txn():
    """Returns True if stored everything, False on an ID collision."""
    # Checking if a request_uuid => task_id relationship exists.
    task_req_to_id = None
    if task_req_to_id_key:
      task_req_to_id = task_req_to_id_key.get()
      if task_req_to_id:
        raise TaskExistsException(task_req_to_id.task_id)
      expire_at = utils.utcnow() + datetime.timedelta(days=7)
      task_req_to_id = task_request.TaskRequestID(
          key=task_req_to_id_key,
          task_id=task_pack.pack_result_summary_key(result_summary.key),
          expire_at=expire_at)

    existing = request.key.get()
    if existing:
      return existing.txn_uuid == request.txn_uuid
    if to_run and request.rbe_instance:
      rbe.enqueue_rbe_task(request, to_run)
    ndb.put_multi(
        filter(bool, [
            request, result_summary, to_run, secret_bytes, build_task,
            task_req_to_id
        ]))
    return True

  # Try to transactionally insert the request retrying on task ID collisions
  # (which should be rare).
  attempt = 0
  while True:
    attempt += 1
    try:
      if datastore_utils.transaction(txn,
                                     retries=3,
                                     xg=bool(task_req_to_id_key)):
        break
      # There was an existing *different* entity. We need a new root key.
      prev = result_summary.task_id
      reparent(task_request.new_request_key())
      logging.warning('Task ID collision: %s already exists, using %s instead',
                      prev, result_summary.task_id)
    except datastore_utils.CommitError:
      # The transaction likely failed. Retry with the same key to confirm.
      pass
    except TaskExistsException as e:
      logging.warning(
          'Could not schedule new task because task already exists: %s' %
          e.message)
      return _get_task_result_summary_from_task_id(e.message, request)
  logging.debug('Committed txn on attempt %d', attempt)

  # Note: This external_scheduler call is blocking, and adds risk
  # of the HTTP handler being slow or dying after the task was already made
  # live. On the other hand, this call is only being made for tasks in a pool
  # that use an external scheduler, and which are not effectively live unless
  # the external scheduler is aware of them.
  #
  # TODO(vadimsh): This should happen via a transactionally enqueued TQ task.
  if es_cfg:
    external_scheduler.notify_requests(es_cfg, [(request, result_summary)],
                                       False, False)

  if dupe_summary:
    logging.debug(
        'New request %s reusing %s', result_summary.task_id,
        dupe_summary.task_id)
  elif result_summary.state == task_result.State.NO_RESOURCE:
    logging.warning(
        'New request %s denied with NO_RESOURCE', result_summary.task_id)
    logging.debug('New request %s', result_summary.task_id)
  else:
    logging.debug('New request %s', result_summary.task_id)

  ts_mon_metrics.on_task_requested(result_summary, bool(dupe_summary))

  # Either the task was deduped, or forcibly refused. Notify through PubSub.
  if result_summary.state != task_result.State.PENDING:
    # TODO(vadimsh): This should be moved into txn() above.
    _maybe_taskupdate_notify_via_tq(result_summary,
                                    request,
                                    es_cfg,
                                    transactional=False)
    ts_mon_metrics.on_task_status_change_scheduler_latency(result_summary)
  return result_summary


def bot_reap_task(bot_dimensions, queues, bot_details, deadline):
  """Reaps a TaskToRunShard if one is available.

  The process is to find a TaskToRunShard where its .queue_number is set, then
  create a TaskRunResult for it.

  Arguments:
  - bot_dimensions: The dimensions of the bot as a dictionary in
          {string key: list of string values} format.
  - queues: a list of integers with dimensions hashes of queues to poll.
  - bot_details: a BotDetails tuple, non-essential but would be propagated to
          the task.
  - deadline: datetime.datetime of when to give up.

  Returns:
    tuple of (TaskRequest, SecretBytes, TaskRunResult) for the task that was
    reaped. The TaskToRunShard involved is not returned.
  """
  start = time.time()
  bot_id = bot_dimensions[u'id'][0]
  es_cfg = external_scheduler.config_for_bot(bot_dimensions)
  if es_cfg:
    request, secret_bytes, to_run_result = _bot_reap_task_external_scheduler(
        bot_dimensions, bot_details, es_cfg)
    if request:
      return request, secret_bytes, to_run_result
    logging.info('External scheduler did not reap any tasks, trying native '
                 'scheduler.')

  # Used to filter out task requests that don't match bot's dimensions. This
  # can happen if there's a collision on dimension's hash (which is a 32bit
  # number).
  match_bot_dimensions = task_to_run.dimensions_matcher(bot_dimensions)

  # Pool is used exclusively for monitoring metrics to have some break down of
  # performance by pool.
  pool = (bot_dimensions.get(u'pool') or ['-'])[0]

  # Allocate ~10s for _reap_task.
  scan_deadline = deadline - datetime.timedelta(seconds=10)

  iterated = 0
  reenqueued = 0
  expired = 0
  failures = 0
  stale_index = 0
  try:
    q = task_to_run.yield_next_available_task_to_dispatch(
        bot_id, pool, queues, match_bot_dimensions, scan_deadline)
    for to_run in q:
      iterated += 1
      request = task_to_run.task_to_run_key_to_request_key(to_run.key).get()

      # When falling back from external scheduler, ignore other es-owned tasks.
      if es_cfg and not _should_allow_es_fallback(es_cfg, request):
        logging.debug('Skipped es-owned request %s during es fallback',
                      request.task_id)
        continue

      now = utils.utcnow()

      # Hard limit to schedule this task.
      expiration_ts = request.expiration_ts

      # If we found bot that can run the task slice, even if it is slightly
      # expired, we prefer to run it than fallback.
      slice_expiration = request.created_ts
      slice_index = task_to_run.task_to_run_key_slice_index(to_run.key)
      for i in range(slice_index + 1):
        t = request.task_slice(i)
        slice_expiration += datetime.timedelta(seconds=t.expiration_secs)
      if expiration_ts < now <= slice_expiration:
        logging.info('Task slice is expired, but task is not expired; '
                     'slice index: %d, slice expiration: %s, '
                     'task expiration: %s',
                     slice_index, slice_expiration, expiration_ts)

      if expiration_ts < now:
        # The whole task request has expired.
        if expired >= 5:
          # Do not try to expire too many tasks in one poll request, as this
          # kills the polling performance in case of degenerate queue: this
          # happens in the situation where a large backlog >10000 of tasks are
          # expiring simultaneously.
          logging.info('Too many inline expiration; skipping')
          failures += 1
          continue

        # Expiring a TaskToRunShard for TaskSlice may reenqueue a new
        # TaskToRunShard. Don't try to grab a claim: we already hold it.
        summary, new_to_run = _expire_slice(request,
                                            to_run.key,
                                            task_result.State.EXPIRED,
                                            claim=False,
                                            txn_retries=1,
                                            txn_catch_errors=True,
                                            reason='bot_reap_task')
        if not new_to_run:
          if summary:
            expired += 1
          else:
            stale_index += 1
          continue

        # Under normal circumstances, this code path should not be executed,
        # since we already looked for the whole task request expiration, instead
        # of the current task slice expiration. But let's handle this, just in
        # case.
        reenqueued += 1
        # We need to do an adhoc validation to check the new TaskToRunShard,
        # so see if we can harvest it too. This is slightly duplicating work in
        # yield_next_available_task_to_dispatch().
        slice_index = task_to_run.task_to_run_key_slice_index(new_to_run.key)
        t = request.task_slice(slice_index)
        if not match_bot_dimensions(t.properties.dimensions):
          continue

        # Try to claim it for ourselves right away. On failure, give it up for
        # some other bot to claim.
        if not task_to_run.Claim.obtain(new_to_run.key):
          continue
        to_run = new_to_run

      run_result, secret_bytes = _reap_task(bot_dimensions, bot_details,
                                            to_run.key, request)
      if not run_result:
        failures += 1
        # Sad thing is that there is not way here to know the try number.
        logging.info(
            'failed to reap: %s0',
            task_pack.pack_request_key(to_run.request_key))
        continue

      # We successfully reaped a task.
      logging.info('Reaped: %s', run_result.task_id)
      if es_cfg:
        # We reaped a task after falling back from external scheduler. Keep
        # it informed about the reaped task.
        external_scheduler.notify_requests(es_cfg, [(request, run_result)],
                                           True, False)
      logging.debug('TODO(crbug.com/1186759): expiration_ts %s',
                    to_run.expiration_ts)
      return request, secret_bytes, run_result
    return None, None, None
  finally:
    logging.debug(
        'bot_reap_task(%s) in %.3fs: %d iterated, %d reenqueued, %d expired, '
        '%d stale_index, %d failed', bot_id,
        time.time() - start, iterated, reenqueued, expired, stale_index,
        failures)


def bot_claim_slice(bot_dimensions, bot_details, to_run_key, claim_id):
  """Transactionally assigns the given task slice to the given bot.

  Unlike bot_reap_task, this function doesn't try to find a matching task
  in Swarming scheduler queues and instead accepts a candidate TaskToRun as
  input.

  TODO(vadimsh): Stop returning TaskRunResult, it doesn't really carry any
  new information (`task_id` and task slice index are already available in
  `to_run_key`). This will require more refactorings in handlers_bot.py.

  Arguments:
    bot_dimensions: The dimensions of the bot as a dictionary in
        {string key: list of string values} format.
    bot_details: a BotDetails tuple, non-essential but would be propagated to
        the task.
    to_run_key: a key of TaskToRunShardXXX entity identifying a slice to claim.
    claim_id: a string identifying this claim operation, for idempotency.

  Returns:
    Tuple of (TaskRequest, SecretBytes, TaskRunResult) on success.

  Raises:
    ClaimError on fatal errors (e.g. the slice was already claimed or expired).
    CommitError etc. on transient datastore errors.
  """
  assert claim_id

  to_run = to_run_key.get()
  if not to_run:
    raise ClaimError('No task slice')

  # Check dimensions match before trusting any other inputs. This is a security
  # check. A bot must not be able to claim slices it doesn't match even if the
  # bot knows their IDs.
  match_bot_dimensions = task_to_run.dimensions_matcher(bot_dimensions)
  if not match_bot_dimensions(to_run.dimensions):
    raise ClaimError('Dimensions mismatch')

  # Check the slice is still available before running the transaction.
  if not to_run.is_reapable:
    if to_run.claim_id != claim_id:
      raise ClaimError('No longer available')
    # The caller already holds the claim and this is a retry. Just fetch all
    # entities that should already exist.
    run_result_key = task_pack.request_key_to_run_result_key(to_run.request_key)
    request, run_result = ndb.get_multi([to_run.request_key, run_result_key],
                                        use_cache=False,
                                        use_memcache=False)
    if not run_result:
      raise Error('TaskRunResult is unexpectedly missing')
    if run_result.current_task_slice != to_run.task_slice_index:
      raise ClaimError('Obsolete')
    secret_bytes = None
    if request.task_slice(to_run.task_slice_index).properties.has_secret_bytes:
      secret_bytes = request.secret_bytes_key.get()
    return request, secret_bytes, run_result

  # Transactionally claim the slice. This can raise ClaimError or CommitError.
  request = to_run.request_key.get()
  run_result, secret_bytes = _reap_task(bot_dimensions, bot_details, to_run_key,
                                        request, claim_id, 3, False)
  return request, secret_bytes, run_result


def bot_update_task(run_result_key, bot_id, output, output_chunk_start,
                    exit_code, duration, hard_timeout, io_timeout, cost_usd,
                    cas_output_root, cipd_pins, performance_stats, canceled):
  """Updates a TaskRunResult and TaskResultSummary, along TaskOutputChunk.

  Arguments:
  - run_result_key: ndb.Key to TaskRunResult.
  - bot_id: Self advertised bot id to ensure it's the one expected.
  - output: Data to append to this command output.
  - output_chunk_start: Index of output in the stdout stream.
  - exit_code: Mark that this task completed.
  - duration: Time spent in seconds for this task, excluding overheads.
  - hard_timeout: Bool set if an hard timeout occured.
  - io_timeout: Bool set if an I/O timeout occured.
  - cost_usd: Cost in $USD of this task up to now.
  - cas_output_root: task_request.CASReference or None.
  - cipd_pins: None or task_result.CipdPins
  - performance_stats: task_result.PerformanceStats instance or None. Can only
        be set when the task is completing.
  - canceled: Bool set if the task was canceled before running.
  Invalid states, these are flat out refused:
  - A command is updated after it had an exit code assigned to.

  Returns:
    TaskRunResult.state or None in case of failure.
  """
  assert output_chunk_start is None or isinstance(output_chunk_start, int)
  assert output is None or isinstance(output, str)
  if cost_usd is not None and cost_usd < 0.:
    raise ValueError('cost_usd must be None or greater or equal than 0')
  if duration is not None and duration < 0.:
    raise ValueError('duration must be None or greater or equal than 0')
  if (duration is None) != (exit_code is None):
    raise ValueError(
        'had unexpected duration; expected iff a command completes\n'
        'duration: %r; exit: %r' % (duration, exit_code))
  if performance_stats and duration is None:
    raise ValueError(
        'duration must be set when performance_stats is set\n'
        'duration: %s; performance_stats: %s' % (duration, performance_stats))

  packed = task_pack.pack_run_result_key(run_result_key)
  logging.debug(
      'bot_update_task(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', packed,
      bot_id,
      len(output) if output else output, output_chunk_start, exit_code,
      duration, hard_timeout, io_timeout, cost_usd, cas_output_root, cipd_pins,
      performance_stats)

  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request = task_pack.result_summary_key_to_request_key(
      result_summary_key).get()

  need_cancel = False
  es_cfg = None
  # Kill this task if parent task is not running nor pending.
  if request.parent_task_id:
    parent_run_key = task_pack.unpack_run_result_key(request.parent_task_id)
    parent = parent_run_key.get(use_cache=False, use_memcache=False)
    need_cancel = parent.state not in task_result.State.STATES_RUNNING
    if need_cancel:
      es_cfg = external_scheduler.config_for_task(request)

  now = utils.utcnow()
  run = lambda: _bot_update_tx(
      run_result_key, bot_id, output, output_chunk_start, exit_code, duration,
      hard_timeout, io_timeout, cost_usd, cas_output_root, cipd_pins,
      need_cancel, performance_stats, now, result_summary_key, request, es_cfg,
      canceled)
  try:
    logging.info('Starting transaction')
    smry, run_result, error = datastore_utils.transaction(run, retries=3)
    logging.info('Transaction committed')
  except datastore_utils.CommitError as e:
    logging.info('Got commit error: %s', e)
    # It is important that the caller correctly surface this error as the bot
    # will retry on HTTP 500.
    return None
  if smry and smry.state != task_result.State.RUNNING:
    # Take no chance and explicitly clear the ndb memcache entry. A very rare
    # race condition is observed where a stale version of the entities it kept
    # in memcache.
    ndb.get_context()._clear_memcache([result_summary_key,
                                       run_result_key]).check_success()
  assert bool(error) != bool(run_result), (error, run_result)
  if error:
    logging.error('Task %s %s', packed, error)
    return None

  update_pubsub_success = _maybe_pubsub_notify_now(smry, request)

  # Caller must retry if PubSub enqueue fails.
  if not update_pubsub_success:
    return None
  if smry.state not in task_result.State.STATES_RUNNING:
    ok = utils.enqueue_task(
      '/internal/taskqueue/important/tasks/cancel-children-tasks',
      'cancel-children-tasks',
      payload=utils.encode_to_json({'task': smry.task_id})
    )
    if not ok:
      raise Error(
          'Failed to push cancel children task for %s' % smry.task_id)

  # Hack a bit to tell the bot what it needs to hear (see handler_bot.py). It's
  # kind of an ugly hack but the other option is to return the whole run_result.
  run_result_state = run_result.state
  if run_result.killing:
    run_result_state = task_result.State.KILLED
  if request.has_build_task:
    update_id = utils.time_time_ns()
    if not _buildbucket_update(request.key, run_result_state, update_id):
      return None
  return run_result_state


def bot_terminate_task(run_result_key, bot_id, start_time, client_error):
  """Terminates a task that is currently running as an internal failure.

  Sets the TaskRunResult's state to
  - CLIENT_ERROR if missing_cipd or missing_cas
  - KILLED if it's canceled
  - BOT_DIED otherwise

  Returns:
    str if an error message.
  """
  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request = task_pack.result_summary_key_to_request_key(
      result_summary_key).get()
  now = utils.utcnow()
  packed = task_pack.pack_run_result_key(run_result_key)
  es_cfg = external_scheduler.config_for_task(request)

  def run():
    run_result, result_summary = ndb.get_multi(
        (run_result_key, result_summary_key),
        use_cache=False,
        use_memcache=False)
    if bot_id and run_result.bot_id != bot_id:
      return 'Bot %s sent task kill for task %s owned by bot %s' % (
          bot_id, packed, run_result.bot_id)

    if run_result.state == task_result.State.BOT_DIED:
      # Ignore this failure.
      return None

    run_result.signal_server_version()
    missing_cas = None
    missing_cipd = None
    if client_error:
      missing_cas = client_error.get('missing_cas')
      missing_cipd = client_error.get('missing_cipd')
    if run_result.killing:
      run_result.killing = False
      run_result.state = task_result.State.KILLED
      run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
      run_result.internal_failure = True
      # run_result.abandoned_ts is set when run_result.killing == True
      actual_state_change_time = run_result.abandoned_ts
    elif missing_cipd or missing_cas:
      run_result.state = task_result.State.CLIENT_ERROR
      actual_state_change_time = start_time
      run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
      if missing_cipd:
        run_result.missing_cipd = [
            task_request.CipdPackage.create_from_json(cipd)
            for cipd in missing_cipd
        ]
      if missing_cas:
        run_result.missing_cas = [
            task_request.CASReference.create_from_json(cas)
            for cas in missing_cas
        ]
      run_result.internal_failure = False
    else:
      run_result.state = task_result.State.BOT_DIED
      # this should technically be the exact time when the bot terminates the
      # running this task as a bot died, but this is a close approximation
      actual_state_change_time = start_time
      run_result.internal_failure = True
    run_result.abandoned_ts = now
    run_result.completed_ts = now
    run_result.modified_ts = now
    run_result.dead_after_ts = None
    result_summary.set_from_run_result(run_result, request)

    futures = ndb.put_multi_async((run_result, result_summary))
    _maybe_taskupdate_notify_via_tq(result_summary,
                                    request,
                                    es_cfg,
                                    transactional=True)
    for f in futures:
      f.check_success()
    latency = utils.utcnow() - actual_state_change_time
    ts_mon_metrics.on_dead_task_detection_latency(result_summary.tags, latency,
                                                  False)
    return None

  try:
    msg = datastore_utils.transaction(run)
  except datastore_utils.CommitError as e:
    # At worst, the task will be tagged as BOT_DIED after BOT_PING_TOLERANCE
    # seconds passed on the next cron_handle_bot_died cron job.
    return 'Failed killing task %s: %s' % (packed, e)
  return msg


def cancel_task_with_id(task_id, kill_running, bot_id):
  """Cancels a task if possible, setting it to either CANCELED or KILLED.

  Warning: ACL check must have been done before.

  See cancel_task for argument and return value details."""
  if not task_id:
    logging.error('Cannot cancel a blank task')
    return False, False
  request_key, result_key = task_pack.get_request_and_result_keys(task_id)
  if not request_key or not result_key:
    logging.error('Cannot search for a falsey key. Request: %s Result: %s',
                  request_key, result_key)
    return False, False
  request_obj = request_key.get()
  if not request_obj:
    logging.error('Request for %s was not found.', request_key.id())
    return False, False

  return cancel_task(request_obj, result_key, kill_running, bot_id)


def cancel_task(request, result_key, kill_running, bot_id):
  """Cancels a task if possible, setting it to either CANCELED or KILLED.

  Ensures that the associated TaskToRunShard is canceled (when pending) and
  updates the TaskResultSummary/TaskRunResult accordingly.
  The TaskRunResult.state is immediately set to KILLED for running tasks.

  Warning: ACL check must have been done before.

  Arguments:
    request: TaskRequest instance to cancel.
    result_key: result key for request to cancel.
    kill_running: if true, allow cancelling a task in RUNNING state.
    bot_id: if specified, only cancel task if it is RUNNING on this bot. Cannot
            be specified if kill_running is False.
  Returns:
    tuple(bool, bool)
    - True if the cancellation succeeded. Either the task atomically changed
      from PENDING to CANCELED or it was RUNNING and killing bit has been set.
    - True if the task was running while it was canceled.

  Raises:
    datastore_utils.CommitError if the transaction failed.
  """
  if bot_id:
    assert kill_running, "Can't use bot_id if kill_running is False."
  if result_key.kind() == 'TaskRunResult':
    # Ignore the try number. A user may ask to cancel run result 1, but if it
    # BOT_DIED, it is accepted to cancel try number #2 since the task is still
    # "pending".
    result_key = task_pack.run_result_key_to_result_summary_key(result_key)
  now = utils.utcnow()
  es_cfg = external_scheduler.config_for_task(request)

  if kill_running:
    task_id = task_pack.pack_result_summary_key(result_key)
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/tasks/cancel-children-tasks',
        'cancel-children-tasks',
        payload=utils.encode_to_json({
            'task': task_id,
        }))
    if not ok:
      raise Error('Failed to enqueue task to cancel-children-tasks queue;'
                  ' task_id: %s' % task_id)

  def run():
    result_summary = result_key.get(use_cache=False, use_memcache=False)
    orig_state = result_summary.state
    succeeded, was_running = _cancel_task_tx(request, result_summary,
                                             kill_running, bot_id, now, es_cfg)
    state_changed = result_summary.state != orig_state
    return succeeded, was_running, state_changed, result_summary

  succeeded, was_running, state_changed, result_summary = \
    datastore_utils.transaction(run)
  if state_changed:
    ts_mon_metrics.on_task_status_change_scheduler_latency(result_summary)
  return succeeded, was_running


def cancel_tasks(limit, query, cursor=None):
  # type: (int, ndb.Query, Optional[str])
  #     -> Tuple[str, Sequence[task_result.TaskResultSummary]]
  """
  Raises:
    ValueError if limit is not within [1, 1000] or cursor is not valid.
    handlers_exceptions.InternalException if cancel request could not be
        enqueued.
  """
  results, cursor = datastore_utils.fetch_page(query, limit, cursor)

  if results:
    payload = json.dumps({
        'tasks': [r.task_id for r in results],
        'kill_running': True,
    })
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/tasks/cancel',
        'cancel-tasks',
        payload=payload)
    if not ok:
      raise handlers_exceptions.InternalException(
          'Could not enqueue cancel request, try again later')
  else:
    logging.info('No tasks to cancel.')

  return cursor, results


def expire_slice(to_run_key, terminal_state, reason):
  """Expires a slice represented by the given TaskToRunShard entity.

  Schedules the next slice, if possible, or terminates the task with the given
  terminal_state if there are no more slices that can run. Intended to be used
  for RBE tasks.

  Does nothing if the slice is not in pending state anymore or the task was
  deleted already.

  Arguments:
    to_run_key: an entity key of TaskToRunShard entity to expire.
    terminal_state: the task state to set if this slice is the last one.
    reason: a string tsmon label used to identify how expiration happened.

  Raises:
    datastore_utils.CommitError on transaction errors.
  """
  request_key = task_to_run.task_to_run_key_to_request_key(to_run_key)
  request = request_key.get()
  if not request:
    logging.warning('TaskRequest doesn\'t exist')
    return
  _expire_slice(request,
                to_run_key,
                terminal_state,
                claim=False,
                txn_retries=4,
                txn_catch_errors=False,
                reason=reason)


### Cron job.


def cron_abort_expired_task_to_run():
  """Aborts expired TaskToRunShard requests to execute a TaskRequest on a bot.

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

  This cron job just emits Task Queue tasks handled by task_expire_tasks(...).
  """
  enqueued = []

  def _enqueue_task(to_runs):
    payload = {
        'entities': [(ttr.task_id, ttr.shard_index, ttr.key.integer_id())
                     for ttr in to_runs],
    }
    logging.debug('Expire tasks: %s', payload['entities'])
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/tasks/expire',
        'task-expire',
        payload=utils.encode_to_json(payload))
    if not ok:
      logging.warning('Failed to enqueue task for %d tasks', len(to_runs))
    else:
      enqueued.append(len(to_runs))

  delay_sec = 0.0
  if pools_config.all_pools_migrated_to_rbe():
    logging.info('Delaying the expiration check to expire through RBE instead')
    delay_sec = 60.0

  task_to_runs = []
  try:
    for to_run in task_to_run.yield_expired_task_to_run(delay_sec):
      task_to_runs.append(to_run)
      # Enqueue every 50 TaskToRunShards.
      if len(task_to_runs) == 50:
        _enqueue_task(task_to_runs)
        task_to_runs = []
    # Enqueue remaining TaskToRunShards.
    if task_to_runs:
      _enqueue_task(task_to_runs)
  finally:
    logging.debug('Enqueued %d task for %d tasks', len(enqueued), sum(enqueued))


def cron_handle_bot_died():
  """Aborts TaskRunResult where the bot stopped sending updates.

  The task will be canceled.

  Returns:
  - task IDs killed
  - number of task ignored
  """
  count = {'total': 0, 'ignored': 0, 'killed': 0}
  killed = []
  futures = []

  def _handle_future(f):
    key, state_changed, latency, tags = None, False, None, None
    try:
      key, state_changed, latency, tags = f.get_result()
    except datastore_utils.CommitError as e:
      logging.error('Failed to updated dead task. error=%s', e)

    if key:
      killed.append(task_pack.pack_run_result_key(key))
      count['killed'] += 1
    else:
      count['ignored'] += 1
    checked = count['killed'] + count['ignored']
    if checked % 500 == 0:
      logging.info('Checked %d tasks', checked)
    if state_changed:
      ts_mon_metrics.on_dead_task_detection_latency(tags, latency, True)

  def _wait_futures(futs, cap):
    while len(futs) > cap:
      ndb.Future.wait_any(futs)
      _futs = []
      for f in futs:
        if f.done():
          _handle_future(f)
        else:
          _futs.append(f)
      futs = _futs
    return futs

  start = utils.utcnow()
  # Timeout at 9.5 mins, we want to gracefully terminate prior to App Engine
  # handler expiry. This will reduce the deadline exceeded 500 errors arising
  # from the endpoint.
  time_to_stop = start + datetime.timedelta(seconds=int(9.5 * 60))
  try:
    cursor = None
    q = task_result.TaskRunResult.query(
        task_result.TaskRunResult.completed_ts == None)
    while utils.utcnow() <= time_to_stop:
      keys, cursor, more = q.fetch_page(500,
                                        keys_only=True,
                                        start_cursor=cursor)
      if not keys:
        break
      count['total'] += len(keys)
      logging.info('Fetched %d keys', count['total'])
      for run_result_key in keys:
        f = _detect_dead_task_async(run_result_key)
        if f:
          futures.append(f)
        else:
          count['ignored'] += 1
        # Limit the number of futures.
        futures = _wait_futures(futures, 5)
      # wait the remaining ones.
      _wait_futures(futures, 0)
      if not more:
        break
  finally:
    now = utils.utcnow()
    logging.info('cron_handle_bot_died time elapsed: %ss',
                 (now - start).total_seconds())
    if now > time_to_stop:
      logging.warning('Terminating cron_handle_bot_died early')
    if killed:
      logging.warning('BOT_DIED!\n%d tasks:\n%s', count['killed'],
                      '\n'.join('  %s' % i for i in killed))
    logging.info('total %d, killed %d, ignored: %d', count['total'],
                 count['killed'], count['ignored'])
  # These are returned primarily for unit testing verification.
  return killed, count['ignored']


def cron_handle_external_cancellations():
  """Fetch and handle external scheduler cancellations for all pools."""
  known_pools = pools_config.known()
  for pool in known_pools:
    pool_cfg = pools_config.get_pool_config(pool)
    if not pool_cfg.external_schedulers:
      continue
    for es_cfg in pool_cfg.external_schedulers:
      if not es_cfg.enabled:
        continue
      cancellations = external_scheduler.get_cancellations(es_cfg)
      if not cancellations:
        continue

      for c in cancellations:
        data = {
          u'bot_id': c.bot_id,
          u'task_id': c.task_id,
        }
        payload = utils.encode_to_json(data)
        if not utils.enqueue_task(
            '/internal/taskqueue/important/tasks/cancel-task-on-bot',
            queue_name='cancel-task-on-bot', payload=payload):
          logging.error('Failed to enqueue task-cancellation.')


def cron_handle_get_callbacks():
  """Fetch and handle external desired callbacks for all pools."""
  known_pools = pools_config.known()
  for pool in known_pools:
    pool_cfg = pools_config.get_pool_config(pool)
    if not pool_cfg.external_schedulers:
      continue
    for es_cfg in pool_cfg.external_schedulers:
      if not es_cfg.enabled:
        continue
      request_ids = external_scheduler.get_callbacks(es_cfg)
      if not request_ids:
        continue

      items = []
      for task_id in request_ids:
        request_key, result_key = task_pack.get_request_and_result_keys(task_id)
        request = request_key.get()
        result = result_key.get(use_cache=False, use_memcache=False)
        items.append((request, result))
        # Send mini batch to avoid TaskTooLargeError. crbug.com/1175618
        if len(items) >= 20:
          external_scheduler.notify_requests(es_cfg, items, True, True)
          items = []
      if items:
        external_scheduler.notify_requests(es_cfg, items, True, True)


## Task queue tasks.


def task_handle_pubsub_task(payload):
  """Handles task enqueued by _maybe_pubsub_notify_via_tq."""
  # Do not catch errors to trigger task queue task retry. Errors should not
  # happen in normal case.
  logging.debug("Received PubSub notify payload: (%s)", str(payload))
  try:
    _pubsub_notify(payload['task_id'], payload['topic'], payload['auth_token'],
                   payload['userdata'], payload['tags'], payload['state'],
                   payload.get('start_time'))
  except pubsub.Error:
    logging.exception('Fatal error when sending PubSub notification')


def task_expire_tasks(task_to_runs):
  """Expires TaskToRunShardXXX enqueued by cron_abort_expired_task_to_run.

  Arguments:
    task_to_runs: a list of (<task ID>, <TaskToRunShard index>, <entity ID>).
  """
  expired = []
  reenqueued = 0
  skipped = 0

  try:
    for task_id, shard_index, entity_id in task_to_runs:
      # Convert arguments to TaskToRunShardXXX key.
      request_key, _ = task_pack.get_request_and_result_keys(task_id)
      to_run_key = task_to_run.task_to_run_key_from_parts(
          request_key, shard_index, entity_id)

      # Retrieve the request to know what slice to enqueue next (if any).
      request = request_key.get()
      if not request:
        logging.error('Task %s was not found.', task_id)
        continue

      # Expire the slice and schedule the next one (if any). Obtain a claim
      # to make sure bot_reap_task doesn't try to pick it up.
      summary, new_to_run = _expire_slice(
          request,
          to_run_key,
          task_result.State.EXPIRED,
          claim=True,
          txn_retries=4,
          txn_catch_errors=True,
          reason='task_expire_tasks',
      )
      if new_to_run:
        # The next slice was enqueued.
        reenqueued += 1
      elif summary:
        # The task was updated, but there's no new slice => the task expired.
        slice_index = task_to_run.task_to_run_key_slice_index(to_run_key)
        expired.append(
            (task_id, request.task_slice(slice_index).properties.dimensions))
      else:
        # The task was not updated => the slice already expired or the
        # transaction failed.
        skipped += 1
  finally:
    if expired:
      logging.info(
          'EXPIRED!\n%d tasks:\n%s', len(expired),
          '\n'.join('  %s  %s' % (task_id, dims) for task_id, dims in expired))
    logging.info('Reenqueued %d tasks, expired %d, skipped %d', reenqueued,
                 len(expired), skipped)


def task_cancel_running_children_tasks(parent_result_summary_id):
  """Enqueues task queue to cancel non-completed children tasks."""
  q = task_result.yield_result_summary_by_parent_task_id(
      parent_result_summary_id)
  children_tasks_per_version = {}
  for task in q:
    if task.state not in task_result.State.STATES_RUNNING:
      continue
    version = task.server_versions[0]
    children_tasks_per_version.setdefault(version, []).append(task.task_id)

  for version, tasks in sorted(children_tasks_per_version.items()):
    logging.info('Sending %d tasks to version %s', len(tasks), version)
    payload = utils.encode_to_json({
      'tasks': tasks,
      'kill_running': True,
    })
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/tasks/cancel',
        'cancel-tasks', payload=payload,
        # cancel task on specific version of backend module.
        use_dedicated_module=False,
        version=version)
    if not ok:
      raise Error(
          'Failed to enqueue task to cancel queue; version: %s, payload: %s' % (
            version, payload))


def task_buildbucket_update(payload):
  """Handles sending a pubsub update to buildbucket or not.
  """
  request_key = task_pack.result_summary_key_to_request_key(
      task_pack.unpack_result_summary_key(payload["task_id"]))
  state = payload["state"]
  update_id = payload['update_id']
  if not _buildbucket_update(request_key, state, update_id):
    logging.exception(
        'Fatal error when sending buildbucket update notification')
    raise Error(
        'Transient pubsub error. Failed to update buildbucket with payload %s' %
        payload)
