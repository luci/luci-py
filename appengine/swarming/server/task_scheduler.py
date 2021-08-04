# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""High level tasks execution scheduling API.

This is the interface closest to the HTTP handlers.
"""

import collections
import datetime
import logging
import math
import random
import time
import urlparse

from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import pubsub
from components import utils

import ts_mon_metrics

from server import bot_management
from server import config
from server import external_scheduler
from server import pools_config
from server import resultdb
from server import service_accounts_utils
from server import task_pack
from server import task_queues
from server import task_request
from server import task_result
from server import task_to_run


### Private stuff.


_PROBABILITY_OF_QUICK_COMEBACK = 0.05

# When falling back from external scheduler, requests that belong to any
# external scheduler are ignored for this duration at the beginning of their
# life. This number should be larger than the bot polling period.
_ES_FALLBACK_SLACK = datetime.timedelta(minutes=6)


class Error(Exception):
  pass


def _secs_to_ms(value):
  """Converts a seconds value in float to the number of ms as an integer."""
  return int(round(value * 1000.))


def _expire_task_tx(now, request, to_run_key, result_summary_key, capacity,
                    es_cfg):
  """Expires a to_run_key and look for a TaskSlice fallback.

  Called as a ndb transaction by _expire_task().

  2 concurrent GET, one PUT. Optionally with an additional serialized GET.
  """
  to_run_future = to_run_key.get_async()
  result_summary_future = result_summary_key.get_async()
  to_run = to_run_future.get_result()
  if not to_run or not to_run.is_reapable:
    if not to_run.expiration_ts:
      result_summary_future.get_result()
      return None, None
    logging.info('%s/%s: not reapable. but continuing expiration.',
                 to_run.task_id, to_run.task_slice_index)

  # record expiration delay
  delay = (now - to_run.expiration_ts).total_seconds()
  to_run.expiration_delay = max(0, delay)

  if delay <= 0:
    logging.warning(
        '_expire_task_tx: slice_expiration_delay is expected to be > 0, '
        'but was %s sec. run_id=%s, now=%s, expiration_ts=%s', delay,
        to_run.task_id, now, to_run.expiration_ts)

  # In any case, dequeue the TaskToRun.
  to_run.queue_number = None
  to_run.expiration_ts = None
  result_summary = result_summary_future.get_result()
  to_put = [to_run, result_summary]
  # Check if there's a TaskSlice fallback that could be reenqueued.
  new_to_run = None
  offset = result_summary.current_task_slice+1
  rest = request.num_task_slices - offset
  for index in range(rest):
    # Use the lookup created just before the transaction. There's a small race
    # condition in here but we're willing to accept it.
    if len(capacity) > index and capacity[index]:
      # Enqueue a new TasktoRun for this next TaskSlice, it has capacity!
      new_to_run = task_to_run.new_task_to_run(request, index + offset)
      result_summary.current_task_slice = index+offset
      to_put.append(new_to_run)
      break
    if len(capacity) <= index:
      # crbug.com/1030504
      # This invalid situation probably come from the invalid task-expire task
      # parameters (task_id, try_number, task_slice_index), and the task-expire
      # task will be enqueued with a valid parameters in a cron job later.
      # So ignoring here should not be a problem.
      logging.warning(
          'crbug.com/1030504: invalid capacity length or slice index. '
          'index=%d, capacity=%s\n'
          'TaskResultSummary: task_id=%s, current_task_slice=%d, '
          'num_task_slices=%d\n'
          'TaskToRun: task_id=%s, task_slice_index=%d, try_number=%d', index,
          capacity, result_summary.task_id, result_summary.current_task_slice,
          request.num_task_slices, to_run.task_id, to_run.task_slice_index,
          to_run.try_number)

  if not new_to_run:
    # There's no fallback, giving up.
    if result_summary.try_number:
      # It's a retry that is being expired, i.e. the first try had BOT_DIED.
      # Keep the old state. That requires an additional pipelined GET but that
      # shouldn't be the common case.
      run_result = result_summary.run_result_key.get()
      result_summary.set_from_run_result(run_result, request)
    else:
      result_summary.state = task_result.State.EXPIRED
    result_summary.abandoned_ts = now
    result_summary.completed_ts = now
    delay = (now - request.expiration_ts).total_seconds()
    result_summary.expiration_delay = max(0, delay)
    if delay <= 0:
      logging.warning(
          '_expire_task_tx: expiration_delay is expected to be > 0, '
          'but was %s sec. task_id=%s, now=%s, expiration_ts=%s', delay,
          result_summary.task_id, now, request.expiration_ts)

  result_summary.modified_ts = now

  futures = ndb.put_multi_async(to_put)
  _maybe_taskupdate_notify_via_tq(
      result_summary, request, es_cfg, transactional=True)
  for f in futures:
    f.check_success()

  return result_summary, new_to_run


def _expire_task(to_run_key, request, inline):
  """Expires a TaskResultSummary and unschedules the TaskToRun.

  This function is only meant to process PENDING tasks.

  Arguments:
    to_run_key: the TaskToRun to expire
    request: the corresponding TaskRequest instance
    inline: True if this is done as part of a bot polling for a task to run,
            in this case it should abort as quickly as possible

  If a follow up TaskSlice is available, reenqueue a new TaskToRun instead of
  expiring the TaskResultSummary.

  Returns:
    tuple of
    - TaskResultSummary on success
    - TaskToRun if a new TaskSlice was reenqueued
  """
  # Add it to the negative cache *before* running the transaction. Either way
  # the task was already reaped or the task is correctly expired and not
  # reapable.
  if not task_to_run.set_lookup_cache(to_run_key, False) and inline:
    logging.info('Not expiring inline task with negative cache set')
    return None, None

  # Look if the TaskToRun is reapable once before doing the check inside the
  # transaction. This reduces the likelihood of failing this check inside the
  # transaction, which is an order of magnitude more costly.
  to_run = to_run_key.get()
  if not to_run.is_reapable:
    if not to_run.expiration_ts:
      logging.info('Not reapable anymore')
      return None, None
    logging.info('%s/%s: not reapable. but continuing expiration.',
                 to_run.task_id, to_run.task_slice_index)

  result_summary_key = task_pack.request_key_to_result_summary_key(request.key)
  now = utils.utcnow()

  # Do a quick check for capacity for the remaining TaskSlice (if any) before
  # the transaction runs, as has_capacity() cannot be called while the
  # transaction runs.
  index = task_to_run.task_to_run_key_slice_index(to_run_key)
  slices = [
      request.task_slice(i) for i in range(index + 1, request.num_task_slices)
  ]
  capacity = [
    t.wait_for_capacity or bot_management.has_capacity(t.properties.dimensions)
    for t in slices
  ]

  # When running inline, do not retry too much.
  retries = 1 if inline else 4

  es_cfg = external_scheduler.config_for_task(request)

  # It'll be caught by next cron job execution in case of failure.
  run = lambda: _expire_task_tx(
      now, request, to_run_key, result_summary_key, capacity, es_cfg)
  try:
    summary, new_to_run = datastore_utils.transaction(run, retries=retries)
  except datastore_utils.CommitError:
    summary = None
    new_to_run = None
  if summary:
    logging.info(
        'Expired %s', task_pack.pack_result_summary_key(result_summary_key))
    ts_mon_metrics.on_task_expired(summary, to_run_key.get())
  return summary, new_to_run


def _reap_task(bot_dimensions, bot_version, to_run_key, request,
               use_lookup_cache):
  """Reaps a task and insert the results entity.

  Returns:
    (TaskRunResult, SecretBytes) if successful, (None, None) otherwise.
  """
  assert request.key == task_to_run.task_to_run_key_to_request_key(to_run_key)
  result_summary_key = task_pack.request_key_to_result_summary_key(request.key)
  bot_id = bot_dimensions[u'id'][0]

  now = utils.utcnow()
  # Log before the task id in case the function fails in a bad state where the
  # DB TX ran but the reply never comes to the bot. This is the worst case as
  # this leads to a task that results in BOT_DIED without ever starting. This
  # case is specifically handled in cron_handle_bot_died().
  logging.info(
      '_reap_task(%s)', task_pack.pack_result_summary_key(result_summary_key))

  es_cfg = external_scheduler.config_for_task(request)

  def run():
    # 3 GET, 1 PUT at the end.
    to_run_future = to_run_key.get_async()
    result_summary_future = result_summary_key.get_async()
    to_run = to_run_future.get_result()
    t = request.task_slice(to_run.task_slice_index)
    if t.properties.has_secret_bytes:
      secret_bytes_future = request.secret_bytes_key.get_async()
    result_summary = result_summary_future.get_result()
    orig_summary_state = result_summary.state
    secret_bytes = None
    if t.properties.has_secret_bytes:
      secret_bytes = secret_bytes_future.get_result()
    if not to_run:
      logging.error('Missing TaskToRun?\n%s', result_summary.task_id)
      return None, None
    if not to_run.is_reapable:
      logging.info('%s is not reapable', result_summary.task_id)
      return None, None
    if result_summary.bot_id == bot_id:
      # This means two things, first it's a retry, second it's that the first
      # try failed and the retry is being reaped by the same bot. Deny that, as
      # the bot may be deeply broken and could be in a killing spree.
      # TODO(maruel): Allow retry for bot locked task using 'id' dimension.
      logging.warning('%s can\'t retry its own internal failure task',
                      result_summary.task_id)
      return None, None
    to_run.queue_number = None
    to_run.expiration_ts = None
    run_result = task_result.new_run_result(request, to_run, bot_id,
                                            bot_version, bot_dimensions,
                                            result_summary.resultdb_info)
    # Upon bot reap, both .started_ts and .modified_ts matches. They differ on
    # the first ping.
    run_result.started_ts = now
    run_result.modified_ts = now
    # Upon bot reap, set .dead_after_ts taking into consideration the
    # user-provided keep-alive value. This is updated after each ping
    # from the bot."
    run_result.dead_after_ts = now + datetime.timedelta(
        seconds=request.bot_ping_tolerance_secs)
    result_summary.set_from_run_result(run_result, request)
    ndb.put_multi([to_run, run_result, result_summary])
    if result_summary.state != orig_summary_state:
      _maybe_taskupdate_notify_via_tq(
          result_summary, request, es_cfg, transactional=True)
    return run_result, secret_bytes

  # Add it to the negative cache *before* running the transaction. This will
  # inhibit concurrently readers to try to reap this task. The downside is if
  # this request fails in the middle of the transaction, the task may stay
  # unreapable for up to 15 seconds.
  # This is unnecessary and skipped when using an external scheduler, because
  # that already avoids datastore entity contention.
  if use_lookup_cache and not task_to_run.set_lookup_cache(to_run_key, False):
    logging.debug('hit negative cache')
    return None, None

  try:
    run_result, secret_bytes = datastore_utils.transaction(run, retries=0)
  except datastore_utils.CommitError:
    # The challenge here is that the transaction may have failed because:
    # - The DB had an hickup and the TaskToRun, TaskRunResult and
    #   TaskResultSummary haven't been updated.
    # - The entities had been updated by a concurrent transaction on another
    #   handler so it was not reapable anyway. This does cause exceptions as
    #   both GET returns the TaskToRun.queue_number != None but only one succeed
    #   at the PUT.
    #
    # In the first case, we may want to reset the negative cache, while we don't
    # want to in the later case. The trade off are one of:
    # - negative cache is incorrectly set, so the task is not reapable for 15s
    # - resetting the negative cache would cause even more contention
    #
    # We chose the first one here for now, as the when the DB starts misbehaving
    # and the index becomes stale, it means the DB is *already* not in good
    # shape, so it is preferable to not put more stress on it, and skipping a
    # few tasks for 15s may even actively help the DB to stabilize.
    logging.info('CommitError; reaping failed')
    # The bot will reap the next available task in case of failure, no big deal.
    run_result = None
    secret_bytes = None
  return run_result, secret_bytes


def _handle_dead_bot(run_result_key):
  """Handles TaskRunResult where its bot has stopped showing sign of life.

  Transactionally updates the entities depending on the state of this task. The
  task may be retried automatically, canceled or left alone.

  Returns:
    True if the task was killed, False if no action was done.
  """
  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request_key = task_pack.result_summary_key_to_request_key(result_summary_key)
  request_future = request_key.get_async()
  now = utils.utcnow()
  server_version = utils.get_app_version()
  request = request_future.get_result()
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
    return False
  es_cfg = external_scheduler.config_for_task(request)

  def run():
    """Returns True if the task becomes killed or bot died.

    1x GET, 1x GETs 2~3x PUT.
    """
    run_result = run_result_key.get()

    if run_result.state != task_result.State.RUNNING:
      # It was updated already or not updating last. Likely DB index was stale.
      return False

    if not run_result.dead_after_ts or run_result.dead_after_ts > now:
      return False

    run_result.signal_server_version(server_version)
    run_result.modified_ts = now
    run_result.completed_ts = now
    if not run_result.abandoned_ts:
      run_result.abandoned_ts = now
    # set .dead_after_ts to None since the task is terminated.
    run_result.dead_after_ts = None
    # mark as internal failure as the task doesn't get completed normally.
    run_result.internal_failure = True

    result_summary = result_summary_key.get()
    result_summary.modified_ts = now
    result_summary.completed_ts = now
    if not result_summary.abandoned_ts:
      result_summary.abandoned_ts = now
    orig_summary_state = result_summary.state

    # Mark it as KILLED if run_result is in killing state.
    # Otherwise, mark it BOT_DIED. the bot hasn't been sending for the task.
    to_put = (run_result, result_summary)
    if run_result.killing:
      logging.warning('Killing task %s/%s on the missing bot %s',
                      run_result.task_id, run_result.current_task_slice,
                      run_result.bot_id)
      run_result.killing = False
      run_result.state = task_result.State.KILLED
      run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
    else:
      run_result.state = task_result.State.BOT_DIED
    result_summary.set_from_run_result(run_result, request)

    futures = ndb.put_multi_async(to_put)
    # if result_summary.state != orig_summary_state:
    if orig_summary_state != result_summary.state:
      _maybe_taskupdate_notify_via_tq(
          result_summary, request, es_cfg, transactional=True)
    for f in futures:
      f.check_success()
    return True

  try:
    return datastore_utils.transaction(run)
  except datastore_utils.CommitError:
    return False


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
    try:
      _pubsub_notify(task_id, request.pubsub_topic, request.pubsub_auth_token,
                     request.pubsub_userdata)
    except pubsub.TransientError:
      logging.exception('Transient error when sending PubSub notification')
      return False
    except pubsub.Error:
      logging.exception('Fatal error when sending PubSub notification')
      return True # do not retry it
  return True


def _maybe_taskupdate_notify_via_tq(
    result_summary, request, es_cfg, transactional):
  """Enqueues tasks to send PubSub and es notifications for given request.

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
  if request.pubsub_topic:
    task_id = task_pack.pack_result_summary_key(result_summary.key)
    payload = {
      'task_id': task_id,
      'topic': request.pubsub_topic,
      'auth_token': request.pubsub_auth_token,
      'userdata': request.pubsub_userdata,
    }
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/pubsub/notify-task/%s' % task_id,
        'pubsub',
        transactional=transactional,
        payload=utils.encode_to_json(payload))
    if not ok:
      raise datastore_utils.CommitError('Failed to enqueue task')

  if es_cfg:
    external_scheduler.notify_requests(
        es_cfg, [(request, result_summary)], True, False)


def _pubsub_notify(task_id, topic, auth_token, userdata):
  """Sends PubSub notification about task completion.

  Raises pubsub.TransientError on transient errors. Fatal errors are logged, but
  not retried.
  """
  logging.debug(
      'Sending PubSub notify to "%s" (with userdata "%s") about '
      'completion of "%s"', topic, userdata, task_id)
  msg = {'task_id': task_id}
  if userdata:
    msg['userdata'] = userdata
  try:
    pubsub.publish(
        topic=topic,
        message=utils.encode_to_json(msg),
        attributes={'auth_token': auth_token} if auth_token else None)
  except pubsub.Error:
    logging.exception('Fatal error when sending PubSub notification')


def _find_dupe_task(now, h):
  """Finds a previously run task that is also idempotent and completed.

  Fetch items that can be used to dedupe the task. See the comment for this
  property for more details.

  Do not use "task_result.TaskResultSummary.created_ts > oldest" here because
  this would require a composite index. It's unnecessary because TaskRequest.key
  is equivalent to decreasing TaskRequest.created_ts, ordering by key works as
  well and doesn't require a composite index.
  """
  # TODO(maruel): Make a reverse map on successful task completion so this
  # becomes a simple ndb.get().
  cls = task_result.TaskResultSummary
  q = cls.query(cls.properties_hash==h).order(cls.key)
  for i, dupe_summary in enumerate(q.iter(batch_size=1)):
    # It is possible for the query to return stale items.
    if (dupe_summary.state != task_result.State.COMPLETED or
        dupe_summary.failure):
      if i == 2:
        # Indexes are very inconsistent, give up.
        return None
      continue

    # Refuse tasks older than X days. This is due to the isolate server
    # dropping files.
    # TODO(maruel): The value should be calculated from the isolate server
    # setting and be unbounded when no isolated input was used.
    oldest = now - datetime.timedelta(
        seconds=config.settings().reusable_task_age_secs)
    if dupe_summary.created_ts <= oldest:
      return None
    return dupe_summary
  return None


def _dedupe_result_summary(dupe_summary, result_summary, task_slice_index):
  """Copies the results from dupe_summary into result_summary."""
  # PerformanceStats is not copied over, since it's not relevant, nothing
  # ran.
  _copy_summary(
      dupe_summary, result_summary,
      ('created_ts', 'modified_ts', 'name', 'user', 'tags'))
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


def _is_allowed_service_account(service_account, pool_cfg):
  """True if given service account email is permitted to be used in the pool."""
  if service_account in pool_cfg.service_accounts:
    logging.info(
        'Service account "%s" is allowed in the pool "%s" by being listed '
        'explicitly in pools.cfg', service_account, pool_cfg.name)
    return True

  ident = auth.Identity(auth.IDENTITY_USER, service_account)
  for group in pool_cfg.service_accounts_groups:
    if auth.is_group_member(group, ident):
      logging.info(
          'Service account "%s" is allowed in the pool "%s" by being listed '
          'through group "%s"', service_account, pool_cfg.name, group)
      return True

  return False


def _bot_update_tx(run_result_key, bot_id, output, output_chunk_start,
                   exit_code, duration, hard_timeout, io_timeout, cost_usd,
                   outputs_ref, cas_output_root, cipd_pins, need_cancel,
                   performance_stats, now, result_summary_key, server_version,
                   request, es_cfg, canceled):
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

  run_result_future = run_result_key.get_async()
  result_summary_future = result_summary_key.get_async()
  run_result = run_result_future.get_result()
  if not run_result:
    result_summary_future.wait()
    return None, None, 'is missing'

  if run_result.bot_id != bot_id:
    result_summary_future.wait()
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
        result_summary_future.wait()
        return None, None, 'got 2 different exit_code; %s then %s' % (
            run_result.exit_code, exit_code)
      if run_result.duration != duration:
        result_summary_future.wait()
        return None, None, 'got 2 different durations; %s then %s' % (
            run_result.duration, duration)
    else:
      run_result.duration = duration
      run_result.exit_code = exit_code

  if outputs_ref:
    run_result.outputs_ref = outputs_ref
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
        # the task update wlll include `duration` and go to the above path to
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

  run_result.signal_server_version(server_version)
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

  result_summary = result_summary_future.get_result()
  if (result_summary.try_number and
      result_summary.try_number > run_result.try_number):
    # The situation where a shard is retried but the bot running the previous
    # try somehow reappears and reports success, the result must still show
    # the last try's result. We still need to update cost_usd manually.
    result_summary.costs_usd[run_result.try_number-1] = run_result.cost_usd
    result_summary.modified_ts = now
  else:
    # Performance warning: this function calls properties_hash() which will
    # GET SecretBytes entity if there's one.
    result_summary.set_from_run_result(run_result, request)

  to_put.append(result_summary)

  if need_cancel and run_result.state in task_result.State.STATES_RUNNING:
    _cancel_task_tx(
        request, result_summary, True, bot_id, now, es_cfg, run_result)

  ndb.put_multi(to_put)

  return result_summary, run_result, None


def _set_fallbacks_to_exit_code_and_duration(run_result, now):
  """Sets fallback values to exit_code and duration"""
  if run_result.exit_code is None:
    run_result.exit_code = -1
  if not run_result.duration:
    # Calculate an approximate time.
    run_result.duration = (now - run_result.started_ts).total_seconds()
  return run_result


def _cancel_task_tx(request, result_summary, kill_running, bot_id, now, es_cfg,
                    run_result=None):
  """Runs the transaction for cancel_task().

  Arguments:
    request: TaskRequest instance to cancel.
    result_summary: result summary for request to cancel.
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
  if not result_summary.can_be_canceled:
    return False, was_running

  entities = [result_summary]
  if not was_running:
    if bot_id:
      # Deny cancelling a non-running task if bot_id was specified.
      return False, was_running
    # PENDING.
    result_summary.state = task_result.State.CANCELED
    result_summary.completed_ts = now
    to_run_key = task_to_run.request_to_task_to_run_key(
        request,
        result_summary.try_number or 1,
        result_summary.current_task_slice or 0)
    to_run_future = to_run_key.get_async()

    # Add it to the negative cache.
    task_to_run.set_lookup_cache(to_run_key, False)

    to_run = to_run_future.get_result()
    entities.append(to_run)
    to_run.queue_number = None
    to_run.expiration_ts = None
  else:
    if not kill_running:
      # Deny canceling a task that started.
      return False, was_running
    if bot_id and bot_id != result_summary.bot_id:
      # Deny cancelling a task if bot_id was specified, but task is not
      # on this bot.
      return False, was_running
    # RUNNING.
    run_result = run_result or result_summary.run_result_key.get()
    entities.append(run_result)
    # Do not change state to KILLED yet. Instead, use a 2 phase commit:
    # - set killing to True
    # - on next bot report, tell it to kill the task
    # - once the bot reports the task as terminated, set state to KILLED
    run_result.killing = True
    run_result.abandoned_ts = now
    run_result.modified_ts = now
    entities.append(run_result)
  result_summary.abandoned_ts = now
  result_summary.modified_ts = now

  futures = ndb.put_multi_async(entities)
  _maybe_taskupdate_notify_via_tq(
      result_summary, request, es_cfg, transactional=True)
  for f in futures:
    f.check_success()
  return True, was_running


def _get_task_from_external_scheduler(es_cfg, bot_dimensions):
  """Gets a task to run from external scheduler.

  Arguments:
    es_cfg: pool_config.ExternalSchedulerConfig instance.
    bot_dimensions: dimensions {string key: list of string values}

  Returns: (TaskRequest, TaskToRun) if a task was available,
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
  result_summary = result_key.get()

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
  """Ensures the existence of a TaskToRun for the given request, try, slice.

  Ensure that the given request is currently active at a given try_number and
  task_slice_index (modifying the current try or slice if necessary), and that
  no other TaskToRun is pending.

  This is intended for use as part of the external scheduler flow.

  Internally, this runs up to 2 GETs and 1 PUT in a transaction.

  Arguments:
    request: TaskRequest instance
    task_slice_index: slice index to ensure is active.

  Returns:
    TaskToRun: A saved TaskToRun instance corresponding to the given request,
               try_number, and slice, if exists, or None otherwise.
    Boolean: Whether or not it should raise exception
  """
  def run():
    logging.debug('_ensure_active_slice(%s, %d)', request.task_id,
                  task_slice_index)
    to_runs = task_to_run.TaskToRun.query(ancestor=request.key).fetch()
    to_runs = [r for r in to_runs if r.queue_number]
    if to_runs:
      if len(to_runs) != 1:
        logging.warning('_ensure_active_slice: %s != 1 TaskToRuns',
                        len(to_runs))
        return None, True
      assert len(to_runs) == 1, 'Too many pending TaskToRuns.'

    to_run = to_runs[0] if to_runs else None

    if to_run:
      if to_run.task_slice_index == task_slice_index:
        logging.debug('_ensure_active_slice: already active')
        return to_run, False

      # Deactivate old TaskToRun, create new one.
      to_run.queue_number = None
      to_run.expiration_ts = None
      new_to_run = task_to_run.new_task_to_run(request, task_slice_index)
      ndb.put_multi([to_run, new_to_run])
      logging.debug('_ensure_active_slice: added new TaskToRun')
      return new_to_run, False

    result_summary = task_pack.request_key_to_result_summary_key(
        request.key).get()
    if not result_summary:
      logging.warning('_ensure_active_slice: no TaskToRun or TaskResultSummary')
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
    logging.debug('ensure_active_slice: added new TaskToRun (no previous one)')
    return new_to_run, False

  return datastore_utils.transaction(run)


def _bot_reap_task_external_scheduler(bot_dimensions, bot_version, es_cfg):
  """Reaps a TaskToRun (chosen by external scheduler) if available.

  This is a simpler version of bot_reap_task that skips a lot of the steps
  normally taken by the native scheduler.

  Arguments:
    - bot_dimensions: The dimensions of the bot as a dictionary in
          {string key: list of string values} format.
    - bot_version: String version of the bot client.
    - es_cfg: ExternalSchedulerConfig for this bot.
  """
  request, to_run = _get_task_from_external_scheduler(es_cfg, bot_dimensions)
  if not request or not to_run:
    return None, None, None

  run_result, secret_bytes = _reap_task(
      bot_dimensions, bot_version, to_run.key, request, False)
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


def _gen_new_keys(result_summary, to_run, secret_bytes):
  """Creates new keys for the entities.

  Warning: this assumes knowledge about the hierarchy of each entity.
  """
  key = task_request.new_request_key()
  if to_run:
    to_run.key = ndb.Key(to_run.key.kind(), to_run.key.id(), parent=key)
  if secret_bytes:
    secret_bytes.key = ndb.Key(
        secret_bytes.key.kind(), secret_bytes.key.id(), parent=key)
  old = result_summary.task_id
  result_summary.key = ndb.Key(
      result_summary.key.kind(), result_summary.key.id(), parent=key)
  logging.info('%s conflicted, using %s', old, result_summary.task_id)
  return key


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


def check_schedule_request_acl(request):
  """Verifies the current caller can schedule a given task request.

  Arguments:
  - request: TaskRequest entity with information about the new task.

  Raises:
    auth.AuthorizationError if the caller is not allowed to schedule this task.
  """
  # Only terminate tasks don't have a pool. ACLs for them are handled through
  # 'acl.can_edit_bot', see 'terminate' RPC handler. Such tasks do not end up
  # hitting this function, and so we can assume there's a pool set (this is
  # checked in TaskProperties's pre put hook).
  pool = request.pool
  pool_cfg = pools_config.get_pool_config(pool)

  if not pool_cfg:
    logging.warning('Pool "%s" is not in pools.cfg', pool)
    # TODO(crbug.com/1086058): It currently returns 403 Forbidden, but should
    # return 400 BadRequest or 422 Unprocessable Entity, instead.
    raise auth.AuthorizationError(
        'Can\'t submit tasks to pool "%s", not defined in pools.cfg' % pool)

  logging.info(
      'Looking at the pool "%s" in pools.cfg, rev "%s"', pool, pool_cfg.rev)

  # Verify the caller can use the pool at all.
  check_schedule_request_acl_caller(pool_cfg)

  # Verify the requested task service account is allowed in this pool.
  check_schedule_request_acl_service_account(request, pool_cfg)


def check_schedule_request_acl_caller(pool_cfg):
  if not _is_allowed_to_schedule(pool_cfg):
    raise auth.AuthorizationError(
        'User "%s" is not allowed to schedule tasks in the pool "%s", '
        'see pools.cfg' %
        (auth.get_current_identity().to_bytes(), pool_cfg.name))


def check_schedule_request_acl_service_account(request, pool_cfg):
  # request.service_account can be 'bot' or 'none'. We don't care about these,
  # they are always allowed. We care when the service account is a real email.
  has_service_account = service_accounts_utils.is_service_account(
      request.service_account)
  if (has_service_account and
      not _is_allowed_service_account(request.service_account, pool_cfg)):
    raise auth.AuthorizationError(
        'Task service account "%s" as specified in the task request is not '
        'allowed to be used in the pool "%s". Is allowed_service_account or '
        'allowed_service_account_group specified in pools.cfg?' %
        (request.service_account, request.pool))


def schedule_request(request, secret_bytes, enable_resultdb):
  """Creates and stores all the entities to schedule a new task request.

  Assumes ACL check has already happened (see 'check_schedule_request_acl').

  The number of entities created is ~4: TaskRequest, TaskToRun and
  TaskResultSummary and (optionally) SecretBytes. They are in single entity
  group and saved in a single transaction.

  Arguments:
  - request: TaskRequest entity to be saved in the DB. It's key must not be set
             and the entity must not be saved in the DB yet.
  - secret_bytes: SecretBytes entity to be saved in the DB. It's key will be set
             and the entity will be stored by this function. None is allowed if
             there are no SecretBytes for this task.
  - enable_resultdb: Whether we use resultdb or not for this task.

  Returns:
    TaskResultSummary. TaskToRun is not returned.
  """
  assert isinstance(request, task_request.TaskRequest), request
  assert not request.key, request.key

  task_asserted_future = task_queues.assert_task_async(request)
  now = utils.utcnow()
  request.key = task_request.new_request_key()
  result_summary = task_result.new_result_summary(request)
  result_summary.modified_ts = now
  to_run = None
  if secret_bytes:
    secret_bytes.key = request.secret_bytes_key
  resultdb_update_token_future = None

  dupe_summary = None
  for i in range(request.num_task_slices):
    t = request.task_slice(i)
    if t.properties.idempotent:
      dupe_summary = _find_dupe_task(now, t.properties_hash(request))
      if dupe_summary:
        _dedupe_result_summary(dupe_summary, result_summary, i)
        # In this code path, there's not much to do as the task will not be run,
        # previous results are returned. We still need to store the TaskRequest
        # and TaskResultSummary.
        # Since the task is never scheduled, TaskToRun is not stored.
        # Since the has_secret_bytes property is already set for UI purposes,
        # and the task itself will never be run, we skip storing the
        # SecretBytes, as they would never be read and will just consume space
        # in the datastore (and the task we deduplicated with will have them
        # stored anyway, if we really want to get them again).
        secret_bytes = None
        break

  if not dupe_summary:
    # The task has to run.
    index = 0
    while index < request.num_task_slices:
      # This needs to be extremely fast.
      to_run = task_to_run.new_task_to_run(request, index)
      logging.debug('TODO(crbug.com/1186759): expiration_ts %s',
                    to_run.expiration_ts)
      #  Make sure there's capacity if desired.
      t = request.task_slice(index)
      if (t.wait_for_capacity or
          bot_management.has_capacity(t.properties.dimensions)):
        # It's pending at this index now.
        result_summary.current_task_slice = index
        break
      index += 1

    if index == request.num_task_slices:
      # Skip to_run since it's not enqueued.
      to_run = None
      # Same rationale as deduped task.
      secret_bytes = None
      # Instantaneously denied.
      result_summary.abandoned_ts = result_summary.created_ts
      result_summary.completed_ts = result_summary.created_ts
      result_summary.state = task_result.State.NO_RESOURCE

    elif enable_resultdb:
      resultdb_update_token_future = resultdb.create_invocation_async(
          task_pack.pack_run_result_key(to_run.run_result_key), request.realm,
          request.execution_deadline)

  # Determine external scheduler (if relevant) prior to making task live, to
  # make HTTP handler return as fast as possible after making task live.
  es_cfg = external_scheduler.config_for_task(request)

  # This occasionally triggers a task queue. May throw, which is surfaced to the
  # user but it is safe as the task request wasn't stored yet.
  task_asserted_future.get_result()

  if resultdb_update_token_future:
    request.resultdb_update_token = resultdb_update_token_future.get_result()
    result_summary.resultdb_info = task_result.ResultDBInfo(
        hostname=urlparse.urlparse(config.settings().resultdb.server).hostname,
        invocation=resultdb.get_invocation_name(
            task_pack.pack_run_result_key(to_run.run_result_key)),
    )

  # Storing these entities makes this task live. It is important at this point
  # that the HTTP handler returns as fast as possible, otherwise the task will
  # be run but the client will not know about it.
  _gen_key = lambda: _gen_new_keys(result_summary, to_run, secret_bytes)
  extra = filter(bool, [result_summary, to_run, secret_bytes])
  datastore_utils.insert(request, new_key_callback=_gen_key, extra=extra)

  # Note: This external_scheduler call is blocking, and adds risk
  # of the HTTP handler being slow or dying after the task was already made
  # live. On the other hand, this call is only being made for tasks in a pool
  # that use an external scheduler, and which are not effectively live unless
  # the external scheduler is aware of them.
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
    _maybe_taskupdate_notify_via_tq(
        result_summary, request, es_cfg, transactional=False)
  return result_summary


def bot_reap_task(bot_dimensions, bot_version):
  """Reaps a TaskToRun if one is available.

  The process is to find a TaskToRun where its .queue_number is set, then
  create a TaskRunResult for it.

  Arguments:
  - bot_dimensions: The dimensions of the bot as a dictionary in
          {string key: list of string values} format.
  - bot_version: String version of the bot client.

  Returns:
    tuple of (TaskRequest, SecretBytes, TaskRunResult) for the task that was
    reaped. The TaskToRun involved is not returned.
  """
  start = time.time()
  bot_id = bot_dimensions[u'id'][0]
  es_cfg = external_scheduler.config_for_bot(bot_dimensions)
  if es_cfg:
    request, secret_bytes, to_run_result = _bot_reap_task_external_scheduler(
        bot_dimensions, bot_version, es_cfg)
    if request:
      return request, secret_bytes, to_run_result
    logging.info('External scheduler did not reap any tasks, trying native '
                 'scheduler.')

  iterated = 0
  reenqueued = 0
  expired = 0
  failures = 0
  stale_index = 0
  try:
    q = task_to_run.yield_next_available_task_to_dispatch(bot_dimensions)
    for request, to_run in q:
      iterated += 1
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
      if now <= slice_expiration and expiration_ts < now:
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

        # Expiring a TaskToRun for TaskSlice may reenqueue a new TaskToRun.
        summary, new_to_run = _expire_task(to_run.key, request, inline=True)
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
        # We need to do an adhoc validation to check the new TaskToRun, so see
        # if we can harvest it too. This is slightly duplicating work in
        # yield_next_available_task_to_dispatch().
        slice_index = task_to_run.task_to_run_key_slice_index(new_to_run.key)
        t = request.task_slice(slice_index)
        if not task_to_run.match_dimensions(
            t.properties.dimensions, bot_dimensions):
          continue
        to_run = new_to_run

      run_result, secret_bytes = _reap_task(
          bot_dimensions, bot_version, to_run.key, request, True)
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
        '%d stale_index, %d failured', bot_id,
        time.time() - start, iterated, reenqueued, expired, stale_index,
        failures)


def bot_update_task(run_result_key, bot_id, output, output_chunk_start,
                    exit_code, duration, hard_timeout, io_timeout, cost_usd,
                    outputs_ref, cas_output_root, cipd_pins, performance_stats,
                    canceled):
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
  - outputs_ref: task_request.FilesRef instance or None.
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
      'bot_update_task(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
      packed, bot_id,
      len(output) if output else output, output_chunk_start, exit_code,
      duration, hard_timeout, io_timeout, cost_usd, outputs_ref,
      cas_output_root, cipd_pins, performance_stats)

  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request_key = task_pack.result_summary_key_to_request_key(result_summary_key)
  request_future = request_key.get_async()
  server_version = utils.get_app_version()
  request = request_future.get_result()

  need_cancel = False
  es_cfg = None
  # Kill this task if parent task is not running nor pending.
  if request.parent_task_id:
    parent_run_key = task_pack.unpack_run_result_key(request.parent_task_id)
    parent = ndb.get_multi((parent_run_key,))[0]
    need_cancel = parent.state not in task_result.State.STATES_RUNNING
    if need_cancel:
      es_cfg = external_scheduler.config_for_task(request)

  now = utils.utcnow()
  run = lambda: _bot_update_tx(
      run_result_key, bot_id, output, output_chunk_start, exit_code, duration,
      hard_timeout, io_timeout, cost_usd, outputs_ref, cas_output_root,
      cipd_pins, need_cancel, performance_stats, now, result_summary_key,
      server_version, request, es_cfg, canceled)
  try:
    smry, run_result, error = datastore_utils.transaction(run)
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
  # Caller must retry if PubSub enqueue fails.
  if not _maybe_pubsub_notify_now(smry, request):
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
  if run_result.killing:
    return task_result.State.KILLED
  return run_result.state


def bot_terminate_task(run_result_key, bot_id):
  """Terminates a task that is currently running as an internal failure.

  Sets the TaskRunResult's state to
  - KILLED if it's canceled
  - BOT_DIED otherwise

  Returns:
    str if an error message.
  """
  result_summary_key = task_pack.run_result_key_to_result_summary_key(
      run_result_key)
  request = task_pack.result_summary_key_to_request_key(
      result_summary_key).get()
  server_version = utils.get_app_version()
  now = utils.utcnow()
  packed = task_pack.pack_run_result_key(run_result_key)
  es_cfg = external_scheduler.config_for_task(request)

  def run():
    run_result, result_summary = ndb.get_multi(
        (run_result_key, result_summary_key))
    if bot_id and run_result.bot_id != bot_id:
      return 'Bot %s sent task kill for task %s owned by bot %s' % (
          bot_id, packed, run_result.bot_id)

    if run_result.state == task_result.State.BOT_DIED:
      # Ignore this failure.
      return None

    run_result.signal_server_version(server_version)
    if run_result.killing:
      run_result.killing = False
      run_result.state = task_result.State.KILLED
      run_result = _set_fallbacks_to_exit_code_and_duration(run_result, now)
    else:
      run_result.state = task_result.State.BOT_DIED
    run_result.internal_failure = True
    run_result.abandoned_ts = now
    run_result.completed_ts = now
    run_result.modified_ts = now
    run_result.dead_after_ts = None
    result_summary.set_from_run_result(run_result, request)

    futures = ndb.put_multi_async((run_result, result_summary))
    _maybe_taskupdate_notify_via_tq(
        result_summary, request, es_cfg, transactional=True)
    for f in futures:
      f.check_success()

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

  Ensures that the associated TaskToRun is canceled (when pending) and updates
  the TaskResultSummary/TaskRunResult accordingly. The TaskRunResult.state is
  immediately set to KILLED for running tasks.

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
    """1 DB GET, 1 memcache write, 2x DB PUTs, 1x task queue."""
    # Need to get the current try number to know which TaskToRun to fetch.
    result_summary = result_key.get()
    return _cancel_task_tx(
        request, result_summary, kill_running, bot_id, now, es_cfg)

  return datastore_utils.transaction(run)


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
  enqueued = []
  def _enqueue_task(to_runs):
    payload = {'task_to_runs': to_runs}
    ok = utils.enqueue_task(
        '/internal/taskqueue/important/tasks/expire',
        'task-expire',
        payload=utils.encode_to_json(payload))
    if not ok:
      logging.warning('Failed to enqueue task for %d tasks', len(to_runs))
    else:
      enqueued.append(len(to_runs))

  task_to_runs = []
  try:
    for to_run in task_to_run.yield_expired_task_to_run():
      summary_key = task_pack.request_key_to_result_summary_key(
          to_run.request_key)
      task_id = task_pack.pack_result_summary_key(summary_key)
      # TODO(maruel): Use (to_run.task_id, to_run.task_slice_index).
      task_to_runs.append((task_id, to_run.try_number, to_run.task_slice_index))

      # Enqueue every 50 TaskToRun's.
      if len(task_to_runs) == 50:
        logging.debug("expire tasks: %s", task_to_runs)
        _enqueue_task(task_to_runs)
        task_to_runs = []

    # Enqueue remaining TaskToRun's.
    if task_to_runs:
      logging.debug("expire tasks: %s", task_to_runs)
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
  try:
    ignored = 0
    killed = []
    try:
      for run_result_key in task_result.yield_active_run_result_keys():
        result = _handle_dead_bot(run_result_key)
        if result:
          killed.append(task_pack.pack_run_result_key(run_result_key))
        else:
          ignored += 1
    finally:
      if killed:
        logging.error(
            'BOT_DIED!\n%d tasks:\n%s',
            len(killed),
            '\n'.join('  %s' % i for i in killed))
      logging.info('Killed %d; ignored: %d', len(killed), ignored)
    # These are returned primarily for unit testing verification.
    return killed, ignored
  except datastore_errors.NeedIndexError as e:
    # When a fresh new instance is deployed, it takes a few minutes for the
    # composite indexes to be created even if they are empty. Ignore the case
    # where the index is defined but still being created by AppEngine.
    if not str(e).startswith(
        'NeedIndexError: The index for this query is not ready to serve.'):
      raise


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
        result = result_key.get()
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
  _pubsub_notify(
      payload['task_id'], payload['topic'],
      payload['auth_token'], payload['userdata'])


def task_expire_tasks(task_to_runs):
  """Expire tasks enqueued by cron_abort_expired_task_to_run."""
  killed = []
  reenqueued = 0
  skipped = 0

  try:
    for task_id, try_number, task_slice_index in task_to_runs:
      # retrieve request
      request_key, _ = task_pack.get_request_and_result_keys(task_id)
      request = request_key.get()
      if not request:
        logging.error('Task for %s was not found.', task_id)
        continue

      to_run_key = task_to_run.request_to_task_to_run_key(
          request, try_number, task_slice_index)

      # execute task expiration
      summary, new_to_run = _expire_task(to_run_key, request, inline=False)
      if new_to_run:
        # Expiring a TaskToRun for TaskSlice may reenqueue a new TaskToRun.
        reenqueued += 1
      elif summary:
        killed.append(request)
      else:
        # It's not a big deal, the bot will continue running.
        skipped += 1
  finally:
    if killed:
      logging.info(
          'EXPIRED!\n%d tasks:\n%s',
          len(killed),
          '\n'.join(
            '  %s  %s' % (
              i.task_id, i.task_slice(0).properties.dimensions)
            for i in killed))
    logging.info('Reenqueued %d tasks, killed %d, skipped %d', reenqueued,
                 len(killed), skipped)


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
