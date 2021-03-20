# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

from contextlib import contextmanager
import datetime
import logging
import threading
import six

from infra_libs.ts_mon import shared
from infra_libs.ts_mon.common import interface


def _reset_cumulative_metrics():
  """Clear the state when an instance loses its task_num assignment."""
  logging.warning(
      'Instance %s got purged from Datastore, but is still alive. '
      'Clearing cumulative metrics.', shared.instance_key_id())
  for _, metric, _, _, _ in interface.state.store.get_all():
    if metric.is_cumulative():
      metric.reset()


_flush_metrics_lock = threading.Lock()


def need_to_flush_metrics(time_now):
  """Check if metrics need flushing, and update the timestamp of last flush.

  Even though the caller of this function may not successfully flush the
  metrics, we still update the last_flushed timestamp to prevent too much work
  being done in user requests.

  Also, this check-and-update has to happen atomically, to ensure only one
  thread can flush metrics at a time.
  """
  if not interface.state.flush_enabled_fn():
    return False
  datetime_now = datetime.datetime.utcfromtimestamp(time_now)
  minute_ago = datetime_now - datetime.timedelta(seconds=60)
  with _flush_metrics_lock:
    if interface.state.last_flushed > minute_ago:
      return False
    interface.state.last_flushed = datetime_now
  return True


def flush_metrics_if_needed(time_now):
  if not need_to_flush_metrics(time_now):
    return False
  return _flush_metrics(time_now)


@contextmanager
def parallel_flush(time_now):
  """A contextmanager that may start a thread to flush metrics.

  This allows the caller to flush metrics in parallel with the contents of
  the `with` statement. If the timestamp of the last flush is old enough, then
  a new thread is created to flush metrics, and __exit__() on the `with`
  statement blocks the caller thread until the flushing thread terminates.

  Args:
    time_now: the current timestamp, which is used to determine if it needs to
      create a thread and flush metrics.
  Yields:
    The flush thread instance, if a thread was created. None, otherwise.
  """
  flush_thread = None
  if need_to_flush_metrics(time_now):
    flush_thread = threading.Thread(target=_flush_metrics, args=(time_now,))
    flush_thread.start()
  try:
    yield flush_thread
  finally:
    if flush_thread is not None:
      flush_thread.join()


def _flush_metrics(time_now):
  """Return True if metrics were actually sent."""
  if interface.state.target is None:
    # ts_mon is not configured.
    return False

  datetime_now = datetime.datetime.utcfromtimestamp(time_now)
  entity = shared.get_instance_entity()
  if entity.task_num < 0:
    if interface.state.target.task_num >= 0:
      _reset_cumulative_metrics()
    interface.state.target.task_num = -1
    interface.state.last_flushed = entity.last_updated
    updated_sec_ago = (datetime_now - entity.last_updated).total_seconds()
    if updated_sec_ago > shared.INSTANCE_EXPECTED_TO_HAVE_TASK_NUM_SEC:
      logging.warning('Instance %s is %d seconds old with no task_num.',
                      shared.instance_key_id(), updated_sec_ago)
    return False
  interface.state.target.task_num = entity.task_num

  entity.last_updated = datetime_now
  entity_deferred = entity.put_async()

  interface.flush()

  for metric in six.itervalues(interface.state.global_metrics):
    metric.reset()

  entity_deferred.get_result()
  return True
