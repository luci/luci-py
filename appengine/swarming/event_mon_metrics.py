# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging

import gae_event_mon


def initialize():
  gae_event_mon.initialize('swarming')


def send_task_event(task_result_summary):
  """Sends an event_mon event about a swarming task.

  Currently implemented as sending a HTTP request.

  Args:
    task_result_summary: TaskResultSummary object.
  """
  event = gae_event_mon.Event('POINT')
  event.proto.swarming_task_event.id = task_result_summary.task_id

  # Isolate rest of the app from monitoring pipeline issues. They should
  # not cause outage of swarming.
  try:
    event.send()
  except Exception:
    logging.exception('Caught exception while sending event')
