# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server common code used in handlers."""

import datetime
import logging

from google.appengine import runtime
from google.appengine.api import taskqueue

from components import utils


### Utility


def utcnow():
  """Returns a datetime, used for testing."""
  return datetime.datetime.utcnow()


def enqueue_task(url, queue_name, payload=None, name=None,
                 use_dedicated_module=True):
  """Adds a task to a task queue.

  If |use_dedicated_module| is True (default) a task will be executed by
  a separate backend module instance that runs same version as currently
  executing instance. Otherwise it will run on a current version of default
  module.

  Returns True if a task was successfully added, logs error and returns False
  if task queue is acting up.
  """
  try:
    headers = None
    if use_dedicated_module:
      headers = {'Host': utils.get_task_queue_host()}
    # Note that just using 'target=module' here would redirect task request to
    # a default version of a module, not the currently executing one.
    taskqueue.add(
        url=url,
        queue_name=queue_name,
        payload=payload,
        name=name,
        headers=headers)
    return True
  except (
      taskqueue.Error,
      runtime.DeadlineExceededError,
      runtime.apiproxy_errors.CancelledError,
      runtime.apiproxy_errors.DeadlineExceededError,
      runtime.apiproxy_errors.OverQuotaError) as e:
    logging.warning(
        'Problem adding task \'%s\' to task queue \'%s\' (%s): %s',
        url, queue_name, e.__class__.__name__, e)
    return False
