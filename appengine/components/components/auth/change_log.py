# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Models and functions to build and query Auth DB change log."""

import logging
import webapp2

from google.appengine.api import modules
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from components import decorators

from . import config
from . import model
from . import utils


def process_change(auth_db_rev):
  """Called asynchronously (via task queue) on AuthDB changes."""
  # TODO(vadimsh): Get *History entities in historical_revision_key(auth_db_rev)
  # and diff them against previous versions to produce a set of
  # "change log entry" entities (displayed later in UI).
  logging.info('Processing AuthDB change rev %d', auth_db_rev)


### Task queue plumbing.


@model.commit_callback
def on_auth_db_change(auth_db_rev):
  """Called in a transaction that updated AuthDB."""
  # Avoid adding task queues in unit tests, since there are many-many unit tests
  # (in multiple project and repos) that indirectly make AuthDB transactions
  # and mocking out 'enqueue_process_change_task' in all of them is stupid
  # unscalable work. So be evil and detect unit tests right here.
  if not utils.is_unit_test():
    enqueue_process_change_task(auth_db_rev)


def enqueue_process_change_task(auth_db_rev):
  """Transactionally adds a call to 'process_change' to the task queue.

  Pins the task to currently executing version of BACKEND_MODULE module
  (defined in config.py).

  Added as AuthDB commit callback in get_backend_routes() below.
  """
  assert ndb.in_transaction()
  conf = config.ensure_configured()
  try:
    # Pin the task to the module and version.
    taskqueue.add(
        url='/internal/auth/taskqueue/process-change/%d' % auth_db_rev,
        queue_name=conf.PROCESS_CHANGE_TASK_QUEUE,
        headers={'Host': modules.get_hostname(module=conf.BACKEND_MODULE)},
        transactional=True)
  except Exception as e:
    logging.error(
        'Problem adding "process-change" task to the task queue (%s): %s',
        e.__class__.__name__, e)
    raise


class InternalProcessChangeHandler(webapp2.RequestHandler):
  def post(self, auth_db_rev):
    # We don't know task queue name during module loading time, so delay
    # decorator application until the actual call.
    queue_name = config.ensure_configured().PROCESS_CHANGE_TASK_QUEUE
    @decorators.require_taskqueue(queue_name)
    def call_me(_self):
      process_change(int(auth_db_rev))
    call_me(self)


def get_backend_routes():
  """Returns a list of routes with task queue handlers.

  Used from ui/app.py (since it's where WSGI module is defined) and directly
  from auth_service backend module.
  """
  return [
    webapp2.Route(
        r'/internal/auth/taskqueue/process-change/<auth_db_rev:\d+>',
        InternalProcessChangeHandler),
  ]
