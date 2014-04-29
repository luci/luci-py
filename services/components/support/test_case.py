# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import logging
import time

from google.appengine.ext import testbed

from depot_tools import auto_stub

# W0212: Access to a protected member XXX of a client class
# pylint: disable=W0212


class TestCase(auto_stub.TestCase):
  """Support class to enable more unit testing in GAE.

  Adds support for:
    - google.appengine.api.mail.send_mail_to_admins().
    - Running task queues.
  """
  # See APP_DIR to the root directory containing index.yaml and queue.yaml. It
  # will be used to assert the indexes and task queues are properly defined. It
  # can be left to None if no index or task queue is used for the test case.
  APP_DIR = None

  # If taskqueues are enqueued during the unit test, self.app must be set to a
  # webtest.Test instance. It will be used to do the HTTP post when executing
  # the enqueued tasks via the taskqueue module.
  app = None

  def setUp(self):
    """Initializes the commonly used stubs.

    Using init_all_stubs() costs ~10ms more to run all the tests so only enable
    the ones known to be required. Test cases requiring more stubs can enable
    them in their setUp() function.
    """
    super(TestCase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub(
        require_indexes=True, root_path=self.APP_DIR)
    self.testbed.init_logservice_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_modules_stub()

    # Email support.
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    self.old_send_to_admins = self.mock(
        self.mail_stub, '_Dynamic_SendToAdmins', self._SendToAdmins)

    self.testbed.init_taskqueue_stub()
    self._taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
    self._taskqueue_stub._root_path = self.APP_DIR

  def tearDown(self):
    try:
      if not self.has_failed():
        self.assertEqual(0, self.execute_tasks())
      self.testbed.deactivate()
    finally:
      super(TestCase, self).tearDown()

  def _SendToAdmins(self, request, *args, **kwargs):
    """Make sure the request is logged.

    See google_appengine/google/appengine/api/mail_stub.py around line 299,
    MailServiceStub._SendToAdmins().
    """
    self.mail_stub._CacheMessage(request)
    return self.old_send_to_admins(request, *args, **kwargs)

  def execute_tasks(self):
    """Executes enqueued tasks that are ready to run and return the number run.

    A task may trigger another task.

    Sadly, taskqueue_stub implementation does not provide a nice way to run
    them so run the pending tasks manually.
    """
    self.assertEqual([None], self._taskqueue_stub._queues.keys())
    ran_total = 0
    while True:
      # Do multiple loops until no task was run.
      ran = 0
      for queue in self._taskqueue_stub.GetQueues():
        for task in self._taskqueue_stub.GetTasks(queue['name']):
          # Remove 2 seconds for jitter.
          eta = task['eta_usec'] / 1e6 - 2
          if eta >= time.time():
            continue
          self.assertEqual('POST', task['method'])
          logging.info('Task: %s', task['url'])

          # Not 100% sure why the Content-Length hack is needed:
          body = base64.b64decode(task['body'])
          headers = dict(task['headers'])
          headers['Content-Length'] = str(len(body))
          try:
            response = self.app.post(task['url'], body, headers=headers)
          except:
            logging.error(task)
            raise
          # TODO(maruel): Implement task failure.
          self.assertEqual(200, response.status_code)
          self._taskqueue_stub.DeleteTask(queue['name'], task['name'])
          ran += 1
      if not ran:
        return ran_total
      ran_total += ran
