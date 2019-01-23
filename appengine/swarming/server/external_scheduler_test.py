#!/usr/bin/env python
# coding: utf-8
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import random
import sys
import unittest

# Setups environment.
import test_env
test_env.setup_test_env()

from test_support import test_case

from components import utils
from proto.api import plugin_pb2
from proto.api import swarming_pb2
from server import external_scheduler
from server import pools_config
from server import task_request
from server import task_scheduler


def _gen_properties(**kwargs):
  """Creates a TaskProperties."""
  args = {
    'command': [u'command1'],
    'dimensions': {u'os': [u'Windows-3.1.1'], u'pool': [u'default']},
    'env': {},
    'execution_timeout_secs': 24*60*60,
    'io_timeout_secs': None,
  }
  args.update(kwargs)
  args['dimensions_data'] = args.pop('dimensions')
  return task_request.TaskProperties(**args)


def _gen_request(**kwargs):
  """Returns an initialized task_request.TaskRequest."""
  now = utils.utcnow()
  args = {
    # Don't be confused, this is not part of the API. This code is constructing
    # a DB entity, not a swarming_rpcs.NewTaskRequest.
    u'created_ts': now,
    u'manual_tags': [u'tag:1'],
    u'name': u'yay',
    u'priority': 50,
    u'task_slices': [
      task_request.TaskSlice(
        expiration_secs=60,
        properties=_gen_properties(),
        wait_for_capacity=False),
    ],
    u'user': u'Jesus',
  }
  args.update(kwargs)
  ret = task_request.TaskRequest(**args)
  task_request.init_new_request(ret, True, task_request.TEMPLATE_AUTO)
  return ret


class FakeExternalScheduler(object):
  # TODO(akeshet): Make a superset of
  # plugin_prpc_pb2.ExternalSchedulerServiceDescription.
  def __init__(self, test):
    self._test = test

  def NotifyTasks(self, req, credentials):  # pylint: disable=unused-argument
    self._test.assertIsInstance(req, plugin_pb2.NotifyTasksRequest)
    task = plugin_pb2.TaskSpec(
        id=u'1d69b9f088008810',
        tags=[
          u'os:Windows-3.1.1',
          u'pool:default',
          u'priority:50',
          u'service_account:none',
          u'swarming.pool.template:no_config',
          u'tag:1',
          u'user:Jesus',
        ],
        slices=[
          plugin_pb2.SliceSpec(
              dimensions=[u'os:Windows-3.1.1', u'pool:default']),
        ],
        state=swarming_pb2.NO_RESOURCE)
    notif = plugin_pb2.NotifyTasksItem(task=task)
    notif.time.FromDatetime(utils.utcnow())
    expected = plugin_pb2.NotifyTasksRequest(
        scheduler_id=u'foo', notifications=[notif])
    self._test.assertEqual(expected, req)
    return plugin_pb2.NotifyTasksResponse()


class ExternalSchedulerApiTest(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def setUp(self):
    super(ExternalSchedulerApiTest, self).setUp()
    # Make the values deterministic.
    self.mock_now(datetime.datetime(2014, 1, 2, 3, 4, 5, 6))
    self.mock(random, 'getrandbits', lambda _: 0x88)
    # Skip task_scheduler.schedule_request() enqueued work.
    self.mock(utils, 'enqueue_task', self._enqueue)
    # Use the local fake.
    self.mock(external_scheduler, '_get_client', self._get_client)
    self._client = None

  def _enqueue(self, url, queue_name, payload):
    self.assertEqual('/internal/taskqueue/rebuild-task-cache', url)
    self.assertEqual('rebuild-task-cache', queue_name)
    self.assertTrue(payload)
    return True

  def _get_client(self, addr):
    self.assertEqual(u'http://localhost:1', addr)
    self.assertFalse(self._client)
    self._client = FakeExternalScheduler(self)
    return self._client

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(external_scheduler)
        if i[0] != '_' and hasattr(getattr(external_scheduler, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_assign_task(self):
    # TODO(akeshet): Add.
    pass

  def test_config_for_bot(self):
    # TODO(akeshet): Add.
    pass

  def test_config_for_task(self):
    # TODO(akeshet): Add.
    pass

  def test_get_cancellations(self):
    # TODO(akeshet): Add.
    pass

  def test_notify_request(self):
    es_cfg = pools_config.ExternalSchedulerConfig(
        address=u'http://localhost:1',
        id=u'foo',
        dimensions=['key1:value1', 'key2:value2'],
        enabled=True)
    req = request = _gen_request()
    result_summary = task_scheduler.schedule_request(request, None)
    res = external_scheduler.notify_request(es_cfg, req, result_summary)
    self.assertEqual(plugin_pb2.NotifyTasksResponse(), res)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
