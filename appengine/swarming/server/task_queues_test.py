#!/usr/bin/env python
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import hashlib
import logging
import os
import sys
import unittest

# Setups environment.
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)
import test_env_handlers

import webtest

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

import handlers_backend

from components import utils
from server import bot_management
from server import task_queues
from server import task_request


def _assert_bot(dimensions=None):
  bot_dimensions = {
    u'cpu': [u'x86-64', u'x64'],
    u'id': [u'bot1'],
    u'os': [u'Ubuntu-16.04', u'Ubuntu'],
    u'pool': [u'default'],
  }
  bot_dimensions.update(dimensions or {})
  bot_management.bot_event(
      'bot_connected', u'bot1', '1.2.3.4', 'bot1', bot_dimensions, {},
      '1234', False, None, None)
  task_queues.assert_bot_async(bot_dimensions).get_result()


def _gen_request(properties=None):
  """Creates a TaskRequest that expires in 24h."""
  props = {
    'command': [u'command1'],
    'dimensions': {
      u'cpu': [u'x86-64'],
      u'os': [u'Ubuntu-16.04'],
      u'pool': [u'default'],
    },
    'env': {},
    'execution_timeout_secs': 24*60*60,
    'io_timeout_secs': None,
  }
  props.update(properties or {})
  now = utils.utcnow()
  props[u'dimensions_data'] = props.pop(u'dimensions')
  args = {
    'created_ts': now,
    'name': 'Request name',
    'priority': 50,
    'properties': task_request.TaskProperties(**props),
    'expiration_ts': now + datetime.timedelta(seconds=60),
    'tags': [u'tag:1'],
    'user': 'Jesus',
  }
  return task_request.TaskRequest(**args)


class TaskQueuesApiTest(test_env_handlers.AppTestBase):
  def setUp(self):
    super(TaskQueuesApiTest, self).setUp()
    # Setup the backend to handle task queues for 'task-dimensions'.
    self.app = webtest.TestApp(
        handlers_backend.create_application(True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self._enqueue_orig = self.mock(utils, 'enqueue_task', self._enqueue)

  def _enqueue(self, *args, **kwargs):
    return self._enqueue_orig(*args, use_dedicated_module=False, **kwargs)

  def _assert_task(self, tasks=1):
    request = _gen_request()
    task_request.init_new_request(request, True, None)
    task_queues.assert_task(request)
    self.assertEqual(tasks, self.execute_tasks())
    return request

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(task_queues)
        if i[0] != '_' and hasattr(getattr(task_queues, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_BotDimensions(self):
    cls = task_queues.BotDimensions
    with self.assertRaises(datastore_errors.BadValueError):
      cls(id=1).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(dimensions_flat=['a:b']).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(id=1, dimensions_flat=['a:b', 'a:b']).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(id=1, dimensions_flat=['c:d', 'a:b']).put()
    cls(id=1, dimensions_flat=['a:b']).put()

  def test_BotTaskDimensions(self):
    cls = task_queues.BotTaskDimensions
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    with self.assertRaises(datastore_errors.BadValueError):
      cls(dimensions_flat=['a:b']).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(valid_until_ts=now).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(valid_until_ts=now, dimensions_flat=['a:b', 'a:b']).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(valid_until_ts=now, dimensions_flat=['c:d', 'a:b']).put()

    a = cls(valid_until_ts=now, dimensions_flat=['a:b'])
    a.put()
    self.assertEqual(True, a.is_valid({'a': ['b']}))
    self.assertEqual(True, a.is_valid({'a': ['b', 'c']}))
    self.assertEqual(False, a.is_valid({'x': ['c']}))

  def test_TaskDimensions(self):
    cls = task_queues.TaskDimensions
    setcls = task_queues.TaskDimensionsSet
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    with self.assertRaises(datastore_errors.BadValueError):
      cls().put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(sets=[setcls(valid_until_ts=now)]).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(sets=[setcls(dimensions_flat=['a:b'])]).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(sets=[
        setcls(valid_until_ts=now, dimensions_flat=['a:b', 'a:b'])]).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(sets=[
        setcls(valid_until_ts=now, dimensions_flat=['c:d', 'a:b'])]).put()
    with self.assertRaises(datastore_errors.BadValueError):
      cls(sets=[
        setcls(valid_until_ts=now, dimensions_flat=['a:b', 'c:d']),
        setcls(valid_until_ts=now, dimensions_flat=['a:b', 'c:d']),
        ]).put()
    cls(sets=[setcls(valid_until_ts=now, dimensions_flat=['a:b'])]).put()

  def assert_count(self, count, entity):
    actual = entity.query().count()
    if actual != count:
      self.fail([i.to_dict() for i in entity.query()])
    self.assertEqual(count, actual)

  def test_assert_bot_async(self):
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    _assert_bot()
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)

  def test_assert_bot_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    _assert_bot()
    valid_until_ts = request.expiration_ts + task_queues._ADVANCE
    self.assertEqual(
        valid_until_ts,
        task_queues.BotTaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    _assert_bot()
    self.assertEqual(
        valid_until_ts,
        task_queues.BotTaskDimensions.query().get().valid_until_ts)

  def test_assert_task(self):
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self._assert_task()
    self.assert_count(0, bot_management.BotInfo)
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_task_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    valid_until_ts = request.expiration_ts + task_queues._ADVANCE
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    self._assert_task(tasks=0)
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

  def test_get_queues(self):
    # See more complex test below.
    pass

  def test_rebuild_task_cache(self):
    # Tested indirectly by self._assert_task()
    pass

  def test_assert_bot_then_task(self):
    _assert_bot()
    self._assert_task()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

  def test_assert_task_then_bot(self):
    self._assert_task()
    _assert_bot()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

  def test_assert_bot_then_task_with_id(self):
    # Assert a task that includes an 'id' dimension. No task queue is triggered
    # in this case, rebuild_task_cache() is called inlined.
    _assert_bot()
    request = _gen_request(properties={u'dimensions': {u'id': [u'bot1']}})
    task_request.init_new_request(request, True, None)
    task_queues.assert_task(request)
    self.assertEqual(0, self.execute_tasks())
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_cleanup_after_bot(self):
    _assert_bot()
    self._assert_task()
    task_queues.cleanup_after_bot('bot1')
    # BotInfo is deleted separately.
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_bot_dimensions_changed(self):
    # Ensure that stale BotTaskDimensions are deleted when the bot dimensions
    # changes.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    exp = (request.expiration_ts-request.created_ts) + task_queues._ADVANCE
    _assert_bot()
    # One hour later, the bot changes dimensions.
    self.mock_now(now, task_queues._ADVANCE.total_seconds())
    _assert_bot({u'gpu': u'Matrox'})
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

    # One second before expiration.
    self.mock_now(now, exp.total_seconds())
    _assert_bot({u'gpu': u'Matrox'})
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

    # TaskDimension expired. The fact that the bot changed dimensions after an
    # hour didn't impact BotTaskDimensions expiration.
    self.mock_now(now, exp.total_seconds() + 1)
    _assert_bot()
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([], task_queues.get_queues(u'bot1'))

  def test_hash_dimensions(self):
    with self.assertRaises(AttributeError):
      task_queues.hash_dimensions('this is not json')
    # Assert it doesn't return 0.
    self.assertEqual(3649838548, task_queues.hash_dimensions({}))

  def test_tidy_stale(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    _assert_bot()
    request = self._assert_task()
    exp = (request.expiration_ts-request.created_ts) + task_queues._ADVANCE
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

    # No-op.
    task_queues.tidy_stale()
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

    # One second before expiration.
    self.mock_now(now, exp.total_seconds())
    task_queues.tidy_stale()
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues.get_queues(u'bot1'))

    # TaskDimension expired.
    self.mock_now(now, exp.total_seconds() + 1)
    task_queues.tidy_stale()
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self.assertEqual([], task_queues.get_queues(u'bot1'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
