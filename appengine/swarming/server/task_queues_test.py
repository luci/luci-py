#!/usr/bin/env vpython
# coding: utf-8
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import json
import logging
import os
import random
import sys
import unittest

# Setups environment.
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)
import test_env_handlers

from parameterized import parameterized
import webtest

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

import handlers_backend

from components import datastore_utils
from components import utils
from server import bot_management
from server import task_queues
from server import task_request


def _gen_properties(**kwargs):
  """Creates a TaskProperties."""
  args = {
      'command': [u'command1'],
      'dimensions': {
          u'cpu': [u'x86-64'],
          u'os': [u'Ubuntu-16.04'],
          u'pool': [u'default'],
      },
      'env': {},
      'execution_timeout_secs': 24 * 60 * 60,
      'io_timeout_secs': None,
  }
  args.update(kwargs)
  args[u'dimensions_data'] = args.pop(u'dimensions')
  return task_request.TaskProperties(**args)


def _gen_request(properties=None):
  """Creates a TaskRequest."""
  now = utils.utcnow()
  args = {
      'created_ts':
          now,
      'manual_tags': [u'tag:1'],
      'name':
          'Request name',
      'priority':
          50,
      'task_slices': [
          task_request.TaskSlice(
              expiration_secs=60, properties=properties or _gen_properties()),
      ],
      'user':
          'Jesus',
  }
  req = task_request.TaskRequest(**args)
  task_request.init_new_request(req, True, task_request.TEMPLATE_AUTO)
  return req


def _to_timestamp(dt):
  return int(utils.datetime_to_timestamp(dt) / 1e6)


class TaskQueuesApiTest(test_env_handlers.AppTestBase):

  def setUp(self):
    super(TaskQueuesApiTest, self).setUp()
    # Setup the backend to handle task queues.
    self.app = webtest.TestApp(
        handlers_backend.create_application(True),
        extra_environ={
            'REMOTE_ADDR': self.source_ip,
            'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self._enqueue_async_orig = self.mock(utils, 'enqueue_task_async',
                                         self._enqueue_async)

    def random_dt(a, b):
      return datetime.timedelta(minutes=(a + b) / 2.0)

    self.mock(task_queues, '_random_timedelta_mins', random_dt)

  def _enqueue_async(self, *args, **kwargs):
    return self._enqueue_async_orig(*args, use_dedicated_module=False, **kwargs)

  def _mock_enqueue_task_async(self, *queues):
    # queue name => URL, transactional.
    expected = {
        'rebuild-task-cache': (
            '/internal/taskqueue/important/task_queues/rebuild-cache',
            False,
        ),
        'update-bot-matches': (
            '/internal/taskqueue/important/task_queues/update-bot-matches',
            True,
        ),
        'rescan-matching-task-sets': (
            '/internal/taskqueue/important/task_queues/'
            'rescan-matching-task-sets',
            True,
        ),
    }
    for q in queues:
      self.assertIn(q, expected)

    payloads = tuple([] for _ in queues)

    @ndb.tasklet
    def _enqueue_task_async(url, name, payload, transactional=False):
      self.assertIn(name, queues)
      self.assertEqual(expected[name][0], url)
      self.assertEqual(expected[name][1], transactional)
      payloads[queues.index(name)].append(payload)
      raise ndb.Return(True)

    self.mock(utils, 'enqueue_task_async', _enqueue_task_async)
    return payloads[0] if len(payloads) == 1 else payloads

  def _assert_bot(self, bot_id=u'bot1', dimensions=None):
    bot_dimensions = {
        u'cpu': [u'x86-64', u'x64'],
        u'id': [bot_id],
        u'os': [u'Ubuntu-16.04', u'Ubuntu'],
        u'pool': [u'default'],
    }
    bot_dimensions.update(dimensions or {})
    bot_management.bot_event('request_sleep',
                             bot_id,
                             '1.2.3.4',
                             bot_id,
                             bot_dimensions, {},
                             '1234',
                             False,
                             None,
                             None,
                             None,
                             register_dimensions=True)
    bot_root_key = bot_management.get_root_key(bot_id)
    task_queues._assert_bot_dimensions_async(bot_dimensions).get_result()
    self.execute_tasks()
    return task_queues._assert_bot_old_async(bot_root_key,
                                             bot_dimensions).get_result()

  def _assert_task(self, dimensions=None):
    """Creates one pending TaskRequest and asserts it in task_queues."""
    request = _gen_request(properties=_gen_properties(
        dimensions=dimensions) if dimensions else None)
    task_queues.assert_task_async(request).get_result()
    self.execute_tasks()
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

  def test_assert_bot(self):
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self.assertEqual(0, self._assert_bot())
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)

  def test_assert_bot_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    self.assertEqual(1, self._assert_bot())
    valid_until_ts = request.expiration_ts + task_queues._EXTEND_VALIDITY
    self.assertEqual(
        valid_until_ts,
        task_queues.BotTaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    self.assertEqual(None, self._assert_bot())
    self.assertEqual(
        valid_until_ts,
        task_queues.BotTaskDimensions.query().get().valid_until_ts)

  def test_assert_task_async(self):
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self._assert_task()
    self.assert_count(0, bot_management.BotInfo)
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_task_async_new_impl_fails(self):
    @ndb.tasklet
    def fail(*_args):
      raise datastore_utils.CommitError('Boom')
    self.mock(task_queues, '_assert_task_dimensions_async', fail)

    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self._assert_task()
    self.assert_count(0, bot_management.BotInfo)
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_task_async_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    valid_until_ts = request.expiration_ts + task_queues._EXTEND_VALIDITY
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    self._assert_task()
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

  def test_assert_task_async_or_dims(self):
    self._assert_task(dimensions={
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })
    # Already seen it, no new tasks.
    self._assert_task(dimensions={
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })

  def test_freshen_up_queues(self):
    # See more complex test below.
    pass

  def test_rebuild_task_cache_async(self):
    # Assert that expiration works.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    # We want _yield_BotTaskDimensions_keys() to return multiple
    # BotTaskDimensions ndb.Key to confirm that the inner loops work. This
    # requires a few bots.
    self._assert_bot(bot_id=u'bot1')
    self._assert_bot(bot_id=u'bot2')
    self._assert_bot(bot_id=u'bot3')
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual(0, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(0, task_queues.TaskDimensions.query().count())

    # Intentionally force the code to throttle the number of concurrent RPCs,
    # otherwise the inner loops wouldn't be reached with less than 50 bots, and
    # testing with 50 bots, would make the unit test slow.
    self.mock(task_queues, '_CAP_FUTURES_LIMIT', 1)

    payloads, _ = self._mock_enqueue_task_async('rebuild-task-cache',
                                                'update-bot-matches')

    # The equivalent of self._assert_task() except that we snapshot the payload.
    # Trigger multiple task queues to go deeper in the code.
    request_1 = _gen_request(
        properties=_gen_properties(
            dimensions={
              u'cpu': [u'x86-64'],
              u'pool': [u'default'],
            }))
    task_queues.assert_task_async(request_1).get_result()
    self.assertEqual(1, len(payloads))
    f = task_queues.rebuild_task_cache_async(payloads[-1])
    self.assertEqual(True, f.get_result())
    self.assertEqual(3, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(1, task_queues.TaskDimensions.query().count())
    self.assertEqual(60, request_1.expiration_secs)
    expected = now + task_queues._EXTEND_VALIDITY + datetime.timedelta(
        seconds=request_1.expiration_secs)
    self.assertEqual(expected,
                     task_queues.TaskDimensions.query().get().valid_until_ts)

    request_2 = _gen_request(
        properties=_gen_properties(
            dimensions={
              u'os': [u'Ubuntu-16.04'],
              u'pool': [u'default'],
            }))
    task_queues.assert_task_async(request_2).get_result()
    self.assertEqual(2, len(payloads))
    f = task_queues.rebuild_task_cache_async(payloads[-1])
    self.assertEqual(True, f.get_result())
    self.assertEqual(6, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(2, task_queues.TaskDimensions.query().count())
    self.assertEqual([227177418, 1843498234],
                     task_queues._get_queues(bot_root_key))
    memcache.flush_all()
    self.assertEqual([227177418, 1843498234],
                     task_queues._get_queues(bot_root_key))

    # Now expire the two TaskDimensions, one at a time, and rebuild the task
    # queue.
    offset = (task_queues._EXTEND_VALIDITY + datetime.timedelta(
        seconds=request_1.expiration_secs)).total_seconds() + 1
    self.mock_now(now, offset)
    f = task_queues.rebuild_task_cache_async(payloads[0])
    self.assertEqual(True, f.get_result())
    self.assertEqual(6, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(2, task_queues.TaskDimensions.query().count())
    self.assertEqual([227177418, 1843498234],
                     task_queues._get_queues(bot_root_key))
    # Observe the effect of memcache. See comment in get_queues().
    memcache.flush_all()
    self.assertEqual([], task_queues._get_queues(bot_root_key))

    # Re-running still do not delete TaskDimensions because they are kept until
    # _KEEP_DEAD.
    f = task_queues.rebuild_task_cache_async(payloads[1])
    self.assertEqual(True, f.get_result())
    self.assertEqual(6, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(2, task_queues.TaskDimensions.query().count())
    self.assertEqual([], task_queues._get_queues(bot_root_key))

    # Get past _KEEP_DEAD.
    offset = (
        task_queues._EXTEND_VALIDITY +
        task_queues._KEEP_DEAD + datetime.timedelta(
            seconds=request_1.expiration_secs)
      ).total_seconds() + 1
    self.mock_now(now, offset)
    self.assertEqual([], task_queues._get_queues(bot_root_key))
    f = task_queues.rebuild_task_cache_async(payloads[0])
    self.assertEqual(True, f.get_result())
    self.assertEqual(6, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(1, task_queues.TaskDimensions.query().count())
    self.assertEqual([], task_queues._get_queues(bot_root_key))

  def test_rebuild_task_cache_async_fail(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now, 0)
    payload = utils.encode_to_json({
        'dimensions': {
            'pool': ['foo']
        },
        'dimensions_hash': 123,
        'valid_until_ts': now + datetime.timedelta(minutes=1)
    })

    @ndb.tasklet
    def _raise_ndb_return_false(*_args):
      raise ndb.Return(False)

    self.mock(task_queues, '_refresh_TaskDimensions_async',
              _raise_ndb_return_false)
    self.assertFalse(task_queues.rebuild_task_cache_async(payload).get_result())

  def test_assert_task_async_fail(self):
    # pylint: disable=unused-argument
    @ndb.tasklet
    def _enqueue_task_async(url, name, payload, transactional=False):
      # Enqueueing the task failed.
      raise ndb.Return(False)

    self.mock(utils, 'enqueue_task_async', _enqueue_task_async)

    request = _gen_request()
    with self.assertRaises(task_queues.Error):
      task_queues.assert_task_async(request).get_result()

  def test_or_dimensions_new_tasks(self):
    # Bots are already registered, then new tasks show up
    self.mock_now(datetime.datetime(2020, 1, 2, 3, 4, 5))
    self.assertEqual(
        0,
        self._assert_bot(bot_id=u'bot1',
                         dimensions={
                             u'os': [u'v1', u'v2'],
                             u'gpu': [u'nv'],
                         }))
    self.assertEqual(
        0,
        self._assert_bot(bot_id=u'bot2',
                         dimensions={
                             u'os': [u'v2'],
                             u'gpu': [u'amd'],
                         }))

    payloads, _ = self._mock_enqueue_task_async('rebuild-task-cache',
                                                'update-bot-matches')

    request1 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'default'],
            u'os': [u'v1|v2'],
            u'gpu': [u'nv|amd'],
        }))
    task_queues.assert_task_async(request1).get_result()
    self.assertEqual(1, len(payloads))
    f = task_queues.rebuild_task_cache_async(payloads[-1])
    self.assertEqual(True, f.get_result())
    payloads.pop()

    # Both bots should be able to handle |request1|
    self.assert_count(2, task_queues.BotDimensions)
    self.assert_count(2, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual(4, len(task_queues.TaskDimensions.query().get().sets))
    bot1_root_key = bot_management.get_root_key(u'bot1')
    bot2_root_key = bot_management.get_root_key(u'bot2')
    self.assertEqual(1, len(task_queues._get_queues(bot1_root_key)))
    self.assertEqual(1, len(task_queues._get_queues(bot2_root_key)))

    request2 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'default'],
            u'os': [u'v1'],
            u'gpu': [u'nv|amd'],
        }))
    task_queues.assert_task_async(request2).get_result()
    self.assertEqual(1, len(payloads))
    f = task_queues.rebuild_task_cache_async(payloads[-1])
    self.assertEqual(True, f.get_result())
    payloads.pop()

    # Only bot1 can handle |request2|
    self.assert_count(3, task_queues.BotTaskDimensions)
    self.assert_count(2, task_queues.TaskDimensions)
    self.assertEqual(2, len(task_queues._get_queues(bot1_root_key)))
    self.assertEqual(1, len(task_queues._get_queues(bot2_root_key)))

  def test_or_dimensions_new_bots(self):
    # Tasks are already registered, then new bots show up
    self.mock_now(datetime.datetime(2020, 1, 2, 3, 4, 5))

    payloads, _, _ = self._mock_enqueue_task_async('rebuild-task-cache',
                                                   'update-bot-matches',
                                                   'rescan-matching-task-sets')

    request1 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'default'],
            u'os': [u'v1|v2'],
            u'gpu': [u'nv|amd'],
        }))
    task_queues.assert_task_async(request1).get_result()
    self.assertEqual(1, len(payloads))
    f = task_queues.rebuild_task_cache_async(payloads[-1])
    self.assertEqual(True, f.get_result())
    payloads.pop()

    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual(4, len(task_queues.TaskDimensions.query().get().sets))

    self.assertEqual(
        1,
        self._assert_bot(bot_id=u'bot1',
                         dimensions={
                             u'os': [u'v1'],
                             u'gpu': [u'nv'],
                         }))
    self.assertEqual(
        1,
        self._assert_bot(bot_id=u'bot2',
                         dimensions={
                             u'os': [u'v2'],
                             u'gpu': [u'amd'],
                         }))
    # Both bots should be able to handle |request1|
    self.assert_count(2, task_queues.BotDimensions)
    self.assert_count(2, task_queues.BotTaskDimensions)
    bot1_root_key = bot_management.get_root_key(u'bot1')
    bot2_root_key = bot_management.get_root_key(u'bot2')
    self.assertEqual(1, len(task_queues._get_queues(bot1_root_key)))
    self.assertEqual(1, len(task_queues._get_queues(bot2_root_key)))

  def test_or_dimensions_same_hash(self):
    self.mock_now(datetime.datetime(2020, 1, 2, 3, 4, 5))
    self.assertEqual(
        0, self._assert_bot(bot_id=u'bot1', dimensions={u'os': [u'v1']}))
    self.assertEqual(
        0, self._assert_bot(bot_id=u'bot2', dimensions={u'os': [u'v2']}))
    self.assertEqual(
        0, self._assert_bot(bot_id=u'bot3', dimensions={u'os': [u'v3']}))

    payloads, _ = self._mock_enqueue_task_async('rebuild-task-cache',
                                                'update-bot-matches')

    # Both requests should have the same dimension_hash
    request1 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'default'],
            u'os': [u'v1|v2|v3'],
        }))
    request2 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'default'],
            u'os': [u'v3|v2|v1'],
        }))
    task_queues.assert_task_async(request1).get_result()
    task_queues.assert_task_async(request2).get_result()
    self.assertEqual(2, len(payloads))
    while payloads:
      f = task_queues.rebuild_task_cache_async(payloads[-1])
      self.assertEqual(True, f.get_result())
      payloads.pop()

    self.assert_count(3, task_queues.BotDimensions)
    self.assert_count(3, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual(3, len(task_queues.TaskDimensions.query().get().sets))
    bot1_root_key = bot_management.get_root_key(u'bot1')
    bot2_root_key = bot_management.get_root_key(u'bot2')
    bot3_root_key = bot_management.get_root_key(u'bot3')
    self.assertEqual(1, len(task_queues._get_queues(bot1_root_key)))
    self.assertEqual(1, len(task_queues._get_queues(bot2_root_key)))
    self.assertEqual(1, len(task_queues._get_queues(bot3_root_key)))

  def test_bot_dimensions_to_flat(self):
    actual = task_queues.bot_dimensions_to_flat({
        u'a': [u'c', u'bee'],
        u'cee': [u'zee']
    })
    self.assertEqual([u'a:bee', u'a:c', u'cee:zee'], actual)

  def test_dimensions_to_flat_long_ascii(self):
    key = u'a' * 64
    actual = task_queues.bot_dimensions_to_flat({
        key: [
            # Too long.
            u'b' * 257,
            # Ok.
            u'c' * 256,
        ],
    })
    expected = [
        key + u':' + u'b' * 256 + u'…',
        key + u':' + u'c' * 256,
    ]
    self.assertEqual(expected, actual)

  def test_dimensions_to_flat_long_unicode(self):
    key = u'a' * 64
    actual = task_queues.bot_dimensions_to_flat({
        key: [
            # Ok.
            u'⌛' * 256,
            # Too long.
            u'⛔' * 257,
        ],
    })
    expected = [
        key + u':' + u'⌛' * 256,
        key + u':' + u'⛔' * 256 + u'…',
    ]
    self.assertEqual(expected, actual)

  def test_dimensions_to_flat_long_unicode_non_BMP(self):
    # For non-BMP characters, the length is effectively halved for now.
    key = u'a' * 64
    # Python considers emoji in the supplemental plane to have length 2 on UCS2
    # builds, and length 1 on UCS4 builds.
    l = 128 if sys.maxunicode < 65536 else 256
    actual = task_queues.bot_dimensions_to_flat({
        key: [
            # Too long.
            u'💥' * (l + 1),
            # Ok.
            u'😬' * l,
        ],
    })
    expected = [
        key + u':' + u'💥' * l + u'…',
        key + u':' + u'😬' * l,
    ]
    self.assertEqual(expected, actual)

  def test_dimensions_to_flat_duplicate_value(self):
    actual = task_queues.bot_dimensions_to_flat({u'a': [u'c', u'c']})
    self.assertEqual([u'a:c'], actual)

  def test_python_len_non_BMP(self):
    # Here are emojis in the base plane. They are 1 character.
    self.assertEqual(1, len(u'⌛'))
    self.assertEqual(1, len(u'⛔'))
    # Python considers emoji in the supplemental plane to have length 2 on UCS2
    # builds, and length 1 on UCS4 builds.
    l = 2 if sys.maxunicode < 65536 else 1
    self.assertEqual(l, len(u'😬'))
    self.assertEqual(l, len(u'💥'))

  def test_probably_has_capacity_empty(self):
    # The bot can service this dimensions.
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # By default, nothing has capacity.
    self.assertEqual(None, task_queues.probably_has_capacity(d))
    self.assertEqual(None, memcache.get('bot1', namespace='task_queues'))

  def test_probably_has_capacity(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # A bot coming online doesn't register capacity automatically.
    self._assert_bot()
    self.assertEqual([], memcache.get('bot1', namespace='task_queues'))
    self.assertEqual(1, bot_management.BotInfo.query().count())
    self.assertEqual(None, task_queues.probably_has_capacity(d))

  def test_probably_has_capacity_get_queues(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # Capacity registers there only once there's a request enqueued and
    # get_queues() is called.
    self._assert_bot()
    request = _gen_request(properties=_gen_properties(dimensions=d))
    task_queues.assert_task_async(request).get_result()
    self.execute_tasks()
    self.assertEqual(None, task_queues.probably_has_capacity(d))

    # It get sets only once get_queues() is called.
    bot_root_key = bot_management.get_root_key(u'bot1')
    task_queues._get_queues(bot_root_key)
    self.assertEqual(True, task_queues.probably_has_capacity(d))
    self.assertEqual(
        [1843498234], memcache.get('bot1', namespace='task_queues'))

  def test_set_has_capacity(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # By default, nothing has capacity. None means no data.
    now = utils.utcnow()
    self.mock_now(now, 0)
    self.assertEqual(None, task_queues.probably_has_capacity(d))
    # Keep the value for 2 seconds, exclusive.
    task_queues.set_has_capacity(d, 2)
    self.assertEqual(True, task_queues.probably_has_capacity(d))
    self.mock_now(now, 1)
    self.assertEqual(True, task_queues.probably_has_capacity(d))
    # The value expired.
    self.mock_now(now, 2)
    self.assertEqual(None, task_queues.probably_has_capacity(d))

  def test_assert_bot_then_task(self):
    self.assertEqual(0, self._assert_bot())
    self._assert_task()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

  def test_assert_task_async_then_bot(self):
    self._assert_task()
    self.assertEqual(1, self._assert_bot())
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

  def test_assert_bot_then_task_with_id(self):
    # Assert a task that includes an 'id' dimension. No task queue is triggered
    # in this case, rebuild_task_cache_async() is called inlined.
    self.assertEqual(0, self._assert_bot())
    request = _gen_request(
        properties=_gen_properties(dimensions={
            u'id': [u'bot1'],
            u'pool': [u'default']
        }))
    task_queues.assert_task_async(request).get_result()
    self.execute_tasks()
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_task_async_call_rebuild_task_cache_async(self):
    self.assertEqual(0, self._assert_bot())
    dimensions = {
        u'id': [u'bot1'],
        u'pool': [u'default'],
    }
    self.mock_now(datetime.datetime(2020, 1, 2, 3, 4, 5))
    request1 = _gen_request(properties=_gen_properties(dimensions=dimensions))
    task_queues.assert_task_async(request1).get_result()
    self.execute_tasks()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key('bot1')
    self.assertEqual(1, len(task_queues._get_queues(bot_root_key)))

    # expire BotTaskDimensions by changing time.
    memcache.flush_all()
    bot_task_dimensions = task_queues.BotTaskDimensions.query(
        ancestor=bot_root_key).fetch()[0]
    self.mock_now(bot_task_dimensions.valid_until_ts +
                  datetime.timedelta(seconds=1))
    self.assertEqual(0, len(task_queues._get_queues(bot_root_key)))

    # request a task with the same dimensions.
    memcache.flush_all()
    request2 = _gen_request(properties=_gen_properties(dimensions=dimensions))
    task_queues.assert_task_async(request2).get_result()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual(1, len(task_queues._get_queues(bot_root_key)))

  def test_cleanup_after_bot(self):
    self.assertEqual(0, self._assert_bot())
    self._assert_task()
    task_queues.cleanup_after_bot(bot_management.get_root_key('bot1'))
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
    exp = (request.expiration_ts - request.created_ts +
        task_queues._EXTEND_VALIDITY)
    self.assertEqual(1, self._assert_bot())
    # One hour later, the bot changes dimensions.
    self.mock_now(now, task_queues._EXTEND_VALIDITY.total_seconds())
    self.assertEqual(1, self._assert_bot(dimensions={u'gpu': u'Matrox'}))
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

    # One second before expiration.
    self.mock_now(now, exp.total_seconds())
    self.assertEqual(None, self._assert_bot(dimensions={u'gpu': u'Matrox'}))
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

    # BotTaskDimensions expired.
    self.mock_now(now, exp.total_seconds() + 1)
    self.assertEqual(0, self._assert_bot())
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([], task_queues._get_queues(bot_root_key))

  def test_hash_dimensions(self):
    with self.assertRaises(AttributeError):
      task_queues.hash_dimensions('this is not json')
    # Assert it doesn't return 0.
    self.assertEqual(3649838548, task_queues.hash_dimensions({}))

  def test_hash_or_dimensions(self):
    dim1 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'pool-a'],
            u'foo': [u'a|c|b', u'xyz']
        })).task_slice(0).properties.dimensions
    dim2 = _gen_request(
        properties=_gen_properties(dimensions={
            u'pool': [u'pool-a'],
            u'foo': [u'xyz', u'b|c|a']
        })).task_slice(0).properties.dimensions
    self.assertEqual(
        task_queues.hash_dimensions(dim1), task_queues.hash_dimensions(dim2))

  def test_expand_dimensions_to_flats(self):
    expand = task_queues.expand_dimensions_to_flats
    # Without OR
    actual = set(
        tuple(f) for f in expand({
            u'foo': [u'c', u'a', u'b'],
            u'bar': [u'x', u'z', u'y']
        }))
    expected = {
        (u'bar:x', u'bar:y', u'bar:z', u'foo:a', u'foo:b', u'foo:c'),
    }
    self.assertEqual(actual, expected)
    # With OR
    actual = set(
        tuple(f) for f in expand({
            u'foo': [u'a|b|c', u'def'],
            u'bar': [u'x|y', u'z|w']
        }))
    expected = {
        (u'bar:w', u'bar:x', u'foo:a', u'foo:def'),
        (u'bar:w', u'bar:x', u'foo:b', u'foo:def'),
        (u'bar:w', u'bar:x', u'foo:c', u'foo:def'),
        (u'bar:w', u'bar:y', u'foo:a', u'foo:def'),
        (u'bar:w', u'bar:y', u'foo:b', u'foo:def'),
        (u'bar:w', u'bar:y', u'foo:c', u'foo:def'),
        (u'bar:x', u'bar:z', u'foo:a', u'foo:def'),
        (u'bar:x', u'bar:z', u'foo:b', u'foo:def'),
        (u'bar:x', u'bar:z', u'foo:c', u'foo:def'),
        (u'bar:y', u'bar:z', u'foo:a', u'foo:def'),
        (u'bar:y', u'bar:z', u'foo:b', u'foo:def'),
        (u'bar:y', u'bar:z', u'foo:c', u'foo:def'),
    }
    # flats are sorted
    actual = set(tuple(f) for f in expand({
        u'foo': [u'a|y|b|z', u'c'],
    }))
    expected = {
        (u'foo:a', u'foo:c'),
        (u'foo:b', u'foo:c'),
        (u'foo:c', u'foo:y'),
        (u'foo:c', u'foo:z'),
    }
    self.assertEqual(actual, expected)

  def test_cron_tidy_tasks(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    exp = (request.expiration_ts - request.created_ts +
        task_queues._EXTEND_VALIDITY)
    self.assert_count(1, task_queues.TaskDimensions)

    # No-op.
    task_queues.cron_tidy_tasks()
    self.assert_count(1, task_queues.TaskDimensions)

    # Just before _KEEP_DEAD.
    self.mock_now(now, (exp + task_queues._KEEP_DEAD).total_seconds())
    task_queues.cron_tidy_tasks()
    self.assert_count(1, task_queues.TaskDimensions)

    # TaskDimension expired, after KEEP_DEAD.
    self.mock_now(now, (exp + task_queues._KEEP_DEAD).total_seconds() + 1)
    task_queues.cron_tidy_tasks()
    self.assert_count(0, task_queues.TaskDimensions)

  def test_cron_tidy_bots(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    self.assertEqual(0, self._assert_bot())
    request = self._assert_task()
    exp = (request.expiration_ts - request.created_ts +
        task_queues._EXTEND_VALIDITY)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

    # TaskDimension expired but is still kept; get_queues() doesn't return it
    # anymore even if still in the DB. BotTaskDimensions is not evicted yet,
    # since the cron allows some time for assert_bot_async to do the expiration.
    self.mock_now(now, exp.total_seconds() + 1)
    task_queues.cron_tidy_bots()
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assertEqual([], task_queues._get_queues(bot_root_key))

    # After _TIDY_BOT_DIMENSIONS_CRON_LAG the cron job kicks out stale bot
    # dimensions for good.
    self.mock_now(
        now,
        (exp + task_queues._TIDY_BOT_DIMENSIONS_CRON_LAG).total_seconds() + 1)
    task_queues.cron_tidy_bots()
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assertEqual([], task_queues._get_queues(bot_root_key))

  def test_expiry_map(self):
    sets = [
        {
            'dimensions': ('a:b', 'a:c'),
            'expiry': 1663040000
        },
        {
            'dimensions': ('a:d', 'a:e'),
            'expiry': 1663041111
        },
    ]
    expiry_map = task_queues._sets_to_expiry_map(sets)
    self.assertEqual(sets, task_queues._expiry_map_to_sets(expiry_map))
    without_ts = task_queues._expiry_map_to_sets(expiry_map, False)
    self.assertEqual([
        {
            'dimensions': ('a:b', 'a:c')
        },
        {
            'dimensions': ('a:d', 'a:e')
        },
    ], without_ts)

  @staticmethod
  def _stored_task_dims_expiry(expiry):
    """Expiry of sets stored in TaskDimensionsSets given a task slice expiry."""
    expiry += datetime.timedelta(minutes=15)  # mocked randomization
    expiry += datetime.timedelta(hours=4)  # extra time added in the txn
    return expiry

  @parameterized.expand([
      ({
          'pool': ['p'],
          'k': ['v1']
      }, 'pool:p:1496061212'),
      ({
          'pool': ['p'],
          'k': ['v1|v2']
      }, 'pool:p:3930364299'),
      ({
          'pool': ['a:b'],
          'k': ['v1']
      }, 'pool:a%3Ab:2715169585'),
      ({
          'id': ['b'],
          'pool': ['p'],
          'k': ['v1']
      }, 'bot:b:174937362'),
      ({
          'id': ['b'],
          'pool': ['p'],
          'k': ['v1|v2']
      }, 'bot:b:903320086'),
  ])
  def test_assert_task_dimensions_async(self, task_dims, expected_sets_id):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    expected_sets = task_queues.expand_dimensions_to_flats(task_dims)

    tq_tasks = []

    @ndb.tasklet
    def mocked_tq_task(task_sets_id, task_sets_dims, enqueued_ts):
      self.assertEqual(task_sets_id, expected_sets_id)
      self.assertEqual(task_sets_dims, expected_sets)
      self.assertEqual(enqueued_ts, utils.utcnow())
      tq_tasks.append(1)
      raise ndb.Return(True)

    self.mock(task_queues, '_tq_update_bot_matches_async', mocked_tq_task)

    def assert_task(expiry):
      task_queues._assert_task_dimensions_async(task_dims, expiry).get_result()
      self.execute_tasks()
      seen = len(tq_tasks)
      del tq_tasks[:]
      return seen

    requested_exp = now + datetime.timedelta(hours=1)
    stored_exp = self._stored_task_dims_expiry(requested_exp)

    # Add a never seen before task, should enqueue a TQ task.
    self.assertEqual(assert_task(requested_exp), 1)

    # Check entities are correct and have correct expiry.
    sets_key = ndb.Key(task_queues.TaskDimensionsSets, expected_sets_id)
    sets_ent = sets_key.get()
    self.assertEqual(sets_ent.to_dict(), {
        'sets': [{
            'dimensions': dims
        } for dims in expected_sets],
    })
    info_key = ndb.Key(task_queues.TaskDimensionsInfo, 1, parent=sets_key)
    info_ent = info_key.get()
    self.assertEqual(
        info_ent.to_dict(), {
            'sets': [{
                'dimensions': dims,
                'expiry': _to_timestamp(stored_exp)
            } for dims in expected_sets],
            'next_cleanup_ts':
            stored_exp + datetime.timedelta(minutes=5),
        })

    # A bit later (before the stored expiry) add the exact same task. Should
    # result in no TQ tasks and untouched entities.
    now += datetime.timedelta(hours=1.5)
    self.mock_now(now)
    self.assertEqual(assert_task(now + datetime.timedelta(hours=1)), 0)
    self.assertEqual(sets_key.get(), sets_ent)
    self.assertEqual(info_key.get(), info_ent)

    # Adding a task even later results in the bump to the stored expiry (but no
    # new TQ tasks).
    now += datetime.timedelta(hours=6)
    self.mock_now(now)
    self.assertEqual(assert_task(now + datetime.timedelta(hours=1)), 0)
    self.assertEqual(
        info_key.get().next_cleanup_ts,
        self._stored_task_dims_expiry(now + datetime.timedelta(hours=1)) +
        datetime.timedelta(minutes=5))

  def test_assert_task_dimensions_async_collision(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    # Simulate a hash collision, since it is hard (but not impossible) to get
    # one for real.
    self.mock(task_queues, 'hash_dimensions', lambda _dims: 42)

    expected_sets_id = 'pool:p:42'
    sets_key = ndb.Key(task_queues.TaskDimensionsSets, expected_sets_id)
    info_key = ndb.Key(task_queues.TaskDimensionsInfo, 1, parent=sets_key)

    tq_tasks = []

    @ndb.tasklet
    def mocked_tq_task(task_sets_id, task_sets_dims, enqueued_ts):
      self.assertEqual(task_sets_id, expected_sets_id)
      self.assertEqual(len(task_sets_dims), 1)
      self.assertEqual(enqueued_ts, utils.utcnow())
      tq_tasks.append(tuple(task_sets_dims[0]))
      raise ndb.Return(True)

    self.mock(task_queues, '_tq_update_bot_matches_async', mocked_tq_task)

    def assert_task(dim, exp):
      task_queues._assert_task_dimensions_async({
          'pool': ['p'],
          'key': [dim],
      }, exp).get_result()
      self.execute_tasks()
      seen = tq_tasks[:]
      del tq_tasks[:]
      return seen

    # Create a new TaskDimensionsSets.
    exp1 = now + datetime.timedelta(hours=1)
    stored_exp1 = self._stored_task_dims_expiry(exp1)
    self.assertEqual(assert_task('1', exp1), [('key:1', 'pool:p')])

    # Append a new set to an existing TaskDimensionsSets.
    now += datetime.timedelta(hours=1)
    self.mock_now(now)
    exp2 = now + datetime.timedelta(hours=1)
    stored_exp2 = self._stored_task_dims_expiry(exp2)
    self.assertEqual(assert_task('2', exp2), [('key:2', 'pool:p')])

    # Have both of them now.
    self.assertEqual(
        info_key.get().to_dict(), {
            'sets': [
                {
                    'dimensions': ['key:1', 'pool:p'],
                    'expiry': _to_timestamp(stored_exp1),
                },
                {
                    'dimensions': ['key:2', 'pool:p'],
                    'expiry': _to_timestamp(stored_exp2),
                },
            ],
            'next_cleanup_ts':
            stored_exp1 + datetime.timedelta(minutes=5),
        })

    # Refresh expiry of the second set, it should kick the first out as stale.
    now = stored_exp2 - datetime.timedelta(hours=0.5)
    self.mock_now(now)
    exp3 = now + datetime.timedelta(hours=1)
    stored_exp3 = self._stored_task_dims_expiry(exp3)
    self.assertEqual(assert_task('2', exp3), [])

    # Have only one set now.
    self.assertEqual(
        info_key.get().to_dict(), {
            'sets': [
                {
                    'dimensions': ['key:2', 'pool:p'],
                    'expiry': _to_timestamp(stored_exp3),
                },
            ],
            'next_cleanup_ts':
            stored_exp3 + datetime.timedelta(minutes=5),
        })

  def test_update_bot_matches_async(self):
    # Tested as a part of the overall workflow.
    pass

  def test_rescan_matching_task_sets_async(self):
    # Tested as a part of the overall workflow.
    pass

  def test_tidy_task_dimension_sets_async(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)

    # Use a transaction to be able to use _put_task_dimensions_sets_async that
    # has an assert inside. We don't really care about atomicity here.
    @ndb.transactional_tasklet(xg=True)
    def prep():
      # Fully survives the cleanup.
      yield task_queues._put_task_dimensions_sets_async(
          'set0', {
              ('0:a', ): now + datetime.timedelta(hours=2),
              ('0:b', ): now + datetime.timedelta(hours=2),
          })
      # Partially survives the cleanup.
      yield task_queues._put_task_dimensions_sets_async(
          'set1', {
              ('1:a', ): now + datetime.timedelta(hours=1),
              ('1:b', ): now + datetime.timedelta(hours=2),
          })
      # Doesn't survive the cleanup.
      yield task_queues._put_task_dimensions_sets_async(
          'set2', {
              ('2:a', ): now + datetime.timedelta(hours=1),
              ('2:b', ): now + datetime.timedelta(hours=1),
          })

    prep().get_result()

    self.mock_now(now + datetime.timedelta(hours=1.5))
    self.assertTrue(task_queues.tidy_task_dimension_sets_async().get_result())

    def fetch(sets_id):
      sets_key = ndb.Key(task_queues.TaskDimensionsSets, sets_id)
      info_key = ndb.Key(task_queues.TaskDimensionsInfo, 1, parent=sets_key)
      sets, info = ndb.get_multi([sets_key, info_key])
      if not sets:
        # Both must be missing at the same time.
        self.assertIsNone(info)
        return None
      self.assertIsNotNone(info)
      # Must contain same sets (sans `expiry`).
      expless = [{'dimensions': s['dimensions']} for s in info.sets]
      self.assertEqual(sets.sets, expless)
      return task_queues._sets_to_expiry_map(info.sets)

    # Fully survived.
    self.assertEqual(
        fetch('set0'), {
            ('0:a', ): now + datetime.timedelta(hours=2),
            ('0:b', ): now + datetime.timedelta(hours=2),
        })
    # Partially survived.
    self.assertEqual(fetch('set1'), {
        ('1:b', ): now + datetime.timedelta(hours=2),
    })
    # Fully gone.
    self.assertIsNone(fetch('set2'))

    # Noop run for code coverage.
    self.assertTrue(task_queues.tidy_task_dimension_sets_async().get_result())

    # Full cleanup.
    self.mock_now(now + datetime.timedelta(hours=2, minutes=6))
    self.assertTrue(task_queues.tidy_task_dimension_sets_async().get_result())

    # All gone now.
    self.assertIsNone(fetch('set0'))
    self.assertIsNone(fetch('set1'))

  @parameterized.expand(['pool', 'id'])
  def test_backfill_task_sets_async(self, root_kind):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    def new_ent_id(set_id, dim_hash):
      kind = 'pool' if root_kind == 'pool' else 'bot'
      pfx = task_queues.TaskDimensionsSets.id_prefix(kind, set_id)
      return '%s:%d' % (pfx, dim_hash)

    # Creates an entity in old format.
    def old(set_id, expiry_map, dim_hash=123):
      root = ndb.Key('TaskDimensionsRoot', '%s:%s' % (root_kind, set_id))
      sets = [
          task_queues.TaskDimensionsSet(
              dimensions_flat=list(dims),
              valid_until_ts=exp,
          ) for dims, exp in sorted(expiry_map.items())
      ]
      task_queues.TaskDimensions(id=dim_hash, parent=root, sets=sets).put()

    # Creates an entity in new format.
    def new(set_id, expiry_map, dim_hash=123):
      ndb.transaction_async(lambda: task_queues._put_task_dimensions_sets_async(
          new_ent_id(set_id, dim_hash), expiry_map)).get_result()

    all_expected = {}

    # Records what entity in new format we expect to see after the backfill.
    def expected(set_id, expiry_map, dim_hash=123):
      all_expected[new_ent_id(set_id, dim_hash)] = expiry_map

    # Ancient entity that will be completely ignored.
    old('ancient', {('a:b', ): now - datetime.timedelta(hours=1)})

    # Old entity that doesn't need a migration.
    old(
        'uptodate', {
            ('a:1', ): now + datetime.timedelta(hours=1),
            ('a:2', ): now + datetime.timedelta(hours=1),
        })
    new(
        'uptodate', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=2),
            ('a:3', ): now + datetime.timedelta(hours=1),
        })
    expected(
        'uptodate', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=2),
            ('a:3', ): now + datetime.timedelta(hours=1),
        })

    # Entity that will be partially migrated.
    old(
        'mixed', {
            ('a:1', ): now + datetime.timedelta(hours=1),
            ('a:2', ): now + datetime.timedelta(hours=2),
            ('a:4', ): now + datetime.timedelta(hours=1),
        })
    new(
        'mixed', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=1),
            ('a:3', ): now + datetime.timedelta(hours=1),
        })
    expected(
        'mixed', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=2),
            ('a:3', ): now + datetime.timedelta(hours=1),
            ('a:4', ): now + datetime.timedelta(hours=1),
        })

    # Entities that will be fully migrated.
    old(
        'new', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=2),
        })
    expected(
        'new', {
            ('a:1', ): now + datetime.timedelta(hours=2),
            ('a:2', ): now + datetime.timedelta(hours=2),
        })

    # Run the migration.
    stats = task_queues.BackfillStats()
    task_queues.backfill_task_sets_async(stats).get_result()
    self.assertEqual(stats.added, 1)
    self.assertEqual(stats.updated, 1)
    self.assertEqual(stats.untouched, 1)
    self.assertEqual(stats.failures, 0)
    self.assertEqual(stats.noop_txns, 0)

    # Check the final state matches expectations.
    infos = {
        ent.key.parent().string_id(): task_queues._sets_to_expiry_map(ent.sets)
        for ent in task_queues.TaskDimensionsInfo.query()
    }
    self.assertEqual(all_expected, infos)

    # Running it again doesn't touch anything.
    stats = task_queues.BackfillStats()
    task_queues.backfill_task_sets_async(stats).get_result()
    self.assertEqual(stats.added, 0)
    self.assertEqual(stats.updated, 0)
    self.assertEqual(stats.untouched, 3)
    self.assertEqual(stats.failures, 0)
    self.assertEqual(stats.noop_txns, 0)

  @staticmethod
  def _create_task_dims_set(sets_id, *task_dims):
    exp_map = {
        tuple(dims): utils.utcnow() + datetime.timedelta(hours=10)
        for dims in task_dims
    }
    ndb.transaction_async(lambda: task_queues._put_task_dimensions_sets_async(
        sets_id, exp_map)).get_result()

  @staticmethod
  def _delete_task_dims_set(sets_id):
    ndb.transaction_async(lambda: task_queues.
                          _delete_task_dimensions_sets_async(sets_id
                                                             )).get_result()

  def test_assert_bot_dimensions_async(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    tq = self._mock_enqueue_task_async('rescan-matching-task-sets')

    dims = {
        'id': ['bot-id'],
        'pool': ['pool1', 'pool2'],
        'dim': ['0'],
    }
    bot_matches_key = ndb.Key(task_queues.BotDimensionsMatches, 'bot-id')

    # The first call ever, no queues are assigned yet.
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [])

    # Created the entity.
    bot_matches = bot_matches_key.get()
    self.assertEqual(
        bot_matches.to_dict(), {
            'dimensions':
            [u'dim:0', u'id:bot-id', u'pool:pool1', u'pool:pool2'],
            'last_addition_ts': None,
            'last_cleanup_ts': now,
            'last_rescan_enqueued_ts': now,
            'last_rescan_finished_ts': None,
            'matches': [],
            'next_rescan_ts': now + datetime.timedelta(minutes=30),
            'rescan_counter': 1
        })

    # Enqueued the rescan task.
    self.assertEqual(len(tq), 1)
    self.assertEqual(
        json.loads(tq[0]), {
            u'bot_id': u'bot-id',
            u'rescan_counter': 1,
            u'rescan_reason':
            u'dims added [dim:0 id:bot-id pool:pool1 pool:pool2]',
        })
    del tq[:]

    # Mock assignment of the queues. This happens in the TQ task usually.
    self._create_task_dims_set('bot:bot-id:1', ['id:bot-id'])
    self._create_task_dims_set('pool:pool1:2', ['pool:pool1'])
    self._create_task_dims_set('pool:pool1:3', ['pool:pool1', 'dim:0'])
    self._create_task_dims_set('pool:pool2:4', ['pool:pool2'])
    self._create_task_dims_set('pool:pool2:5', ['pool:pool2', 'dim:0'])
    bot_matches.matches = [
        'bot:bot-id:1',
        'pool:pool1:2',
        'pool:pool1:3',
        'pool:pool2:4',
        'pool:pool2:5',
    ]
    bot_matches.put()

    # The next call discovers them and doesn't submit any TQ tasks.
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2, 3, 4, 5])
    self.assertEqual(len(tq), 0)

    # Some time later the rescan is triggered.
    now += datetime.timedelta(minutes=31)
    self.mock_now(now)
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2, 3, 4, 5])

    # Enqueued the rescan task.
    self.assertEqual(len(tq), 1)
    self.assertEqual(json.loads(tq[0]), {
        u'bot_id': u'bot-id',
        u'rescan_counter': 2,
        u'rescan_reason': u'periodic',
    })
    del tq[:]

    # Bots dimensions change. Unmatching queues are no longer reported.
    dims['dim'] = ['1']
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2, 4])

    # Enqueued the rescan task.
    self.assertEqual(len(tq), 1)
    self.assertEqual(
        json.loads(tq[0]), {
            u'bot_id': u'bot-id',
            u'rescan_counter': 3,
            u'rescan_reason': u'dims added [dim:1], dims removed [dim:0]',
        })
    del tq[:]

    # Changes are reflected in the entity.
    bot_matches = bot_matches_key.get()
    self.assertEqual(
        bot_matches.to_dict(),
        {
            'dimensions':
            [u'dim:1', u'id:bot-id', u'pool:pool1', u'pool:pool2'],
            'last_addition_ts': None,
            'last_cleanup_ts': now,
            'last_rescan_enqueued_ts': now,
            'last_rescan_finished_ts': None,  # we mocked this out
            'matches': [u'bot:bot-id:1', u'pool:pool1:2', u'pool:pool2:4'],
            'next_rescan_ts': now + datetime.timedelta(minutes=30),
            'rescan_counter': 3
        })

    # Some task set disappears (e.g. gets cleaned up by the cron).
    now += datetime.timedelta(minutes=5)
    self.mock_now(now)
    self._delete_task_dims_set('pool:pool2:4')

    # It is no longer assigned to the bot. This doesn't enqueue a task.
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2])
    self.assertEqual(len(tq), 0)

    # Changes are reflected in the entity.
    prev_state = bot_matches.to_dict()
    bot_matches = bot_matches_key.get()
    self.assertEqual(
        bot_matches.to_dict(),
        {
            'dimensions':
            [u'dim:1', u'id:bot-id', u'pool:pool1', u'pool:pool2'],
            'last_addition_ts': None,
            'last_cleanup_ts': now,
            'last_rescan_enqueued_ts': prev_state['last_rescan_enqueued_ts'],
            'last_rescan_finished_ts': None,  # we mocked this out
            'matches': [u'bot:bot-id:1', u'pool:pool1:2'],
            'next_rescan_ts': prev_state['next_rescan_ts'],
            'rescan_counter': prev_state['rescan_counter'],
        })

  def test_tq_rescan_matching_task_sets_async(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    # Prepare all active task dims sets.
    self._create_task_dims_set('bot:bot-id:1', ['id:bot-id'])
    self._create_task_dims_set('bot:bot-id:2', ['id:bot-id', 'dim:0'])
    self._create_task_dims_set('bot:bot-id:3', ['id:bot-id', 'dim:1'])
    self._create_task_dims_set('pool:pool1:4', ['pool:pool1'])
    self._create_task_dims_set('pool:pool1:5', ['pool:pool1', 'dim:0'])
    self._create_task_dims_set('pool:pool1:6', ['pool:pool1', 'dim:1'])
    self._create_task_dims_set('pool:pool2:7', ['pool:pool2'])
    self._create_task_dims_set('pool:pool2:8', ['pool:pool2', 'dim:0'])
    self._create_task_dims_set('pool:pool2:9', ['pool:pool2', 'dim:1'])

    # Put some extra garbage that must no be visited since it is outside of the
    # range of datastore scan query.
    self._create_task_dims_set('bot:bot-id:', ['id:bot-id'])
    self._create_task_dims_set('bot:bot-id:/', ['id:bot-id'])
    self._create_task_dims_set('bot:bot-id::', ['id:bot-id'])
    self._create_task_dims_set('pool:pool1:', ['pool:pool1'])
    self._create_task_dims_set('pool:pool1:/', ['pool:pool1'])
    self._create_task_dims_set('pool:pool1::', ['pool:pool1'])

    # Prepare BotDimensionsMatches in some initial pre-scan state: it has new
    # dimensions (dim:1), but matches are still for old ones (dim:0).
    task_queues.BotDimensionsMatches(
        id='bot-id',
        dimensions=[u'dim:1', u'id:bot-id', u'pool:pool1', u'pool:pool2'],
        matches=[
            u'bot:bot-id:1',
            u'bot:bot-id:2',
            u'bot:bot-id:444',  # missing, should be unmatched
            u'pool:missing:555',  # missing, should be unmatched
            u'pool:pool1:4',
            u'pool:pool1:5',
            u'pool:pool2:7',
            u'pool:pool2:8',
        ],
        last_rescan_enqueued_ts=now,
        next_rescan_ts=now + datetime.timedelta(hours=1),
        rescan_counter=555,
    ).put()

    # Run the rescan.
    self.assertTrue(
        task_queues._tq_rescan_matching_task_sets_async('bot-id', 555,
                                                        'reason').get_result())

    # The state was updated correctly to match on `dim:1`. Old entries were
    # kicked out.
    ent = ndb.Key(task_queues.BotDimensionsMatches, 'bot-id').get()
    self.assertEqual(
        ent.to_dict(), {
            'dimensions':
            [u'dim:1', u'id:bot-id', u'pool:pool1', u'pool:pool2'],
            'last_addition_ts':
            None,
            'last_cleanup_ts':
            now,
            'last_rescan_enqueued_ts':
            now,
            'last_rescan_finished_ts':
            now,
            'matches': [
                u'bot:bot-id:1',
                u'bot:bot-id:3',
                u'pool:pool1:4',
                u'pool:pool1:6',
                u'pool:pool2:7',
                u'pool:pool2:9',
            ],
            'next_rescan_ts':
            now + datetime.timedelta(hours=1),
            'rescan_counter':
            555,
        })

  def test_tq_update_bot_matches_async_pool(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    task_sets_id = 'pool:p:1'
    task_sets_dims = [['pool:p', 'dim:0'], ['pool:p', 'dim:1']]
    self._create_task_dims_set(task_sets_id, *task_sets_dims)

    def register_bot(bot_id, dims, matches):
      task_queues.BotDimensionsMatches(
          id=bot_id,
          dimensions=sorted(dims + ['id:' + bot_id]),
          matches=matches,
          last_addition_ts=utils.EPOCH,
      ).put()

    # Bots that should be matched.
    register_bot('match-0', ['pool:p', 'dim:0'], ['pool:p:555'])
    register_bot('match-1', ['pool:p', 'dim:1'], [])
    register_bot('match-2', ['pool:p', 'dim:0', 'dim:1'], [])
    register_bot('match-3', ['pool:p', 'dim:0', 'extra:0'], [])

    # Will be visited, but not updated (since it already has the match)
    register_bot('untouched-0', ['pool:p', 'dim:0'], [task_sets_id])

    # Bots that should be left untouched.
    register_bot('mismatch-0', ['pool:p'], ['pool:p:555'])
    register_bot('mismatch-1', ['pool:p', 'dim:nope'], ['pool:p:555'])
    register_bot('mismatch-2', ['pool:another', 'dim:0'], ['pool:p:555'])

    self.assertTrue(
        task_queues._tq_update_bot_matches_async(task_sets_id, task_sets_dims,
                                                 now).get_result())

    def assert_matched(bot_id, matches):
      ent = ndb.Key(task_queues.BotDimensionsMatches, bot_id).get()
      self.assertEqual(ent.matches, matches)
      self.assertEqual(ent.last_addition_ts, now)

    def assert_untouched(bot_id, matches):
      ent = ndb.Key(task_queues.BotDimensionsMatches, bot_id).get()
      self.assertEqual(ent.matches, matches)
      self.assertEqual(ent.last_addition_ts, utils.EPOCH)

    assert_matched('match-0', [task_sets_id, 'pool:p:555'])
    assert_matched('match-1', [task_sets_id])
    assert_matched('match-2', [task_sets_id])
    assert_matched('match-3', [task_sets_id])

    assert_untouched('untouched-0', [task_sets_id])
    assert_untouched('mismatch-0', ['pool:p:555'])
    assert_untouched('mismatch-1', ['pool:p:555'])
    assert_untouched('mismatch-2', ['pool:p:555'])

  def test_tq_update_bot_matches_async_bot(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    task_queues.BotDimensionsMatches(
        id='bot-id',
        dimensions=['id:bot-id', 'dim:0', 'dim:1', 'pool:p'],
        matches=['pool:p:123', 'bot:bot-id:456'],
        last_addition_ts=utils.EPOCH,
    ).put()

    def match_task(task_sets_id, task_sets_dims):
      self._create_task_dims_set(task_sets_id, *task_sets_dims)
      self.assertTrue(
          task_queues._tq_update_bot_matches_async(task_sets_id, task_sets_dims,
                                                   now).get_result())

    # Will be matched.
    match_task('bot:bot-id:1', [['id:bot-id', 'dim:0']])
    match_task('bot:bot-id:2', [['id:bot-id', 'dim:1']])
    match_task('bot:bot-id:3', [['id:bot-id', 'dim:0', 'dim:1']])
    match_task('bot:bot-id:4', [['id:bot-id', 'dim:z'], ['id:bot-id', 'dim:0']])
    match_task('bot:bot-id:5', [['id:bot-id']])

    # Will not be matched, wrong dims.
    match_task('bot:bot-id:6', [['id:bot-id', 'dim:3']])
    match_task('bot:bot-id:7', [['id:bot-id', 'dim:0', 'extra:1']])

    ent = ndb.Key(task_queues.BotDimensionsMatches, 'bot-id').get()
    self.assertEqual(ent.matches, [
        u'bot:bot-id:1',
        u'bot:bot-id:2',
        u'bot:bot-id:3',
        u'bot:bot-id:4',
        u'bot:bot-id:456',
        u'bot:bot-id:5',
        u'pool:p:123',
    ])

  def test_tq_update_bot_matches_async_skip(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    task_queues.BotDimensionsMatches(
        id='bot-id',
        dimensions=['id:bot-id', 'dim:0', 'dim:1', 'pool:p'],
        matches=[],
        last_addition_ts=utils.EPOCH,
    ).put()

    def matches():
      return ndb.Key(task_queues.BotDimensionsMatches, 'bot-id').get().matches

    def call(sets_id, tasks_dims):
      return task_queues._tq_update_bot_matches_async(sets_id, tasks_dims,
                                                      now).get_result()

    # Will do nothing, no TaskDimensionsSets entity is stored.
    self.assertTrue(call('bot:bot-id:1', [['id:bot-id']]))
    self.assertEqual(matches(), [])

    # Will do nothing, all sets are not in the entity.
    self._create_task_dims_set('bot:bot-id:1', ['id:bot-id', 'dim:0'])
    self.assertTrue(
        call('bot:bot-id:1', [['id:bot-id'], ['id:bot-id', 'dim:1']]))
    self.assertEqual(matches(), [])

  def test_tq_update_bot_matches_async_no_matches(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    def call(sets_id, tasks_dims):
      return task_queues._tq_update_bot_matches_async(sets_id, tasks_dims,
                                                      now).get_result()

    # No matching bot.
    self._create_task_dims_set('bot:bot-id:1', ['id:bot-id'])
    self.assertTrue(call('bot:bot-id:1', [['id:bot-id']]))

    # No matches in a pool.
    self._create_task_dims_set('pool:p:1', ['pool:p'])
    self.assertTrue(call('pool:p:1', [['pool:p']]))


class TestMapAsync(test_env_handlers.AppTestBase):
  # Page size in queries.
  PAGE = 4

  @staticmethod
  def populate(bot_dims):
    ndb.put_multi([
        task_queues.BotDimensionsMatches(id=bot_id, dimensions=dims)
        for bot_id, dims in bot_dims.items()
    ])
    return sorted(bot_dims.keys())

  @staticmethod
  def query(dim=None):
    q = task_queues.BotDimensionsMatches.query()
    if dim:
      q = q.filter(task_queues.BotDimensionsMatches.dimensions == dim)
    return q, task_queues._Logger('')

  @staticmethod
  def call(queries, cb, max_pages=None):
    return task_queues._map_async(queries,
                                  cb,
                                  max_concurrency=3,
                                  page_size=TestMapAsync.PAGE,
                                  max_pages=max_pages).get_result()

  def test_single_query_sync_cb(self):
    seen = []

    def cb(ent):
      seen.append(ent.key.string_id())

    bots = self.populate({'bot%d' % i: [] for i in range(10)})
    self.assertTrue(self.call([self.query()], cb))
    self.assertEqual(sorted(seen), bots)

  def test_many_queries_sync_cb(self):
    seen = []

    def cb(ent):
      seen.append(ent.key.string_id())

    bots = []
    bots.extend(self.populate({'bot1-%d' % i: ['k:v1'] for i in range(10)}))
    bots.extend(self.populate({'bot2-%d' % i: ['k:v2'] for i in range(10)}))
    bots.extend(self.populate({'bot3-%d' % i: ['k:v3'] for i in range(10)}))

    queries = [self.query('k:v1'), self.query('k:v2'), self.query('k:v3')]
    self.assertTrue(self.call(queries, cb))
    self.assertEqual(sorted(seen), bots)

  def test_single_query_async_cb(self):
    seen = []

    @ndb.tasklet
    def cb(ent):
      seen.append(ent.key.string_id())
      yield ndb.sleep(random.uniform(0.0, 0.1))
      raise ndb.Return(True)

    bots = self.populate({'bot%d' % i: [] for i in range(10)})
    self.assertTrue(self.call([self.query()], cb))
    self.assertEqual(sorted(seen), bots)

  def test_many_queries_async_cb(self):
    seen = []

    @ndb.tasklet
    def cb(ent):
      seen.append(ent.key.string_id())
      yield ndb.sleep(random.uniform(0.0, 0.1))
      raise ndb.Return(True)

    bots = []
    bots.extend(self.populate({'bot1-%d' % i: ['k:v1'] for i in range(10)}))
    bots.extend(self.populate({'bot2-%d' % i: ['k:v2'] for i in range(10)}))
    bots.extend(self.populate({'bot3-%d' % i: ['k:v3'] for i in range(10)}))

    queries = [self.query('k:v1'), self.query('k:v2'), self.query('k:v3')]
    self.assertTrue(self.call(queries, cb))
    self.assertEqual(sorted(seen), bots)

  def test_query_timeout(self):
    seen = []

    @ndb.tasklet
    def cb(ent):
      seen.append(ent.key.string_id())
      yield ndb.sleep(random.uniform(0.0, 0.1))
      raise ndb.Return(True)

    b1 = self.populate({'b1-%d' % i: ['k:v1'] for i in range(self.PAGE * 2)})
    b2 = self.populate({'b2-%d' % i: ['k:v2'] for i in range(self.PAGE * 2)})
    b3 = self.populate({'b3-%d' % i: ['k:v3'] for i in range(self.PAGE * 3)})

    queries = [self.query('k:v1'), self.query('k:v2'), self.query('k:v3')]
    self.assertFalse(self.call(queries, cb, max_pages=2))

    # The last page if k:v3 query is missing.
    expected = []
    expected.extend(b1)
    expected.extend(b2)
    expected.extend(b3[:self.PAGE * 2])
    self.assertEqual(sorted(seen), expected)

  def test_processing_error(self):
    seen = []

    @ndb.tasklet
    def cb(ent):
      seen.append(ent.key.string_id())
      yield ndb.sleep(random.uniform(0.0, 0.1))
      raise ndb.Return(ent.key.string_id() != 'bot5')

    bots = self.populate({'bot%d' % i: [] for i in range(10)})
    self.assertFalse(self.call([self.query()], cb))
    self.assertEqual(sorted(seen), bots)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
