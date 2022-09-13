#!/usr/bin/env vpython
# coding: utf-8
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import random
import sys
import unittest

# Setups environment.
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)
import test_env_handlers

import webtest

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

import handlers_backend

from components import utils
from server import bot_management
from server import task_queues
from server import task_request


def _assert_bot(bot_id=u'bot1', dimensions=None):
  bot_dimensions = {
      u'cpu': [u'x86-64', u'x64'],
      u'id': [bot_id],
      u'os': [u'Ubuntu-16.04', u'Ubuntu'],
      u'pool': [u'default'],
  }
  bot_dimensions.update(dimensions or {})
  bot_management.bot_event(
      'request_sleep',
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
  return task_queues._assert_bot_old_async(bot_root_key,
                                           bot_dimensions).get_result()


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

  def _enqueue_async(self, *args, **kwargs):
    return self._enqueue_async_orig(*args, use_dedicated_module=False, **kwargs)

  def _mock_enqueue_task_async_for_rebuild_task_cache(self):
    payloads = []

    @ndb.tasklet
    def _enqueue_task_async(url, name, payload):
      self.assertEqual(
          '/internal/taskqueue/important/task_queues/rebuild-cache', url)
      self.assertEqual('rebuild-task-cache', name)
      payloads.append(payload)
      raise ndb.Return(True)

    self.mock(utils, 'enqueue_task_async', _enqueue_task_async)
    return payloads

  def _assert_task(self, dimensions=None, tasks=1):
    """Creates one pending TaskRequest and asserts it in task_queues."""
    request = _gen_request(properties=_gen_properties(
        dimensions=dimensions) if dimensions else None)
    task_queues.assert_task_async(request).get_result()
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

  def test_assert_bot(self):
    self.assert_count(0, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)
    self.assertEqual(0, _assert_bot())
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(0, task_queues.BotTaskDimensions)
    self.assert_count(0, task_queues.TaskDimensions)

  def test_assert_bot_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    self.assertEqual(1, _assert_bot())
    valid_until_ts = request.expiration_ts + task_queues._EXTEND_VALIDITY
    self.assertEqual(
        valid_until_ts,
        task_queues.BotTaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    self.assertEqual(None, _assert_bot())
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

  def test_assert_task_async_no_update(self):
    # Ensure the entity was not updated when not needed.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    request = self._assert_task()
    valid_until_ts = request.expiration_ts + task_queues._EXTEND_VALIDITY
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

    self.mock_now(now, 60)
    self._assert_task(tasks=0)
    self.assertEqual(
        valid_until_ts, task_queues.TaskDimensions.query().get().valid_until_ts)

  def test_assert_task_async_or_dims(self):
    self._assert_task(dimensions={
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    },
                      tasks=1)
    # Already seen it, no new tasks.
    self._assert_task(dimensions={
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    },
                      tasks=0)

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
    _assert_bot(bot_id=u'bot1')
    _assert_bot(bot_id=u'bot2')
    _assert_bot(bot_id=u'bot3')
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual(0, task_queues.BotTaskDimensions.query().count())
    self.assertEqual(0, task_queues.TaskDimensions.query().count())

    # Intentionally force the code to throttle the number of concurrent RPCs,
    # otherwise the inner loops wouldn't be reached with less than 50 bots, and
    # testing with 50 bots, would make the unit test slow.
    self.mock(task_queues, '_CAP_FUTURES_LIMIT', 1)

    payloads = self._mock_enqueue_task_async_for_rebuild_task_cache()

    # The equivalent of self._assert_task(tasks=1) except that we snapshot the
    # payload.
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
    def _enqueue_task_async(url, name, payload):
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
        _assert_bot(
            bot_id=u'bot1',
            dimensions={
                u'os': [u'v1', u'v2'],
                u'gpu': [u'nv'],
            }))
    self.assertEqual(
        0,
        _assert_bot(
            bot_id=u'bot2', dimensions={
                u'os': [u'v2'],
                u'gpu': [u'amd'],
            }))

    payloads = self._mock_enqueue_task_async_for_rebuild_task_cache()

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

    payloads = self._mock_enqueue_task_async_for_rebuild_task_cache()

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
        _assert_bot(
            bot_id=u'bot1', dimensions={
                u'os': [u'v1'],
                u'gpu': [u'nv'],
            }))
    self.assertEqual(
        1,
        _assert_bot(
            bot_id=u'bot2', dimensions={
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
    self.assertEqual(0, _assert_bot(
        bot_id=u'bot1', dimensions={u'os': [u'v1']}))
    self.assertEqual(0, _assert_bot(
        bot_id=u'bot2', dimensions={u'os': [u'v2']}))
    self.assertEqual(0, _assert_bot(
        bot_id=u'bot3', dimensions={u'os': [u'v3']}))

    payloads = self._mock_enqueue_task_async_for_rebuild_task_cache()
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
    _assert_bot()
    self.assertEqual([], memcache.get('bot1', namespace='task_queues'))
    self.assertEqual(1, bot_management.BotInfo.query().count())
    self.assertEqual(None, task_queues.probably_has_capacity(d))

  def test_probably_has_capacity_get_queues(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # Capacity registers there only once there's a request enqueued and
    # get_queues() is called.
    _assert_bot()
    request = _gen_request(properties=_gen_properties(dimensions=d))
    task_queues.assert_task_async(request).get_result()
    self.assertEqual(1, self.execute_tasks())
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
    self.assertEqual(0, _assert_bot())
    self._assert_task()
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

  def test_assert_task_async_then_bot(self):
    self._assert_task()
    self.assertEqual(1, _assert_bot())
    self.assert_count(1, task_queues.BotDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

  def test_assert_bot_then_task_with_id(self):
    # Assert a task that includes an 'id' dimension. No task queue is triggered
    # in this case, rebuild_task_cache_async() is called inlined.
    self.assertEqual(0, _assert_bot())
    request = _gen_request(
        properties=_gen_properties(dimensions={
            u'id': [u'bot1'],
            u'pool': [u'default']
        }))
    task_queues.assert_task_async(request).get_result()
    self.assertEqual(0, self.execute_tasks())
    self.assert_count(1, bot_management.BotInfo)
    self.assert_count(1, task_queues.BotDimensions)
    print(task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)

  def test_assert_task_async_call_rebuld_task_cache_async(self):
    self.assertEqual(0, _assert_bot())
    dimensions = {
        u'id': [u'bot1'],
        u'pool': [u'default'],
    }
    self.mock_now(datetime.datetime(2020, 1, 2, 3, 4, 5))
    request1 = _gen_request(properties=_gen_properties(dimensions=dimensions))
    task_queues.assert_task_async(request1).get_result()
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
    self.assertEqual(0, _assert_bot())
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
    self.assertEqual(1, _assert_bot())
    # One hour later, the bot changes dimensions.
    self.mock_now(now, task_queues._EXTEND_VALIDITY.total_seconds())
    self.assertEqual(1, _assert_bot(dimensions={u'gpu': u'Matrox'}))
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

    # One second before expiration.
    self.mock_now(now, exp.total_seconds())
    self.assertEqual(None, _assert_bot(dimensions={u'gpu': u'Matrox'}))
    self.assert_count(1, task_queues.BotTaskDimensions)
    self.assert_count(1, task_queues.TaskDimensions)
    self.assertEqual([2980491642], task_queues._get_queues(bot_root_key))

    # BotTaskDimensions expired.
    self.mock_now(now, exp.total_seconds() + 1)
    self.assertEqual(0, _assert_bot())
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
    self.assertEqual(0, _assert_bot())
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
