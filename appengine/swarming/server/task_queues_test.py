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

    # Flip use_dedicated_module to False, we don't have it mocked in tests.
    enqueue_task_async = utils.enqueue_task_async

    def enqueue_task_async_mock(*args, **kwargs):
      return enqueue_task_async(*args, use_dedicated_module=False, **kwargs)

    self.mock(utils, 'enqueue_task_async', enqueue_task_async_mock)

    def random_dt(a, b):
      return datetime.timedelta(minutes=(a + b) / 2.0)

    self.mock(task_queues, '_random_timedelta_mins', random_dt)

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
    task_queues.assert_bot(bot_root_key, bot_dimensions)
    return self.execute_tasks()

  def _assert_task(self, dimensions=None):
    """Creates one pending TaskRequest and asserts it in task_queues."""
    request = _gen_request(properties=_gen_properties(
        dimensions=dimensions) if dimensions else None)
    task_queues.assert_task_async(request).get_result()
    return self.execute_tasks()

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(task_queues)
        if i[0] != '_' and hasattr(getattr(task_queues, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def assert_count(self, count, entity):
    actual = entity.query().count()
    if actual != count:
      self.fail([i.to_dict() for i in entity.query()])
    self.assertEqual(count, actual)

  def test_assert_bot(self):
    self.assert_count(0, task_queues.BotDimensionsMatches)
    self.assert_count(0, task_queues.TaskDimensionsInfo)
    self.assert_count(0, task_queues.TaskDimensionsSets)
    self._assert_bot()
    self.assert_count(1, task_queues.BotDimensionsMatches)
    self.assert_count(0, task_queues.TaskDimensionsInfo)
    self.assert_count(0, task_queues.TaskDimensionsSets)

  def test_assert_task_async(self):
    self.assert_count(0, task_queues.BotDimensionsMatches)
    self.assert_count(0, task_queues.TaskDimensionsInfo)
    self.assert_count(0, task_queues.TaskDimensionsSets)
    self._assert_task()
    self.assert_count(0, task_queues.BotDimensionsMatches)
    self.assert_count(1, task_queues.TaskDimensionsInfo)
    self.assert_count(1, task_queues.TaskDimensionsSets)

  def test_assert_task_async_no_update(self):
    # Ran TQ tasks to register the new dimension set.
    tq_tasks = self._assert_task()
    self.assertEqual(tq_tasks, 2)
    # Already seen it, no new tasks.
    tq_tasks = self._assert_task()
    self.assertEqual(tq_tasks, 0)

  def test_assert_task_async_or_dims(self):
    # Ran TQ tasks to register the new dimension set.
    tq_tasks = self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })
    self.assertEqual(tq_tasks, 2)
    # Already seen it, no new tasks.
    tq_tasks = self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })
    self.assertEqual(tq_tasks, 0)

  def test_freshen_up_queues(self):
    # See more complex test below.
    pass

  def test_or_dimensions_new_tasks(self):
    # There are two bots that can handle two OR alternatives of a task.
    self._assert_bot(bot_id=u'bot1',
                     dimensions={
                         u'os': [u'v1', u'v2'],
                         u'gpu': [u'nv'],
                     })
    self._assert_bot(bot_id=u'bot2',
                     dimensions={
                         u'os': [u'v2'],
                         u'gpu': [u'amd'],
                     })

    # A task that "|" appears.
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })

    # Both bots should be able to handle it.
    bot1_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([787294789], task_queues.freshen_up_queues(bot1_root_key))
    bot2_root_key = bot_management.get_root_key(u'bot2')
    self.assertEqual([787294789], task_queues.freshen_up_queues(bot2_root_key))

    # Another task comes in that matches only bot1.
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1'],
        u'gpu': [u'nv|amd'],
    })

    # Only bot1 can handle the second task.
    self.assertEqual([787294789, 1873318153],
                     task_queues.freshen_up_queues(bot1_root_key))
    self.assertEqual([787294789], task_queues.freshen_up_queues(bot2_root_key))

  def test_or_dimensions_new_bots(self):
    # Register a task that uses "|" dimensions.
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1|v2'],
        u'gpu': [u'nv|amd'],
    })
    # Register a task that matches only bot1.
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1'],
        u'gpu': [u'nv|amd'],
    })

    # Two bots show up matching two different OR alternatives of the first task.
    self._assert_bot(bot_id=u'bot1',
                     dimensions={
                         u'os': [u'v1', u'v2'],
                         u'gpu': [u'nv'],
                     })
    self._assert_bot(bot_id=u'bot2',
                     dimensions={
                         u'os': [u'v2'],
                         u'gpu': [u'amd'],
                     })

    # The bots got correct matches.
    bot1_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([787294789, 1873318153],
                     task_queues.freshen_up_queues(bot1_root_key))
    bot2_root_key = bot_management.get_root_key(u'bot2')
    self.assertEqual([787294789], task_queues.freshen_up_queues(bot2_root_key))

  def test_or_dimensions_same_hash(self):
    self._assert_bot(bot_id=u'bot1', dimensions={u'os': [u'v1']})
    self._assert_bot(bot_id=u'bot2', dimensions={u'os': [u'v2']})
    self._assert_bot(bot_id=u'bot3', dimensions={u'os': [u'v3']})

    # Both requests should have the same dimension_hash.
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v1|v2|v3'],
    })
    self._assert_task({
        u'pool': [u'default'],
        u'os': [u'v3|v2|v1'],
    })

    # All 3 bots are matched to the same queue.
    bot1_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([1060356668], task_queues.freshen_up_queues(bot1_root_key))
    bot2_root_key = bot_management.get_root_key(u'bot2')
    self.assertEqual([1060356668], task_queues.freshen_up_queues(bot2_root_key))
    bot3_root_key = bot_management.get_root_key(u'bot3')
    self.assertEqual([1060356668], task_queues.freshen_up_queues(bot3_root_key))

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

  def test_probably_has_capacity(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # A bot coming online doesn't register capacity automatically.
    self._assert_bot()
    self.assertEqual(1, bot_management.BotInfo.query().count())
    self.assertEqual(None, task_queues.probably_has_capacity(d))

  def test_probably_has_capacity_freshen_up_queues(self):
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    # Capacity registers there only once there's a request enqueued and
    # freshen_up_queues() is called.
    self._assert_bot()
    request = _gen_request(properties=_gen_properties(dimensions=d))
    task_queues.assert_task_async(request).get_result()
    self.execute_tasks()
    self.assertEqual(None, task_queues.probably_has_capacity(d))

    # It gets set only once freshen_up_queues() is called.
    bot_root_key = bot_management.get_root_key(u'bot1')
    task_queues.freshen_up_queues(bot_root_key)
    self.assertEqual(True, task_queues.probably_has_capacity(d))

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
    bot_root_key = bot_management.get_root_key(u'bot1')
    self._assert_bot()
    self._assert_task()
    self.assertEqual([2980491642], task_queues.freshen_up_queues(bot_root_key))

  def test_assert_task_async_then_bot(self):
    bot_root_key = bot_management.get_root_key(u'bot1')
    self._assert_task()
    self._assert_bot()
    self.assertEqual([2980491642], task_queues.freshen_up_queues(bot_root_key))

  def test_cleanup_after_bot(self):
    self._assert_bot()
    self._assert_task()
    task_queues.cleanup_after_bot(bot_management.get_root_key('bot1'))
    # BotInfo is deleted separately.
    self.assert_count(0, task_queues.BotDimensionsMatches)
    self.assert_count(1, bot_management.BotInfo)

  def test_assert_bot_dimensions_changed(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    self._assert_task({
        u'cpu': [u'x86-64'],
        u'os': [u'Amiga'],
        u'pool': [u'default'],
    })
    self._assert_bot(dimensions={u'os': [u'Amiga']})

    # The bot is matched to the task now.
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2828582055], task_queues.freshen_up_queues(bot_root_key))

    # One hour later, the bot changes dimensions.
    now += datetime.timedelta(hours=1)
    self.mock_now(now)
    self._assert_bot(dimensions={u'os': u'Commodore'})

    # It is no longer matched to the task.
    self.assertEqual([], task_queues.freshen_up_queues(bot_root_key))

  def test_task_dimensions_expiry(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)

    self._assert_task()
    self._assert_bot()

    # The bot is matched to the task now.
    bot_root_key = bot_management.get_root_key(u'bot1')
    self.assertEqual([2980491642], task_queues.freshen_up_queues(bot_root_key))

    # Some time later the task dimensions set expires and gets cleaned up.
    now += datetime.timedelta(hours=5)
    self.mock_now(now)
    task_queues.cron_tidy_tasks()
    task_queues.tidy_task_dimension_sets_async().get_result()

    # The bot is no longer matching the task.
    self._assert_bot()
    self.assertEqual([], task_queues.freshen_up_queues(bot_root_key))

  def test_hash_dimensions(self):
    with self.assertRaises(AttributeError):
      task_queues.hash_dimensions('this is not dict')
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

  def test_rebuild_task_cache_async(self):
    # TODO(vadimsh): Delete.
    pass

  def test_cron_tidy_tasks(self):
    # TODO(vadimsh): Delete.
    pass

  def test_cron_tidy_bots(self):
    # TODO(vadimsh): Delete.
    pass

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

    tq_tasks = []

    @ndb.tasklet
    def mocked_rescan(bot_id, rescan_counter, rescan_reason):
      tq_tasks.append({
          u'bot_id': bot_id,
          u'rescan_counter': rescan_counter,
          u'rescan_reason': rescan_reason
      })
      raise ndb.Return(True)

    self.mock(task_queues, '_tq_rescan_matching_task_sets_async', mocked_rescan)

    def assert_no_tq_tasks():
      self.assertEqual(tq_tasks, [])
      self.execute_tasks()
      self.assertEqual(tq_tasks, [])

    def assert_tq_task(expected):
      self.assertEqual(tq_tasks, [])
      self.execute_tasks()
      self.assertEqual(tq_tasks, [expected])
      del tq_tasks[:]

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
    assert_tq_task({
        u'bot_id':
        u'bot-id',
        u'rescan_counter':
        1,
        u'rescan_reason':
        u'dims added [dim:0 id:bot-id pool:pool1 pool:pool2]',
    })

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
    assert_no_tq_tasks()

    # Some time later the rescan is triggered.
    now += datetime.timedelta(minutes=31)
    self.mock_now(now)
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2, 3, 4, 5])

    # Enqueued the rescan task.
    assert_tq_task({
        u'bot_id': u'bot-id',
        u'rescan_counter': 2,
        u'rescan_reason': u'periodic',
    })

    # Bots dimensions change. Unmatching queues are no longer reported.
    dims['dim'] = ['1']
    queues = task_queues._assert_bot_dimensions_async(dims).get_result()
    self.assertEqual(queues, [1, 2, 4])

    # Enqueued the rescan task.
    assert_tq_task({
        u'bot_id':
        u'bot-id',
        u'rescan_counter':
        3,
        u'rescan_reason':
        u'dims added [dim:1], dims removed [dim:0]',
    })

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
    assert_no_tq_tasks()

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
