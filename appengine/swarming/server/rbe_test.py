#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import json
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import mock

from test_support import test_case

from components import utils
from components import datastore_utils

from server import bot_management
from server import pools_config
from server import rbe
from server import task_request
from server import task_to_run

from proto.config import pools_pb2


def pool_config_func(rbe_instance,
                     rbe_mode_percent=100,
                     allocs=None,
                     effective_bot_id_dimension=None):
  rbe_mgration = None
  if rbe_instance is not None:
    rbe_mgration = pools_pb2.Pool.RBEMigration(
        rbe_instance=rbe_instance,
        rbe_mode_percent=rbe_mode_percent,
        bot_mode_allocation=[
            pools_pb2.Pool.RBEMigration.BotModeAllocation(
                mode=mode,
                percent=percent,
            ) for mode, percent in (allocs or {}).items()
        ],
        effective_bot_id_dimension=effective_bot_id_dimension,
    )
  return pools_config.init_pool_config(rbe_migration=rbe_mgration)


class RBETest(test_case.TestCase):

  @staticmethod
  def pool_config_func(rbe_instance,
                       rbe_mode_percent=100,
                       allocs=None,
                       effective_bot_id_dimension=None):
    return pool_config_func(rbe_instance, rbe_mode_percent, allocs,
                            effective_bot_id_dimension)

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('server.rbe._quasi_random_100')
  def test_get_rbe_config_for_bot(self, quasi_random_100, pool_config):
    pools = {}
    pool_config.side_effect = lambda pool: pools[pool]

    # Pure Swarming pool.
    pools['swarming'] = pool_config_func(None)
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['swarming'])
    self.assertIsNone(cfg)

    # A pool with mixed composition of bots.
    pools['many-modes'] = pool_config_func('instance',
                                           allocs={
                                               'SWARMING': 10,
                                               'HYBRID': 60,
                                               'RBE': 30,
                                           })
    # Swarming mode.
    quasi_random_100.return_value = 9
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['many-modes'])
    self.assertIsNone(cfg)
    # Hybrid mode.
    quasi_random_100.return_value = 10
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['many-modes'])
    self.assertEqual(cfg.instance, 'instance')
    self.assertTrue(cfg.hybrid_mode)
    # RBE mode.
    quasi_random_100.return_value = 70
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['many-modes'])
    self.assertEqual(cfg.instance, 'instance')
    self.assertFalse(cfg.hybrid_mode)

    # Older RBE migration config without allocations => use Swarming mode.
    pools['old-config'] = pool_config_func('instance')
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['old-config'])
    self.assertIsNone(cfg)

    # Multi-pool bot.
    pools['pool-a'] = pool_config_func('instance',
                                       allocs={
                                           'SWARMING': 50,
                                           'RBE': 50,
                                       })
    pools['pool-b'] = pool_config_func('instance',
                                       allocs={
                                           'SWARMING': 80,
                                           'RBE': 20,
                                       })
    # All pools agree on a mode.
    quasi_random_100.return_value = 90
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['pool-a', 'pool-b'])
    self.assertEqual(cfg.instance, 'instance')
    self.assertFalse(cfg.hybrid_mode)
    # Pools disagree on a mode.
    quasi_random_100.return_value = 60
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['pool-a', 'pool-b'])
    self.assertEqual(cfg.instance, 'instance')
    self.assertTrue(cfg.hybrid_mode)

    # Multi-pool bot, pools disagree on RBE instance.
    pools['pool-c'] = pool_config_func('instance-c', allocs={
        'RBE': 100,
    })
    pools['pool-d'] = pool_config_func('instance-d', allocs={
        'RBE': 100,
    })
    # All pools agree on a mode.
    quasi_random_100.return_value = 90
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['pool-c', 'pool-d'])
    self.assertEqual(cfg.instance, 'instance-c')
    self.assertFalse(cfg.hybrid_mode)

    # Multi-pool bot uses effective_bot_id_dimension.
    pools['pool-e'] = pool_config_func('instance-c',
                                       allocs={
                                           'RBE': 100,
                                       },
                                       effective_bot_id_dimension='dut_id')
    pools['pool-f'] = pool_config_func('instance-c',
                                       allocs={
                                           'RBE': 100,
                                       },
                                       effective_bot_id_dimension='dut_id')
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['pool-e', 'pool-f'])
    self.assertIsNone(cfg.effective_bot_id_dimension)

  @mock.patch('random.uniform')
  def test_get_rbe_instance_for_task(self, uniform):
    call = rbe.get_rbe_instance_for_task

    self.assertIsNone(call([], pool_config_func(None)))
    self.assertIsNone(call([], pool_config_func('')))

    self.assertEqual(call(['rbe:require'], pool_config_func('inst', 0)), 'inst')
    self.assertIsNone(call(['rbe:prevent'], pool_config_func('inst', 100)))
    self.assertIsNone(
        call(['rbe:require', 'rbe:prevent'], pool_config_func('inst', 100)))

    uniform.return_value = 5.0
    self.assertEqual(call([], pool_config_func('inst', 6)), 'inst')
    self.assertIsNone(call([], pool_config_func('inst', 4)))

  def test_quasi_random_100(self):
    for i in range(1000):
      val = rbe._quasi_random_100(u'bot-%d' % i)
      self.assertGreaterEqual(val, 0)
      self.assertLess(val, 100)


class EnqueueTest(test_case.TestCase):
  maxDiff = None

  def make_request(self, with_effective_bot_id=False):
    def make_slice(name):
      dimensions = {
          u'id': [u'bot-id'],
          u'dim1': [u'val1', u'val2|val3'],
          u'dim2': [u'val4'],
          u'name': [name],
          u'pool': [u'pool'],
      }
      if with_effective_bot_id:
        dimensions[u'dut_id'] = [u'dut1']
      return task_request.TaskSlice(
          properties=task_request.TaskProperties(
              dimensions_data=dimensions,
              execution_timeout_secs=700,
              grace_period_secs=300,
          ),
          expiration_secs=123,
      )
    req = task_request.TaskRequest(
        key=task_request.new_request_key(),
        created_ts=utils.utcnow(),
        name='some-name',
        rbe_instance='some-instance',
        priority=123,
        scheduling_algorithm=pools_pb2.Pool.SCHEDULING_ALGORITHM_LIFO,
        task_slices=[
            make_slice(u'0'),
            make_slice(u'1'),
            make_slice(u'2'),
        ],
    )
    return req, task_to_run.new_task_to_run(req, 2)

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('components.utils.enqueue_task')
  @mock.patch('components.utils.utcnow')
  @mock.patch('random.getrandbits')
  def test_enqueue_rbe_task(self, getrandbits, utcnow, enqueue_task,
                            pool_config):
    getrandbits.return_value = 42
    utcnow.return_value = datetime.datetime(2112, 1, 1, 1, 1, 1)
    enqueue_task.return_value = True

    pools = {}
    pool_config.side_effect = lambda pool: pools[pool]
    pools['pool'] = pool_config_func('instance',
                                     allocs={
                                         'SWARMING': 10,
                                         'HYBRID': 60,
                                         'RBE': 30,
                                     },
                                     effective_bot_id_dimension='dut_id')

    req, ttr = self.make_request()
    req.task_slices[2].wait_for_capacity = True
    datastore_utils.transaction(lambda: rbe.enqueue_rbe_task(req, ttr))

    args, kwargs = enqueue_task.call_args
    kwargs['payload'] = json.loads(kwargs['payload'])

    self.assertEqual(
        args,
        ('/internal/tasks/t/rbe-enqueue/2ed6c6804c8002a10-2', 'rbe-enqueue'))
    self.assertEqual(
        kwargs, {
            'payload': {
                u'body': {
                    u'payload': {
                        u'reservationId': u'sample-app-2ed6c6804c8002a10-2',
                        u'taskId': u'2ed6c6804c8002a10',
                        u'sliceIndex': 2,
                        u'taskToRunId': u'33',
                        u'taskToRunShard': 11,
                        u'debugInfo': {
                            u'created': u'2112-01-01T01:01:01Z',
                            u'pySwarmingVersion': u'v1a',
                            u'taskName': u'some-name',
                        },
                    },
                    u'rbeInstance':
                    u'some-instance',
                    u'executionTimeout':
                    u'1030s',
                    u'expiry':
                    u'2112-01-01T01:07:10Z',
                    u'requestedBotId':
                    u'bot-id',
                    u'constraints': [
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val1']
                        },
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val2', u'val3']
                        },
                        {
                            u'key': u'dim2',
                            u'allowedValues': [u'val4']
                        },
                        {
                            u'key': u'name',
                            u'allowedValues': [u'2']
                        },
                        {
                            u'key': u'pool',
                            u'allowedValues': [u'pool']
                        },
                    ],
                    u'priority':
                    123,
                    u'schedulingAlgorithm':
                    u'SCHEDULING_ALGORITHM_LIFO',
                    u'waitForCapacity':
                    True,
                },
                u'class': u'rbe-enqueue',
            },
            'transactional': True,
            'use_dedicated_module': False,
        })

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('components.utils.enqueue_task')
  @mock.patch('components.utils.utcnow')
  @mock.patch('random.getrandbits')
  def test_enqueue_rbe_task_with_effective_bot_id(self, getrandbits, utcnow,
                                                  enqueue_task, pool_config):
    getrandbits.return_value = 42
    utcnow.return_value = datetime.datetime(2112, 1, 1, 1, 1, 1)
    enqueue_task.return_value = True

    pools = {}
    pool_config.side_effect = lambda pool: pools[pool]
    pools['pool'] = pool_config_func('instance',
                                     allocs={'RBE': 100},
                                     effective_bot_id_dimension='dut_id')

    req, ttr = self.make_request(with_effective_bot_id=True)
    datastore_utils.transaction(lambda: rbe.enqueue_rbe_task(req, ttr))

    args, kwargs = enqueue_task.call_args
    kwargs['payload'] = json.loads(kwargs['payload'])

    self.assertEqual(
        args,
        ('/internal/tasks/t/rbe-enqueue/2ed6c6804c8002a10-2', 'rbe-enqueue'))
    self.assertEqual(
        kwargs, {
            'payload': {
                u'body': {
                    u'payload': {
                        u'reservationId': u'sample-app-2ed6c6804c8002a10-2',
                        u'taskId': u'2ed6c6804c8002a10',
                        u'sliceIndex': 2,
                        u'taskToRunId': u'33',
                        u'taskToRunShard': 2,
                        u'debugInfo': {
                            u'created': u'2112-01-01T01:01:01Z',
                            u'pySwarmingVersion': u'v1a',
                            u'taskName': u'some-name',
                        },
                    },
                    u'rbeInstance':
                    u'some-instance',
                    u'executionTimeout':
                    u'1030s',
                    u'expiry':
                    u'2112-01-01T01:07:10Z',
                    u'requestedBotId':
                    u'pool--dut1',
                    u'constraints': [
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val1']
                        },
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val2', u'val3']
                        },
                        {
                            u'key': u'dim2',
                            u'allowedValues': [u'val4']
                        },
                        {
                            u'key': u'name',
                            u'allowedValues': [u'2']
                        },
                        {
                            u'key': u'pool',
                            u'allowedValues': [u'pool']
                        },
                    ],
                    u'priority':
                    123,
                    u'schedulingAlgorithm':
                    u'SCHEDULING_ALGORITHM_LIFO',
                },
                u'class': u'rbe-enqueue',
            },
            'transactional': True,
            'use_dedicated_module': False,
        })

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('components.utils.enqueue_task')
  @mock.patch('components.utils.utcnow')
  @mock.patch('random.getrandbits')
  def test_enqueue_rbe_task_with_effective_bot_id_from_bot(
      self, getrandbits, utcnow, enqueue_task, pool_config):
    getrandbits.return_value = 42
    utcnow.return_value = datetime.datetime(2112, 1, 1, 1, 1, 1)
    enqueue_task.return_value = True

    pools = {}
    pool_config.side_effect = lambda pool: pools[pool]
    pools['pool'] = pool_config_func('instance',
                                     allocs={'RBE': 100},
                                     effective_bot_id_dimension='dut_id')

    # create the bot_info
    bot_management.bot_event(event_type='request_sleep',
                             bot_id='bot-id',
                             dimensions={
                                 u'dut_id': ['dut1'],
                             },
                             state={'state': 'real'},
                             version='1234',
                             register_dimensions=True,
                             rbe_effective_bot_id="pool--dut1",
                             set_rbe_effective_bot_id=True)

    req, ttr = self.make_request()
    datastore_utils.transaction(lambda: rbe.enqueue_rbe_task(req, ttr))

    args, kwargs = enqueue_task.call_args
    kwargs['payload'] = json.loads(kwargs['payload'])

    self.assertEqual(
        args,
        ('/internal/tasks/t/rbe-enqueue/2ed6c6804c8002a10-2', 'rbe-enqueue'))
    self.assertEqual(
        kwargs, {
            'payload': {
                u'body': {
                    u'payload': {
                        u'reservationId': u'sample-app-2ed6c6804c8002a10-2',
                        u'taskId': u'2ed6c6804c8002a10',
                        u'sliceIndex': 2,
                        u'taskToRunId': u'33',
                        u'taskToRunShard': 11,
                        u'debugInfo': {
                            u'created': u'2112-01-01T01:01:01Z',
                            u'pySwarmingVersion': u'v1a',
                            u'taskName': u'some-name',
                        },
                    },
                    u'rbeInstance':
                    u'some-instance',
                    u'executionTimeout':
                    u'1030s',
                    u'expiry':
                    u'2112-01-01T01:07:10Z',
                    u'requestedBotId':
                    u'pool--dut1',
                    u'constraints': [
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val1']
                        },
                        {
                            u'key': u'dim1',
                            u'allowedValues': [u'val2', u'val3']
                        },
                        {
                            u'key': u'dim2',
                            u'allowedValues': [u'val4']
                        },
                        {
                            u'key': u'name',
                            u'allowedValues': [u'2']
                        },
                        {
                            u'key': u'pool',
                            u'allowedValues': [u'pool']
                        },
                    ],
                    u'priority':
                    123,
                    u'schedulingAlgorithm':
                    u'SCHEDULING_ALGORITHM_LIFO',
                },
                u'class': u'rbe-enqueue',
            },
            'transactional': True,
            'use_dedicated_module': False,
        })


  @mock.patch('components.utils.enqueue_task')
  @mock.patch('components.utils.utcnow')
  @mock.patch('random.getrandbits')
  def test_enqueue_rbe_cancel(self, getrandbits, utcnow, enqueue_task):
    getrandbits.return_value = 42
    utcnow.return_value = datetime.datetime(2112, 1, 1, 1, 1, 1)
    enqueue_task.return_value = True

    req, ttr = self.make_request()
    datastore_utils.transaction(lambda: rbe.enqueue_rbe_cancel(req, ttr))

    args, kwargs = enqueue_task.call_args
    kwargs['payload'] = json.loads(kwargs['payload'])

    self.assertEqual(
        args,
        ('/internal/tasks/t/rbe-cancel/2ed6c6804c8002a10-2', 'rbe-cancel'))
    self.assertEqual(
        kwargs, {
            'payload': {
                u'body': {
                    u'rbeInstance': u'some-instance',
                    u'reservationId': u'sample-app-2ed6c6804c8002a10-2',
                    u'debugInfo': {
                        u'created': u'2112-01-01T01:01:01Z',
                        u'pySwarmingVersion': u'v1a',
                        u'taskName': u'some-name'
                    },
                },
                u'class': u'rbe-cancel',
            },
            'transactional': True,
            'use_dedicated_module': False,
        })


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
