#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import datetime
import hashlib
import hmac
import json
import logging
import sys
import unittest
import uuid

import test_env
test_env.setup_test_env()

import mock

from google.protobuf import timestamp_pb2

from test_support import test_case

from components import auth
from components import utils
from components import datastore_utils

from server import bot_groups_config
from server import pools_config
from server import rbe
from server import task_request
from server import task_to_run

from proto.config import bots_pb2
from proto.config import pools_pb2
from proto.internals import rbe_pb2


class RBETest(test_case.TestCase):
  @staticmethod
  def pool_config(rbe_instance, rbe_mode_percent=100, allocs=None):
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
      )
    return pools_config.init_pool_config(rbe_migration=rbe_mgration)

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('server.rbe._quasi_random_100')
  def test_get_rbe_config_for_bot(self, quasi_random_100, pool_config):
    pools = {}
    pool_config.side_effect = lambda pool: pools[pool]

    # Pure Swarming pool.
    pools['swarming'] = self.pool_config(None)
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['swarming'])
    self.assertIsNone(cfg)

    # A pool with mixed composition of bots.
    pools['many-modes'] = self.pool_config('instance',
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
    pools['old-config'] = self.pool_config('instance')
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['old-config'])
    self.assertIsNone(cfg)

    # Multi-pool bot.
    pools['pool-a'] = self.pool_config('instance',
                                       allocs={
                                           'SWARMING': 50,
                                           'RBE': 50,
                                       })
    pools['pool-b'] = self.pool_config('instance',
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
    pools['pool-c'] = self.pool_config('instance-c', allocs={
        'RBE': 100,
    })
    pools['pool-d'] = self.pool_config('instance-d', allocs={
        'RBE': 100,
    })
    # All pools agree on a mode.
    quasi_random_100.return_value = 90
    cfg = rbe.get_rbe_config_for_bot('bot-id', ['pool-c', 'pool-d'])
    self.assertEqual(cfg.instance, 'instance-c')
    self.assertFalse(cfg.hybrid_mode)

  @mock.patch('random.uniform')
  def test_get_rbe_instance_for_task(self, uniform):
    call = rbe.get_rbe_instance_for_task

    self.assertIsNone(call([], self.pool_config(None)))
    self.assertIsNone(call([], self.pool_config('')))

    self.assertEqual(call(['rbe:require'], self.pool_config('inst', 0)), 'inst')
    self.assertIsNone(call(['rbe:prevent'], self.pool_config('inst', 100)))
    self.assertIsNone(
        call(['rbe:require', 'rbe:prevent'], self.pool_config('inst', 100)))

    uniform.return_value = 5.0
    self.assertEqual(call([], self.pool_config('inst', 6)), 'inst')
    self.assertIsNone(call([], self.pool_config('inst', 4)))

  def test_quasi_random_100(self):
    for i in range(1000):
      val = rbe._quasi_random_100(u'bot-%d' % i)
      self.assertGreaterEqual(val, 0)
      self.assertLess(val, 100)


class PollTokenTest(test_case.TestCase):
  MOCKED_UUID = 'totally-uuid'
  NOW = datetime.datetime(2022, 1, 2, 3, 4, 5)
  SECRET = 'hmac-key'

  def setUp(self):
    super(PollTokenTest, self).setUp()
    self.mock(uuid, 'uuid4', lambda: self.MOCKED_UUID)
    self.mock(auth, 'get_peer_identity', mock.Mock())
    self.mock(auth, 'get_auth_details', mock.Mock())
    self.mock(utils, 'utcnow', lambda: self.NOW)

    class MockedSecret(object):
      def access(_self):
        return self.SECRET

    self.mock(rbe, '_get_shared_hmac_secret', MockedSecret)

  def mock_auth_identity(self, kind, name):
    auth.get_peer_identity.return_value = auth.Identity(kind, name)

  def mock_gce_vm_details(self, gce_project, gce_instance):
    auth.get_auth_details.return_value = auth.AuthDetails(
        is_superuser=False, gce_project=gce_project, gce_instance=gce_instance)

  @staticmethod
  def mock_bot_auth(require_luci_machine_token=False,
                    require_service_account=None,
                    require_gce_vm_token=None,
                    ip_whitelist=None):
    return bot_groups_config.BotAuth(
        log_if_failed=False,
        require_luci_machine_token=require_luci_machine_token,
        require_service_account=require_service_account or [],
        require_gce_vm_token=require_gce_vm_token,
        ip_whitelist=ip_whitelist,
    )

  @staticmethod
  def timestamp_pb(secs):
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(PollTokenTest.NOW + datetime.timedelta(seconds=secs))
    return ts

  def decode_token(self, tok):
    envelope = rbe_pb2.TaggedMessage()
    envelope.MergeFromString(base64.b64decode(tok))
    self.assertEqual(envelope.payload_type, rbe_pb2.TaggedMessage.POLL_STATE)

    digest = hmac.new(
        self.SECRET,
        '%d\n%s' % (rbe_pb2.TaggedMessage.POLL_STATE, envelope.payload),
        hashlib.sha256).digest()
    self.assertEqual(digest, envelope.hmac_sha256)

    state = rbe_pb2.PollState()
    state.MergeFromString(envelope.payload)
    return state

  def test_with_luci_machine_token(self):
    self.mock_auth_identity(auth.IDENTITY_BOT, 'some-fqdn')
    tok = rbe.generate_poll_token(
        bot_id='some-fqdn--id',
        rbe_instance='some-instance',
        enforced_dimensions={'pool': ['blah']},
        bot_auth_cfg=self.mock_bot_auth(
            require_luci_machine_token=True,
            ip_whitelist='ip-list',
        ),
    )
    decoded = self.decode_token(tok)
    self.assertEqual(
        decoded,
        rbe_pb2.PollState(
            id=self.MOCKED_UUID,
            expiry=self.timestamp_pb(rbe.POLL_TOKEN_EXPIRY.total_seconds()),
            rbe_instance='some-instance',
            enforced_dimensions=[
                rbe_pb2.PollState.Dimension(
                    key='id',
                    values=['some-fqdn--id'],
                ),
                rbe_pb2.PollState.Dimension(
                    key='pool',
                    values=['blah'],
                ),
            ],
            debug_info=rbe_pb2.PollState.DebugInfo(
                created=self.timestamp_pb(0),
                swarming_version='v1a',
                request_id='7357B3D7091D',
            ),
            ip_allowlist='ip-list',
            luci_machine_token_auth=rbe_pb2.PollState.LUCIMachineTokenAuth(
                machine_fqdn='some-fqdn', ),
        ))

  def test_with_service_account(self):
    self.mock_auth_identity(auth.IDENTITY_USER, 'sa@example.com')
    tok = rbe.generate_poll_token(
        bot_id='some-id',
        rbe_instance='some-instance',
        enforced_dimensions={'pool': ['blah']},
        bot_auth_cfg=self.mock_bot_auth(
            require_service_account=['ignore@example.com', 'sa@example.com'],
            ip_whitelist='ip-list',
        ),
    )
    decoded = self.decode_token(tok)
    self.assertEqual(
        decoded,
        rbe_pb2.PollState(
            id=self.MOCKED_UUID,
            expiry=self.timestamp_pb(rbe.POLL_TOKEN_EXPIRY.total_seconds()),
            rbe_instance='some-instance',
            enforced_dimensions=[
                rbe_pb2.PollState.Dimension(
                    key='id',
                    values=['some-id'],
                ),
                rbe_pb2.PollState.Dimension(
                    key='pool',
                    values=['blah'],
                ),
            ],
            debug_info=rbe_pb2.PollState.DebugInfo(
                created=self.timestamp_pb(0),
                swarming_version='v1a',
                request_id='7357B3D7091D',
            ),
            ip_allowlist='ip-list',
            service_account_auth=rbe_pb2.PollState.ServiceAccountAuth(
                service_account='sa@example.com', ),
        ))

  def test_with_gce_vm_token(self):
    self.mock_auth_identity(auth.IDENTITY_BOT, 'not-really-used')
    self.mock_gce_vm_details('gce-proj', 'gce-inst')
    tok = rbe.generate_poll_token(
        bot_id='some-id',
        rbe_instance='some-instance',
        enforced_dimensions={'pool': ['blah']},
        bot_auth_cfg=self.mock_bot_auth(
            require_gce_vm_token=bot_groups_config.BotAuthGCE('gce-proj'),
            ip_whitelist='ip-list',
        ),
    )
    decoded = self.decode_token(tok)
    self.assertEqual(
        decoded,
        rbe_pb2.PollState(
            id=self.MOCKED_UUID,
            expiry=self.timestamp_pb(rbe.POLL_TOKEN_EXPIRY.total_seconds()),
            rbe_instance='some-instance',
            enforced_dimensions=[
                rbe_pb2.PollState.Dimension(
                    key='id',
                    values=['some-id'],
                ),
                rbe_pb2.PollState.Dimension(
                    key='pool',
                    values=['blah'],
                ),
            ],
            debug_info=rbe_pb2.PollState.DebugInfo(
                created=self.timestamp_pb(0),
                swarming_version='v1a',
                request_id='7357B3D7091D',
            ),
            ip_allowlist='ip-list',
            gce_auth=rbe_pb2.PollState.GCEAuth(
                gce_project='gce-proj',
                gce_instance='gce-inst',
            ),
        ))

  def test_with_ip_allowlist_only(self):
    self.mock_auth_identity(auth.IDENTITY_BOT, 'not-really-used')
    tok = rbe.generate_poll_token(
        bot_id='some-id',
        rbe_instance='some-instance',
        enforced_dimensions={'pool': ['blah']},
        bot_auth_cfg=self.mock_bot_auth(ip_whitelist='ip-list', ),
    )
    decoded = self.decode_token(tok)
    self.assertEqual(
        decoded,
        rbe_pb2.PollState(
            id=self.MOCKED_UUID,
            expiry=self.timestamp_pb(rbe.POLL_TOKEN_EXPIRY.total_seconds()),
            rbe_instance='some-instance',
            enforced_dimensions=[
                rbe_pb2.PollState.Dimension(
                    key='id',
                    values=['some-id'],
                ),
                rbe_pb2.PollState.Dimension(
                    key='pool',
                    values=['blah'],
                ),
            ],
            debug_info=rbe_pb2.PollState.DebugInfo(
                created=self.timestamp_pb(0),
                swarming_version='v1a',
                request_id='7357B3D7091D',
            ),
            ip_allowlist='ip-list',
            ip_allowlist_auth=rbe_pb2.PollState.IPAllowlistAuth(),
        ))


class EnqueueTest(test_case.TestCase):
  maxDiff = None

  def make_request(self):
    def make_slice(name):
      return task_request.TaskSlice(
          properties=task_request.TaskProperties(
              dimensions_data={
                  u'id': [u'bot-id'],
                  u'dim1': [u'val1', u'val2|val3'],
                  u'dim2': [u'val4'],
                  u'name': [name],
              },
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

  @mock.patch('components.utils.enqueue_task')
  @mock.patch('components.utils.utcnow')
  @mock.patch('random.getrandbits')
  def test_enqueue_rbe_task(self, getrandbits, utcnow, enqueue_task):
    getrandbits.return_value = 42
    utcnow.return_value = datetime.datetime(2112, 1, 1, 1, 1, 1)
    enqueue_task.return_value = True

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
                        u'taskToRunShard': 15,
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
