#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import datetime
import hashlib
import hmac
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

from server import bot_groups_config
from server import pools_config
from server import rbe

from proto.config import bots_pb2
from proto.config import pools_pb2
from proto.internals import rbe_pb2


class RBETest(test_case.TestCase):
  @staticmethod
  def bot_groups_config(rbe_mode_percent=0,
                        enable_rbe_on=None,
                        disable_rbe_on=None):
    kwargs = {f: None for f in bot_groups_config.BotGroupConfig._fields}
    kwargs['rbe_migration'] = bots_pb2.BotGroup.RBEMigration(
        rbe_mode_percent=rbe_mode_percent or 0,
        enable_rbe_on=enable_rbe_on or [],
        disable_rbe_on=disable_rbe_on or [],
    )
    return bot_groups_config.BotGroupConfig(**kwargs)

  @staticmethod
  def pool_config(rbe_instance):
    rbe_mgration = None
    if rbe_instance is not None:
      rbe_mgration = pools_pb2.Pool.RBEMigration(rbe_instance=rbe_instance)
    return pools_config.init_pool_config(rbe_migration=rbe_mgration)

  @mock.patch('server.pools_config.get_pool_config')
  @mock.patch('server.rbe._quasi_random_100')
  def test_get_rbe_instance(self, quasi_random_100, pool_config):
    pools = {
        'pool-1-a': self.pool_config('instance-1'),
        'pool-1-b': self.pool_config('instance-1'),
        'pool-2': self.pool_config('instance-2'),
        'pool-no-rbe-1': self.pool_config(None),
        'pool-no-rbe-2': self.pool_config(''),
    }
    pool_config.side_effect = lambda pool: pools[pool]

    quasi_random_100.return_value = 30.0

    # Randomizer.
    instance = rbe.get_rbe_instance('bot-id', ['pool-1-a'],
                                    self.bot_groups_config(rbe_mode_percent=25))
    self.assertIsNone(instance)
    instance = rbe.get_rbe_instance('bot-id', ['pool-1-a'],
                                    self.bot_groups_config(rbe_mode_percent=30))
    self.assertEqual(instance, 'instance-1')

    # Explicitly enabled.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a'],
        self.bot_groups_config(rbe_mode_percent=25, enable_rbe_on=['bot-id']))
    self.assertEqual(instance, 'instance-1')

    # Explicitly disabled.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a'],
        self.bot_groups_config(rbe_mode_percent=30, disable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

    # The pool is not using RBE.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-no-rbe-1', 'pool-no-rbe-1'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

    # Pools agree on RBE instance.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a', 'pool-1-b'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertEqual(instance, 'instance-1')

    # Pools disagree on RBE instance.
    instance = rbe.get_rbe_instance(
        'bot-id', ['pool-1-a', 'pool-2'],
        self.bot_groups_config(enable_rbe_on=['bot-id']))
    self.assertIsNone(instance)

  def test_quasi_random_100(self):
    for i in range(1000):
      val = rbe._quasi_random_100(u'bot-%d' % i)
      self.assertGreaterEqual(val, 0.0)
      self.assertLessEqual(val, 100.0)


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

    self.mock(rbe, '_get_poll_token_hmac_secret', MockedSecret)

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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
