#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import datetime
import logging
import sys
import unittest

import test_env

test_env.setup_test_env()

from proto.internals import session_pb2
from server import bot_groups_config
from server import bot_session
from server import hmac_secret
from test_support import test_case


def _bot_group_config(**kwargs):
  kwargs = kwargs.copy()
  for f in bot_groups_config.BotGroupConfig._fields:
    if f not in kwargs:
      kwargs[f] = None
  return bot_groups_config.BotGroupConfig(**kwargs)


TEST_CONFIG = _bot_group_config(
    auth=[
        bot_groups_config.BotAuth(log_if_failed=True,
                                  require_luci_machine_token=True,
                                  require_service_account=[],
                                  require_gce_vm_token=None,
                                  ip_whitelist=None),
        bot_groups_config.BotAuth(
            log_if_failed=True,
            require_luci_machine_token=False,
            require_service_account=['sa1@example.com', 'sa2@example.com'],
            require_gce_vm_token=None,
            ip_whitelist=None),
        bot_groups_config.BotAuth(
            log_if_failed=True,
            require_luci_machine_token=False,
            require_service_account=[],
            require_gce_vm_token=bot_groups_config.BotAuthGCE('gce-proj'),
            ip_whitelist=None),
        bot_groups_config.BotAuth(log_if_failed=False,
                                  require_luci_machine_token=False,
                                  require_service_account=[],
                                  require_gce_vm_token=None,
                                  ip_whitelist='ip-allowlist'),
    ],
    dimensions={
        'ignored': [],
        'dim1': ['a', 'b'],
        'dim2': ['d', 'c'],
    },
    bot_config_script='script.py',
    bot_config_script_sha256='script-sha256',
    system_service_account='system@example.com',
    logs_cloud_project='logs-cloud-project',
)

TEST_TIME = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)

EXPECTED_BOT_CONFIG = session_pb2.BotConfig(
    expiry={
        'seconds': 1262405045,
        'nanos': 6000
    },
    debug_info={
        'created': {
            'seconds': 1262401445,
            'nanos': 6000
        },
        'swarming_version': 'py/v1a',
        'request_id': '7357B3D7091D',
    },
    bot_auth=[
        {
            'require_luci_machine_token': True,
            'log_if_failed': True
        },
        {
            'require_service_account': ['sa1@example.com', 'sa2@example.com'],
            'log_if_failed': True
        },
        {
            'require_gce_vm_token': {
                'project': 'gce-proj'
            },
            'log_if_failed': True
        },
        {
            'ip_whitelist': 'ip-allowlist'
        },
    ],
    system_service_account='system@example.com',
    logs_cloud_project='logs-cloud-project',
    rbe_instance='rbe',
)


class BotSessionTest(test_case.TestCase):

  def setUp(self):
    super(BotSessionTest, self).setUp()
    self.mock_now(TEST_TIME)
    self.mock_secret('some-secret')

  def mock_secret(self, val):

    class MockedSecret(object):

      def access(_self):
        return val

    self.mock(hmac_secret, 'get_shared_hmac_secret', MockedSecret)

  def test_round_trip(self):
    original = session_pb2.Session(bot_id='bot-id', session_id='session-id')
    tok = bot_session.marshal(original)
    unmarshalled = bot_session.unmarshal(tok)
    self.assertEqual(original, unmarshalled)

  def test_bad_hmac(self):
    original = session_pb2.Session(bot_id='bot-id', session_id='session-id')
    tok = bot_session.marshal(original)
    self.mock_secret('another-secret')
    with self.assertRaises(bot_session.BadSessionToken):
      bot_session.unmarshal(tok)

  def test_wrong_format(self):
    tok = session_pb2.SessionToken()
    tok.aead_encrypted.cipher_text = 'bzz'
    serialized = base64.b64encode(tok.SerializeToString())
    with self.assertRaises(bot_session.BadSessionToken):
      bot_session.unmarshal(serialized)

  def test_is_expired_session(self):
    good = session_pb2.Session()
    good.expiry.FromDatetime(TEST_TIME + datetime.timedelta(seconds=1))
    self.assertFalse(bot_session.is_expired_session(good))

    bad = session_pb2.Session()
    bad.expiry.FromDatetime(TEST_TIME - datetime.timedelta(seconds=1))
    self.assertTrue(bot_session.is_expired_session(bad))

  def test_handshake_config(self):
    data = bot_session._handshake_config_extract(TEST_CONFIG)
    self.assertEqual(data, [
        'config_script_name:script.py',
        'config_script_sha256:script-sha256',
        'dimension:dim1:a',
        'dimension:dim1:b',
        'dimension:dim2:c',
        'dimension:dim2:d',
    ])
    digest = bot_session._handshake_config_hash(TEST_CONFIG)
    self.assertEqual(len(digest), 32)

  def test_create(self):
    session = bot_session.create('bot-id', 'session-id', TEST_CONFIG, 'rbe')
    self.assertEqual(
        session,
        session_pb2.Session(
            bot_id='bot-id',
            session_id='session-id',
            expiry={
                'seconds': 1262405045,
                'nanos': 6000
            },
            debug_info={
                'created': {
                    'seconds': 1262401445,
                    'nanos': 6000
                },
                'swarming_version': 'py/v1a',
                'request_id': '7357B3D7091D',
            },
            bot_config=EXPECTED_BOT_CONFIG,
            handshake_config_hash=bot_session._handshake_config_hash(
                TEST_CONFIG),
        ))
    bot_session.debug_log(session)

  def test_update(self):
    session = session_pb2.Session()
    bot_session.update(session, TEST_CONFIG, 'rbe', 'pool--dut_123', 'dut_id')
    bot_config = EXPECTED_BOT_CONFIG
    bot_config.rbe_effective_bot_id = 'pool--dut_123'
    bot_config.rbe_effective_bot_id_dimension = 'dut_id'

    self.assertEqual(
        session,
        session_pb2.Session(
            expiry={
                'seconds': 1262405045,
                'nanos': 6000
            },
            debug_info={
                'created': {
                    'seconds': 1262401445,
                    'nanos': 6000
                },
                'swarming_version': 'py/v1a',
                'request_id': '7357B3D7091D',
            },
            bot_config=bot_config,
        ))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
