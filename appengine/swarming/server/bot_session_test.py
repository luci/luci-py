#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import logging
import sys
import unittest

import test_env

test_env.setup_test_env()

from proto.internals import session_pb2
from server import bot_session
from server import hmac_secret
from test_support import test_case


class BotSessionTest(test_case.TestCase):

  def setUp(self):
    super(BotSessionTest, self).setUp()
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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
