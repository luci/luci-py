#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import acl
from depot_tools import auto_stub


class IpUtilsTest(unittest.TestCase):
  def test_parse_ip(self):
    data = [
      # (input, expected)
      ('allo', (None, None)),
      ('0.0.0.0', ('v4', 0L)),
      ('255.255.255.255', ('v4', 4294967295L)),
      ('255.256.255.255', (None, None)),
      ('0:0:0:0:0:0:0:0', ('v6', 0L)),
      (
        'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
        ('v6', 340282366920938463463374607431768211455L)),
    ]
    actual = [(v, acl.parse_ip(v)) for v, _ in data]
    self.assertEqual(data, actual)

  def test_ip_to_str(self):
    data = [
      # (input, expected)
      (('v4', 0L), 'v4-0'),
      (('v4', 4294967295L), 'v4-4294967295'),
      (('v6', 0L), 'v6-0'),
      (
        ('v6', 340282366920938463463374607431768211455L),
        'v6-340282366920938463463374607431768211455'),
    ]
    actual = [(v, acl.ip_to_str(*v)) for v, _ in data]
    self.assertEqual(data, actual)


class TestGenerateHmacSignature(unittest.TestCase):
  def test_generate_hmac_signature_length(self):
    self.assertEqual(
        acl.HMAC_HASH_BYTES,
        len(acl.generate_hmac_signature('secret', ['a'])))

  def test_generate_hmac_signature_uses_secret(self):
    # generate_hmac_signature as a function should depend on its inputs.
    hmac1 = acl.generate_hmac_signature('secret1', ['a', 'b'])
    hmac2 = acl.generate_hmac_signature('secret2', ['a', 'b'])
    self.assertNotEqual(hmac1, hmac2)

  def test_generate_hmac_signature_uses_data(self):
    # generate_hmac_signature as a function should depend on its inputs.
    # Also boundaries between data strings matter.
    hmacs = (
        acl.generate_hmac_signature('secret', ['a']),
        acl.generate_hmac_signature('secret', ['a', 'b']),
        acl.generate_hmac_signature('secret', ['ab']),
    )
    self.assertTrue(len(set(hmacs)) == len(hmacs))

  def test_generate_hmac_signature_rejects_bad_inputs(self):
    # Empty secret is not allowed.
    with self.assertRaises(AssertionError):
      acl.generate_hmac_signature('', ['a'])
    # Empty data block is not allowed.
    with self.assertRaises(AssertionError):
      acl.generate_hmac_signature('s', [])
    # Unicode is not allowed.
    with self.assertRaises(AssertionError):
      acl.generate_hmac_signature('s', [u'a'])
    # Zero byte is not allowed.
    with self.assertRaises(AssertionError):
      acl.generate_hmac_signature('s', ['a\0b'])


class TokenTests(auto_stub.TestCase):
  # Default time.time() value in tests.
  now = 1384294279.5

  def setUp(self):
    super(TokenTests, self).setUp()
    self.mock(acl.time, 'time', lambda: float(self.now))

  def test_token_encodes_token_data(self):
    access_id = 'some_access_id'
    secret = 'random_secret'
    cases = (
        None,
        {},
        {'a': ''},
        {'a': 'b'},
        {'a': 'looong b with spaces'},
        {'looong key with spaces': 'b'},
        {'a': 'b', 'c': 'd', 'e': 'f'},
    )
    # Ensure token_data gets through token encode/decode intact.
    for token_data in cases:
      token = acl.generate_token(access_id, secret, 3600, token_data)
      self.assertEqual(
          token_data or {}, # None token_data is decoded as an empty dict.
          acl.validate_token(token, access_id, secret))

  def test_rejects_token_data_with_underscore(self):
    with self.assertRaises(AssertionError):
      acl.generate_token('access_id', 'secret', 3600, {'_a': 'b'})

  def test_rejects_badly_formated_tokens(self):
    token_with_odd_data_block_len = base64.urlsafe_b64encode(
        '\0'.join(['a', 'b', 'c']) + 'x' * acl.HMAC_HASH_BYTES)
    cases = (
        '',
        'im not a token',
        '~~~~',
        token_with_odd_data_block_len,
    )
    for token in cases:
      with self.assertRaises(acl.InvalidTokenError) as err:
        acl.validate_token(token, 'access_id', 'secret')
      self.assertTrue(err.exception.message.startswith('Bad token format'))

  def test_rejects_modified_token(self):
    access_id = 'some_access_id'
    secret = 'random_secret'
    token = acl.generate_token(access_id, secret, 3600)
    cases = (
        token[:10],
        token[10:],
        'A' + token,
        token + 'A',
        token[:4] + 'A' + token[4:],
        token[:4] + 'A' + token[5:],
    )
    for token in cases:
      with self.assertRaises(acl.InvalidTokenError):
        acl.validate_token(token, access_id, secret)

  def test_secret_key_matters(self):
    access_id = 'some_access_id'
    token = acl.generate_token(access_id, 'secret1', 3600)
    self.assertEqual({}, acl.validate_token(token, access_id, 'secret1'))
    with self.assertRaises(acl.InvalidTokenError) as err:
      acl.validate_token(token, access_id, 'secret2')
    self.assertEqual(err.exception.message, 'Token signature is invalid.')

  def test_access_id_matters(self):
    secret = 'some_secret'
    token = acl.generate_token('id 1', secret, 3600)
    self.assertEqual({}, acl.validate_token(token, 'id 1', secret))
    with self.assertRaises(acl.InvalidTokenError) as err:
      acl.validate_token(token, 'id 2', secret)
    self.assertEqual(err.exception.message, 'Token signature is invalid.')

  def test_expiration_matters(self):
    access_id = 'some_access_id'
    secret = 'some_secret'
    start_time = 1384294279.5
    # Generate token that expires in 10 min.
    self.now = start_time
    token = acl.generate_token(access_id, secret, 10 * 60)
    # Usable now.
    self.assertEqual({}, acl.validate_token(token, access_id, secret))
    # Usable in 9 min 59 sec.
    self.now = start_time + 9 * 60 + 59
    self.assertEqual({}, acl.validate_token(token, access_id, secret))
    # Expires in 10 min. 1 sec.
    self.now = start_time + 10 * 60 + 1
    with self.assertRaises(acl.InvalidTokenError) as err:
      acl.validate_token(token, access_id, secret)
    self.assertEqual(err.exception.message, 'Token expired 1 sec ago.')


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
