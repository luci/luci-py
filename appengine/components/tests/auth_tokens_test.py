#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import string
import sys
import unittest

import test_env
test_env.setup_test_env()

from components.auth import api
from components.auth import tokens
from test_support import test_case


URL_SAFE_ALPHABET = set(string.letters + string.digits + '-_')


class StringConvertersTest(test_case.TestCase):
  """Tests for to_encoding and normalize_* functions."""

  def test_to_encoding_str(self):
    result = tokens.to_encoding('abc', 'ascii')
    self.assertTrue(isinstance(result, str))
    self.assertEqual('abc', result)

  def test_to_encoding_unicode_ascii(self):
    result = tokens.to_encoding(u'abc', 'ascii')
    self.assertTrue(isinstance(result, str))
    self.assertEqual('abc', result)

  def test_to_encoding_unicode_not_ascii(self):
    # 'Hello' in Russian cyrillic: 'privet'.
    test = u'\u043f\u0440\u0438\u0432\u0435\u0442'
    # It's not convertible to ASCII.
    with self.assertRaises(UnicodeEncodeError):
      tokens.to_encoding(test, 'ascii')
    # Fine in UTF-8.
    result = tokens.to_encoding(test, 'utf-8')
    self.assertTrue(isinstance(result, str))
    self.assertEqual('\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82', result)

  def test_to_encoding_not_a_string(self):
    with self.assertRaises(TypeError):
      tokens.to_encoding(None, 'ascii')
    with self.assertRaises(TypeError):
      tokens.to_encoding(123, 'ascii')

  def test_normalize_message_one(self):
    self.assertEqual(['abc'], tokens.normalize_message('abc'))

  def test_normalize_message_list(self):
    self.assertEqual(['abc', 'def'], tokens.normalize_message(['abc', 'def']))
    self.assertEqual(['abc', 'def'], tokens.normalize_message(('abc', 'def')))

  def test_normalize_message_unicode(self):
    self.assertEqual(
        ['\xd0\xbf', 'abc'], tokens.normalize_message([u'\u043f', 'abc']))

  def test_normalize_embedded_reserved_keys(self):
    with self.assertRaises(ValueError):
      tokens.normalize_embedded({'_i': ''})

  def test_normalized_embedded_ascii(self):
    result = tokens.normalize_embedded({u'a': u'b'})
    self.assertEqual({'a': 'b'}, result)
    self.assertTrue(isinstance(result.keys()[0], str))
    self.assertTrue(isinstance(result['a'], str))

  def test_normalized_embedded_non_ascii(self):
    with self.assertRaises(UnicodeEncodeError):
      tokens.normalize_embedded({u'\u043f': 'b'})
    with self.assertRaises(UnicodeEncodeError):
      tokens.normalize_embedded({'a': u'\u043f'})

  def test_normalized_embedded_not_a_string(self):
    with self.assertRaises(TypeError):
      tokens.normalize_embedded({123: 'b'})
    with self.assertRaises(TypeError):
      tokens.normalize_embedded({'a': None})
    with self.assertRaises(TypeError):
      tokens.normalize_embedded({'a': 123})


class Base64Test(test_case.TestCase):
  """Tests for base64_encode and base64_decode functions."""

  def test_base64_encode_types(self):
    with self.assertRaises(TypeError):
      tokens.base64_encode(None)
    with self.assertRaises(TypeError):
      tokens.base64_encode(u'unicode')

  def test_base64_decode_types(self):
    with self.assertRaises(TypeError):
      tokens.base64_decode(None)
    with self.assertRaises(TypeError):
      tokens.base64_decode(u'unicode')

  def test_base64_encode_is_url_safe(self):
    for a in xrange(255):
      original = chr(a)
      encoded = tokens.base64_encode(original)
      self.assertEqual(original, tokens.base64_decode(encoded))
      self.assertTrue(URL_SAFE_ALPHABET.issuperset(encoded), encoded)

  def test_base64_encode_decode(self):
    # Encode a bunch of strings of different lengths to test all
    # possible paddings (to see how padding stripping works).
    msg = 'somewhat long message with binary \x00\x01\x02\x03 inside'
    for i in xrange(len(msg)):
      self.assertEqual(
          msg[:i], tokens.base64_decode(tokens.base64_encode(msg[:i])))


class ComputeMacTest(test_case.TestCase):
  """Tests for 'compute_mac' function."""

  algo = 'hmac-sha256'

  def test_compute_mac(self):
    # Different data -> different MACs.
    # Also boundaries between data strings matter.
    macs = (
      tokens.compute_mac(self.algo, 'secret', []),
      tokens.compute_mac(self.algo, 'secret', ['']),
      tokens.compute_mac(self.algo, 'secret', ['', '']),
      tokens.compute_mac(self.algo, 'secret', ['\x00']),
      tokens.compute_mac(self.algo, 'secret', ['a', 'b']),
      tokens.compute_mac(self.algo, 'secret', ['ab']),
      tokens.compute_mac(self.algo, 'secret', ['0' * 10]),
      tokens.compute_mac(self.algo, 'secret', ['0'] + 10 * ['']),
    )
    self.assertTrue(len(set(macs)) == len(macs))

  def test_compute_mac_length(self):
    self.assertEqual(
        tokens.MAC_ALGOS[self.algo][1],
        len(tokens.compute_mac(self.algo, 'secret', ['a'])))

  def test_compute_mac_uses_secret(self):
    # Different secrets -> different MACs.
    mac1 = tokens.compute_mac(self.algo, 'secret1', ['a', 'b'])
    mac2 = tokens.compute_mac(self.algo, 'secret2', ['a', 'b'])
    self.assertNotEqual(mac1, mac2)


class TokenEncodeDecodeTest(test_case.TestCase):
  """Test for encode_token and decode_token functions."""

  algo = 'hmac-sha256'
  mac_len = tokens.MAC_ALGOS[algo][1]

  def test_simple(self):
    # Test case: (version, message, embedded).
    cases = (
      (1, [], {}),
      (255, [], {}),
      (1, ['Hello'], {}),
      (1, [], {'a': 'b'}),
      (1, ['', 'some', 'more'], {'a': 'b', '_i': 'd'}),
    )
    for version, message, embedded in cases:
      tok = tokens.encode_token(self.algo, version, 'secret', message, embedded)
      self.assertTrue(URL_SAFE_ALPHABET.issuperset(tok))
      decoded_version, decoded_embedded = tokens.decode_token(
          self.algo, tok, ['secret'], message)
      self.assertEqual(version, decoded_version)
      self.assertEqual(embedded, decoded_embedded)

  def test_many_secrets(self):
    tok = tokens.encode_token(self.algo, 1, 'old', ['msg'], {'a': 'b'})
    self.assertEqual(
        (1, {'a': 'b'}),
        tokens.decode_token(self.algo, tok, ['new', 'old'], ['msg']))

  def test_bad_secret(self):
    tok = tokens.encode_token(self.algo, 1, 'ancient', ['msg'], {'a': 'b'})
    with self.assertRaises(tokens.InvalidTokenError):
      tokens.decode_token(self.algo, tok, ['new', 'old'], ['msg'])

  def test_encode_token_uses_secret(self):
    tok1 = tokens.encode_token(self.algo, 1, 'secret1', [], {})
    tok2 = tokens.encode_token(self.algo, 1, 'secret2', [], {})
    # Grab last several bytes of base64 encoded token: it's a tail of MAC tag.
    self.assertNotEqual(tok1[-self.mac_len], tok2[-self.mac_len])

  def test_encode_token_tags_version(self):
    tok1 = tokens.encode_token(self.algo, 1, 'secret', [], {})
    tok2 = tokens.encode_token(self.algo, 2, 'secret', [], {})
    # Grab last several bytes of base64 encoded token: it's a tail of MAC tag.
    self.assertNotEqual(tok1[-self.mac_len:], tok2[-self.mac_len:])

  def test_encode_token_tags_embedded_data(self):
    tok1 = tokens.encode_token(self.algo, 1, 'secret', [], {'a': '1'})
    tok2 = tokens.encode_token(self.algo, 1, 'secret', [], {'a': '2'})
    # Grab last several bytes of base64 encoded token: it's a tail of MAC tag.
    self.assertNotEqual(tok1[-self.mac_len:], tok2[-self.mac_len:])

  def test_encode_token_tags_message(self):
    tok1 = tokens.encode_token(self.algo, 1, 'secret', ['1'], {})
    tok2 = tokens.encode_token(self.algo, 1, 'secret', ['2'], {})
    # Grab last several bytes of base64 encoded token: it's a tail of MAC tag.
    self.assertNotEqual(tok1[-self.mac_len:], tok2[-self.mac_len:])

  def test_rejects_modified(self):
    tok = tokens.encode_token(self.algo, 1, 'secret', ['msg'], {'a': 'b'})
    decode = lambda x: tokens.decode_token(self.algo, x, ['secret'], ['msg'])
    # Works if not modified.
    decode(tok)
    # Try simple modifications.
    for i in xrange(len(tok)):
      # Truncation.
      with self.assertRaises(tokens.InvalidTokenError):
        decode(tok[:i])
      # Insertion.
      with self.assertRaises(tokens.InvalidTokenError):
        decode(tok[:i] + 'A' + tok[i:])
      # Substitution.
      with self.assertRaises(tokens.InvalidTokenError):
        decode(tok[:i] + chr((ord(tok[i]) + 1) % 255) + tok[i+1:])
    # Expansion.
    with self.assertRaises(tokens.InvalidTokenError):
      decode('A' + tok)
    with self.assertRaises(tokens.InvalidTokenError):
      decode(tok + 'A')


class SimpleToken(tokens.TokenKind):
  secret_key = api.SecretKey('secret', 'local')
  expiration_sec = 3600


class GoodToken(tokens.TokenKind):
  algo = 'hmac-sha256'
  expiration_sec = 3600
  secret_key = api.SecretKey('local', 'local')
  version = 1


class TestToken(test_case.TestCase):
  """Tests for Token class."""

  def setUp(self):
    super(TestToken, self).setUp()
    self.mock_get_secret()

  def mock_get_secret(self):
    """Capture calls to api.get_secret."""
    calls = []
    def mocked_get_secret(key):
      calls.append(key)
      return ['1', '2', '3']
    self.mock(tokens.api, 'get_secret', mocked_get_secret)
    return calls

  def test_works(self):
    tok = SimpleToken.generate('message', {'embedded': 'some'})
    self.assertEqual({'embedded': 'some'}, SimpleToken.validate(tok, 'message'))

  def test_depends_on_message(self):
    tok = SimpleToken.generate('message 1')
    with self.assertRaises(tokens.InvalidTokenError):
      SimpleToken.validate(tok, 'message 2')

  def test_uses_secret_key(self):
    # 'generate' uses key.
    calls = self.mock_get_secret()
    tok = SimpleToken.generate()
    self.assertEqual([SimpleToken.secret_key], calls)

    # 'validate' as well.
    calls = self.mock_get_secret()
    SimpleToken.validate(tok)
    self.assertEqual([SimpleToken.secret_key], calls)

  def test_checks_version(self):
    class TokenV1(tokens.TokenKind):
      secret_key = api.SecretKey('secret', 'local')
      expiration_sec = 3600
      version = 1

    class TokenV2(tokens.TokenKind):
      secret_key = api.SecretKey('secret', 'local')
      expiration_sec = 3600
      version = 2

    tok = TokenV1.generate()
    with self.assertRaises(tokens.InvalidTokenError):
      TokenV2.validate(tok)

  def test_checks_issued_time(self):
    origin = datetime.datetime(2014, 1, 1, 1, 1, 1)

    # Make token issued at TS 'origin'.
    self.mock_now(origin, 0)
    tok = SimpleToken.generate()

    # If clocks moves forward (as it should), token is valid.
    self.mock_now(origin, 1800)
    SimpleToken.validate(tok)

    # If clocks moves slightly backward, it's still OK. Happens if token is
    # generated on one machine, but validated on another one with slightly late
    # clock.
    self.mock_now(origin, -tokens.ALLOWED_CLOCK_DRIFT_SEC+5)
    SimpleToken.validate(tok)

    # If token is from far future, then something is fishy...
    self.mock_now(origin, -3600)
    with self.assertRaises(tokens.InvalidTokenError):
      SimpleToken.validate(tok)

  def test_checks_expiration_time(self):
    origin = datetime.datetime(2014, 1, 1, 1, 1, 1)

    # Make token issued at TS 'origin'.
    self.mock_now(origin, 0)
    tok = SimpleToken.generate()

    # Valid before expiration.
    self.mock_now(origin, SimpleToken.expiration_sec - 10)
    SimpleToken.validate(tok)

    # Invalid after expiration.
    self.mock_now(origin, SimpleToken.expiration_sec + 10)
    with self.assertRaises(tokens.InvalidTokenError):
      SimpleToken.validate(tok)

  def test_checks_embedded_expiration(self):
    origin = datetime.datetime(2014, 1, 1, 1, 1, 1)

    # Make token issues at TS 'origin' that expires in 30 sec (instead of 1h).
    self.mock_now(origin, 0)
    tok = SimpleToken.generate(expiration_sec=30)

    # Valid before expiration.
    self.mock_now(origin, 29)
    SimpleToken.validate(tok)

    # Invalid after expiration.
    self.mock_now(origin, 31)
    with self.assertRaises(tokens.InvalidTokenError):
      SimpleToken.validate(tok)

  def test_is_configured_ok(self):
    # Should not raise. test_is_configured_* below are deviations from that.
    self.assertTrue(GoodToken.generate())

  def test_is_configured_bad_algo(self):
    class BadToken(GoodToken):
      algo = 'some-unknown-algo'
    with self.assertRaises(ValueError):
      BadToken.generate()

  def test_is_configured_bad_expiration(self):
    class BadToken(GoodToken):
      expiration_sec = None
    with self.assertRaises(ValueError):
      BadToken.generate()

  def test_is_configured_bad_secret_key(self):
    class BadToken(GoodToken):
      secret_key = None
    with self.assertRaises(ValueError):
      BadToken.generate()

  def test_is_configured_bad_version(self):
    class BadToken(GoodToken):
      version = 256
    with self.assertRaises(ValueError):
      BadToken.generate()


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
