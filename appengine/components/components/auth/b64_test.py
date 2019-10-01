#!/usr/bin/env python
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import string
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()


from components.auth import b64
from test_support import test_case


URL_SAFE_ALPHABET = set(string.letters + string.digits + '-_')


class Base64Test(test_case.TestCase):
  """Tests for base64_encode and base64_decode functions."""

  def test_base64_encode_types(self):
    with self.assertRaises(TypeError):
      b64.encode(None)
    with self.assertRaises(TypeError):
      b64.encode(u'unicode')

  def test_base64_decode_types(self):
    with self.assertRaises(TypeError):
      b64.decode(None)
    with self.assertRaises(TypeError):
      b64.decode(u'unicode')

  def test_base64_encode_is_url_safe(self):
    for a in xrange(255):
      original = chr(a)
      encoded = b64.encode(original)
      self.assertEqual(original, b64.decode(encoded))
      self.assertTrue(URL_SAFE_ALPHABET.issuperset(encoded), encoded)

  def test_base64_encode_decode(self):
    # Encode a bunch of strings of different lengths to test all
    # possible paddings (to see how padding stripping works).
    msg = 'somewhat long message with binary \x00\x01\x02\x03 inside'
    for i in xrange(len(msg)):
      self.assertEqual(msg[:i], b64.decode(b64.encode(msg[:i])))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
