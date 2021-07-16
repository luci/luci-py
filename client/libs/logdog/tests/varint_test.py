#!/usr/bin/env vpython
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

from io import BytesIO

import binascii
import itertools
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.abspath(os.path.join(
    __file__.decode(sys.getfilesystemencoding()),
    os.pardir, os.pardir, os.pardir)))
sys.path.insert(0, ROOT_DIR)

from libs.logdog import varint


class VarintTestCase(unittest.TestCase):

  def testVarintEncodingRaw(self):
    for base, exp in (
        (0, b'\x00'),
        (1, b'\x01'),
        (0x7F, b'\x7f'),
        (0x80, b'\x80\x01'),
        (0x81, b'\x81\x01'),
        (0x18080, b'\x80\x81\x06'),
    ):
      bytesIO = BytesIO()
      count = varint.write_uvarint(bytesIO, base)
      act = bytesIO.getvalue()

      self.assertEqual(act, exp,
          "Encoding for %d (%r) doesn't match expected (%r)" % (base, act, exp))
      self.assertEqual(count, len(act),
          "Length of %d (%d) doesn't match encoded length (%d)" % (
              base, len(act), count))

  def testVarintEncodeDecode(self):
    seed = (b'\x00', b'\x01', b'\x55', b'\x7F', b'\x80', b'\x81', b'\xff')
    for perm in itertools.permutations(seed):
      perm = b''.join(perm)

      while len(perm) > 0:
        exp = int(binascii.hexlify(perm), 16)
        bytesIO = BytesIO()
        count = varint.write_uvarint(bytesIO, exp)
        bytesIO.seek(0)
        act, count = varint.read_uvarint(bytesIO)

        self.assertEqual(act, exp,
            "Decoded %r (%d) doesn't match expected (%d)" % (
                binascii.hexlify(bytesIO.getvalue()), act, exp))
        self.assertEqual(count, len(bytesIO.getvalue()),
            "Decoded length (%d) doesn't match expected (%d)" % (
                count, len(bytesIO.getvalue())))

        if perm == 0:
          break
        perm = perm[1:]


if __name__ == '__main__':
  unittest.main()
