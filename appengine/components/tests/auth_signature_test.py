#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

from support import test_case
from components.auth import signature


class SignatureTest(test_case.TestCase):
  def test_get_x509_certificate_by_name_ok(self):
    certs = signature.get_own_public_certificates()
    self.assertTrue(certs)
    pem = signature.get_x509_certificate_by_name(
        certs, certs['certificates'][0]['key_name'])
    self.assertEqual(certs['certificates'][0]['x509_certificate_pem'], pem)

  def test_get_x509_certificate_by_name_fail(self):
    certs = signature.get_own_public_certificates()
    with self.assertRaises(signature.CertificateError):
      signature.get_x509_certificate_by_name(certs, 'not-a-certname')

  def test_signature_correct(self):
    blob = '123456789'
    key_name, sig = signature.sign_blob(blob)
    cert = signature.get_x509_certificate_by_name(
        signature.get_own_public_certificates(), key_name)
    self.assertTrue(signature.check_signature(blob, cert, sig))

  def test_signature_wrong(self):
    blob = '123456789'
    key_name, sig = signature.sign_blob(blob)
    sig = chr(ord(sig[0]) + 1) + sig[1:]
    cert = signature.get_x509_certificate_by_name(
        signature.get_own_public_certificates(), key_name)
    self.assertFalse(signature.check_signature(blob, cert, sig))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
