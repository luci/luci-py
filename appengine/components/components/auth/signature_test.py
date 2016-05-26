#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import collections
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from components.auth import signature
from test_support import test_case


# Skip check_signature tests if PyCrypto is not available.
try:
  import Crypto
  has_pycrypto = True
except ImportError:
  has_pycrypto = False


FetchResult = collections.namedtuple('FetchResult', ['status_code', 'content'])


FAKE_SERVICE_ACCOUNT_CERTS = """
{
 "faffca3b64d5bb61da829ace9aed119dceb2f63c": "abc",
 "cba74246d54580c5ee7a6a778c997a7cb1abc918": "def"
}
"""

class SignatureTest(test_case.TestCase):
  def test_get_service_account_certificates(self):
    def do_fetch(url, **_kwargs):
      self.assertEqual(
        url,
        'https://www.googleapis.com/robot/v1/metadata/x509/'
        '123%40appspot.gserviceaccount.com')
      return FetchResult(200, FAKE_SERVICE_ACCOUNT_CERTS)
    self.mock(signature.urlfetch, 'fetch', do_fetch)

    certs = signature.get_service_account_certificates(
        '123@appspot.gserviceaccount.com')
    certs.pop('timestamp')
    self.assertEqual(certs, {'certificates': [
      {
        'key_name': u'cba74246d54580c5ee7a6a778c997a7cb1abc918',
        'x509_certificate_pem': u'def',
      },
      {
        'key_name': u'faffca3b64d5bb61da829ace9aed119dceb2f63c',
        'x509_certificate_pem': u'abc',
      },
    ]})
    self.assertEqual(
        signature.get_x509_certificate_by_name(
            certs, 'cba74246d54580c5ee7a6a778c997a7cb1abc918'),
        'def')

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

  if has_pycrypto:
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
