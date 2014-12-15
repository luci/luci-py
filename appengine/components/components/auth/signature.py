# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Functions to produce and verify RSA+SHA256+SHA512 signatures.

Based on app_identity.sign_blob() and app_identity.get_public_certificates()
functions, and thus private keys are managed by GAE.
"""

import base64
import hashlib
import json

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch

from components import utils


# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'CertificateError',
  'check_signature',
  'get_own_public_certificates',
  'get_service_public_certificates',
  'get_x509_certificate_by_name',
  'sign_blob',
]


class CertificateError(Exception):
  """Errors when working with a certificate."""


@utils.cache_with_expiration(3600)
def get_own_public_certificates():
  """Returns jsonish object with public certificates of current service."""
  certs = app_identity.get_public_certificates()
  return {
    'certificates': [
      {
        'key_name': cert.key_name,
        'x509_certificate_pem': cert.x509_certificate_pem,
      }
      for cert in certs
    ],
    'timestamp': utils.datetime_to_timestamp(utils.utcnow()),
  }


def get_service_public_certificates(service_url):
  """Returns jsonish object with public certificates of a service.

  Service at |service_url| must have 'auth' component enabled (to serve
  the certificates).
  """
  cache_key = 'pub_certs:%s' % service_url
  certs = memcache.get(cache_key)
  if not certs:
    protocol = 'http://' if utils.is_local_dev_server() else 'https://'
    assert service_url.startswith(protocol)
    result = urlfetch.fetch(
        url='%s/auth/api/v1/server/certificates' % service_url,
        method='GET',
        headers={'X-URLFetch-Service-Id': utils.get_urlfetch_service_id()},
        follow_redirects=False,
        deadline=10,
        validate_certificate=True)
    if result.status_code != 200:
      raise CertificateError(
          'Failed to grab public certs from %s: HTTP %d' %
          (service_url, result.status_code))
    certs = json.loads(result.content)
    memcache.set(cache_key, certs, time=3600)
  return certs


def get_x509_certificate_by_name(certs, key_name):
  """Given jsonish object with certificates returns x509 cert with given name.

  Args:
    certs: return value of get_own_public_certificates() or
        get_service_public_certificates().
    key_name: name of the certificate.

  Returns:
    PEM encoded x509 certificate.

  Raises:
    CertificateError if no such cert.
  """
  for cert in certs['certificates']:
    if cert['key_name'] == key_name:
      return cert['x509_certificate_pem']
  raise CertificateError('Certificate \'%s\' not found' % key_name)


def sign_blob(blob):
  """Signs a blob using current service's private key.

  Uses GAE app_identity.sign_blob function. It has a limit of 8KB on a size of
  a blob, so |blob| is hashed first (with sha512). So final signature is
  RSA+SHA256(sha512(blob)).

  Returns:
    Tuple (name of a key used, signature).
  """
  # app_identity.sign_blob is producing RSA+SHA256 signature. Sadly, it isn't
  # documented anywhere. But it should be relatively stable since this API is
  # used by OAuth2 libraries (and so changing signature method may break a lot
  # of stuff).
  return app_identity.sign_blob(hashlib.sha512(blob).digest())


def check_signature(blob, x509_certificate_pem, signature):
  """Verifies signature produced by 'sign_blob' function.

  Args:
    blob: binary buffer to check the signature for.
    x509_certificate_pem: PEM encoded x509 certificate, may be obtained with
        get_service_public_certificates() and get_x509_certificate_by_name().
    signature: the signature, as returned by sign_blob function.

  Returns:
    True if signature is correct.
  """
  # See http://stackoverflow.com/a/12921889.

  # Lazy import Crypto, since not all service that use 'components' may need it.
  from Crypto.Hash import SHA256
  from Crypto.PublicKey import RSA
  from Crypto.Signature import PKCS1_v1_5
  from Crypto.Util import asn1

  # Convert PEM to DER. There's a function for this in 'ssl' module
  # (ssl.PEM_cert_to_DER_cert), but 'ssl' is not importable in GAE sandbox
  # on dev server (C extension is not whitelisted).
  lines = x509_certificate_pem.strip().split('\n')
  if (len(lines) < 3 or
      lines[0] != '-----BEGIN CERTIFICATE-----' or
      lines[-1] != '-----END CERTIFICATE-----'):
    raise CertificateError('Invalid certificate format')
  der = base64.b64decode(''.join(lines[1:-1]))

  # Extract subjectPublicKeyInfo field from X.509 certificate (see RFC3280).
  cert = asn1.DerSequence()
  cert.decode(der)
  tbsCertificate = asn1.DerSequence()
  tbsCertificate.decode(cert[0])
  subjectPublicKeyInfo = tbsCertificate[6]

  verifier = PKCS1_v1_5.new(RSA.importKey(subjectPublicKeyInfo))
  digest = hashlib.sha512(blob).digest()
  return verifier.verify(SHA256.new(digest), signature)
