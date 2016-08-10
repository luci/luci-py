#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import collections
import datetime
import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from components.auth import api
from components.auth import delegation
from components.auth import model
from components.auth import signature
from components.auth import tokens
from components.auth.proto import delegation_pb2

from test_support import test_case


FAKE_IDENT = model.Identity.from_bytes('user:a@a.com')


def fake_token_proto():
  """Just a fake envelope to test base64 serialization."""
  return delegation_pb2.DelegationToken(
      serialized_subtoken='serialized_subtoken',
      signer_id='signer_id',
      signing_key_id='signing_key_id',
      pkcs1_sha256_sig='pkcs1_sha256_sig')


def fake_subtoken_proto(issuer_id='user:abc@example.com', **kwargs):
  kwargs['issuer_id'] = issuer_id
  kwargs.setdefault('audience', ['*'])
  kwargs.setdefault('services', ['*'])
  kwargs.setdefault('creation_time', int(utils.time_time()))
  kwargs.setdefault('validity_duration', 3600)
  return delegation_pb2.Subtoken(**kwargs)


class SerializationTest(test_case.TestCase):
  def test_serialization_works(self):
    msg = fake_token_proto()
    tok = delegation.serialize_token(msg)
    self.assertEqual(msg, delegation.deserialize_token(tok))

  def test_serialize_huge(self):
    msg = fake_token_proto()
    msg.serialized_subtoken = 'huge' * 10000
    with self.assertRaises(delegation.BadTokenError):
      delegation.serialize_token(msg)

  def test_deserialize_huge(self):
    msg = fake_token_proto()
    msg.serialized_subtoken = 'huge' * 10000
    tok = tokens.base64_encode(msg.SerializeToString())
    with self.assertRaises(delegation.BadTokenError):
      delegation.deserialize_token(tok)

  def test_deserialize_not_base64(self):
    msg = fake_token_proto()
    tok = delegation.serialize_token(msg)
    tok += 'not base 64'
    with self.assertRaises(delegation.BadTokenError):
      delegation.deserialize_token(tok)

  def test_deserialize_bad_proto(self):
    tok = tokens.base64_encode('not a proto')
    with self.assertRaises(delegation.BadTokenError):
      delegation.deserialize_token(tok)


class SignatureCheckerTest(test_case.TestCase):
  def test_default_works(self):
    checker = delegation.get_signature_checker()
    self_id = model.get_service_self_identity().to_bytes()
    self.assertTrue(checker.is_trusted_signer(self_id))
    # 'key' is name of fake key in the testbed.
    self.assertTrue(checker.get_x509_certificate_pem(self_id, 'key'))

  def test_bad_key_id(self):
    checker = delegation.get_signature_checker()
    self_id = model.get_service_self_identity().to_bytes()
    with self.assertRaises(signature.CertificateError):
      checker.get_x509_certificate_pem(self_id, 'bad key id')


class SignatureTest(test_case.TestCase):
  def test_round_trip(self):
    tok = fake_subtoken_proto()
    self.assertEqual(tok, delegation.unseal_token(delegation.seal_token(tok)))

  def test_bad_signer_id(self):
    msg = delegation.seal_token(fake_subtoken_proto())
    msg.signer_id = 'not an identity'
    with self.assertRaises(delegation.BadTokenError):
      delegation.unseal_token(msg)

  def test_unknown_signer_id(self):
    checker = delegation.SignatureChecker() # empty, no trusted signers
    self.mock(delegation, 'get_signature_checker', lambda: checker)
    with self.assertRaises(delegation.BadTokenError):
      delegation.unseal_token(delegation.seal_token(fake_subtoken_proto()))

  def test_unknown_signing_key_id(self):
    msg = delegation.seal_token(fake_subtoken_proto())
    msg.signing_key_id = 'blah'
    with self.assertRaises(delegation.BadTokenError):
      delegation.unseal_token(msg)

  def test_bad_signature(self):
    msg = delegation.seal_token(fake_subtoken_proto())
    msg.pkcs1_sha256_sig = msg.pkcs1_sha256_sig[:-1] + 'A'
    with self.assertRaises(delegation.BadTokenError):
      delegation.unseal_token(msg)


class ValidationTest(test_case.TestCase):
  def test_passes_validation(self):
    tok = fake_subtoken_proto('user:abc@example.com')
    ident = delegation.check_subtoken(tok, FAKE_IDENT)
    self.assertEqual('user:abc@example.com', ident.to_bytes())

  def test_negative_validatity_duration(self):
    tok = fake_subtoken_proto('user:abc@example.com', validity_duration=-3600)
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_expired(self):
    now = int(utils.time_time())
    tok = fake_subtoken_proto(
        'user:abc@example.com', creation_time=now-120, validity_duration=60)
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_not_active_yet(self):
    now = int(utils.time_time())
    tok = fake_subtoken_proto('user:abc@example.com', creation_time=now+120)
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_allowed_clock_drift(self):
    now = utils.utcnow()
    self.mock_now(now)
    tok = fake_subtoken_proto('user:abc@example.com')
    # Works -29 sec before activation.
    self.mock_now(now, -29)
    self.assertTrue(delegation.check_subtoken(tok, FAKE_IDENT))
    # Doesn't work before that.
    self.mock_now(now, -31)
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_expiration_moment(self):
    now = utils.utcnow()
    self.mock_now(now)
    tok = fake_subtoken_proto('user:abc@example.com', validity_duration=3600)
    # Active at now + 3599.
    self.mock_now(now, 3599)
    self.assertTrue(delegation.check_subtoken(tok, FAKE_IDENT))
    # Expired at now + 3601.
    self.mock_now(now, 3601)
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_subtoken_services(self):
    tok = fake_subtoken_proto(
        'user:abc@example.com', services=['service:app-id'])
    # Passes.
    self.mock(
        model, 'get_service_self_identity',
        lambda: model.Identity.from_bytes('service:app-id'))
    self.assertTrue(delegation.check_subtoken(tok, FAKE_IDENT))
    # Fails.
    self.mock(
        model, 'get_service_self_identity',
        lambda: model.Identity.from_bytes('service:another-app-id'))
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, FAKE_IDENT)

  def test_subtoken_audience(self):
    groups = {'abc': ['user:b@b.com']}
    self.mock(
        api, 'is_group_member', lambda g, i: i.to_bytes() in groups.get(g, []))
    tok = fake_subtoken_proto(
          'user:abc@example.com', audience=['user:a@a.com', 'group:abc'])
    # Works.
    make_id = model.Identity.from_bytes
    self.assertTrue(
        delegation.check_subtoken(tok, make_id('user:a@a.com')))
    self.assertTrue(
        delegation.check_subtoken(tok, make_id('user:b@b.com')))
    # Other ids are rejected.
    with self.assertRaises(delegation.BadTokenError):
      delegation.check_subtoken(tok, make_id('user:c@c.com'))


class FullRoundtripTest(test_case.TestCase):
  def test_works(self):
    # Subtoken proto.
    tok = fake_subtoken_proto(
        'user:initial@a.com', audience=['user:final@a.com'])
    # Sign, serialize.
    blob = delegation.serialize_token(delegation.seal_token(tok))
    # Deserialize, check sig, validate.
    make_id = model.Identity.from_bytes
    ident = delegation.check_delegation_token(blob, make_id('user:final@a.com'))
    self.assertEqual(make_id('user:initial@a.com'), ident)


class CreateTokenTest(test_case.TestCase):

  Response = collections.namedtuple('Response', ['status_code', 'content'])

  def test_success(self):
    self.mock_now(datetime.datetime(2015, 1, 1))

    @ndb.tasklet
    def urlfetch(url, payload, **_rest):
      urlfetch.called = True
      self.assertEqual(
          url,
          'https://example.com/auth_service/api/v1/delegation/token/create')
      payload = json.loads(payload)
      self.assertEqual(payload, urlfetch.expected_payload)
      res = {
        'delegation_token': 'deadbeef',
        'validity_duration': payload['validity_duration'],
      }
      raise ndb.Return(self.Response(200, json.dumps(res, sort_keys=True)))

    urlfetch.expected_payload = {
      'audience': [
        'group:g',
        'user:a1@example.com',
        'user:a2@example.com',
      ],
      'services': ['service:1', 'service:2'],
      'validity_duration': 3000,
      'impersonate': 'user:i@example.com',
    }
    urlfetch.called = False

    self.mock(delegation, '_urlfetch_async', urlfetch)

    model.AuthReplicationState(
        key=model.replication_state_key(),
        primary_url='https://example.com'
    ).put()

    args = {
      'audience': [
        'user:a1@example.com',
        model.Identity('user', 'a2@example.com'),
        'group:g',
      ],
      'services': [
        'service:1',
        model.Identity('service', '2')
      ],
      'max_validity_duration_sec': 3000,
      'impersonate': model.Identity('user', 'i@example.com'),
    }
    result = delegation.delegate(**args)
    self.assertTrue(urlfetch.called)
    self.assertEqual(result.token, 'deadbeef')
    self.assertEqual(
        result.expiry, utils.utcnow() + datetime.timedelta(seconds=3000))

    # Get from cache.
    urlfetch.called = False
    delegation.delegate(**args)  # must not increase urlfetch.call_count
    self.assertFalse(urlfetch.called)

    # Get from cache with larger validity duration.
    urlfetch.called = False
    args['min_validity_duration_sec'] = 5000
    args['max_validity_duration_sec'] = 5000
    urlfetch.expected_payload['validity_duration'] = 5000
    result = delegation.delegate(**args)
    self.assertTrue(urlfetch.called)
    self.assertEqual(result.token, 'deadbeef')
    self.assertEqual(
        result.expiry, utils.utcnow() + datetime.timedelta(seconds=5000))
    self.assertTrue(urlfetch.called)

  def test_http_500(self):
    res = ndb.Future()
    res.set_result(self.Response(500, 'Server internal error'))
    self.mock(delegation, '_urlfetch_async', lambda  **_k: res)

    with self.assertRaises(delegation.DelegationTokenCreationError):
      delegation.delegate(auth_service_url='https://example.com')

  def test_http_403(self):
    res = ndb.Future()
    res.set_result(self.Response(403, 'Not authorized'))
    self.mock(delegation, '_urlfetch_async', lambda  **_k: res)

    with self.assertRaises(delegation.DelegationAuthorizationError):
      delegation.delegate(auth_service_url='https://example.com')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
