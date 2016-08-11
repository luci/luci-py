#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import json
import logging
import sys
import time
import unittest

import test_env
test_env.setup_test_env()

import webapp2
import webtest

from google.appengine.ext import ndb

from components import auth
from components import utils
from components.auth import delegation as auth_delegation
from components.auth import handler
from components.auth.proto import delegation_pb2
from test_support import test_case

from proto import config_pb2
import config
import delegation


def decode_token(token):
  return auth_delegation.unseal_token(auth_delegation.deserialize_token(token))


class HandlersTest(test_case.TestCase):
  def setUp(self):
    super(HandlersTest, self).setUp()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(delegation.get_rest_api_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': '127.1.2.3'})
    # Don't bother with XSRF tokens in unit tests.
    self.mock(
        delegation.CreateDelegationTokenHandler, 'xsrf_token_enforce_on', [])
    # Simplify auth.
    def dumb_auth(req):
      return auth.Identity.from_bytes(req.headers['Mock-Peer-Id'])
    self.mock(
        handler.AuthenticatingHandler, 'get_auth_methods',
        classmethod(lambda *_: [dumb_auth]))

  def create_token(self, body, peer_id):
    return self.app.post(
        '/auth_service/api/v1/delegation/token/create',
        json.dumps(body),
        headers={
          'Content-Type': 'application/json; charset=utf-8',
          'Mock-Peer-Id': peer_id,
        },
        expect_errors=True)

  def test_default_works(self):
    resp = self.create_token({
      'audience': ['*'],
      'services': ['*'],
    }, 'user:a@a.com')
    self.assertEqual(resp.status_code, 201)
    self.assertEqual(3600, resp.json_body['validity_duration'])
    self.assertEqual('1', resp.json_body['subtoken_id'])

    t = decode_token(resp.json_body['delegation_token'])
    self.assertEqual('user:a@a.com', t.issuer_id)
    self.assertTrue(t.creation_time >= time.time() - 30)
    self.assertEqual(3600, t.validity_duration)
    self.assertEqual(t.audience, ['*'])
    self.assertEqual(t.services, ['*'])
    self.assertFalse(t.HasField('impersonator_id'))
    self.assertTrue(t.subtoken_id is not None)

    # Entity is created.
    key = ndb.Key(delegation.AuthDelegationSubtoken, t.subtoken_id)
    ent = key.get()
    self.assertTrue(ent)
    self.assertTrue(ent.subtoken)
    self.assertEqual('127.1.2.3', ent.caller_ip)
    self.assertEqual('v1a', ent.auth_service_version)
    self.assertEqual('user:a@a.com', ent.issuer_id)
    self.assertEqual(['*'], ent.services)
    self.assertEqual(
        t.creation_time*1e6, utils.datetime_to_timestamp(ent.creation_time))
    self.assertEqual('', ent.impersonator_id)

  def test_with_impersonation(self):
    # This function is tested separately below.
    self.mock(
        delegation, 'check_can_create_token',
        lambda *_, **__: delegation.DEFAULT_RULE)

    resp = self.create_token({
      'audience': ['user:b@a.com'],
      'services': ['service:a'],
      'validity_duration': 12345,
      'impersonate': 'user:c@a.com',
    }, 'user:a@a.com')
    self.assertEqual(resp.status_code, 201)
    self.assertEqual(12345, resp.json_body['validity_duration'])

    t = decode_token(resp.json_body['delegation_token'])
    self.assertEqual('user:c@a.com', t.issuer_id)
    self.assertTrue(t.creation_time >= time.time() - 30)
    self.assertEqual(12345, t.validity_duration)
    self.assertEqual(['user:b@a.com'], t.audience)
    self.assertEqual(['service:a'], t.services)
    self.assertEqual('user:a@a.com', t.impersonator_id)

  def test_huge_token(self):
    resp = self.create_token({
      'audience': ['service:%d' % i for i in xrange(8000)],
    }, 'user:a@a.com')
    self.assertEqual(resp.status_code, 400)

  def test_audience_validation(self):
    def call(aud):
      return self.create_token({
        'audience': aud,
        'services': ['*'],
      }, 'user:a@a.com').status_code
    self.assertEqual(201, call(['group:group', 'user:abc@a.com']))
    self.assertEqual(201, call(['*']))
    self.assertEqual(400, call([]))
    self.assertEqual(400, call('not a list'))
    self.assertEqual(400, call([123]))
    self.assertEqual(400, call(['group:bad#group']))
    self.assertEqual(400, call(['not an identity']))

  def test_services_validation(self):
    def call(srv):
      return self.create_token({
        'audience': ['*'],
        'services': srv,
      }, 'user:a@a.com').status_code
    self.assertEqual(201, call(['service:abc', 'user:abc@a.com']))
    self.assertEqual(201, call(['*']))
    self.assertEqual(400, call([]))
    self.assertEqual(400, call('not a list'))
    self.assertEqual(400, call([123]))
    self.assertEqual(400, call(['group:not-an-id']))
    self.assertEqual(400, call(['not an identity']))

  def test_validity_duration_validation(self):
    def call(dur):
      return self.create_token({
        'audience': ['*'],
        'services': ['*'],
        'validity_duration': dur,
      }, 'user:a@a.com').status_code
    self.assertEqual(201, call(3600))
    self.assertEqual(400, call('not a number'))
    self.assertEqual(400, call(-1000))
    self.assertEqual(400, call(1))
    self.assertEqual(400, call(100000000000))

  def test_impersonate_validation(self):
    def call(imp):
      return self.create_token({
        'audience': ['*'],
        'services': ['*'],
        'impersonate': imp,
      }, 'user:a@a.com').status_code
    self.assertEqual(201, call('user:a@a.com'))
    self.assertEqual(400, call('a@a.com'))


class CheckCanCreateTokenTest(test_case.TestCase):
  def setUp(self):
    super(CheckCanCreateTokenTest, self).setUp()
    self.rules = []
    self.mock(
        config, 'get_delegation_config',
        lambda: config_pb2.DelegationConfig(rules=self.rules))

  def add_rule(self, **kwargs):
    r = config_pb2.DelegationConfig.Rule(**kwargs)
    self.rules.append(r)
    return r

  def test_get_delegation_rule(self):
    self.mock(
        auth, 'is_group_member',
        lambda g, m: g == 'gr' and m.to_bytes() == 'service:g')

    self.add_rule(
        user_id=['service:a', 'group:gr'],
        target_service=['service:b'],
        max_validity_duration=1)
    self.add_rule(
        user_id=['service:a'],
        target_service=['*'],
        max_validity_duration=2)
    self.add_rule(
        user_id=['*'],
        target_service=['service:c'],
        max_validity_duration=3)
    self.add_rule(
        user_id=['*'],
        target_service=['service:c', 'service:d'],
        max_validity_duration=4)
    self.add_rule(
        user_id=['*'],
        target_service=['*'],
        max_validity_duration=5)

    def test(expected_max_validity_duration, user_id, services):
      rule = delegation.get_delegation_rule(user_id, services)
      self.assertEqual(
          expected_max_validity_duration, rule.max_validity_duration)

    test(1, 'service:a', ['service:b'])
    test(1, 'service:g', ['service:b'])
    test(2, 'service:a', ['service:x'])
    test(2, 'service:a', ['service:c'])
    test(3, 'service:x', ['service:c'])
    test(4, 'service:x', ['service:c', 'service:d'])
    test(5, 'service:x', ['service:c', 'service:d', 'service:e'])
    test(1, 'service:a', ['*'])
    test(3, 'service:x', ['*'])

  def make_subtoken(self, **kwargs):
    return delegation_pb2.Subtoken(**kwargs)

  def test_validity_duration(self):
    rule = self.add_rule(
        user_id=['*'], target_service=['*'], max_validity_duration=300)

    tok = self.make_subtoken(issuer_id='user:a@a.com', validity_duration=300)
    r = delegation.check_can_create_token('user:a@a.com', tok)
    self.assertEqual(rule, r)

    with self.assertRaises(auth.AuthorizationError):
      tok = self.make_subtoken(issuer_id='user:a@a.com', validity_duration=400)
      delegation.check_can_create_token('user:a@a.com', tok)

  def test_impersonation_disallowed_by_default(self):
    # Making delegation token.
    tok = self.make_subtoken(issuer_id='user:a@a.com', validity_duration=300)
    r = delegation.check_can_create_token('user:a@a.com', tok)
    self.assertEqual(delegation.DEFAULT_RULE, r)

    # Making impersonation token.
    with self.assertRaises(auth.AuthorizationError):
      tok = self.make_subtoken(issuer_id='user:a@a.com', validity_duration=300)
      delegation.check_can_create_token('user:not-a@a.com', tok)

  def test_allowed_to_impersonate(self):
    rule = self.add_rule(
        user_id=['user:a@a.com'],
        target_service=['*'],
        max_validity_duration=300,
        allowed_to_impersonate=[
          'user:directly@a.com',
          'user:*@viaglob.com',
          'group:via-group',
        ])
    self.mock(
        auth, 'is_group_member',
        lambda g, m: g == 'via-group' and m.to_bytes() == 'user:in-group@a.com')

    tok = self.make_subtoken(
        issuer_id='user:directly@a.com', validity_duration=300)
    r = delegation.check_can_create_token('user:a@a.com', tok)
    self.assertEqual(rule, r)

    tok = self.make_subtoken(
        issuer_id='user:someone@viaglob.com', validity_duration=300)
    r = delegation.check_can_create_token('user:a@a.com', tok)
    self.assertEqual(rule, r)

    tok = self.make_subtoken(
        issuer_id='user:in-group@a.com', validity_duration=300)
    r = delegation.check_can_create_token('user:a@a.com', tok)
    self.assertEqual(rule, r)

    # Trying to impersonate someone not allowed.
    with self.assertRaises(auth.AuthorizationError):
      tok = self.make_subtoken(
          issuer_id='user:unknown@a.com', validity_duration=300)
      delegation.check_can_create_token('user:a@a.com', tok)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
