#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import sys
import unittest
import urlparse

from test_support import test_env
test_env.setup_test_env()

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from components.auth import api
from components.auth import openid
from test_support import test_case


class OpenIDTest(test_case.TestCase):
  def setUp(self):
    super(OpenIDTest, self).setUp()
    self.mock_now(datetime.datetime(2015, 1, 1, 1, 1, 1, 1))
    self.mock(api, 'get_secret', lambda _s: 'sekret')
    self.mock(logging, 'warning', lambda *_: None)

  def test_get_config(self):
    self.mock(app_identity, 'get_default_version_hostname', lambda: 'localhost')
    conf = openid.get_config()
    self.assertEqual({
      'client_id': None,
      'client_secret': None,
      'redirect_uri': 'http://localhost/auth/openid/callback',
    }, conf.to_dict())
    conf.populate(client_id='abc', client_secret='def')
    conf.put()
    self.assertEqual({
      'client_id': 'abc',
      'client_secret': 'def',
      'redirect_uri': 'http://localhost/auth/openid/callback',
    }, openid.get_config().to_dict())

  def test_flow(self):
    doc = {
      u'authorization_endpoint': u'https://blah.com/auth',
      u'token_endpoint': u'https://blah.com/token',
      u'userinfo_endpoint': u'https://blah.com/userinfo',
    }
    self.mock(openid, 'get_discovery_document', lambda: doc)
    conf = openid.AuthOpenIDConfig(
        client_id='abc',
        client_secret='secret',
        redirect_uri='http://local/redirect')

    auth_uri = openid.generate_authentication_uri(conf, {'state': 'blah'})
    self.assertEqual(
        'https://blah.com/auth?'
        'client_id=abc&'
        'redirect_uri=http%3A%2F%2Flocal%2Fredirect&'
        'response_type=code&'
        'scope=openid+email+profile&'
        'state=AXsiX2kiOiIxNDIwMDc0MDYxMDAwIiwic3RhdGUiOiJibGFoIn3byVSnpaofusNW'
        '16NCHCju-BHgzL3O5yQpB5bMaj0fZg', auth_uri)

    state = urlparse.parse_qs(urlparse.urlparse(auth_uri).query)['state'][0]
    self.assertEqual({'state': 'blah'}, openid.validate_state(state))

    expected_calls = [
      (
        'POST https://blah.com/token',
        {
          'headers': {'Content-Type': 'application/x-www-form-urlencoded'},
          'payload': 'client_secret=secret&code=codez&'
                     'grant_type=authorization_code&'
                     'client_id=abc&redirect_uri=http%3A%2F%2Flocal%2Fredirect',
        },
        {
          'token_type': 'Bearer',
          'access_token': 'accezz_token',
        }),
      (
        'GET https://blah.com/userinfo',
        {'headers': {'Authorization': 'Bearer accezz_token'}, 'payload': None},
        {
          'sub': '123',
          'email': 'def@example.com',
        }),
    ]
    def mocked_fetch_json(method, url, payload=None, headers=None):
      call = '%s %s' % (method, url)
      if not expected_calls or expected_calls[0][0] != call:
        self.fail('Unexpected call %s' % call)
      _, params, out = expected_calls.pop(0)
      self.assertEqual(params, {'payload': payload, 'headers': headers})
      return out

    self.mock(openid, '_fetch_json', mocked_fetch_json)
    user_info = openid.handle_authorization_code(conf, 'codez')
    self.assertEqual({'email': 'def@example.com', 'sub': '123'}, user_info)

  def test_normalize_dest_url(self):
    self.assertEqual(
        '/abc/def',
        openid.normalize_dest_url('http://local', 'http://local/abc/def'))
    self.assertEqual(
        '/abc/def', openid.normalize_dest_url('http://local', '/abc/def'))
    with self.assertRaises(ValueError):
      openid.normalize_dest_url('http://local', None)
    with self.assertRaises(ValueError):
      openid.normalize_dest_url('http://local', 'http://another/abc/def')
    with self.assertRaises(ValueError):
      openid.normalize_dest_url('http://local', 'abc/def')

  def test_create_login_url(self):
    class R(object):
      host_url = 'http://local'
    self.assertEqual(
        'http://local/auth/openid/login?r=%2Fabc%2Fdef%3Fx%3D1%26y%3D2',
        openid.create_login_url(R(), 'http://local/abc/def?x=1&y=2'))
    self.assertEqual(
        'http://local/auth/openid/login?r=%2Fabc%2Fdef%3Fx%3D1%26y%3D2',
        openid.create_login_url(R(), '/abc/def?x=1&y=2'))

  def test_make_session(self):
    session = openid.make_session({
      'email': u'email',
      'name': u'name',
      'picture': u'picture',
      'sub': u'user-id',
    }, expiration_sec=3600)
    self.assertEqual(
        ndb.Key(openid.AuthOpenIDUser, 'user-id', openid.AuthOpenIDSession, 1),
        session.key)
    self.assertEqual({
      'closed_ts': None,
      'created_ts': datetime.datetime(2015, 1, 1, 1, 1, 1, 1),
      'email': u'email',
      'expiration_ts': datetime.datetime(2015, 1, 1, 2, 1, 1, 1),
      'is_open': True,
      'name': u'name',
      'picture': u'picture',
    }, session.to_dict())

    session_cookie = openid.make_session_cookie(session)
    self.assertEqual(
      'AnsiX2kiOiIxNDIwMDc0MDYxMDAwIiwic3MiOiIxIiwic3ViIjoidXNlci1pZCJ93v490y1l'
      'YLFkvz8f6fculiqLwhe28bKL9TxXobrQxFg', session_cookie)

  def test_get_open_session_ok(self):
    session = openid.make_session({
      'email': u'email',
      'name': u'name',
      'picture': u'picture',
      'sub': u'user-id',
    }, expiration_sec=3600)
    session_cookie = openid.make_session_cookie(session)
    self.assertEqual(session, openid.get_open_session(session_cookie))

  def test_get_open_session_bad_cookie(self):
    session = openid.make_session({
      'email': u'email',
      'name': u'name',
      'picture': u'picture',
      'sub': u'user-id',
    }, expiration_sec=3600)
    session_cookie = openid.make_session_cookie(session)
    self.assertIsNone(openid.get_open_session(session_cookie+'blah'))
    self.assertIsNone(openid.get_open_session(None))
    session.key.delete()
    self.assertIsNone(openid.get_open_session(session_cookie))

  def test_session_expiration(self):
    session = openid.make_session({
      'email': u'email',
      'name': u'name',
      'picture': u'picture',
      'sub': u'user-id',
    }, expiration_sec=3600)
    session_cookie = openid.make_session_cookie(session)
    self.mock_now(datetime.datetime(2015, 1, 2, 1, 1, 1, 1))
    self.assertIsNone(openid.get_open_session(session_cookie))

  def test_session_close(self):
    session = openid.make_session({
      'email': u'email',
      'name': u'name',
      'picture': u'picture',
      'sub': u'user-id',
    }, expiration_sec=3600)
    session_cookie = openid.make_session_cookie(session)
    openid.close_session(session_cookie)
    self.assertIsNone(openid.get_open_session(session_cookie))

  def test_session_cookie_serialization(self):
    for i in xrange(1, 1024):
      session = openid.AuthOpenIDSession(
          id=i, parent=ndb.Key(openid.AuthOpenIDUser, 'blah'))
      cookie = openid.make_session_cookie(session)
      openid.SessionCookie.validate(cookie)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
