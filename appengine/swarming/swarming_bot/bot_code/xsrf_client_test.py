#!/usr/bin/env python
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import time
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

# Creates a server mock for functions in net.py.
import net_utils

import xsrf_client


class UrlHelperTest(net_utils.TestCase):
  def setUp(self):
    super(UrlHelperTest, self).setUp()
    self.mock(logging, 'error', lambda *_: None)
    self.mock(logging, 'exception', lambda *_: None)
    self.mock(logging, 'info', lambda *_: None)
    self.mock(logging, 'warning', lambda *_: None)
    self.mock(time, 'sleep', lambda _: None)

  def testXsrfRemoteGET(self):
    self.expected_requests([('http://localhost/a', {}, 'foo', None)])

    remote = xsrf_client.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a'))

  def testXsrfRemoteSimple(self):
    self.expected_requests(
        [
          (
            'http://localhost/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {
              'expiration_sec': 100,
              'xsrf_token': 'token',
            },
          ),
          (
            'http://localhost/a',
            {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token'}},
            'foo',
            None,
          ),
        ])

    remote = xsrf_client.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a', data={'foo': 'bar'}))

  def testXsrfRemoteRefresh(self):
    self.expected_requests(
        [
          (
            'http://localhost/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {
              'expiration_sec': 100,
              'xsrf_token': 'token',
            },
          ),
          (
            'http://localhost/a',
            {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token'}},
            'bar',
            None,
          ),
          (
            'http://localhost/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {
              'expiration_sec': 100,
              'xsrf_token': 'token2',
            },
          ),
          (
            'http://localhost/a',
            {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token2'}},
            'foo',
            None,
          ),
        ])

    now = xsrf_client._utcnow()
    remote = xsrf_client.XsrfRemote('http://localhost/')
    remote.url_read('/a', data={'foo': 'bar'})
    self.mock(
        xsrf_client, '_utcnow', lambda: now + datetime.timedelta(seconds=91))
    remote.url_read('/a', data={'foo': 'bar'})

  def testXsrfRemoteCustom(self):
    # Use the new swarming bot API as an example of custom XSRF request handler.
    self.expected_requests(
        [
          (
            'http://localhost/swarming/api/v1/bot/handshake',
            {
              'data': {'attributes': 'b'},
              'headers': {'X-XSRF-Token-Request': '1'},
            },
            {
              'expiration_sec': 100,
              'ignored': True,
              'xsrf_token': 'token',
            },
          ),
          (
            'http://localhost/a',
            {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token'}},
            'foo',
            None,
          ),
        ])

    remote = xsrf_client.XsrfRemote(
        'http://localhost/',
        '/swarming/api/v1/bot/handshake')
    remote.xsrf_request_params = {'attributes': 'b'}
    self.assertEqual('foo', remote.url_read('/a', data={'foo': 'bar'}))


if __name__ == '__main__':
  logging.basicConfig(level=logging.ERROR)
  unittest.main()
