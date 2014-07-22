#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unittest to exercise the code in url_helper.py."""


import logging
import os
import stat
import sys
import tempfile
import time
import unittest
import urllib2

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from depot_tools import auto_stub
from mox import mox

import url_helper  # pylint: disable=W0403


class UrlHelperTest(auto_stub.TestCase):
  def setUp(self):
    self._mox = mox.Mox()

    self.mock(logging, 'error', lambda *_: None)
    self.mock(logging, 'exception', lambda *_: None)
    self.mock(logging, 'info', lambda *_: None)
    self.mock(logging, 'warning', lambda *_: None)
    self.mock(time, 'sleep', lambda _: None)
    self._mox.StubOutWithMock(urllib2, 'urlopen')

  def tearDown(self):
    self._mox.UnsetStubs()

  def testXsrfRemoteGET(self):
    def url_read(req):
      self.assertEqual('http://localhost/a', req)
      return 'foo'
    self.mock(url_helper, 'UrlOpen', url_read)

    remote = url_helper.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a'))

  def testXsrfRemoteSimple(self):
    reqs = [
      (
        ('http://localhost/auth/api/v1/accounts/self/xsrf_token',),
        {},
        'token',
      ),
      (
        ('http://localhost/a',),
        {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token'}},
        'foo',
      ),
    ]

    def url_read(*args, **kwargs):
      expected_args, expected_kwargs, result = reqs.pop(0)
      self.assertEqual(expected_args, args)
      self.assertEqual(expected_kwargs, kwargs)
      return result
    self.mock(url_helper, 'UrlOpen', url_read)

    remote = url_helper.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a', data={'foo': 'bar'}))

  def testXsrfRemoteRefresh(self):
    reqs = [
      (
        ('http://localhost/auth/api/v1/accounts/self/xsrf_token',),
        {},
        'token',
      ),
      (
        ('http://localhost/a',),
        {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token'}},
        None,
      ),
      (
        ('http://localhost/auth/api/v1/accounts/self/xsrf_token',),
        {},
        'token2',
      ),
      (
        ('http://localhost/a',),
        {'data': {'foo': 'bar'}, 'headers': {'X-XSRF-Token': 'token2'}},
        'foo',
      ),
    ]

    def url_read(*args, **kwargs):
      expected_args, expected_kwargs, result = reqs.pop(0)
      self.assertEqual(expected_args, args)
      self.assertEqual(expected_kwargs, kwargs)
      return result
    self.mock(url_helper, 'UrlOpen', url_read)

    remote = url_helper.XsrfRemote('http://localhost/')
    remote.url_read('/a', data={'foo': 'bar'})

  def testDownloadFile(self):
    local_file = tempfile.NamedTemporaryFile(delete=False)
    local_file.close()
    try:
      self._mox.StubOutWithMock(url_helper, 'UrlOpen')
      file_data = 'data'
      url_helper.UrlOpen(mox.IgnoreArg()).AndReturn(file_data)
      self._mox.ReplayAll()

      self.assertTrue(url_helper.DownloadFile(local_file.name,
                                              'http://www.fakeurl.com'))
      with open(local_file.name) as f:
        self.assertEqual(file_data, f.read())

      self._mox.VerifyAll()
    finally:
      os.remove(local_file.name)

  def testDownloadFileDownloadError(self):
    try:
      fake_file = 'fake_local_file.fake'

      self._mox.StubOutWithMock(url_helper, 'UrlOpen')
      url_helper.UrlOpen(mox.IgnoreArg()).AndReturn(None)
      self._mox.ReplayAll()

      self.assertFalse(url_helper.DownloadFile(fake_file,
                                               'http://www.fakeurl.com'))
      self._mox.VerifyAll()
    finally:
      if os.path.exists(fake_file):
        os.remove(fake_file)

  def testDownloadFileSavingErrors(self):
    file_readonly = None
    try:
      file_readonly = tempfile.NamedTemporaryFile(delete=False)
      file_readonly.close()
      os.chmod(file_readonly.name, stat.S_IREAD)

      self._mox.StubOutWithMock(url_helper, 'UrlOpen')

      url_helper.UrlOpen(mox.IgnoreArg()).AndReturn('data')
      self._mox.ReplayAll()

      self.assertFalse(url_helper.DownloadFile(file_readonly.name,
                                               'http://www.fakeurl.com'))

      self._mox.VerifyAll()
    finally:
      if file_readonly:
        os.remove(file_readonly.name)


if __name__ == '__main__':
  unittest.main()
