#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unittest to exercise the code in url_helper.py."""


import logging
import os
import stat
import StringIO
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
from third_party.mox import mox

import url_helper  # pylint: disable=W0403


class UrlHelperTest(auto_stub.TestCase):
  def setUp(self):
    self._mox = mox.Mox()

    self.mock(logging, 'error', lambda *_: None)
    self.mock(logging, 'exception', lambda *_: None)
    self.mock(logging, 'info', lambda *_: None)
    self.mock(logging, 'warning', lambda *_: None)
    self._mox.StubOutWithMock(time, 'sleep')
    self._mox.StubOutWithMock(urllib2, 'urlopen')

  def tearDown(self):
    self._mox.UnsetStubs()

  def testXsrfRemoteGET(self):
    def assert_url(url):
      def has_url(req):
        self.assertEqual(url, req.get_full_url())
        return True
      return mox.Func(has_url)

    url_helper.urllib2.urlopen(
        assert_url('http://localhost/a?UrlOpenAttempt=0'),
        timeout=300).AndReturn(StringIO.StringIO('foo'))
    self._mox.ReplayAll()

    remote = url_helper.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a'))
    self._mox.VerifyAll()

  def testXsrfRemoteSimple(self):
    def assert_url(url):
      def has_url(req):
        self.assertEqual(url, req.get_full_url())
        return True
      return mox.Func(has_url)

    xsrf_url = (
      'http://localhost/auth/api/v1/accounts/self/xsrf_token?UrlOpenAttempt=0')
    url_helper.urllib2.urlopen(
        assert_url(xsrf_url), timeout=300).AndReturn(StringIO.StringIO('token'))
    url_helper.urllib2.urlopen(
        assert_url('http://localhost/a'),
        timeout=300).AndReturn(StringIO.StringIO('foo'))
    self._mox.ReplayAll()

    remote = url_helper.XsrfRemote('http://localhost/')
    self.assertEqual('foo', remote.url_read('/a', data={'foo': 'bar'}))
    self._mox.VerifyAll()

  def testXsrfRemoteRefresh(self):
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')

    url_helper.UrlOpen(
        'http://localhost/auth/api/v1/accounts/self/xsrf_token',
        max_tries=40).AndReturn('token')
    url_helper.UrlOpen(
        'http://localhost/a', data={'foo': 'bar'}, max_tries=40,
        headers={'X-XSRF-Token': 'token'}
        ).AndReturn(None)
    url_helper.UrlOpen(
        'http://localhost/auth/api/v1/accounts/self/xsrf_token',
        max_tries=40).AndReturn('token2')
    url_helper.UrlOpen(
        'http://localhost/a', data={'foo': 'bar'}, max_tries=40,
        headers={'X-XSRF-Token': 'token2'}
        ).AndReturn('foo')
    self._mox.ReplayAll()

    remote = url_helper.XsrfRemote('http://localhost/')
    remote.url_read('/a', data={'foo': 'bar'})
    #self.assertEqual('foo', remote.url_read('/a', data={'foo': 'bar'}))
    self._mox.VerifyAll()

  def testUrlOpenInvalidTryCount(self):
    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', max_tries=-1), None)

    self._mox.VerifyAll()

  def testUrlOpenInvalidWaitDuration(self):
    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', wait_duration=-1), None)

    self._mox.VerifyAll()

  def testUrlOpenGETSuccess(self):
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(mox.IgnoreArg(),
                               timeout=mox.IgnoreArg()).AndReturn(
                                   StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url), response)

    self._mox.VerifyAll()

  def testUrlOpenPOSTSuccess(self):
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndReturn(
            StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url, data=''), response)

    self._mox.VerifyAll()

  def testUrlOpenSuccessAfterFailure(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndRaise(
            urllib2.URLError('url'))
    time.sleep(mox.IgnoreArg())
    response = 'True'
    url_helper.urllib2.urlopen(mox.IgnoreArg(),
                               timeout=mox.IgnoreArg()).AndReturn(
                                   StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', max_tries=2), response)

    self._mox.VerifyAll()

  def testUrlOpenFailure(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndRaise(
            urllib2.URLError('url'))
    self._mox.ReplayAll()

    self.assertIsNone(url_helper.UrlOpen('url', max_tries=1))

    self._mox.VerifyAll()

  def testUrlOpenHTTPErrorNoRetry(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndRaise(
            urllib2.HTTPError('url', 400, 'error message', None, None))
    self._mox.ReplayAll()

    # Even though we set max_tries to 10, we should only try once since
    # we get an HTTPError.
    self.assertIsNone(url_helper.UrlOpen('url', max_tries=10))

    self._mox.VerifyAll()

  def testUrlOpenHTTPErrorWithRetry(self):
    response = 'response'

    # Urlopen failure attempt.
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndRaise(
            urllib2.HTTPError('url', 500, 'error message', None, None))
    time.sleep(mox.IgnoreArg())

    # Urlopen success attempt.
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), timeout=mox.IgnoreArg()).AndReturn(
            StringIO.StringIO(response))

    self._mox.ReplayAll()

    # Since the HTTPError was a server error, we should retry and get the
    # desired response after the error.
    self.assertEqual(response, url_helper.UrlOpen('url', max_tries=10))

    self._mox.VerifyAll()

  def testEnsureCountKeyIncludedInOpen(self):
    def assert_data(index):
      def has_data(req):
        self.assertEqual(
            'http://localhost?UrlOpenAttempt=%d' % index, req.get_full_url())
        return True
      return mox.Func(has_data)

    attempts = 5
    for i in range(attempts):
      url_helper.urllib2.urlopen(
          assert_data(i), timeout=mox.IgnoreArg()).AndRaise(
              urllib2.URLError('url'))
      if i != attempts - 1:
        time.sleep(mox.IgnoreArg())
    self._mox.ReplayAll()

    self.assertEqual(
        None, url_helper.UrlOpen('http://localhost', max_tries=attempts))
    self._mox.VerifyAll()

  def testCountKeyInData(self):
    data = {url_helper.COUNT_KEY: 1}
    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', data=data), None)
    self._mox.VerifyAll()

  def testNonAcsiiData(self):
    data = {'r': u'not ascii \xa3 \u04bb'}
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(mox.IgnoreArg(),
                               timeout=mox.IgnoreArg()).AndReturn(
                                   StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url, data=data), response)

    self._mox.VerifyAll()

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
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.FATAL)
  unittest.main()
