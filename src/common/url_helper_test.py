#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Unittest to exercise the code in url_helper.py."""




import logging
import os
import stat
import StringIO
import tempfile
import time
import unittest
import urllib
import urllib2


from common import url_helper
from third_party.mox import mox


class UrlHelperTest(unittest.TestCase):
  def setUp(self):
    self._mox = mox.Mox()

    self._mox.StubOutWithMock(logging, 'error')
    self._mox.StubOutWithMock(time, 'sleep')
    self._mox.StubOutWithMock(urllib2, 'urlopen')

  def tearDown(self):
    self._mox.UnsetStubs()

  def testUrlOpenInvalidTryCount(self):
    url_helper.logging.error(mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', max_tries=-1), None)

    self._mox.VerifyAll()

  def testUrlOpenInvalidWaitDuration(self):
    url_helper.logging.error(mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', wait_duration=-1), None)

    self._mox.VerifyAll()

  def testUrlOpenGETSuccess(self):
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(mox.StrContains(url)).AndReturn(
        StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url, method='GET'), response)

    self._mox.VerifyAll()

  def testUrlOpenPOSTSuccess(self):
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(url, mox.IgnoreArg()).AndReturn(
        StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url, method='POST'),
                     response)

    self._mox.VerifyAll()

  def testUrlOpenPOSTFORMSuccess(self):
    url = 'http://my.url.com'

    response = 'True'
    url_helper.urllib2.urlopen(mox.IsA(urllib2.Request)).AndReturn(
        StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen(url, method='POSTFORM'),
                     response)

    self._mox.VerifyAll()

  def testUrlOpenSuccessAfterFailure(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(urllib2.URLError('url'))
    time.sleep(mox.IgnoreArg())
    response = 'True'
    url_helper.urllib2.urlopen(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn(
        StringIO.StringIO(response))

    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', max_tries=2), response)

    self._mox.VerifyAll()

  def testUrlOpenFailure(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(urllib2.URLError('url'))
    logging.error(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    self.assertIsNone(url_helper.UrlOpen('url', max_tries=1))

    self._mox.VerifyAll()

  def testUrlOpenHTTPErrorNoRetry(self):
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
            urllib2.HTTPError('url', 400, 'error message', None, None))
    logging.error(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    # Even though we set max_tries to 10, we should only try once since
    # we get an HTTPError.
    self.assertIsNone(url_helper.UrlOpen('url', max_tries=10))

    self._mox.VerifyAll()

  def testUrlOpenHTTPErrorWithRetry(self):
    response = 'response'

    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
            urllib2.HTTPError('url', 500, 'error message', None, None))
    logging.warning(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
                    mox.IgnoreArg())
    url_helper.urllib2.urlopen(
        mox.IgnoreArg(), mox.IgnoreArg()).AndReturn(StringIO.StringIO(response))

    self._mox.ReplayAll()

    # Since the HTTPError was a server error, we should retry and get the
    # desired response after the error.
    self.assertEqual(response, url_helper.UrlOpen('url', max_tries=10))

    self._mox.VerifyAll()

  def testEnsureCountKeyIncludedInOpen(self):
    attempts = 5
    for i in range(attempts):
      encoded_data = urllib.urlencode({url_helper.COUNT_KEY: i})

      url_helper.urllib2.urlopen(
          mox.IgnoreArg(), encoded_data).AndRaise(urllib2.URLError('url'))
      logging.info(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
                   mox.IgnoreArg())
      if i != attempts - 1:
        time.sleep(mox.IgnoreArg())

    logging.error(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', max_tries=attempts), None)
    self._mox.VerifyAll()

  def testCountKeyInData(self):
    data = {url_helper.COUNT_KEY: 1}

    logging.error(mox.StrContains('existed in the data'), url_helper.COUNT_KEY)
    self._mox.ReplayAll()

    self.assertEqual(url_helper.UrlOpen('url', data=data), None)
    self._mox.VerifyAll()

  def testDownloadFile(self):
    local_file = None
    try:
      local_file = tempfile.NamedTemporaryFile(delete=False)
      local_file.close()

      self._mox.StubOutWithMock(url_helper, 'UrlOpen')
      file_data = 'data'
      url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn(file_data)
      self._mox.ReplayAll()

      self.assertTrue(url_helper.DownloadFile(local_file.name,
                                              'http://www.fakeurl.com'))
      with open(local_file.name) as f:
        self.assertEqual(file_data, f.read())

      self._mox.VerifyAll()
    finally:
      if local_file:
        os.remove(local_file.name)

  def testDownloadFileDownloadError(self):
    try:
      fake_file = 'fake_local_file.fake'

      self._mox.StubOutWithMock(url_helper, 'UrlOpen')
      url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn(None)
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

      url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('data')
      url_helper.logging.error(mox.StrContains('Failed'), mox.IgnoreArg(),
                               mox.IgnoreArg())
      self._mox.ReplayAll()

      self.assertFalse(url_helper.DownloadFile('fake_file.fake',
                                               'http://www.fakeurl.com'))

      self._mox.VerifyAll()
    finally:
      if file_readonly:
        os.remove(file_readonly.name)

  def testEncodeMultipartFormData(self):
    fields = [('x', 'y'), (1, 2)]
    files = [('key', 'filename', 'file data')]

    # Ensure that EncodeMultipartFormData works with any combination of fields
    # and files.
    content_type, body = url_helper.EncodeMultipartFormData()
    self.assertTrue(content_type.startswith('multipart/form-data; boundary='))
    self.assertEqual('', body)

    content_type, body = url_helper.EncodeMultipartFormData(fields=fields)
    self.assertTrue(content_type.startswith('multipart/form-data; boundary='))
    self.assertTrue('name="x"\r\n\r\ny' in body, body)
    self.assertTrue('name="1"\r\n\r\n2' in body, body)

    content_type, body = url_helper.EncodeMultipartFormData(files=files)
    self.assertTrue(content_type.startswith('multipart/form-data; boundary='))
    self.assertTrue('name="key"; filename="filename"' in body, body)
    self.assertTrue('file data' in body, body)

    content_type, body = url_helper.EncodeMultipartFormData(fields=fields,
                                                            files=files)
    self.assertTrue(content_type.startswith('multipart/form-data; boundary='))
    self.assertTrue('name="x"\r\n\r\ny' in body, body)
    self.assertTrue('name="1"\r\n\r\n2' in body, body)


if __name__ == '__main__':
  unittest.main()
