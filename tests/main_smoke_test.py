#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""This test connects to the test isolation server and ensures that is can
upload and then retrieve a hash value.

To run against a local dev server with verbosity for a single test case, use the
following format:
  ./tests/main_smoke_test.py http://localhost:8080/ -v \
      AppTestSignedIn.testStoreAndRetrieveEmptyHash
"""

import binascii
import hashlib
import logging
import sys
import time
import unittest
import urllib
import urllib2
import zlib

import find_depot_tools  # pylint: disable=W0611

from third_party import upload

# The url of the test isolate server.
ISOLATE_SERVER_URL_TEMPLATE = 'https://%s-dot-isolateserver-dev.appspot.com/'
ISOLATE_SERVER_URL = ISOLATE_SERVER_URL_TEMPLATE % 'test'

# Some basic binary data stored as a byte string.
BINARY_DATA = (chr(0) + chr(57) + chr(128) + chr(255)) * 2

# The maximum number of times to retry url errors.
MAX_URL_ATTEMPTS = 5

# The size of data that must be sent to the blobstore directly (30mb).
MIN_SIZE_FOR_BLOBSTORE = 1024 * 1024 *30


def encode_multipart_formdata(fields, files,
                              mime_mapper=lambda _: 'application/octet-stream'):
  """Encodes a Multipart form data object.

  Args:
    fields: a sequence (name, value) elements for
      regular form fields.
    files: a sequence of (name, filename, value) elements for data to be
      uploaded as files.
    mime_mapper: function to return the mime type from the filename.

  Returns:
    content_type: for httplib.HTTP instance
    body: for httplib.HTTP instance
  """
  boundary = hashlib.md5(str(time.time())).hexdigest()
  body_list = []
  for (key, value) in fields:
    body_list.append('--' + boundary)
    body_list.append('Content-Disposition: form-data; name="%s"' % key)
    body_list.append('')
    body_list.append(value)
    body_list.append('--' + boundary)
    body_list.append('')
  for (key, filename, value) in files:
    body_list.append('--' + boundary)
    body_list.append('Content-Disposition: form-data; name="%s"; '
                     'filename="%s"' % (key, filename))
    body_list.append('Content-Type: %s' % mime_mapper(filename))
    body_list.append('')
    body_list.append(value)
    body_list.append('--' + boundary)
    body_list.append('')
  if body_list:
    body_list[-2] += '--'
  body = '\r\n'.join(body_list)
  content_type = 'multipart/form-data; boundary=%s' % boundary
  return content_type, body


class CachedCredentials(upload.KeyringCreds):
  EMAIL = None
  PASSWORD = None

  def GetUserCredentials(self):
    if self.PASSWORD is None:
      self.EMAIL, self.PASSWORD = super(
          CachedCredentials, self).GetUserCredentials()
    return self.EMAIL, self.PASSWORD


class TestCase(unittest.TestCase):
  def setUp(self):
    super(TestCase, self).setUp()
    case = self.id().split('.', 1)[1].replace('.', '').replace('_', '')
    self.namespace = 'temporary' + str(long(time.time())) + case


class AppTestSignedIn(TestCase):
  EMAIL = None
  CREDENTIALS = None
  RPC = None

  def setUp(self):
    super(AppTestSignedIn, self).setUp()
    if AppTestSignedIn.RPC is None:
      if not ISOLATE_SERVER_URL.rstrip('/').endswith('.com'):
        # Cheap check for local server.
        print('Using test server: %s' % ISOLATE_SERVER_URL)
        AppTestSignedIn.RPC = upload.GetRpcServer(
            ISOLATE_SERVER_URL, 'test@example.com')
      else:
        print('Using real server')
        AppTestSignedIn.EMAIL = upload.GetEmail(
            "Email (login for uploading to %s)" %
            ISOLATE_SERVER_URL)
        AppTestSignedIn.CREDENTIALS = CachedCredentials(
            ISOLATE_SERVER_URL, ISOLATE_SERVER_URL, self.EMAIL)
        AppTestSignedIn.RPC = upload.HttpRpcServer(
            ISOLATE_SERVER_URL, AppTestSignedIn.CREDENTIALS.GetUserCredentials)
    self.token = urllib.quote(
        self.fetch(ISOLATE_SERVER_URL + 'content/get_token', payload=None))

  def fetch_no_retry(
      self, url, payload, content_type='application/octet-stream'):
    return self.RPC.Send(
        url[len(ISOLATE_SERVER_URL):],
        payload, content_type=content_type,
        extra_headers={'content-length': len(payload or '')})

  def fetch(self, url, payload, content_type='application/octet-stream'):
    last_error = Exception()
    for attempt in range(MAX_URL_ATTEMPTS):
      try:
        return self.fetch_no_retry(url, payload, content_type)
      except urllib2.HTTPError:
        # Always re-raise if we reached the server.
        raise
      except urllib2.URLError as e:
        last_error = e
        print 'Error connecting to server: %s' % str(e)
        if attempt != (MAX_URL_ATTEMPTS - 1):
          time.sleep(0.1)

    # If we end up here, we failed to reached the server so raise an error.
    raise last_error

  def upload(self, hash_key, content, priority=1):
    """Stores an object."""
    # Add the hash content and then retrieve it.
    if len(content) > MIN_SIZE_FOR_BLOBSTORE:
      # Query the server for a direct upload url.
      response = self.fetch(
          ISOLATE_SERVER_URL +
          'content/generate_blobstore_url/%s/%s?token=%s' %
            (self.namespace, hash_key, self.token),
          payload='')
      upload_url = response
      self.assertTrue(upload_url)

      content_type, body = encode_multipart_formdata(
          [('token', self.token)],
          [('content', hash_key, content)])
      response = self.fetch(upload_url, payload=body, content_type=content_type)
    else:
      response = self.fetch(
          ISOLATE_SERVER_URL + 'content/store/%s/%s?priority=%d&token=%s' % (
              self.namespace, hash_key, priority, self.token),
          payload=content)
    self.assertEqual('Content saved.', response)

  def verify_is_present(self, hash_key):
    """Verifies the object is present."""
    for i in range(MAX_URL_ATTEMPTS):
      response = self.fetch_no_retry(
          ISOLATE_SERVER_URL + 'content/contains/%s?token=%s' %
            (self.namespace, self.token),
          payload=binascii.unhexlify(hash_key))
      contains_response = response.decode()
      if contains_response == chr(1):
        break
      # GAE is exposing its internal data inconsistency.
      if i != (MAX_URL_ATTEMPTS - 1):
        print('Visible datastore inconsistency, retrying.')
        time.sleep(0.1)

    self.assertEqual(chr(1), contains_response)

  def retrieve(self, hash_key, content):
    """Retrieves an object."""
    response = self.fetch(
        ISOLATE_SERVER_URL + 'content/retrieve/%s/%s' % (
            self.namespace, hash_key),
        payload=None)
    if content != response:
      # Process it manually in case it's several megabytes, unittest happily
      # print it out.
      self.fail(
          'Expected %s (%d bytes), got %s (%d bytes)' %
          (content[:512], len(content),
            response[:512], len(response)))

  def upload_and_retrieve(self, hash_key, content, priority=1):
    """Stores and retrieve an object."""
    self.upload(hash_key, content, priority)
    self.verify_is_present(hash_key)
    self.retrieve(hash_key, content)

  def testStoreAndRetrieveHashNotInBlobstore(self):
    hash_key = hashlib.sha1(BINARY_DATA).hexdigest()

    self.upload_and_retrieve(hash_key, BINARY_DATA)

  def testStoreAndRetrieveHashfromBlobstore_40mb(self):
    # Try and upload a 40mb blobstore.
    content = (BINARY_DATA * 128) * 1024 * 40
    hash_key = hashlib.sha1(content).hexdigest()

    self.upload_and_retrieve(hash_key, content)

  def testStoreAndRetrieveEmptyHash(self):
    hash_key = hashlib.sha1().hexdigest()

    self.upload_and_retrieve(hash_key, '')

  def test_gzip(self):
    self.namespace += '-gzip'
    hash_key = hashlib.sha1(BINARY_DATA).hexdigest()
    compressed = zlib.compress(BINARY_DATA)
    self.upload_and_retrieve(hash_key, compressed)

  def test_gzip_512mb(self):
    # The goal here is to explode the 128mb memory limit of the F1 AppEngine
    # instance.
    self.namespace += '-gzip'
    content = (BINARY_DATA * 128) * 1024 * 512
    hash_key = hashlib.sha1(content).hexdigest()
    compressed = zlib.compress(content)
    self.upload_and_retrieve(hash_key, compressed)

  def testCorrupted(self):
    hash_key = hashlib.sha1(BINARY_DATA + 'x').hexdigest()
    try:
      # The upload is refused right away.
      self.upload(hash_key, BINARY_DATA)
      self.fail()
    except urllib2.HTTPError as e:
      self.assertEqual(400, e.code)

  def testCorrupted_gzip(self):
    self.namespace += '-gzip'
    hash_key = hashlib.sha1(BINARY_DATA + 'x').hexdigest()
    compressed = zlib.compress(BINARY_DATA)
    try:
      # The upload is refused right away.
      self.upload(hash_key, compressed)
      self.fail()
    except urllib2.HTTPError as e:
      self.assertEqual(400, e.code)

  def testCorrupted_gzip_512mb(self):
    # Try and upload a corrupted 512mb blob. The goal here is to explode the
    # 128mb memory limit of the F1 AppEngine instance.
    content = (BINARY_DATA * 128) * 1024 * 512
    hash_key = hashlib.sha1(content + 'x').hexdigest()
    compressed = zlib.compress(BINARY_DATA)
    try:
      # The upload is refused right away.
      self.upload(hash_key, compressed)
      self.fail()
    except urllib2.HTTPError as e:
      self.assertEqual(400, e.code)

  def testGetToken(self):
    response1 = self.fetch(
        ISOLATE_SERVER_URL + 'content/get_token', payload=None)
    # The length can vary, but it is a truncated base64 of a sha1 to 16 chars +
    # '-' + epoch in hours, so it's 23 chars until January 2084. If this code is
    # still used by 2084, maruel is impressed.
    self.assertEqual(23, len(response1), response1)
    response2 = self.fetch(
        ISOLATE_SERVER_URL + 'content/get_token', payload=None)
    self.assertEqual(23, len(response2), response2)
    # There's a small chance the responses differ so do not assert they are
    # equal even if they should in practice.


class AppTestSignedOut(TestCase):
  def setUp(self):
    super(AppTestSignedOut, self).setUp()
    self.token = None
    try:
      self.token = urllib.quote(
          urllib2.urlopen(ISOLATE_SERVER_URL + 'content/get_token').read())
    except urllib2.HTTPError:
      pass
    if self.token:
      print('Testing with whitelisted IP')
    else:
      print('Testing unauthencated')

  @staticmethod
  def fetch(url, payload):
    # Do not use upload's implementation since the goal here is to try out
    # without google account authentication.
    request = urllib2.Request(url, data=payload)
    request.add_header('content-type', 'application/octet-stream')
    request.add_header('content-length', len(payload or ''))
    logging.info(url)

    # Do not retry here.
    try:
      return urllib2.urlopen(request).read()
    except Exception, e:
      return e.code

  def testContains(self):
    hash_key = hashlib.sha1().hexdigest()
    if self.token:
      url = ISOLATE_SERVER_URL + 'content/contains/%s?token=%s' % (
          self.namespace, self.token)
    else:
      url = ISOLATE_SERVER_URL + 'content/contains/%s' % self.namespace

    response = self.fetch(url, payload=binascii.unhexlify(hash_key))
    if self.token:
      self.assertEqual('\x00', response)
    else:
      self.assertEqual(401, response)

  def testGetBlobStoreUrl(self):
    hash_key = hashlib.sha1().hexdigest()
    if self.token:
      url = ISOLATE_SERVER_URL + (
          'content/generate_blobstore_url/%s/%s?token=%s' % (
            self.namespace, hash_key, self.token))
    else:
      url = ISOLATE_SERVER_URL + 'content/generate_blobstore_url/%s/%s' % (
          self.namespace, hash_key)

    response = self.fetch(url, payload='')
    if self.token:
      # It returns an url.
      self.assertTrue(
          response.startswith(ISOLATE_SERVER_URL + '_ah/upload/'), response)
    else:
      self.assertEqual(401, response)

  def testStore(self):
    hash_key = hashlib.sha1().hexdigest()
    if self.token:
      url = ISOLATE_SERVER_URL + 'content/store/%s/%s?token=%s' % (
              self.namespace, hash_key, self.token)
    else:
      url = ISOLATE_SERVER_URL + 'content/store/%s/%s' % (
              self.namespace, hash_key)

    response = self.fetch(url, payload='')
    if self.token:
      self.assertEqual('Content saved.', response)
    else:
      self.assertEqual(401, response)


if __name__ == '__main__':
  if len(sys.argv) > 1:
    if '.' in sys.argv[1] or 'localhost' in sys.argv[1]:
      ISOLATE_SERVER_URL = sys.argv.pop(1)
    else:
      ISOLATE_SERVER_URL = ISOLATE_SERVER_URL_TEMPLATE % sys.argv.pop(1)
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
