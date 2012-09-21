#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

# This test connects to the test isolation server and ensures that is can
# upload and then retrieve a hash value.

import hashlib
import mimetools
import time
import unittest
import urllib
import urllib2
from hashlib import md5


# The url of the test isolate server.
ISOLATE_SERVER_URL = 'http://test.isolateserver.appspot.com/'

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
  boundary = md5(str(time.time())).hexdigest()
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


class AppTest(unittest.TestCase):
  def setUp(self):
    self.namespace = 'temporary' + str(time.time())

  def fetch(self, suburl, params, base_url=ISOLATE_SERVER_URL, payload=None,
            method='GET', content_type='application/octet-stream'):
    if method == 'POST' and payload == None:
      payload = ''

    full_url = base_url + suburl
    if params:
      # Ensure we are in the test namepace.
      params['namespace'] = self.namespace
      full_url += "?" + urllib.urlencode(params)

    request = urllib2.Request(full_url, data=payload)

    request.add_header('content-type', content_type)
    request.add_header('content-length', len(payload or ''))

    for attempt in range(MAX_URL_ATTEMPTS):
      try:
        return urllib2.urlopen(request)
      except urllib2.HTTPError:
        # Always re-raise if we reached the server.
        raise
      except urllib2.URLError as e:
        # Sleep with an exponential backoff.
        time.sleep(1.5 ** attempt)

    # If we end up here, we failed to reached the server so raise an error.
    raise urllib2.URLError()

  def RemoveAndVerify(self, hash_key):
    """Removes the hash key from the server and verify it is deleted."""
    # Remove the given hash content, if it already exists.
    self.fetch('content/remove', {'hash_key': hash_key}, method='POST')

    # Make sure we can't get the content, since it was deleted.
    response = self.fetch('content/contains', {'hash_key': hash_key})
    self.assertEqual('False', response.read())

  def UploadHashAndRetriveHelper(self, hash_key, hash_contents):
    self.RemoveAndVerify(hash_key)

    # Add the hash content and then retrieve it.
    if len(hash_contents) > MIN_SIZE_FOR_BLOBSTORE:
      # Query the server for a direct upload url.
      response = urllib2.urlopen(ISOLATE_SERVER_URL +
                                 '/content/generate_blobstore_url')
      upload_url = response.read()
      self.assertTrue(upload_url)

      content_type, body = encode_multipart_formdata(
          [('hash_key', hash_key), ('namespace', self.namespace)],
          [('hash_contents', 'hash_contents', hash_contents)])
      response = self.fetch('', params=None, base_url=upload_url, payload=body,
                            content_type=content_type)
    else:
      response = self.fetch('content/store', {'hash_key': hash_key},
                            payload=hash_contents)
    self.assertEqual('hash content saved.', response.read())

    response = self.fetch('content/retrieve', {'hash_key': hash_key})
    self.assertEqual(hash_contents, response.read())

    self.RemoveAndVerify(hash_key)

  def testStoreAndRetrieveHashNotInBlobstore(self):
    hash_key = hashlib.sha1(BINARY_DATA).hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, BINARY_DATA)

  def testStoreAndRetrieveHashfromBlobstore(self):
    # Try and upload a 40mb blobstore.
    hash_contents = (BINARY_DATA * 128) * 1024 * 40
    hash_key = hashlib.sha1(hash_contents).hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, hash_contents)

  def testStoreAndRetriveEmptyHash(self):
    hash_key = hashlib.sha1().hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, '')


if __name__ == '__main__':
  unittest.main()
