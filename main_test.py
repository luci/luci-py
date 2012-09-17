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

# The url of the test isolate server.
ISOLATE_SERVER_URL = 'http://test.isolateserver.appspot.com/'

# Some basic binary data stored as a byte string.
BINARY_DATA = (chr(0) + chr(57) + chr(128) + chr(255)) * 2

# The maximum number of times to retry url errors.
MAX_URL_ATTEMPTS = 5


def fetch(suburl, params, payload=None, method='GET',
          content_type='application/octet-stream'):
  if method == 'POST' and payload == None:
    payload = ''

  base_url = ISOLATE_SERVER_URL + suburl

  # Ensure we are in the test namepace.
  params['namespace'] = 'test'

  full_url = base_url + "?" + urllib.urlencode(params)
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

class AppTest(unittest.TestCase):
  def RemoveAndVerify(self, hash_key):
    """Removes the hash key from the server and verify it is deleted."""
    # Remove the given hash content, if it already exists.
    fetch('content/remove', {'hash_key': hash_key}, method='POST')

    # Make sure we can't get the content, since it was deleted.
    response = fetch('content/contains', {'hash_key': hash_key})
    self.assertEqual('False', response.read())

  def UploadHashAndRetriveHelper(self, hash_key, hash_content):
    self.RemoveAndVerify(hash_key)

    # Add the hash content and then retrieve it.
    response = fetch('content/store', {'hash_key': hash_key},
                     payload=hash_content)
    self.assertEqual('hash content saved.', response.read())

    response = fetch('content/retrieve', {'hash_key': hash_key})
    self.assertEqual(hash_content, response.read())

    self.RemoveAndVerify(hash_key)

  def testStoreAndRetrieveHashNotInBlobstore(self):
    hash_key = hashlib.sha1(BINARY_DATA).hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, BINARY_DATA)

  def testStoreAndRetrieveHashfromBlobstore(self):
    # Try and upload a 20mb blobstore.
    hash_contents = BINARY_DATA * 1024 * 20
    hash_key = hashlib.sha1(hash_contents).hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, hash_contents)

  def testStoreAndRetriveEmptyHash(self):
    hash_key = hashlib.sha1().hexdigest()

    self.UploadHashAndRetriveHelper(hash_key, '')


if __name__ == '__main__':
  unittest.main()
