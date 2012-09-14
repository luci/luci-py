#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

# This test connects to the test isolation server and ensures that is can
# upload and then retrieve a hash value.

import hashlib
import mimetools
import unittest
import urllib
import urllib2

# The url of the test isolate server.
ISOLATE_SERVER_URL = 'http://test.isolateserver.appspot.com/'

# Some basic binary data stored as a byte string.
BINARY_DATA = (chr(0) + chr(57) + chr(128) + chr(255)) * 2

# This class is mostly copied from http://www.doughellmann.com/PyMOTW/urllib2/
class MultiPartForm(object):
  """Accumulate the data to be used when posting a form."""

  def __init__(self):
    self.form_fields = []
    self.files = []
    self.boundary = mimetools.choose_boundary()
    return

  def get_content_type(self):
    return 'multipart/form-data; boundary=%s' % self.boundary

  def add_field(self, name, value):
    self.form_fields.append((name, value))
    return

  def add_file(self, fieldname, body, mimetype='application/octet-stream'):
    self.files.append((fieldname, mimetype, body))
    return

  def __str__(self):
    """Return a string representing the form data, including attached files."""
    # Build a list of lists, each containing "lines" of the
    # request.  Each part is separated by a boundary string.
    # Once the list is built, return a string where each
    # line is separated by '\r\n'.
    parts = []
    part_boundary = '--' + self.boundary

    # Add the form fields
    for name, value in self.form_fields:
      parts.extend([
          part_boundary,
          'Content-Disposition: form-data; name="%s"' % name,
          '',
          value,
      ])

    # Add the files to upload
    for field_name, content_type, body in self.files:
      parts.extend([
          part_boundary,
          'Content-Disposition: file; name="%s"; filename="%s"' % \
            (field_name, field_name),
          '',
          body,
      ])

    # Add closing boundary marker and then return CR+LF separated data
    parts.append('--' + self.boundary + '--')
    parts.append('')
    return '\r\n'.join(parts)



def fetch(suburl, params, upload_data=None):
  # Ensure we are in the test namepace.
  params['namespace'] = 'test'
  request = urllib2.Request(ISOLATE_SERVER_URL + suburl)

  form = MultiPartForm()
  for (key, value) in params.iteritems():
    form.add_field(key, value)

  if upload_data is not None:
    form.add_file('hash_content', upload_data)

  body = str(form)
  request.add_header('Content-type', form.get_content_type())
  request.add_header('Content-length', len(body))
  request.add_data(body)

  return urllib2.urlopen(request)


# TODO(csharp): Make sure only the modifying requests are POST and that
# the rest are GET.
class AppTest(unittest.TestCase):
  def RemoveAndVerify(self, hash_key):
    """Removes the hash key from the server and verify it is deleted."""
    # Remove the given hash content, if it already exists.
    fetch('content/remove', {'hash_key': hash_key})

    # Make sure we can't get the content, since it was deleted.
    response = fetch('content/contains', {'hash_key': hash_key})
    self.assertEqual('False', response.read())

  def UploadHashAndRetriveHelper(self, hash_key, hash_content):
    # Ensure the hash content doesn't already exist
    response = fetch('content/contains', {'hash_key': hash_key})
    self.assertEqual('False', response.read())

    # Add the hash content and then retrieve it.
    response = fetch('content/store', {'hash_key': hash_key}, hash_content)
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


if __name__ == '__main__':
  unittest.main()
