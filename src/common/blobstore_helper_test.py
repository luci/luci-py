#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for Blobstore Helper class."""




import time
import unittest


from google.appengine import runtime
from google.appengine.api import files
from google.appengine.ext import blobstore
from google.appengine.ext import testbed
from common import blobstore_helper
from third_party.mox import mox


class BlobstoreHelperTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testCreateBlobstore(self):
    # This must be a class variable because otherwise python gets confused
    # inside of MockCreate and thinks attempt is just a local variable.
    self.attempt = 0
    old_create = files.blobstore.create

    def MockCreate(mime_type):
      if self.attempt < 1:
        self.attempt += 1
        raise files.ApiTemporaryUnavailableError()

      return old_create(mime_type)

    self._mox.StubOutWithMock(files.blobstore, 'create')
    self._mox.StubOutWithMock(time, 'sleep')
    files.blobstore.create = MockCreate
    time.sleep(mox.IgnoreArg())
    self._mox.ReplayAll()

    self.assertIsNotNone(blobstore_helper.CreateBlobstore('data'))
    self.assertEqual(1, self.attempt)

    self._mox.VerifyAll()

  def testCreateBlobstoreError(self):
    # Make sure that the code gives up trying to create a blobstore if it fails
    # too often.
    self._mox.StubOutWithMock(files.blobstore, 'create')
    self._mox.StubOutWithMock(time, 'sleep')
    for _ in range(blobstore_helper.MAX_BLOBSTORE_WRITE_TRIES):
      files.blobstore.create(mox.IgnoreArg()).AndRaise(
          files.ApiTemporaryUnavailableError())
      time.sleep(mox.IgnoreArg())
    self._mox.ReplayAll()

    self.assertIsNone(blobstore_helper.CreateBlobstore('data'))
    self._mox.VerifyAll()

  def testCreateBlobstoreTimeout(self):
    self._mox.StubOutWithMock(files.blobstore, 'create')
    files.blobstore.create(mox.IgnoreArg()).AndRaise(
        runtime.DeadlineExceededError)
    self._mox.ReplayAll()

    self.assertIsNone(blobstore_helper.CreateBlobstore('data'))
    self._mox.VerifyAll()

  def testGetBlobStore(self):
    self.assertIsNone(blobstore_helper.GetBlobstore(None))
    self.assertIsNone(blobstore_helper.GetBlobstore('fake key'))
    self.assertIsNone(blobstore_helper.GetBlobstore(123))

    blobstore_data = 'data'
    blob_key = blobstore_helper.CreateBlobstore(blobstore_data)
    self.assertEqual(blobstore_data, blobstore_helper.GetBlobstore(blob_key))

  def testGetHugeBlob(self):
    huge_blob = 'a' * (blobstore.MAX_BLOB_FETCH_SIZE * 5) + 'end_of_blob'

    blob_key = blobstore_helper.CreateBlobstore(huge_blob)

    read_blob = blobstore_helper.GetBlobstore(blob_key)
    self.assertEqual(len(read_blob), len(huge_blob))
    self.assertEqual(read_blob, huge_blob)

  def testGetBlobStoreExceptions(self):
    class BrokenBlobReader(object):
      def read(self, size):  # pylint: disable=g-bad-name,unused-argument
        raise blobstore.InternalError()

    self._mox.StubOutWithMock(blobstore_helper.blobstore, 'BlobReader')
    blobstore_helper.blobstore.BlobReader(mox.IgnoreArg()).AndReturn(
        BrokenBlobReader())
    self._mox.ReplayAll()

    blob_key = blobstore_helper.CreateBlobstore('valid data')
    self.assertIsNone(blobstore_helper.GetBlobstore(blob_key))

    self._mox.VerifyAll()


if __name__ == '__main__':
  unittest.main()
