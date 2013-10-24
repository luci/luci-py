#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for Result Helper class."""



import datetime
import unittest


from google.appengine.ext import testbed
from google.appengine.ext import ndb

from common import result_helper
from common import swarm_constants
from third_party.mox import mox


class ResultHelperTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testStoreEmpty(self):
    result_helper.StoreResults('')

  def testStoreAndGetResults(self):
    results = result_helper.Results()
    self.assertEqual('', results.GetResults())

    self.assertEqual(0, result_helper.Results.query().count())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    results_data = 'data'
    results = result_helper.StoreResults(results_data)
    self.assertEqual(results_data, results.GetResults())

    # Make sure deleting the results delete the result chunks.
    # Wrapping in toplevel ensures it doesn't return until the async requests
    # finish.
    @ndb.toplevel
    def Delete():
      results.key.delete()

    Delete()

    self.assertEqual(0, result_helper.Results.query().count())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

  def testStoreUnicode(self):
    results_data = u'a unicode string'
    results = result_helper.StoreResults(results_data)

    self.assertEqual(results_data, results.GetResults())

  def testGetHugeResults(self):
    # The results should be more than 1MB to ensure that multiple chunks are
    # used.
    huge_results = 'blob_chunk' * (1024 * 1024 * 5)

    results = result_helper.StoreResults(huge_results)

    read_results = results.GetResults()
    self.assertEqual(len(read_results), len(huge_results))
    self.assertEqual(read_results, huge_results)

    # Make sure that multiple chunks were created.
    self.assertTrue(1 < result_helper.ResultChunk.query().count())

    # Ensure that all the chunks are deleted when the result is.
    results.key.delete()
    self.assertEqual(0, result_helper.ResultChunk.query().count())

  def testDeleteOldResults(self):
    self._mox.StubOutWithMock(result_helper, '_GetCurrentTime')

    result_helper._GetCurrentTime().AndReturn(
        datetime.datetime.utcnow() +
        datetime.timedelta(
            days=swarm_constants.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS - 1))
    result_helper._GetCurrentTime().AndReturn(
        datetime.datetime.utcnow() +
        datetime.timedelta(
            days=swarm_constants.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS + 1))
    self._mox.ReplayAll()

    self.assertEqual(0, result_helper.Results.query().count())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    # Create the Results model and ensure it isn't deleted the first time.
    result_helper.StoreResults('dummy data')
    ndb.Future.wait_all(result_helper.DeleteOldResults())
    self.assertTrue(0 < result_helper.Results.query().count())
    self.assertTrue(0 < result_helper.ResultChunk.query().count())

    ndb.Future.wait_all(result_helper.DeleteOldResults())
    self.assertEqual(0, result_helper.Results.query().count())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    self._mox.VerifyAll()

  def testDeleteOldResultChunks(self):
    self._mox.StubOutWithMock(result_helper, '_GetCurrentTime')

    result_helper._GetCurrentTime().AndReturn(
        datetime.datetime.utcnow() +
        datetime.timedelta(
            days=result_helper.SWARM_RESULT_CHUNK_OLD_TIME_TO_LIVE_DAYS - 1))
    result_helper._GetCurrentTime().AndReturn(
        datetime.datetime.utcnow() +
        datetime.timedelta(
            days=result_helper.SWARM_RESULT_CHUNK_OLD_TIME_TO_LIVE_DAYS + 1))
    self._mox.ReplayAll()

    self.assertEqual(0, result_helper.ResultChunk.query().count())

    # Create the ResultChunk model and ensure it isn't deleted the first time.
    result_helper.ResultChunk(chunk='random chunk').put()
    ndb.Future.wait_all(result_helper.DeleteOldResultChunks())
    self.assertTrue(0 < result_helper.ResultChunk.query().count())

    ndb.Future.wait_all(result_helper.DeleteOldResultChunks())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  unittest.main()
