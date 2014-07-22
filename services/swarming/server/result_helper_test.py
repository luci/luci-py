#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from server import result_helper
from support import test_case
from mox import mox

# pylint: disable=W0212


class ResultHelperTest(test_case.TestCase):
  def setUp(self):
    super(ResultHelperTest, self).setUp()
    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()
    super(ResultHelperTest, self).tearDown()

  def testStoreEmpty(self):
    self.assertTrue(result_helper.StoreResults(''))

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
    # We should be storing all data as just raw bytes, so add \xb8 which isn't
    # valid ascii to ensure we do.
    results_data = b'a unicode string with a random unicode char \xb8'
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
            days=result_helper.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS - 1))
    result_helper._GetCurrentTime().AndReturn(
        datetime.datetime.utcnow() +
        datetime.timedelta(
            days=result_helper.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS + 1))
    self._mox.ReplayAll()

    self.assertEqual(0, result_helper.Results.query().count())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    # Create the Results model and ensure it isn't deleted the first time.
    result_helper.StoreResults('dummy data')
    ndb.delete_multi(result_helper.QueryOldResults())
    self.assertTrue(0 < result_helper.Results.query().count())
    self.assertTrue(0 < result_helper.ResultChunk.query().count())

    ndb.delete_multi(result_helper.QueryOldResults())
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
    ndb.delete_multi(result_helper.QueryOldResultChunks())
    self.assertTrue(0 < result_helper.ResultChunk.query().count())

    ndb.delete_multi(result_helper.QueryOldResultChunks())
    self.assertEqual(0, result_helper.ResultChunk.query().count())

    self._mox.VerifyAll()


if __name__ == '__main__':
  unittest.main()
