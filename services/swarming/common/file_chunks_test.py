#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tests for File and FileChunks."""


import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import testbed

from common import file_chunks
from common import swarm_constants


class FileTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

  def tearDown(self):
    self.testbed.deactivate()

  def testStoreFile(self):
    file_id = 'file_id'
    expected_file = 'file'

    self.assertEqual(None, file_chunks.RetrieveFile(file_id))

    file_chunks.StoreFile(file_id, expected_file)
    self.assertEqual(expected_file,
                     file_chunks.RetrieveFile(file_id))

    # Change the file and ensure it replaces the old version.
    expected_file_new = 'new_file'
    self.assertNotEqual(expected_file_new, expected_file)

    file_chunks.StoreFile(file_id, expected_file_new)
    self.assertEqual(expected_file_new,
                     file_chunks.RetrieveFile(file_id))

    # Ensure we only have one file and its chunk stored.
    self.assertEqual(1, file_chunks.File.query().count())
    self.assertEqual(1, file_chunks.FileChunk.query().count())

  def testStoreLarge(self):
    # Ensure that files are properly stored when they need to span multiple
    # chunks.
    file_id = 'file_id'
    expected_file = 'start'
    expected_file += 'script' * swarm_constants.MAX_CHUNK_SIZE
    expected_file += 'end'

    file_chunks.StoreFile(file_id, expected_file)

    self.assertEqual(expected_file, file_chunks.RetrieveFile(file_id))

    # Ensure that we have multiple chunks.
    self.assertTrue(file_chunks.FileChunk.query().count() > 1)

    # Replace the file with an empty one and ensure all chunks are deleted.
    file_chunks.StoreFile(file_id, '')
    self.assertEqual(0, file_chunks.FileChunk.query().count())

  def testStoreMultipleFile(self):
    file_id_1 = '1'
    file_id_2 = '2'
    file_1 = 'The first file'
    file_2 = 'The second file'

    file_chunks.StoreFile(file_id_1, file_1)
    file_chunks.StoreFile(file_id_2, file_2)

    self.assertEqual(file_1, file_chunks.RetrieveFile(file_id_1))
    self.assertEqual(file_2, file_chunks.RetrieveFile(file_id_2))


if __name__ == '__main__':
  unittest.main()
