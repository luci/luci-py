#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Tests for TestRequest class."""


import logging
import unittest

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

from server import test_helper
from server import test_request


class TestRequestTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

  def tearDown(self):
    self.testbed.deactivate()

  def testGetTestRequestKeys(self):
   # Ensure that matching works even when the datastore is not being
    # consistent.
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

    # Ensure it works with no keys.
    empty_test_request = test_helper.CreateRequest(num_instances=0)
    self.assertEqual(0, len(empty_test_request.runner_keys))

    # Try with one runner.
    request = test_helper.CreateRequest(num_instances=1)
    self.assertEqual(1, len(request.runner_keys))

    # Test with more than 1 runner.
    instances = 5
    request = test_helper.CreateRequest(num_instances=instances)
    self.assertEqual(instances, len(request.runner_keys))

  def testGetMatchingTestRequests(self):
    # Ensure that matching works even when the datastore is not being
    # consistent.
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

    # Ensure it works with no matches.
    self.assertNotEqual('unknown', test_helper.REQUEST_MESSAGE_TEST_CASE_NAME)
    matches = test_request.GetAllMatchingTestRequests('unknown')
    self.assertEqual(0, len(matches))

    # Check with one matching request.
    test_helper.CreateRequest(1)

    matches = test_request.GetAllMatchingTestRequests(
        test_helper.REQUEST_MESSAGE_TEST_CASE_NAME)
    self.assertEqual(1, len(matches))

    # Add another request to ensure it works with multiple requests.
    test_helper.CreateRequest(1)

    matches = test_request.GetAllMatchingTestRequests(
        test_helper.REQUEST_MESSAGE_TEST_CASE_NAME)
    self.assertEqual(2, len(matches))

  def testDeleteIfNoMoreRunner(self):
    # Create a request with no runners and ensure it gets deleted.
    request = test_helper.CreateRequest(num_instances=0)
    self.assertEqual(1, test_request.TestRequest.query().count())

    request.RemoveRunner(None)
    self.assertEqual(0, test_request.TestRequest.query().count())

    # Create a request with runner and ensure it isn't deleted.
    request = test_helper.CreateRequest(1)
    self.assertEqual(1, test_request.TestRequest.query().count())

    request.RemoveRunner(None)
    self.assertEqual(1, test_request.TestRequest.query().count())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
