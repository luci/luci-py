#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for TestRequest class."""



import logging
import unittest

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
    # Ensure it works with no keys.
    empty_test_request = test_helper.CreateRequest(num_instances=0)
    self.assertEqual(0, len(empty_test_request.GetAllKeys()))

    # Try with one runner.
    request = test_helper.CreateRequest(num_instances=1)
    self.assertEqual(1, len(request.GetAllKeys()))

    # Test with more than 1 runner.
    instances = 5
    request = test_helper.CreateRequest(num_instances=instances)
    self.assertEqual(instances, len(request.GetAllKeys()))

  def testGetMatchingTestRequests(self):
    request_name = 'request'

    # Ensure it works with no matches.
    self.assertNotEqual('unknown', request_name)
    matches = test_request.GetAllMatchingTestRequests('unknown')
    self.assertEqual(0, len(matches))

    # Check with one matching request.
    request = test_request.TestRequest(name=request_name)
    request.put()

    matches = test_request.GetAllMatchingTestRequests(request_name)
    self.assertEqual(1, len(matches))

    # Add another request to ensure it works with multiple requests.
    request = test_request.TestRequest(name=request_name)
    request.put()

    matches = test_request.GetAllMatchingTestRequests(request_name)
    self.assertEqual(2, len(matches))

  def testDeleteIfNoMoreRunner(self):
    # Create a request with no runners and ensure it gets deleted.
    request = test_request.TestRequest()
    request.put()
    self.assertEqual(1, test_request.TestRequest.query().count())

    request.DeleteIfNoMoreRunners()
    self.assertEqual(0, test_request.TestRequest.query().count())

    # Create a request with runner and ensure it isn't deleted.
    request = test_helper.CreateRequest(1)
    self.assertEqual(1, test_request.TestRequest.query().count())

    request.DeleteIfNoMoreRunners()
    self.assertEqual(1, test_request.TestRequest.query().count())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
