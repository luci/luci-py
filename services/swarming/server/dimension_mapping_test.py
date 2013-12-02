#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

import test_case
from server import dimension_mapping


def _WaitForResults(futures):
  return [future.get_result() for future in futures]


class DimensionMappingTest(test_case.TestCase):
  def testDeleteOldDimensions(self):
    dimension_mapping.DimensionMapping().put()

    _WaitForResults(dimension_mapping.DeleteOldDimensionMapping())
    self.assertEqual(1, dimension_mapping.DimensionMapping.query().count())

    # Add an old dimension and ensure it gets removed.
    old_date = datetime.datetime.utcnow().date() - datetime.timedelta(
        days=dimension_mapping.DIMENSION_MAPPING_DAYS_TO_LIVE + 5)
    old_mapping = dimension_mapping.DimensionMapping(last_seen=old_date)
    # Remove the put hook so the model doesn't force last seen to today.
    old_mapping._pre_put_hook = lambda: None
    old_mapping.put()

    _WaitForResults(dimension_mapping.DeleteOldDimensionMapping())
    self.assertEqual(1, dimension_mapping.DimensionMapping.query().count())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
