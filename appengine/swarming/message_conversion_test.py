#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

import swarming_test_env
swarming_test_env.setup_test_env()

from google.appengine.ext import ndb

from test_support import test_case

import message_conversion
from server import task_result


class TestMessageConversion(test_case.TestCase):

  def test_task_result_to_rpc_empty_cipd_pins_packages(self):
    message_conversion.task_result_to_rpc(
        task_result.TaskResultSummary(
            key=ndb.Key(
                'TaskResultSummary',
                1,
                parent=ndb.Key('TaskRequest', 0x7fffffffff447fde)),
            cipd_pins=task_result.CipdPins()), False)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
