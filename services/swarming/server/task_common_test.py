#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
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
from server import task_common
from server import test_helper


class TaskCommonApiTest(test_case.TestCase):
  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    module = task_common
    expected = set(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - set(i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing - set(['utcnow']))

  def test_validate_priority(self):
    with self.assertRaises(TypeError):
      task_common.validate_priority('1')
    with self.assertRaises(ValueError):
      task_common.validate_priority(-1)
    with self.assertRaises(ValueError):
      task_common.validate_priority(task_common.MAXIMUM_PRIORITY+1)
    task_common.validate_priority(0)
    task_common.validate_priority(1)
    task_common.validate_priority(task_common.MAXIMUM_PRIORITY)

  def test_milliseconds_since_epoch(self):
    test_helper.mock_now(self, datetime.datetime(1970, 1, 2, 3, 4, 5, 6789))
    delta = task_common.milliseconds_since_epoch(None)
    self.assertEqual(97445007, delta)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
