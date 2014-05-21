#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import json
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from server import task_glue
from support import test_case


class TaskGlueTest(test_case.TestCase):
  def test_convert_test_case(self):
    data = {
      'configurations': [
        {
          'config_name': 'ignored',
          'deadline_to_run': 63,
          'dimensions': {
            'OS': 'Windows-3.1.1',
            'hostname': 'localhost',
          },
          'num_instances': 1,
          'priority': 50,
        },
      ],
      'data': [
        ('http://localhost/foo', 'foo.zip'),
        ('http://localhost/bar', 'bar.zip'),
      ],
      'env_vars': {
        'foo': 'bar',
        'joe': '2',
      },
      'requestor': 'Jesus',
      'test_case_name': 'Request name',
      'tests': [
        {
          'action': ['command1', 'arg1'],
          'hard_time_out': 66.1,
          'io_time_out': 68.1,
          'test_name': 'very ignored',
        },
        {
          'action': ['command2', 'arg2'],
          'test_name': 'very ignored but must be different',
          'hard_time_out': 60000000,
          'io_time_out': 60000000,
        },
      ],
    }
    actual = task_glue.convert_test_case(json.dumps(data))
    expected = {
      'name': u'Request name',
      'user': u'Jesus',
      'properties': {
        'commands': [
          [u'command1', u'arg1'],
          [u'command2', u'arg2'],
        ],
        'data': [
          [u'http://localhost/foo', u'foo.zip'],
          [u'http://localhost/bar', u'bar.zip'],
        ],
        'dimensions': {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'},
        'env': {u'foo': u'bar', u'joe': u'2'},
        'execution_timeout_secs': 66,
        'io_timeout_secs': 68,
      },
      'priority': 50,
      'scheduling_expiration_secs': 63,
    }
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
