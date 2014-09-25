#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import random
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.api import datastore_errors

from components import utils
from server import task_common
from server import task_scheduler
from support import test_case


def _gen_request_data(properties=None, **kwargs):
  base_data = {
    'name': 'Request name',
    'user': 'Jesus',
    'properties': {
      'commands': [[u'command1']],
      'data': [],
      'dimensions': {},
      'env': {},
      'execution_timeout_secs': 24*60*60,
      'io_timeout_secs': None,
    },
    'priority': 50,
    'scheduling_expiration_secs': 60,
    'tags': [u'tag1'],
  }
  base_data.update(kwargs)
  base_data['properties'].update(properties or {})
  return base_data


class TaskCommonApiTest(test_case.TestCase):
  def setUp(self):
    super(TaskCommonApiTest, self).setUp()
    self.testbed.init_search_stub()

  def test_all_apis_are_tested(self):
    # Ensures there's a test for each public API.
    module = task_common
    expected = frozenset(
        i for i in dir(module)
        if i[0] != '_' and hasattr(getattr(module, i), 'func_name'))
    missing = expected - frozenset(
        i[5:] for i in dir(self) if i.startswith('test_'))
    self.assertFalse(missing - frozenset(['utcnow']))

  def test_validate_priority(self):
    with self.assertRaises(TypeError):
      task_common.validate_priority('1')
    with self.assertRaises(datastore_errors.BadValueError):
      task_common.validate_priority(-1)
    with self.assertRaises(datastore_errors.BadValueError):
      task_common.validate_priority(task_common.MAXIMUM_PRIORITY+1)
    task_common.validate_priority(0)
    task_common.validate_priority(1)
    task_common.validate_priority(task_common.MAXIMUM_PRIORITY)

  def test_milliseconds_since_epoch(self):
    self.mock_now(datetime.datetime(1970, 1, 2, 3, 4, 5, 6789))
    delta = task_common.milliseconds_since_epoch(None)
    self.assertEqual(97445007, delta)

  def test_match_dimensions(self):
    data_true = (
      ({}, {}),
      ({}, {'a': 'b'}),
      ({'a': 'b'}, {'a': 'b'}),
      ({'os': 'amiga'}, {'os': ['amiga', 'amiga-3.1']}),
      ( {'os': 'amiga', 'foo': 'bar'},
        {'os': ['amiga', 'amiga-3.1'], 'a': 'b', 'foo': 'bar'}),
    )

    for request_dimensions, bot_dimensions in data_true:
      self.assertEqual(
          True,
          task_common.match_dimensions(request_dimensions, bot_dimensions))

    data_false = (
      ({'os': 'amiga'}, {'os': ['Win', 'Win-3.1']}),
    )
    for request_dimensions, bot_dimensions in data_false:
      self.assertEqual(
          False,
          task_common.match_dimensions(request_dimensions, bot_dimensions))

  def test_pack_result_summary_key(self):
    def getrandbits(i):
      self.assertEqual(i, 8)
      return 0x02
    self.mock(random, 'getrandbits', getrandbits)
    self.mock_now(utils.EPOCH, 3)

    _request, result_summary = task_scheduler.make_request(_gen_request_data())
    bot_dimensions = {'hostname': 'localhost'}
    _request, to_run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')

    actual = task_common.pack_result_summary_key(result_summary.key)
    # 0xbb8 = 3000ms = 3 secs; 0x02 = random;  0x00 = try_number, e.g. it is a
    # TaskResultSummary.
    self.assertEqual('bb80200', actual)

    with self.assertRaises(AssertionError):
      task_common.pack_result_summary_key(to_run_result.key)

  def test_pack_run_result_key(self):
    def getrandbits(i):
      self.assertEqual(i, 8)
      return 0x02
    self.mock(random, 'getrandbits', getrandbits)
    self.mock_now(utils.EPOCH, 3)

    _request, result_summary = task_scheduler.make_request(_gen_request_data())
    bot_dimensions = {'hostname': 'localhost'}
    _request, to_run_result = task_scheduler.bot_reap_task(
        bot_dimensions, 'localhost', 'abc')

    actual = task_common.pack_run_result_key(to_run_result.key)
    # 0xbb8 = 3000ms = 3 secs; 0x02 = random;  0x01 = try_number, e.g. it is a
    # TaskRunResult.
    self.assertEqual('bb80201', actual)

    with self.assertRaises(AssertionError):
      task_common.pack_run_result_key(result_summary.key)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
