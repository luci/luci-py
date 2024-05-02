#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.api import datastore_errors
from google.protobuf import struct_pb2

from test_support import test_case

from components import utils
from server import bq_state


class BotManagementTest(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(bq_state)
        if i[0] != '_' and hasattr(getattr(bq_state, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_cron_trigger_tasks(self):
    # Triggers tasks for a cron job, the very first time.
    # 2020-01-02T03:04:05.678900
    now = datetime.datetime(2020, 1, 2, 3, 4, 5, 678900)
    self.mock_now(now)
    urls = []
    table_name = 'mytable'
    now_rounded = datetime.datetime(*now.timetuple()[:5])
    # we want 10 items, so set up the recent time 14 minutes ago
    now_cuttof = now_rounded - datetime.timedelta(minutes=14)
    state = bq_state.BqState(id=table_name, ts=now_rounded, recent=now_cuttof)
    state.put()

    def enqueue_task(url, task_name):
      urls.append(url)
      self.assertEqual('tqname', task_name)
      return True
    self.mock(utils, 'enqueue_task', enqueue_task)
    actual = bq_state.cron_trigger_tasks(table_name, '/internal/taskqueue/foo/',
                                         'tqname', 100, 10)
    self.assertEqual(10, actual)
    self.assertEqual(1, bq_state.BqState.query().count())
    expected = {
        # Values are exclusive; they are the next values to process.
        'recent': datetime.datetime(2020, 1, 2, 3, 0),
        'ts': now,
    }
    self.assertEqual(expected, bq_state.BqState.get_by_id('mytable').to_dict())
    expected = [
        # Only backfill.
        '/internal/taskqueue/foo/2020-01-02T02:50',
        '/internal/taskqueue/foo/2020-01-02T02:51',
        '/internal/taskqueue/foo/2020-01-02T02:52',
        '/internal/taskqueue/foo/2020-01-02T02:53',
        '/internal/taskqueue/foo/2020-01-02T02:54',
        '/internal/taskqueue/foo/2020-01-02T02:55',
        '/internal/taskqueue/foo/2020-01-02T02:56',
        '/internal/taskqueue/foo/2020-01-02T02:57',
        '/internal/taskqueue/foo/2020-01-02T02:58',
        '/internal/taskqueue/foo/2020-01-02T02:59',
    ]
    self.assertEqual(expected, urls)

  def test_cron_trigger_tasks_zero(self):
    # 2020-01-02T03:04:05.678900
    now = datetime.datetime(2020, 1, 2, 3, 4, 5, 678900)
    self.mock_now(now)
    self.mock(utils, 'enqueue_task', self.fail)
    actual = bq_state.cron_trigger_tasks(
        'mytable', '/internal/taskqueue/foo/', 'tqname', 0, 10)
    self.assertEqual(0, actual)
    self.assertEqual(1, bq_state.BqState.query().count())
    expected = {
        'recent': datetime.datetime(2020, 1, 2, 3, 0),
        'ts': now,
    }
    self.assertEqual(expected, bq_state.BqState.get_by_id('mytable').to_dict())

  def test_send_to_bq_empty(self):
    # Empty, nothing is done. No need to mock the HTTP client.
    bq_state.send_to_bq('foo', [])

  def test_send_to_bq(self):
    payloads = []
    def json_request(url, method, payload, scopes, deadline):
      self.assertEqual(
          'https://www.googleapis.com/bigquery/v2/projects/sample-app/datasets/'
          'swarming/tables/foo/insertAll', url)
      payloads.append(payload)
      self.assertEqual('POST', method)
      self.assertEqual(bq_state.bqh.INSERT_ROWS_SCOPE, scopes)
      self.assertEqual(600, deadline)
      return {'insertErrors': []}
    self.mock(bq_state.net, 'json_request', json_request)

    rows = [
        ('key1', struct_pb2.Struct()),
        ('key2', struct_pb2.Struct()),
    ]
    bq_state.send_to_bq('foo', rows)
    expected = [
        {
            'ignoreUnknownValues': False,
            'kind': 'bigquery#tableDataInsertAllRequest',
            'skipInvalidRows': True,
        },
    ]
    actual_rows = payloads[0].pop('rows')
    self.assertEqual(expected, payloads)
    self.assertEqual(2, len(actual_rows))

  def test_send_to_bq_fail(self):
    # Test the failure code path.
    payloads = []
    def json_request(url, method, payload, scopes, deadline):
      self.assertEqual(
          'https://www.googleapis.com/bigquery/v2/projects/sample-app/datasets/'
            'swarming/tables/foo/insertAll',
          url)
      first = not payloads
      payloads.append(payload)
      self.assertEqual('POST', method)
      self.assertEqual(bq_state.bqh.INSERT_ROWS_SCOPE, scopes)
      self.assertEqual(600, deadline)
      # Return an error on the first call.
      if first:
        return {
            'insertErrors': [{
                'index': 0,
                'errors': [{
                    'reason': 'sadness',
                    'message': 'Oh gosh',
                },],
            },],
        }
      return {'insertErrors': []}
    self.mock(bq_state.net, 'json_request', json_request)

    rows = [
        ('key1', struct_pb2.Struct()),
        ('key2', struct_pb2.Struct()),
    ]
    with self.assertRaises(bq_state.BQError):
      bq_state.send_to_bq('foo', rows)

  def test_should_export(self):
    now = datetime.datetime(2020, 1, 2, 3, 4, 0, 0)
    minute = datetime.timedelta(seconds=60)

    self.assertTrue(bq_state.should_export('task_requests', now))

    bq_state.BqMigrationState(
        id='task_requests',
        python_to_go=now + 2 * minute,
    ).put()

    self.assertTrue(bq_state.should_export('task_requests', now))
    self.assertTrue(bq_state.should_export('task_requests', now + minute))
    self.assertFalse(bq_state.should_export('task_requests', now + 2 * minute))


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
