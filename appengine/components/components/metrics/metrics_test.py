#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import auth
from components import net
from components.metrics import metrics
from test_support import test_case


class DescriptorTest(test_case.TestCase):
  def test_to_from_dict(self):
    d1 = metrics.Descriptor(
        name='abc',
        description='Blah',
        labels={'A': '1', 'B': '2'},
        metric_type='gauge',
        value_type='int64')
    self.assertEqual([('A', '1'), ('B', '2')], d1.labels)
    self.assertEqual({
      'description': 'Blah',
      'labels': {'A': '1', 'B': '2'},
      'metric_type': 'gauge',
      'name': 'abc',
      'value_type': 'int64'
    }, d1.to_dict())
    d2 = metrics.Descriptor.from_dict(d1.to_dict())
    self.assertEqual(vars(d1), vars(d2))

  def test_validate_value_int(self):
    d = metrics.Descriptor(name='abc', description='Blah', value_type='int64')
    d.validate_value(1)
    d.validate_value(1L)
    with self.assertRaises(TypeError):
      d.validate_value(None)
    with self.assertRaises(TypeError):
      d.validate_value(1.0)
    with self.assertRaises(TypeError):
      d.validate_value('boo')

  def test_validate_value_double(self):
    d = metrics.Descriptor(name='abc', description='Blah', value_type='double')
    d.validate_value(1.0)
    with self.assertRaises(TypeError):
      d.validate_value(None)
    with self.assertRaises(TypeError):
      d.validate_value(1)
    with self.assertRaises(TypeError):
      d.validate_value(1L)
    with self.assertRaises(TypeError):
      d.validate_value('boo')

  def test_validate_labels(self):
    d = metrics.Descriptor(
        name='abc', description='Blah', labels={'A': 'a', 'B': 'b'})
    d.validate_labels({'A': 'a value', 'B': 'b value'})
    with self.assertRaises(TypeError):
      d.validate_labels(None)
    with self.assertRaises(TypeError):
      d.validate_labels({'A': 'a value'})
    with self.assertRaises(TypeError):
      d.validate_labels({'A': 'a value', 'B': 123})
    with self.assertRaises(TypeError):
      d.validate_labels({'A': 'a value', 'B': ''})


class BufferTest(test_case.TestCase):
  maxDiff = None

  EXPECTED_FLUSH_TASK = {
    'descriptors': {
      'name1': {
        'description': 'Desc 1',
        'labels': {'A': 'a', 'B': 'b'},
        'metric_type': 'gauge',
        'name': 'name1',
        'value_type': 'int64',
      },
      'name2': {
        'description': 'Desc 2',
        'labels': {'C': 'c', 'D': 'd'},
        'metric_type': 'gauge',
        'name': 'name2',
        'value_type': 'int64',
      },
      'name3': {
        'description': 'Desc 3',
        'labels': {},
        'metric_type': 'gauge',
        'name': 'name3',
        'value_type': 'double',
      },
    },
    'points': [
      {
        'point': {
          'start': 1420167845000000,
          'end': 1420167845000000,
          'value': 456,
        },
        'labels': ('1', '2'),
        'desc': 'name1',
      },
      {
        'point': {
          'start': 1420167845000000,
          'end': 1420167845000000,
          'value': 789,
        },
        'labels': ('3', '4'),
        'desc': 'name1',
      },
      {
        'point': {
          'start': 1420167845000000,
          'end': 1420167845000000,
          'value': 555,
        },
        'labels': ('x', 'z'),
        'desc': 'name2',
      },
      {
        'point': {
          'start': 1420167845000000,
          'end': 1420167845000000,
          'value': 3.0,
        },
        'labels': (),
        'desc': 'name3',
      },
    ],
  }

  def setUp(self):
    super(BufferTest, self).setUp()
    conf = metrics.MonitoringConfig()
    conf.project_id = '123'
    conf.service_account_key = auth.ServiceAccountKey(
        client_email='client_email',
        private_key='private_key',
        private_key_id='private_key_id')
    conf.store(updated_by=auth.Anonymous)

  def test_conflicting_descriptors(self):
    d1 = metrics.Descriptor('name', 'Description', labels={'A': 'a', 'B': 'b'})
    d2 = metrics.Descriptor('name', 'Description', labels={'A': 'a', 'B': 'b'})
    d3 = metrics.Descriptor('name', 'Description', labels={'C': 'c', 'D': 'd'})
    buf = metrics.Buffer()
    # No conflict.
    buf.set_gauge(d1, 0, {'A': 'blah', 'B': 'blah'})
    buf.set_gauge(d1, 0, {'A': 'blah 2', 'B': 'blah 2'})
    buf.set_gauge(d2, 0, {'A': 'blah 3', 'B': 'blah 3'})
    # Conflict: same metric name, different set of labels.
    with self.assertRaises(ValueError):
      buf.set_gauge(d3, 0, {'C': 'blah', 'D': 'blah'})

  def test_flush(self):
    self.mock_now(datetime.datetime(2015, 1, 2, 3, 4, 5))

    calls = []
    self.mock(metrics, '_enqueue_flush', lambda *args: calls.append(args))

    d1 = metrics.Descriptor('name1', 'Desc 1', labels={'A': 'a', 'B': 'b'})
    d2 = metrics.Descriptor('name2', 'Desc 2', labels={'C': 'c', 'D': 'd'})
    d3 = metrics.Descriptor('name3', 'Desc 3', value_type='double')

    buf = metrics.Buffer()
    # Overwrite.
    buf.set_gauge(d1, 123, labels={'A': '1', 'B': '2'})
    buf.set_gauge(d1, 456, labels={'A': '1', 'B': '2'})
    # Another label set.
    buf.set_gauge(d1, 789, labels={'A': '3', 'B': '4'})
    # Another descriptor.
    buf.set_gauge(d2, 555, labels={'C': 'x', 'D': 'z'})
    # Another metric type.
    buf.set_gauge(d3, 3.0)

    buf.flush(task_queue_name='task-queue')

    self.assertEqual(1, len(calls))
    self.assertEqual('task-queue', calls[0][1])
    self.assertEqual(self.EXPECTED_FLUSH_TASK, calls[0][0])

  def test_execute_flush(self):
    calls = []

    @ndb.tasklet
    def mocked_json_request_async(**kwargs):
      calls.append(kwargs)
    self.mock(net, 'json_request_async', mocked_json_request_async)

    # To test batching.
    self.mock(metrics, '_MAX_BATCH_SIZE', 3)

    service_account_key = auth.ServiceAccountKey(
        client_email='client_email',
        private_key='private_key',
        private_key_id='private_key_id')

    metrics._execute_flush(self.EXPECTED_FLUSH_TASK)
    self.assertEqual([
      # Register metrics.
      {
        'method': 'POST',
        'payload': {
          'description': 'Desc 1',
          'labels': [
            {
              'description': 'a',
              'key': 'custom.cloudmonitoring.googleapis.com/A',
            },
            {
              'description': 'b',
              'key': 'custom.cloudmonitoring.googleapis.com/B',
            },
          ],
          'name': 'custom.cloudmonitoring.googleapis.com/name1',
          'typeDescriptor': {'metricType': 'gauge', 'valueType': 'int64'},
        },
        'scopes': ['https://www.googleapis.com/auth/monitoring'],
        'service_account_key': service_account_key,
        'url':
            'https://www.googleapis.com/cloudmonitoring/v2beta2/'
            'projects/123/metricDescriptors',
      },
      {
        'method': 'POST',
        'payload': {
          'description': 'Desc 2',
          'labels': [
            {
              'description': 'c',
              'key': 'custom.cloudmonitoring.googleapis.com/C',
            },
            {
              'description': 'd',
              'key': 'custom.cloudmonitoring.googleapis.com/D',
            },
          ],
          'name': 'custom.cloudmonitoring.googleapis.com/name2',
          'typeDescriptor': {'metricType': 'gauge', 'valueType': 'int64'},
        },
        'scopes': ['https://www.googleapis.com/auth/monitoring'],
        'service_account_key': service_account_key,
        'url':
            'https://www.googleapis.com/cloudmonitoring/v2beta2/'
            'projects/123/metricDescriptors',
      },
      {
        'method': 'POST',
        'payload': {
          'description': 'Desc 3',
          'labels': [],
          'name': 'custom.cloudmonitoring.googleapis.com/name3',
          'typeDescriptor': {'metricType': 'gauge', 'valueType': 'double'},
        },
        'scopes': ['https://www.googleapis.com/auth/monitoring'],
        'service_account_key': service_account_key,
        'url':
            'https://www.googleapis.com/cloudmonitoring/v2beta2/'
            'projects/123/metricDescriptors',
      },
      # Sending actual values (first batch).
      {
        'method': 'POST',
        'payload': {
          'timeseries': [
            {
              'point': {
                'end': '2015-01-02T03:04:05Z',
                'int64Value': 456,
                'start': '2015-01-02T03:04:05Z',
              },
              'timeseriesDesc': {
                'labels': {
                  'custom.cloudmonitoring.googleapis.com/A': '1',
                  'custom.cloudmonitoring.googleapis.com/B': '2',
                },
                'metric': 'custom.cloudmonitoring.googleapis.com/name1',
              },
            },
            {
              'point': {
                'int64Value': 789,
                'end': '2015-01-02T03:04:05Z',
                'start': '2015-01-02T03:04:05Z',
              },
              'timeseriesDesc': {
                'labels': {
                  'custom.cloudmonitoring.googleapis.com/A': '3',
                  'custom.cloudmonitoring.googleapis.com/B': '4',
                },
                'metric': 'custom.cloudmonitoring.googleapis.com/name1',
              },
            },
            {
              'point': {
                'int64Value': 555,
                'end': '2015-01-02T03:04:05Z',
                'start': '2015-01-02T03:04:05Z',
              },
              'timeseriesDesc': {
                'labels': {
                  'custom.cloudmonitoring.googleapis.com/C': 'x',
                  'custom.cloudmonitoring.googleapis.com/D': 'z',
                },
                'metric': 'custom.cloudmonitoring.googleapis.com/name2',
              },
            },
          ],
        },
        'scopes': ['https://www.googleapis.com/auth/monitoring'],
        'service_account_key': service_account_key,
        'url':
            'https://www.googleapis.com/cloudmonitoring/v2beta2/'
            'projects/123/timeseries:write',
      },
      # Second batch.
      {
        'method': 'POST',
        'payload': {
          'timeseries': [
            {
              'point': {
                'doubleValue': 3.0,
                'end': '2015-01-02T03:04:05Z',
                'start': '2015-01-02T03:04:05Z',
              },
              'timeseriesDesc': {
                'labels': {},
                'metric': 'custom.cloudmonitoring.googleapis.com/name3',
              },
            },
          ],
        },
        'scopes': ['https://www.googleapis.com/auth/monitoring'],
        'service_account_key': service_account_key,
        'url':
            'https://www.googleapis.com/cloudmonitoring/v2beta2/'
            'projects/123/timeseries:write',
      }], calls)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
