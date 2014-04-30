#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import sys
import unittest

import test_env
test_env.setup_test_env()

import stats
import stats_gviz

# From components/third_party/
import webtest

from components import stats_framework_mock
from support import test_case

# pylint: disable=R0201


class Store(stats.webapp2.RequestHandler):
  def get(self):
    """Generates fake stats."""
    stats.add_entry(stats.STORE, 2048, 'GS; inline')
    self.response.write('Yay')


class Return(stats.webapp2.RequestHandler):
  def get(self):
    """Generates fake stats."""
    stats.add_entry(stats.RETURN, 4096, 'memcache')
    self.response.write('Yay')


class Lookup(stats.webapp2.RequestHandler):
  def get(self):
    """Generates fake stats."""
    stats.add_entry(stats.LOOKUP, 200, 103)
    self.response.write('Yay')


class Dupe(stats.webapp2.RequestHandler):
  def get(self):
    """Generates fake stats."""
    stats.add_entry(stats.DUPE, 1024, 'inline')
    self.response.write('Yay')


def to_str(now, delta):
  """Converts a datetime to unicode."""
  now = now + datetime.timedelta(seconds=delta)
  return unicode(now.strftime(stats.utils.DATETIME_FORMAT))


class StatsTest(test_case.TestCase, stats_framework_mock.MockMixIn):
  def setUp(self):
    super(StatsTest, self).setUp()
    routes = [
        ('/store', Store),
        ('/return', Return),
        ('/lookup', Lookup),
        ('/dupe', Dupe),
        ('/generate_stats', stats.InternalStatsUpdateHandler),
        ('/results/days', stats_gviz.StatsGvizDaysHandler),
        ('/results/hours', stats_gviz.StatsGvizHoursHandler),
        ('/results/minutes', stats_gviz.StatsGvizMinutesHandler),
    ]
    real_app = stats.webapp2.WSGIApplication(routes, debug=True)
    self.app = webtest.TestApp(
        real_app, extra_environ={'REMOTE_ADDR': 'fake-ip'})
    stats_framework_mock.configure(self)
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now, 0)

  def _test_handler(self, url, added_data):
    stats_framework_mock.reset_timestamp(stats.STATS_HANDLER, self.now)

    self.assertEqual('Yay', self.app.get(url).body)

    self.assertEqual(
        1, len(list(stats.stats_framework.yield_entries(None, None))))

    self.mock_now(self.now, 60)
    headers = {'X-AppEngine-Cron': 'true'}
    resp = self.app.get('/generate_stats', headers=headers, status=200)
    self.assertEqual('Processed 10 minutes', resp.body)
    url = '/results/minutes?duration=1&now=2010-01-02 03:04:05'
    expected = {
      u'reqId': u'0',
      u'status': u'ok',
      u'table': {
        u'cols': [
          {
            u'id': u'key',
            u'label': u'Time',
            u'type': u'datetime',
          },
          {
            u'id': u'requests',
            u'label': u'Total',
            u'type': u'number',
          },
          {
            u'id': u'other_requests',
            u'label': u'Other',
            u'type': u'number',
          },
          {
            u'id': u'failures',
            u'label': u'Failures',
            u'type': u'number',
          },
          {
            u'id': u'uploads',
            u'label': u'Uploads',
            u'type': u'number',
          },
          {
            u'id': u'downloads',
            u'label': u'Downloads',
            u'type': u'number',
          },
          {
            u'id': u'contains_requests',
            u'label': u'Lookups',
            u'type': u'number',
          },
          {
            u'id': u'uploads_bytes',
            u'label': u'Uploaded',
            u'type': u'number',
          },
          {
            u'id': u'downloads_bytes',
            u'label': u'Downloaded',
            u'type': u'number',
          },
          {
            u'id': u'contains_lookups',
            u'label': u'Items looked up',
            u'type': u'number',
          },
        ],
        u'rows': [
          {
            u'c': [
              {u'v': u'Date(2010,0,2,3,4,0)'},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
              {u'v': 0},
            ],
          },
        ],
      },
      u'version': u'0.6',
    }
    # TODO(maruel): Use column names instead of indexes. The javascript should
    # be fixed at the same time.
    for i, v in added_data.iteritems():
      expected['table']['rows'][0]['c'][i]['v'] = v
    self.assertEqual(
        expected, self.app.get(url, headers={'X-DataSource-Auth': '1'}).json)

  def test_store(self):
    expected = {
      1: 1,
      4: 1,
      7: 2048,
    }
    self._test_handler('/store', expected)

  def test_return(self):
    expected = {
      1: 1,
      5: 1,
      8: 4096,
    }
    self._test_handler('/return', expected)

  def test_lookup(self):
    expected = {
      1: 1,
      6: 1,
      9: 200,
    }
    self._test_handler('/lookup', expected)

  def test_dupe(self):
    expected = {
      1: 1,
      2: 1,
    }
    self._test_handler('/dupe', expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
