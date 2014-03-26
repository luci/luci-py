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

from components import stats_framework_mock

# From tools/third_party/
import webtest

# For TestCase.
import test_case


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
        ('/results', stats.StatsJsonHandler),
    ]
    real_app = stats.webapp2.WSGIApplication(routes, debug=True)
    self.app = webtest.TestApp(
        real_app, extra_environ={'REMOTE_ADDR': 'fake-ip'})
    stats_framework_mock.configure(self)
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now, 0)

  def _test_handler(self, url, added_data):
    stats_framework_mock.reset_timestamp(stats.get_stats_handler(), self.now)

    self.assertEqual('Yay', self.app.get(url).body)

    self.assertEqual(
        1, len(list(stats.stats_framework.yield_entries(None, None))))

    self.mock_now(self.now, 60)
    self.assertEqual(
        'Processed 10 minutes', self.app.get('/generate_stats').body)
    url = '/results?days=0&hours=0&minutes=1&now=2010-01-02 03:04:05'
    minute = {
      u'key': u'2010-01-02 03:04:00',
      u'values': {
        u'contains_lookups': 0,
        u'contains_requests': 0,
        u'downloads': 0,
        u'downloads_bytes': 0,
        u'failures': 0,
        u'requests': 1,
        u'uploads': 0,
        u'uploads_bytes': 0,
      },
    }
    minute['values'].update(added_data)
    expected = {
      u'days': [],
      u'hours': [],
      u'minutes': [minute],
      u'now': u'2010-01-02 03:04:05',
    }
    self.assertEqual(expected, self.app.get(url).json)

  def test_store(self):
    expected = {
      u'uploads': 1,
      u'uploads_bytes': 2048,
    }
    self._test_handler('/store', expected)

  def test_return(self):
    expected = {
      u'downloads': 1,
      u'downloads_bytes': 4096,
    }
    self._test_handler('/return', expected)

  def test_lookup(self):
    expected = {
      u'contains_lookups': 200,
      u'contains_requests': 1,
    }
    self._test_handler('/lookup', expected)

  def test_dupe(self):
    # It is currently ignored.
    expected = {
    }
    self._test_handler('/dupe', expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
