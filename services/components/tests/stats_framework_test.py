#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import calendar
import datetime
import sys
import time
import unittest

import test_env
test_env.setup_test_env()

import webapp2
from google.appengine.ext import ndb

from components import stats_framework
from components import stats_framework_mock

# From tools/third_party/
import webtest

# For TestCase.
import test_case


# pylint: disable=W0212


class InnerSnapshot(ndb.Model):
  c = ndb.StringProperty(default='', indexed=False)

  def accumulate(self, rhs):
    return stats_framework.accumulate(self, rhs)


class Snapshot(ndb.Model):
  """Fake statistics."""
  requests = ndb.IntegerProperty(default=0, indexed=False)
  b = ndb.FloatProperty(default=0, indexed=False)
  inner = ndb.LocalStructuredProperty(InnerSnapshot)

  def __init__(self, **kwargs):
    # This is the recommended way to use ndb.LocalStructuredProperty inside a
    # snapshot.
    #
    # Warning: The only reason it works is because Snapshot is itself inside a
    # ndb.LocalStructuredProperty.
    kwargs.setdefault('inner', InnerSnapshot())
    super(Snapshot, self).__init__(**kwargs)

  def accumulate(self, rhs):
    # accumulate() specifically handles where default value is a class instance.
    return stats_framework.accumulate(self, rhs)


def get_now():
  """Returns an hard coded 'utcnow'.

  It is important because the timestamp near the hour or day limit could induce
  the creation of additional 'hours' stats, which would make the test flaky.
  """
  return datetime.datetime(2010, 1, 2, 3, 4, 5, 6)


def strip_seconds(timestamp):
  """Returns timestamp with seconds and microseconds stripped."""
  return datetime.datetime(*timestamp.timetuple()[:5], second=0)


class StatsFrameworkTest(test_case.TestCase, stats_framework_mock.MockMixIn):
  def test_empty(self):
    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, self.fail)

    self.assertEqual(0, handler.stats_root_cls.query().count())
    self.assertEqual(0, handler.stats_day_cls.query().count())
    self.assertEqual(0, handler.stats_hour_cls.query().count())
    self.assertEqual(0, handler.stats_minute_cls.query().count())

  def test_too_recent(self):
    # Other tests assumes TOO_RECENT == 1. Update accordingly if needed.
    self.assertEquals(1, stats_framework.TOO_RECENT)

  def test_framework_empty(self):
    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, self.fail)
    now = get_now()
    self.mock_now(now, 0)
    handler._set_last_processed_time(strip_seconds(now))
    i = handler.process_next_chunk(0)
    self.assertEqual(0, i)
    self.assertEqual(1, handler.stats_root_cls.query().count())
    self.assertEqual(0, handler.stats_day_cls.query().count())
    self.assertEqual(0, handler.stats_hour_cls.query().count())
    self.assertEqual(0, handler.stats_minute_cls.query().count())
    root = handler.root_key.get()
    self.assertEqual(strip_seconds(now), root.timestamp)

  def test_framework_fresh(self):
    # Ensures the processing will run for 120 minutes starting
    # StatisticsFramework.MAX_BACKTRACK days ago.
    called = []

    def gen_data(start, end):
      """Returns fake statistics."""
      self.assertEqual(start + 60, end)
      called.append(start)
      return Snapshot(
          requests=1, b=1, inner=InnerSnapshot(c='%d,' % len(called)))

    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, gen_data)

    now = get_now()
    self.mock_now(now, 0)
    start_date = now - datetime.timedelta(
        days=stats_framework.StatisticsFramework.MAX_BACKTRACK)
    limit = handler.MAX_MINUTES_PER_PROCESS

    i = handler.process_next_chunk(5)
    self.assertEqual(limit, i)

    # Fresh new stats gathering always starts at midnight.
    midnight = datetime.datetime(*start_date.date().timetuple()[:3])
    expected_calls = [
      calendar.timegm((midnight + datetime.timedelta(minutes=i)).timetuple())
      for i in range(limit)
    ]
    self.assertEqual(expected_calls, called)

    # Verify root.
    root = handler.stats_root_cls.query().fetch()
    self.assertEqual(1, len(root))
    # When timestamp is not set, it starts at the begining of the day,
    # MAX_BACKTRACK days ago, then process MAX_MINUTES_PER_PROCESS.
    timestamp = midnight + datetime.timedelta(seconds=(limit - 1)*60)
    expected = {
      'created': now,
      'timestamp': timestamp,
    }
    self.assertEqual(expected, root[0].to_dict())

    # Verify days.
    expected = [
      {
        'key': midnight.date(),
        'values': {
          'requests': limit,
          'b': float(limit),
          'inner': {
            'c': u''.join('%d,' % i for i in xrange(1, limit + 1)),
          },
        },
      }
    ]
    days = handler.stats_day_cls.query().fetch()
    self.assertEqual(expected, [d.to_dict() for d in days])
    # These are left out from .to_dict().
    self.assertEqual(now, days[0].created)
    self.assertEqual(now, days[0].modified)
    self.assertEqual(3, days[0].hours_bitmap)

    # Verify hours.
    expected = [
      {
        'key': (midnight + datetime.timedelta(seconds=i*60*60)),
        'values': {
          'requests': 60,
          'b': 60.,
          'inner': {
            'c': u''.join(
                '%d,' % i for i in xrange(60 * i + 1, 60 * (i + 1) + 1)),
          },
        },
      }
      for i in range(limit / 60)
    ]
    hours = handler.stats_hour_cls.query().fetch()
    self.assertEqual(expected, [d.to_dict() for d in hours])
    for h in hours:
      # These are left out from .to_dict().
      self.assertEqual(now, h.created)
      self.assertEqual((1<<60)-1, h.minutes_bitmap)

    # Verify minutes.
    expected = [
      {
        'key': (midnight + datetime.timedelta(seconds=i*60)),
        'values': {
          'requests': 1,
          'b': 1.,
          'inner': {
            'c': u'%d,' % (i + 1),
          },
        },
      } for i in range(limit)
    ]
    minutes = handler.stats_minute_cls.query().fetch()
    self.assertEqual(expected, [d.to_dict() for d in minutes])
    for m in minutes:
      # These are left out from .to_dict().
      self.assertEqual(now, m.created)

  def test_framework_last_few(self):
    called = []

    def gen_data(start, end):
      """Returns fake statistics."""
      self.assertEqual(start + 60, end)
      called.append(start)
      return Snapshot(
          requests=1, b=1, inner=InnerSnapshot(c='%d,' % len(called)))

    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, gen_data)

    now = get_now()
    self.mock_now(now, 0)
    handler._set_last_processed_time(
        strip_seconds(now) - datetime.timedelta(seconds=3*60))
    i = handler.process_next_chunk(1)
    self.assertEqual(2, i)
    self.assertEqual(1, handler.stats_root_cls.query().count())
    self.assertEqual(1, handler.stats_day_cls.query().count())
    self.assertEqual(1, handler.stats_hour_cls.query().count())
    self.assertEqual(2, handler.stats_minute_cls.query().count())
    root = handler.root_key.get()
    self.assertEqual(
        strip_seconds(now) - datetime.timedelta(seconds=60), root.timestamp)

    # Trying to process more won't do anything.
    i = handler.process_next_chunk(1)
    self.assertEqual(0, i)
    root = handler.root_key.get()
    self.assertEqual(
        strip_seconds(now) - datetime.timedelta(seconds=60), root.timestamp)

    out = stats_framework.generate_stats_data(100, 100, 100, now, handler)
    for key in ('days', 'hours', 'minutes'):
      out[key] = [i.to_dict() for i in out[key]]
    expected = {
      'days': [
        {
          'key': now.date(),
          'values': {'requests': 0, 'b': 0.0, 'inner': {'c': u''}},
        },
      ],
      'hours': [
        {
          'key': datetime.datetime(*now.timetuple()[:4]),
          'values': {'requests': 2, 'b': 2.0, 'inner': {'c': u'1,2,'}},
        },
      ],
      'minutes': [
        {
          'key': datetime.datetime(
              *(now - datetime.timedelta(seconds=60)).timetuple()[:5]),
          'values': {'requests': 1, 'b': 1.0, 'inner': {'c': u'2,'}},
        },
        {
          'key': datetime.datetime(
              *(now - datetime.timedelta(seconds=120)).timetuple()[:5]),
          'values': {'requests': 1, 'b': 1.0, 'inner': {'c': u'1,'}},
        },
      ],
      'now': '2010-01-02 03:04:05',
    }
    self.assertEqual(expected, out)

    # Limit the number of items returned.
    out = stats_framework.generate_stats_data(
        0, 0, 1, now - datetime.timedelta(seconds=60), handler)
    for key in ('days', 'hours', 'minutes'):
      out[key] = [i.to_dict() for i in out[key]]
    expected = {
      'days': [],
      'hours': [],
      'minutes': [
        {
          'key': datetime.datetime(
              *(now - datetime.timedelta(seconds=60)).timetuple()[:5]),
          'values': {'requests': 1, 'b': 1.0, 'inner': {'c': u'2,'}},
        },
      ],
      'now': '2010-01-02 03:03:05',
    }
    self.assertEqual(expected, out)

  def test_keys(self):
    handler = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, self.fail)
    date = datetime.datetime(2010, 1, 2)
    self.assertEqual(
        ndb.Key('StatsRoot', 'test_framework', 'StatsDay', '2010-01-02'),
        handler.day_key(date.date()))

    self.assertEqual(
        ndb.Key(
          'StatsRoot', 'test_framework',
          'StatsDay', '2010-01-02',
          'StatsHour', '00'),
        handler.hour_key(date))
    self.assertEqual(
        ndb.Key(
          'StatsRoot', 'test_framework',
          'StatsDay', '2010-01-02',
          'StatsHour', '00',
          'StatsMinute', '00'),
        handler.minute_key(date))

  def test_yield_empty(self):
    self.testbed.init_modules_stub()
    self.assertEqual(
        0, len(list(stats_framework.yield_entries(None, None))))


def generate_snapshot(start_time, end_time):
  values = Snapshot()
  for entry in stats_framework.yield_entries(start_time, end_time):
    values.requests += 1
    for l in entry.entries:
      values.inner.c += l
  return values


class StatsFrameworkLogTest(test_case.TestCase, stats_framework_mock.MockMixIn):
  def setUp(self):
    super(StatsFrameworkLogTest, self).setUp()
    stats_framework_mock.configure(self)
    self.h = stats_framework.StatisticsFramework(
        'test_framework', Snapshot, generate_snapshot)

    # pylint: disable=E0213
    class GenerateHandler(webapp2.RequestHandler):
      def get(self2):
        stats_framework.add_entry('Hello')
        self2.response.write('Yay')

    class JsonHandler(webapp2.RequestHandler):
      def get(self2):
        self2.response.headers['Content-Type'] = (
            'application/json; charset=utf-8')
        data = stats_framework.generate_stats_data_from_request(
            self2.request, self.h)
        self2.response.write(stats_framework.utils.encode_to_json(data))

    routes = [
        ('/generate', GenerateHandler),
        ('/json', JsonHandler),
    ]
    real_app = webapp2.WSGIApplication(routes, debug=True)
    self.app = webtest.TestApp(
        real_app, extra_environ={'REMOTE_ADDR': 'fake-ip'})
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now, 0)

  def test_yield_entries(self):
    stats_framework_mock.reset_timestamp(self.h, self.now)

    self.assertEqual(
        0, len(list(stats_framework.yield_entries(None, None))))
    self.assertEqual(
        0, len(list(stats_framework.yield_entries(1, time.time()))))

    self.assertEqual('Yay', self.app.get('/generate').body)

    self.assertEqual(
        1, len(list(stats_framework.yield_entries(None, None))))
    self.assertEqual(
        1, len(list(stats_framework.yield_entries(1, time.time()))))
    self.assertEqual(
        0, len(list(stats_framework.yield_entries(
          None, stats_framework_mock.now_epoch()))))

  def test_json_empty(self):
    stats_framework_mock.reset_timestamp(self.h, self.now)
    expected = {
      u'days': [],
      u'hours': [],
      u'minutes': [],
      u'now': u'2010-01-02 03:04:05',
    }
    self.assertEqual(expected, self.app.get('/json').json)

  def test_json_empty_processed(self):
    stats_framework_mock.reset_timestamp(self.h, self.now)
    self.h.process_next_chunk(0)
    expected = {
      u'days': [
        {
          u'key': u'2010-01-02',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'hours': [
        {
          u'key': u'2010-01-02 03:00:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:00:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'minutes': [
        {
          u'key': u'2010-01-02 03:04:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 03:03:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 03:02:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 03:01:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 03:00:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:59:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:58:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:57:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:56:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
        {
          u'key': u'2010-01-02 02:55:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'now': u'2010-01-02 03:04:05',
    }
    self.assertEqual(expected, self.app.get('/json').json)

  def test_json_empty_processed_limit(self):
    stats_framework_mock.reset_timestamp(self.h, self.now)
    self.h.process_next_chunk(0)
    expected = {
      u'days': [
      ],
      u'hours': [
        {
          u'key': u'2010-01-02 03:00:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'minutes': [
        {
          u'key': u'2010-01-02 03:04:00',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'now': u'2010-01-02 03:04:05',
    }
    self.assertEqual(
        expected, self.app.get('/json?days=0&hours=1&minutes=1').json)

  def test_json_two(self):
    stats_framework_mock.reset_timestamp(self.h, self.now)
    self.assertEqual('Yay', self.app.get('/generate').body)
    self.assertEqual('Yay', self.app.get('/generate').body)
    self.h.process_next_chunk(0)
    expected = {
      u'days': [
        {
          u'key': u'2010-01-02',
          u'values': {u'b': 0.0, u'inner': {u'c': u''}, u'requests': 0},
        },
      ],
      u'hours': [
        {
          u'key': u'2010-01-02 03:00:00',
          u'values': {
            u'b': 0.0,
            u'inner': {u'c': u'HelloHello'},
            u'requests': 2,
          },
        },
      ],
      u'minutes': [
        {
          u'key': u'2010-01-02 03:04:00',
          u'values': {
            u'b': 0.0,
            u'inner': {u'c': u'HelloHello'},
            u'requests': 2,
          },
        },
      ],
      u'now': u'2010-01-02 03:04:05',
    }
    self.assertEqual(
        expected, self.app.get('/json?days=1&hours=1&minutes=1').json)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
