#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Disable 'Access to a protected member ...'. NDB uses '_' for other purposes.
# pylint: disable=W0212

import datetime
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import utils
from test_support import test_case


class Rambling(ndb.Model):
  """Fake statistics."""
  a = ndb.IntegerProperty()
  b = ndb.FloatProperty()
  c = ndb.DateTimeProperty()
  d = ndb.DateProperty()

  def to_dict(self):
    out = super(Rambling, self).to_dict()
    out['e'] = datetime.timedelta(seconds=1.1)
    out['f'] = '\xc4\xa9'
    return out


class UtilsTest(test_case.TestCase):
  def test_json(self):
    r = Rambling(
        a=2,
        b=0.2,
        c=datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
        d=datetime.date(2012, 1, 2))
    actual = utils.to_json_encodable([r])
    # Confirm that default is tight encoding and sorted keys.
    expected = [
      {
        'a': 2,
        'b': 0.2,
        'c': u'2012-01-02 03:04:05',
        'd': u'2012-01-02',
        'e': 1,
        'f': u'\u0129',
      },
    ]
    self.assertEqual(expected, actual)

    self.assertEqual([0, 1], utils.to_json_encodable(range(2)))
    self.assertEqual([0, 1], utils.to_json_encodable(i for i in (0, 1)))
    self.assertEqual([0, 1], utils.to_json_encodable(xrange(2)))

  def test_datetime_to_rfc2822(self):
    self.assertEqual(
      'Mon, 02 Jan 2012 03:04:05 -0000',
      utils.datetime_to_rfc2822(datetime.datetime(2012, 1, 2, 3, 4, 5)))

  def test_milliseconds_since_epoch(self):
    self.mock_now(datetime.datetime(1970, 1, 2, 3, 4, 5, 6789))
    delta = utils.milliseconds_since_epoch(None)
    self.assertEqual(97445007, delta)

  def test_cache(self):
    calls = []

    @utils.cache
    def get_me():
      calls.append(1)
      return len(calls)

    self.assertEqual(1, get_me())
    self.assertEqual(1, get_me())
    self.assertEqual(1, len(calls))

  def test_clear_cache(self):
    calls = []

    @utils.cache
    def get_me():
      calls.append(1)
      return len(calls)

    self.assertEqual(1, get_me())
    utils.clear_cache(get_me)
    self.assertEqual(2, get_me())
    self.assertEqual(2, len(calls))


class FakeNdbContext(object):
  def __init__(self):
    self.get_calls = []
    self.set_calls = []
    self.cached_value = None

  @ndb.tasklet
  def memcache_get(self, key):
    self.get_calls.append(key)
    raise ndb.Return(self.cached_value)

  @ndb.tasklet
  def memcache_set(self, key, value, time=None):
    self.cached_value = value
    self.set_calls.append((key, value, time))


class MemcacheTest(test_case.TestCase):

  def setUp(self):
    super(MemcacheTest, self).setUp()

    self.f_calls = []
    self.f_value = 'value'
    self.ctx = FakeNdbContext()
    self.mock(ndb, 'get_context', lambda: self.ctx)

  @utils.memcache('f', ['a', 'b', 'c', 'd'], time=54)
  def f(self, a, b, c=3, d=4, e=5):
    self.f_calls.append((a, b, c, d, e))
    return self.f_value

  @utils.memcache_async('f', ['a', 'b', 'c', 'd'], time=54)
  @ndb.tasklet
  def f_async(self, a, b, c=3, d=4, e=5):
    self.f_calls.append((a, b, c, d, e))
    raise ndb.Return(self.f_value)

  def test_async(self):
    self.f_async(1, 2, 3, 4, 5).get_result()
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [(1, 2, 3, 4, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 3, 4]', ('value',), 54)])

  def test_call(self):
    self.f(1, 2, 3, 4, 5)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [(1, 2, 3, 4, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 3, 4]', ('value',), 54)])

    self.ctx.get_calls = []
    self.f_calls = []
    self.ctx.set_calls = []
    self.f(1, 2, 3, 4)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [])
    self.assertEqual(self.ctx.set_calls, [])

  def test_none(self):
    self.f_value = None
    self.assertEqual(self.f(1, 2, 3, 4), None)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [(1, 2, 3, 4, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 3, 4]', (None,), 54)])

    self.ctx.get_calls = []
    self.f_calls = []
    self.ctx.set_calls = []
    self.assertEqual(self.f(1, 2, 3, 4), None)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [])
    self.assertEqual(self.ctx.set_calls, [])


  def test_call_without_optional_arg(self):
    self.f(1, 2)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [(1, 2, 3, 4, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 3, 4]', ('value',), 54)])

  def test_call_kwargs(self):
    self.f(1, 2, c=30, d=40)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 30, 40]'])
    self.assertEqual(self.f_calls, [(1, 2, 30, 40, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 30, 40]', ('value',), 54)])

  def test_call_all_kwargs(self):
    self.f(a=1, b=2, c=30, d=40)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 30, 40]'])
    self.assertEqual(self.f_calls, [(1, 2, 30, 40, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 30, 40]', ('value',), 54)])

  def test_call_packed_args(self):
    self.f(*[1, 2])
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 3, 4]'])
    self.assertEqual(self.f_calls, [(1, 2, 3, 4, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 3, 4]', ('value',), 54)])

  def test_call_packed_kwargs(self):
    self.f(1, 2, **{'c':30, 'd': 40})
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 30, 40]'])
    self.assertEqual(self.f_calls, [(1, 2, 30, 40, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 30, 40]', ('value',), 54)])

  def test_call_packed_both(self):
    self.f(*[1, 2], **{'c':30, 'd': 40})
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[1, 2, 30, 40]'])
    self.assertEqual(self.f_calls, [(1, 2, 30, 40, 5)])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[1, 2, 30, 40]', ('value',), 54)])

  def test_empty_key_arg(self):
    @utils.memcache('f')
    def f(a):
      # pylint: disable=unused-argument
      return 1

    f(1)
    self.assertEqual(self.ctx.get_calls, ['utils.memcache/v1a/f[]'])
    self.assertEqual(
        self.ctx.set_calls,
        [('utils.memcache/v1a/f[]', (1,), None)])

  def test_nonexisting_arg(self):
    with self.assertRaises(KeyError):
      # pylint: disable=unused-variable
      @utils.memcache('f', ['b'])
      def f(a):
        # pylint: disable=unused-argument
        pass

  def test_invalid_args(self):
    with self.assertRaises(TypeError):
      # pylint: disable=no-value-for-parameter
      self.f()

    with self.assertRaises(TypeError):
      # pylint: disable=no-value-for-parameter
      self.f(b=3)

    with self.assertRaises(TypeError):
      # pylint: disable=unexpected-keyword-arg
      self.f(1, 2, x=3)

  def test_args_prohibited(self):
    with self.assertRaises(NotImplementedError):
      # pylint: disable=unused-variable
      @utils.memcache('f', [])
      def f(a, *args):
        # pylint: disable=unused-argument
        pass

  def test_kwargs_prohibited(self):
    with self.assertRaises(NotImplementedError):
      # pylint: disable=unused-variable
      @utils.memcache('f', [])
      def f(**kwargs):
        # pylint: disable=unused-argument
        pass


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
