#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Disable 'Access to a protected member ...'. NDB uses '_' for other purposes.
# pylint: disable=W0212

import datetime
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from support import test_case


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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
