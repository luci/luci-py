#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

import test_case
from components import utils


class Rambling(ndb.Model):
  """Fake statistics."""
  a = ndb.IntegerProperty()
  b = ndb.FloatProperty()
  c = ndb.DateTimeProperty()
  d = ndb.DateProperty()


class UtilsTest(test_case.TestCase):
  def test_smart_json(self):
    r = Rambling(
        a=2,
        b=0.2,
        c=datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
        d=datetime.date(2012, 1, 2))
    actual = utils.SmartJsonEncoder().encode([r])
    # Confirm that default is tight encoding and sorted keys.
    expected = (
        '[{"a":2,'
        '"b":0.2,'
        '"c":"2012-01-02 03:04:05",'
        '"d":"2012-01-02"'
        '}]')
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
