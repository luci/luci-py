#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components.datastore_utils import monotonic
from support import test_case


class EntityX(ndb.Model):
  a = ndb.IntegerProperty()


class MonotonicTest(test_case.TestCase):
  def setUp(self):
    super(MonotonicTest, self).setUp()
    self.parent = ndb.Key('Root', 1)

  def test_insert(self):
    data = EntityX(id=1, parent=self.parent)
    actual = monotonic.insert(data, None)
    expected = ndb.Key('EntityX', 1, parent=self.parent)
    self.assertEqual(expected, actual)

  def test_insert_already_present(self):
    EntityX(id=1, parent=self.parent).put()
    data = EntityX(id=1, parent=self.parent)
    actual = monotonic.insert(data, None)
    self.assertEqual(None, actual)

  def test_insert_new_key(self):
    data = EntityX(id=1, parent=self.parent)
    actual = monotonic.insert(data, self.fail)
    expected = ndb.Key('EntityX', 1, parent=self.parent)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present(self):
    EntityX(id=1, parent=self.parent).put()
    data = EntityX(id=1, parent=self.parent)
    new_key = ndb.Key('EntityX', 2, parent=self.parent)
    actual = monotonic.insert(data, lambda: new_key)
    expected = ndb.Key('EntityX', 2, parent=self.parent)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present_twice(self):
    EntityX(id=1, parent=self.parent).put()
    EntityX(id=2, parent=self.parent).put()
    data = EntityX(id=1, parent=self.parent)
    new_keys = [
      ndb.Key('EntityX', 2, parent=self.parent),
      ndb.Key('EntityX', 3, parent=self.parent),
    ]
    actual = monotonic.insert(data, lambda: new_keys.pop(0))
    self.assertEqual([], new_keys)
    expected = ndb.Key('EntityX', 3, parent=self.parent)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present_twice_fail_after(self):
    EntityX(id=1, parent=self.parent).put()
    EntityX(id=2, parent=self.parent).put()
    EntityX(id=3, parent=self.parent).put()
    data = EntityX(id=1, parent=self.parent)
    new_keys = [
      ndb.Key('EntityX', 2, parent=self.parent),
      ndb.Key('EntityX', 3, parent=self.parent),
    ]
    actual = monotonic.insert(
        data, lambda: new_keys.pop(0) if new_keys else None)
    self.assertEqual([], new_keys)
    self.assertEqual(None, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
