#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import hashlib
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

# For TestCase.
import test_case

from components import datastore_utils


class Entity(ndb.Model):
  a = ndb.IntegerProperty()


def int_ceil_div(value, divisor):
  """Returns the ceil() value of a integer based division."""
  return (value + divisor - 1) / divisor


class ShardingTest(test_case.TestCase):
  def test_shard_key(self):
    actual = datastore_utils.shard_key('1234', 2, 'Root')
    expected = "Key('Root', '12')"
    self.assertEqual(expected, str(actual))

  def test_hashed_shard_key(self):
    actual = datastore_utils.hashed_shard_key('1234', 2, 'Root')
    expected = "Key('Root', '%s')" % hashlib.md5('1234').hexdigest()[:2]
    self.assertEqual(expected, str(actual))

  def test_insert(self):
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    actual = datastore_utils.insert(data, None)
    expected = ndb.Key('Root', '1', 'Entity', 1)
    self.assertEqual(expected, actual)

  def test_insert_already_present(self):
    Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root')).put()
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    actual = datastore_utils.insert(data, None)
    self.assertEqual(None, actual)

  def test_insert_new_key(self):
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    actual = datastore_utils.insert(data, self.fail)
    expected = ndb.Key('Root', '1', 'Entity', 1)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present(self):
    Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root')).put()
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    new_key = ndb.Key(
        'Entity', 2, parent=datastore_utils.shard_key('2', 1, 'Root'))
    actual = datastore_utils.insert(data, lambda: new_key)
    expected = ndb.Key('Root', '2', 'Entity', 2)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present_twice(self):
    Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root')).put()
    Entity(id=2, parent=datastore_utils.shard_key('2', 1, 'Root')).put()
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    new_keys = [
      ndb.Key('Entity', 2, parent=datastore_utils.shard_key('2', 1, 'Root')),
      ndb.Key('Entity', 3, parent=datastore_utils.shard_key('3', 1, 'Root')),
    ]
    actual = datastore_utils.insert(data, lambda: new_keys.pop(0))
    self.assertEqual([], new_keys)
    expected = ndb.Key('Root', '3', 'Entity', 3)
    self.assertEqual(expected, actual)

  def test_insert_new_key_already_present_twice_fail_after(self):
    Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root')).put()
    Entity(id=2, parent=datastore_utils.shard_key('2', 1, 'Root')).put()
    Entity(id=3, parent=datastore_utils.shard_key('3', 1, 'Root')).put()
    data = Entity(id=1, parent=datastore_utils.shard_key('1', 1, 'Root'))
    new_keys = [
      ndb.Key('Entity', 2, parent=datastore_utils.shard_key('2', 1, 'Root')),
      ndb.Key('Entity', 3, parent=datastore_utils.shard_key('3', 1, 'Root')),
    ]
    actual = datastore_utils.insert(
        data, lambda: new_keys.pop(0) if new_keys else None)
    self.assertEqual([], new_keys)
    self.assertEqual(None, actual)

  def test_pop_future(self):
    items = [ndb.Future() for _ in xrange(5)]
    items[1].set_result(None)
    items[3].set_result('foo')
    inputs = items[:]
    datastore_utils.pop_future_done(inputs)
    self.assertEqual([items[0], items[2], items[4]], inputs)

  def test_page_queries(self):
    for i in range(40):
      Entity(id=i, a=i%4).put()
    queries = [
      Entity.query(),
      Entity.query(Entity.a == 1),
      Entity.query(Entity.a == 2),
    ]
    actual = list(datastore_utils.page_queries(queries))

    # The order won't be deterministic. The only important this is that exactly
    # all the items are returned as chunks.
    expected = [
      [Entity(id=i, a=1) for i in xrange(1, 40, 4)],
      [Entity(id=i, a=2) for i in xrange(2, 42, 4)],
      [Entity(id=i, a=i%4) for i in xrange(1, 21)],
      [Entity(id=i, a=i%4) for i in xrange(21, 40)],
    ]
    self.assertEqual(len(expected), len(actual))
    for line in actual:
      # Items may be returned out of order.
      try:
        i = expected.index(line)
      except ValueError:
        self.fail('%s not found in %s' % (line, actual))
      self.assertEqual(expected.pop(i), line)

  def test_incremental_map(self):
    for i in range(40):
      Entity(id=i, a=i%4).put()
    queries = [
      Entity.query(),
      Entity.query(Entity.a == 1),
      Entity.query(Entity.a == 2),
    ]
    actual = []
    # Use as much default arguments as possible.
    datastore_utils.incremental_map(queries, actual.append)

    # The order won't be deterministic. The only important this is that exactly
    # all the items are returned as chunks and there is 3 chunks.
    expected = sorted(
        [Entity(id=i, a=1) for i in xrange(1, 40, 4)] +
        [Entity(id=i, a=2) for i in xrange(2, 42, 4)] +
        [Entity(id=i, a=i%4) for i in xrange(1, 21)] +
        [Entity(id=i, a=i%4) for i in xrange(21, 40)],
        key=lambda x: (x.key.id, x.to_dict()))
    map_page_size = 20
    self.assertEqual(int_ceil_div(len(expected), map_page_size), len(actual))
    actual = sorted(sum(actual, []), key=lambda x: (x.key.id, x.to_dict()))
    self.assertEqual(expected, actual)

  def test_incremental_map_throttling(self):
    for i in range(40):
      Entity(id=i, a=i%4).put()
    queries = [
      Entity.query(),
      Entity.query(Entity.a == 1),
      Entity.query(Entity.a == 2),
    ]
    actual = []
    def map_fn(items):
      actual.extend(items)
      # Note that it is returning more Future than what is called. It's fine.
      for _ in xrange(len(items) * 5):
        n = ndb.Future('yo dawg')
        # TODO(maruel): It'd be nice to not set them completed right away to
        # have better code coverage but I'm not sure how to do this.
        n.set_result('yo')
        yield n

    def filter_fn(item):
      return item.a == 2

    datastore_utils.incremental_map(
        queries=queries,
        map_fn=map_fn,
        filter_fn=filter_fn,
        max_inflight=1,
        map_page_size=2,
        fetch_page_size=3)

    # The order won't be deterministic so sort it.
    expected = sorted(
        [Entity(id=i, a=2) for i in xrange(2, 42, 4)] * 2,
        key=lambda x: (x.key.id, x.to_dict()))
    actual.sort(key=lambda x: (x.key.id, x.to_dict()))
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
