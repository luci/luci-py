#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components.datastore_utils import monotonic
from components.datastore_utils import txn
from test_support import test_case


# Access to a protected member _XX of a client class - pylint: disable=W0212


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

  def test_insert_transaction_failure(self):
    EntityX(id=1, parent=self.parent).put()
    calls = []
    def transaction_async(*args, **kwargs):
      calls.append(1)
      if len(calls) < 2:
        raise txn.CommitError()
      return old_transaction_async(*args, **kwargs)

    old_transaction_async = self.mock(
        txn, 'transaction_async', transaction_async)

    actual = monotonic.insert(EntityX(id=2, parent=self.parent))
    expected = ndb.Key('EntityX', 2, parent=self.parent)
    self.assertEqual(expected, actual)
    self.assertEqual([1, 1], calls)

  def test_get_versioned_root_model(self):
    cls = monotonic.get_versioned_root_model('fidoula')
    self.assertEqual('fidoula', cls._get_kind())
    self.assertTrue(issubclass(cls, ndb.Model))
    self.assertEqual(53, cls(current=53).current)

  def test_get_versioned_most_recent(self):
    # First entity id is HIGH_KEY_ID, second is HIGH_KEY_ID-1.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent_key = ndb.Key(cls, 'foo')
    for i in (monotonic.HIGH_KEY_ID, monotonic.HIGH_KEY_ID-1):
      monotonic.store_new_version(EntityX(parent=parent_key), cls)
      actual = monotonic.get_versioned_most_recent(EntityX, parent_key)
      expected = EntityX(key=ndb.Key('EntityX', i, parent=parent_key))
      self.assertEqual(expected, actual)

  def test_get_versioned_most_recent_with_root(self):
    # First entity id is HIGH_KEY_ID, second is HIGH_KEY_ID-1.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent_key = ndb.Key(cls, 'foo')
    for i in (monotonic.HIGH_KEY_ID, monotonic.HIGH_KEY_ID-1):
      monotonic.store_new_version(EntityX(parent=parent_key), cls)
      actual = monotonic.get_versioned_most_recent_with_root(
          EntityX, parent_key)
      expected = (
        cls(key=parent_key, current=i),
        EntityX(key=ndb.Key('EntityX', i, parent=parent_key)),
      )
      self.assertEqual(expected, actual)

  def test_get_versioned_most_recent_with_root_already_saved(self):
    # Stores the root entity with .current == None.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent_key = ndb.Key(cls, 'foo')
    cls(key=parent_key).put()
    monotonic.store_new_version(EntityX(parent=parent_key), cls)

    actual = monotonic.get_versioned_most_recent_with_root(EntityX, parent_key)
    expected = (
      cls(key=parent_key, current=monotonic.HIGH_KEY_ID),
      EntityX(key=ndb.Key('EntityX', monotonic.HIGH_KEY_ID, parent=parent_key)),
    )
    self.assertEqual(expected, actual)

  def test_get_versioned_most_recent_with_root_already_saved_invalid(self):
    # Stores the root entity with an invalid .current value.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent_key = ndb.Key(cls, 'foo')
    cls(key=parent_key, current=23).put()
    monotonic.store_new_version(EntityX(parent=parent_key), cls)

    actual = monotonic.get_versioned_most_recent_with_root(EntityX, parent_key)
    expected = (
      cls(key=parent_key, current=23),
      EntityX(key=ndb.Key('EntityX', 23, parent=parent_key)),
    )
    self.assertEqual(expected, actual)

  def test_get_versioned_most_recent_with_root_unexpected_extra(self):
    cls = monotonic.get_versioned_root_model('fidoula')
    parent_key = ndb.Key(cls, 'foo')
    monotonic.store_new_version(EntityX(parent=parent_key), cls)
    monotonic.store_new_version(EntityX(parent=parent_key), cls)
    EntityX(id=monotonic.HIGH_KEY_ID-2, parent=parent_key).put()

    # The unexpected entity is not registered.
    actual = monotonic.get_versioned_most_recent_with_root(EntityX, parent_key)
    expected = (
      cls(key=parent_key, current=monotonic.HIGH_KEY_ID-1),
      EntityX(
          key=ndb.Key('EntityX', monotonic.HIGH_KEY_ID-1, parent=parent_key)),
    )
    self.assertEqual(expected, actual)

    # The unexpected entity is safely skipped. In particular, root.current was
    # updated properly.
    monotonic.store_new_version(EntityX(parent=parent_key), cls)
    actual = monotonic.get_versioned_most_recent_with_root(EntityX, parent_key)
    expected = (
      cls(key=parent_key, current=monotonic.HIGH_KEY_ID-3),
      EntityX(
          key=ndb.Key('EntityX', monotonic.HIGH_KEY_ID-3, parent=parent_key)),
    )
    self.assertEqual(expected, actual)

  def test_store_new_version(self):
    cls = monotonic.get_versioned_root_model('fidoula')
    parent = ndb.Key(cls, 'foo')
    actual = monotonic.store_new_version(EntityX(a=1, parent=parent), cls)
    self.assertEqual(
        ndb.Key('fidoula', 'foo', 'EntityX', monotonic.HIGH_KEY_ID), actual)
    actual = monotonic.store_new_version(EntityX(a=2, parent=parent), cls)
    self.assertEqual(
        ndb.Key('fidoula', 'foo', 'EntityX', monotonic.HIGH_KEY_ID - 1), actual)

  def test_store_new_version_extra(self):
    # Includes an unrelated entity in the PUT. It must be in the same entity
    # group.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent = ndb.Key(cls, 'foo')
    class Unrelated(ndb.Model):
      b = ndb.IntegerProperty()
    unrelated = Unrelated(id='bar', parent=parent, b=42)
    actual = monotonic.store_new_version(
        EntityX(a=1, parent=parent), cls, extra=[unrelated])
    self.assertEqual(
        ndb.Key('fidoula', 'foo', 'EntityX', monotonic.HIGH_KEY_ID), actual)
    actual = monotonic.store_new_version(EntityX(a=2, parent=parent), cls)
    self.assertEqual(
        ndb.Key('fidoula', 'foo', 'EntityX', monotonic.HIGH_KEY_ID - 1), actual)
    self.assertEqual({'b': 42}, unrelated.key.get().to_dict())

  def test_store_new_version_transaction_failure(self):
    # Ensures that when a transaction fails, the key id is not modified and the
    # retry is on the same key id.
    cls = monotonic.get_versioned_root_model('fidoula')
    parent = ndb.Key(cls, 'foo')
    actual = monotonic.store_new_version(EntityX(a=1, parent=parent), cls)

    calls = []
    def transaction_async(*args, **kwargs):
      calls.append(1)
      if len(calls) < 2:
        raise txn.CommitError()
      return old_transaction_async(*args, **kwargs)
    old_transaction_async = self.mock(
        txn, 'transaction_async', transaction_async)

    actual = monotonic.store_new_version(EntityX(a=2, parent=parent), cls)
    self.assertEqual(
        ndb.Key('fidoula', 'foo', 'EntityX', monotonic.HIGH_KEY_ID - 1), actual)
    self.assertEqual([1, 1], calls)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
