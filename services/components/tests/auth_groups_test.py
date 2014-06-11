#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

from support import test_case

from components.auth import groups
from components.auth import model


def make_group(group_id, nested=(), store=True):
  """Makes a new AuthGroup to use in test, puts it in datastore."""
  entity = model.AuthGroup(key=model.group_key(group_id), nested=nested)
  if store:
    entity.put()
  return entity


class FindGroupReferencesTest(test_case.TestCase):
  """Tests for find_references function."""

  def test_missing_group(self):
    """Non existent group is not references by anything."""
    self.assertEqual(set(), groups.find_references('Missing group'))

  def test_not_referenced(self):
    """Existing orphaned groups is not referenced."""
    # Some mix of groups with references.
    make_group('Group 1')
    make_group('Group 2')
    make_group('Group 3', nested=('Group 1', 'Group 2'))
    make_group('Group 4', nested=('Group 3',))

    # And a group that is not referenced by anything.
    make_group('Standalone')

    # Should not be referenced.
    self.assertEqual(set(), groups.find_references('Standalone'))

  def test_referenced_as_nested_group(self):
    """If group is nested into another group, it's referenced."""
    # Some mix of groups with references, including group to be tested.
    make_group('Referenced')
    make_group('Group 1')
    make_group('Group 2', nested=('Referenced', 'Group 1'))
    make_group('Group 3', nested=('Group 2',))
    make_group('Group 4', nested=('Referenced',))

    # Only direct references are returned.
    self.assertEqual(
        set(['Group 2', 'Group 4']), groups.find_references('Referenced'))


class FindDependencyCycleTest(test_case.TestCase):
  """Tests for find_dependency_cycle function."""

  def test_empty(self):
    group = make_group('A', store=False)
    self.assertEqual([], groups.find_dependency_cycle(group))

  def test_no_cycles(self):
    make_group('A')
    make_group('B', nested=('A',))
    group = make_group('C', nested=('B',), store=False)
    self.assertEqual([], groups.find_dependency_cycle(group))

  def test_self_reference(self):
    group = make_group('A', nested=('A',), store=False)
    self.assertEqual(['A'], groups.find_dependency_cycle(group))

  def test_simple_cycle(self):
    make_group('A', nested=('B',))
    group = make_group('B', nested=('A',), store=False)
    self.assertEqual(['B', 'A'], groups.find_dependency_cycle(group))

  def test_long_cycle(self):
    make_group('A', nested=('B',))
    make_group('B', nested=('C',))
    make_group('C', nested=('D',))
    group = make_group('D', nested=('A',), store=False)
    self.assertEqual(['D', 'A', 'B', 'C'], groups.find_dependency_cycle(group))

  def test_diamond_no_cycles(self):
    make_group('A')
    make_group('B1', nested=('A',))
    make_group('B2', nested=('A',))
    group = make_group('C', nested=('B1', 'B2'), store=False)
    self.assertEqual([], groups.find_dependency_cycle(group))

  def test_diamond_with_cycles(self):
    make_group('A', nested=('C',))
    make_group('B1', nested=('A',))
    make_group('B2', nested=('A',))
    group = make_group('C', nested=('B1', 'B2'), store=False)
    self.assertEqual(['C', 'B1', 'A'], groups.find_dependency_cycle(group))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
