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


class FindGroupReferencesTest(test_case.TestCase):
  """Tests for find_references function."""

  @staticmethod
  def make_group(group_id, nested=()):
    """Makes a new AuthGroup to use it test, puts it in datastore."""
    entity = model.AuthGroup(id=group_id, parent=model.ROOT_KEY, nested=nested)
    entity.put()

  @staticmethod
  def make_service(service_id, used_groups=()):
    """Makes a new AuthServiceConfig to use in test, puts it in datastore."""
    # Actual rules doesn't matter as long as they mention requested groups.
    entity = model.AuthServiceConfig(
        id=service_id,
        parent=model.ROOT_KEY,
        rules=[
          model.AccessRule(model.ALLOW_RULE, group, [model.READ], '^.*$')
          for group in used_groups
        ])
    entity.put()

  def test_missing_group(self):
    """Non existent group is not references by anything."""
    self.assertEqual((set(), set()), groups.find_references('Missing group'))

  def test_not_referenced(self):
    """Existing orphaned groups is not referenced."""
    # Some mix of groups with references.
    self.make_group('Group 1')
    self.make_group('Group 2')
    self.make_group('Group 3', nested=('Group 1', 'Group 2'))
    self.make_group('Group 4', nested=('Group 3',))

    # And some rules.
    self.make_service('Service 1', used_groups=('Group 1', 'Group 2'))

    # And a group that is not referenced by anything.
    self.make_group('Standalone')

    # Should not be referenced.
    self.assertEqual((set(), set()), groups.find_references('Standalone'))

  def test_referenced_as_nested_group(self):
    """If group is nested into another group, it's referenced."""
    # Some mix of groups with references, including group to be tested.
    self.make_group('Referenced')
    self.make_group('Group 1')
    self.make_group('Group 2', nested=('Referenced', 'Group 1'))
    self.make_group('Group 3', nested=('Group 2',))
    self.make_group('Group 4', nested=('Referenced',))

    # Only direct references are returned.
    self.assertEqual(
        (set(['Group 2', 'Group 4']), set()),
        groups.find_references('Referenced'))

  def test_referenced_in_rules(self):
    """If group is mentioned in ACL rules, it's referenced."""
    # Create a bunch of groups.
    self.make_group('Referenced')
    self.make_group('Group 1')

    # Create a bunch of services.
    self.make_service('Service 1', used_groups=('Referenced', 'Group 1'))
    self.make_service('Service 2', used_groups=('Referenced',))
    self.make_service('Service 3', used_groups=('Group 1',))

    # Only services that reference the group are returned.
    self.assertEqual(
        (set(), set(['Service 1', 'Service 2'])),
        groups.find_references('Referenced'))

  def test_referenced_in_rules_and_as_nested_group(self):
    """Group can be both nested and referenced in ACL rules."""
    # Some mix of groups with references, including group to be tested.
    self.make_group('Referenced')
    self.make_group('Group 1')
    self.make_group('Group 2', nested=('Referenced', 'Group 1'))

    # Create a bunch of services.
    self.make_service('Service 1', used_groups=('Referenced', 'Group 1'))
    self.make_service('Service 2', used_groups=('Referenced',))
    self.make_service('Service 3', used_groups=('Group 2',))

    # Only group and services that reference the group are returned.
    self.assertEqual(
        (set(['Group 2']), set(['Service 1', 'Service 2'])),
        groups.find_references('Referenced'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
