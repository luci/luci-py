#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for parse.py."""

import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import datastore_utils
from test_support import test_case

import cleanup
import instance_group_managers
import instance_templates
import models


class DeleteInstanceGroupManagerTest(test_case.TestCase):
  """Tests for cleanup.delete_instance_group_manager."""

  def test_entity_not_found(self):
    """Ensures nothing happens when the entity is not found."""
    key = ndb.Key(models.InstanceGroupManager, 'fake-key')

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failIf(key.get())

  def test_url_specified(self):
    """Ensures nothing happens when the entity still has a URL."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
        url='url',
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        active=key.parent(),
    ).put()

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failUnless(key.get())

  def test_active_instances(self):
    """Ensures nothing happens when there are active Instances."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
        instances=[
            ndb.Key(models.Instance, 'fake-key'),
        ],
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        active=key.parent(),
    ).put()

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failUnless(key.get())

  def test_parent_doesnt_exist(self):
    """Ensures nothing happens when the parent doesn't exist."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        active=key.parent(),
    ).put()

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failUnless(key.get())

  def test_root_doesnt_exist(self):
    """Ensures nothing happens when the root doesn't exist."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failUnless(key.get())

  def test_active(self):
    """Ensures nothing happens when the entity is active."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        active=[
            key,
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        active=key.parent(),
    ).put()

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failUnless(key.get())

  def test_deletes_drained(self):
    """Ensures a drained entity is deleted."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        drained=[
            ndb.Key(models.InstanceGroupManager, 'fake-key-1'),
            key,
            ndb.Key(models.InstanceGroupManager, 'fake-key-2'),
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        active=key.parent(),
    ).put()
    expected_drained = [
        ndb.Key(models.InstanceGroupManager, 'fake-key-1'),
        ndb.Key(models.InstanceGroupManager, 'fake-key-2'),
    ]

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failIf(key.get())
    self.assertItemsEqual(key.parent().get().drained, expected_drained)

  def test_deletes_implicitly_drained(self):
    """Ensures an implicitly drained entity is deleted."""
    key = models.InstanceGroupManager(
        key=instance_group_managers.get_instance_group_manager_key(
            'base-name',
            'revision',
            'zone',
        ),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent(),
        drained=[
            ndb.Key(models.InstanceGroupManager, 'fake-key-1'),
            key,
            ndb.Key(models.InstanceGroupManager, 'fake-key-2'),
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent().parent(),
        drained=[
            key.parent(),
        ],
    ).put()
    expected_drained = [
        ndb.Key(models.InstanceGroupManager, 'fake-key-1'),
        ndb.Key(models.InstanceGroupManager, 'fake-key-2'),
    ]

    future = cleanup.delete_instance_group_manager(key)
    future.wait()

    self.failIf(key.get())
    self.assertItemsEqual(key.parent().get().drained, expected_drained)


class DeleteInstanceTemplateRevisionTest(test_case.TestCase):
  """Tests for cleanup.delete_instance_template_revision."""

  def test_entity_not_found(self):
    """Ensures nothing happens when the entity is not found."""
    key = ndb.Key(models.InstanceTemplateRevision, 'fake-key')

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failIf(key.get())

  def test_url_specified(self):
    """Ensures nothing happens when the entity still has a URL."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
        url='url',
    ).put()
    models.InstanceTemplate(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failUnless(key.get())

  def test_active_instance_group_managers(self):
    """Ensures nothing happens when there are active InstanceGroupManagers."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
        active=[
            ndb.Key(models.InstanceGroupManager, 'fake-key'),
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failUnless(key.get())

  def test_drained_instance_group_managers(self):
    """Ensures nothing happens when there are drained InstanceGroupManagers."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
        drained=[
            ndb.Key(models.InstanceGroupManager, 'fake-key'),
        ],
    ).put()
    models.InstanceTemplate(
        key=key.parent(),
        drained=[
            key,
        ],
    ).put()

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failUnless(key.get())

  def test_parent_doesnt_exist(self):
    """Ensures nothing happens when the parent doesn't exist."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
    ).put()

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failUnless(key.get())

  def test_active(self):
    """Ensures nothing happens when the entity is active."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
    ).put()
    models.InstanceTemplate(
        key=key.parent(),
        active=key,
    ).put()

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failUnless(key.get())

  def test_deletes(self):
    """Ensures the entity is deleted."""
    key = models.InstanceTemplateRevision(
        key=instance_templates.get_instance_template_revision_key(
            'base-name',
            'revision',
        ),
    ).put()
    models.InstanceTemplate(
        key=key.parent(),
        drained=[
            ndb.Key(models.InstanceTemplateRevision, 'fake-key-1'),
            key,
            ndb.Key(models.InstanceTemplateRevision, 'fake-key-2'),
        ],
    ).put()

    expected_drained = [
        ndb.Key(models.InstanceTemplateRevision, 'fake-key-1'),
        ndb.Key(models.InstanceTemplateRevision, 'fake-key-2'),
    ]

    future = cleanup.delete_instance_template_revision(key)
    future.wait()

    self.failIf(key.get())
    self.assertItemsEqual(key.parent().get().drained, expected_drained)


if __name__ == '__main__':
  unittest.main()
