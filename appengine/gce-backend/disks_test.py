#!/usr/bin/python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Unit tests for disks.py."""

import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import net
from test_support import test_case

import disks
import instances
import models


class CreateTest(test_case.TestCase):
  """Tests for disks.create."""

  @staticmethod
  def create_entities(project, snapshot_url, create_parents):
    """Creates a models.Instance entity.

    Args:
      project: The project name to create disks in.
      snapshot_url: The snapshot url to create a disks from.
      create_parents: Whether or not to create parent entities.

    Returns:
      An ndb.Key for a models.InstanceTemplateRevision instance.
    """
    key = models.Instance(id='template revision zone instance').put()
    if create_parents:
      igm_key = instances.get_instance_group_manager_key(key)
      models.InstanceGroupManager(key=igm_key).put()
      itr_key = igm_key.parent()
      models.InstanceTemplateRevision(
          key=itr_key,
          project=project,
          snapshot_url=snapshot_url,
      ).put()
    return key

  def test_entity_doesnt_exist(self):
    """Ensures nothing happens when the entity doesn't exist."""
    def create_disk(*_args, **_kwargs):
      self.fail('create_disk called')
    self.mock(disks.gce.Project, 'create_disk', create_disk)
    key = ndb.Key(models.Instance, 'fake-key')
    disks.create(key)

  def test_parent_doesnt_exist(self):
    """Ensures nothing happens when the entity's parent doesn't exist."""
    def create_disk(*_args, **_kwargs):
      self.fail('create_disk called')
    self.mock(disks.gce.Project, 'create_disk', create_disk)
    key = self.create_entities('project', 'snapshot', False)
    disks.create(key)

  def test_no_project(self):
    """Ensures nothing happens when there is no project."""
    def create_disk(*_args, **_kwargs):
      self.fail('create_disk called')
    self.mock(disks.gce.Project, 'create_disk', create_disk)
    key = self.create_entities(None, 'snapshot', True)
    disks.create(key)

  def test_no_snapshot(self):
    """Ensures nothing happens when there is no snapshot."""
    def create_disk(*_args, **_kwargs):
      self.fail('create_disk called')
    self.mock(disks.gce.Project, 'create_disk', create_disk)
    key = self.create_entities('project', None, True)
    disks.create(key)

  def test_create_disk_error(self):
    """Ensures nothing happens when create_disk fails."""
    def create_disk(*_args, **_kwargs):
      raise net.Error('500', 500, '500')
    self.mock(disks.gce.Project, 'create_disk', create_disk)

    key = self.create_entities('project', 'snapshot', True)
    with self.assertRaises(net.Error):
      disks.create(key)

  def test_disk_already_created(self):
    """Ensures nothing happens when the disk is already created."""
    def create_disk(*_args, **_kwargs):
      raise net.Error('409', 409, '409')
    self.mock(disks.gce.Project, 'create_disk', create_disk)

    key = self.create_entities('project', 'snapshot', True)
    disks.create(key)

  def test_ok(self):
    """Ensures disk is created."""
    def create_disk(*_args, **_kwargs):
      return
    self.mock(disks.gce.Project, 'create_disk', create_disk)

    key = self.create_entities('project', 'snapshot', True)
    disks.create(key)


if __name__ == '__main__':
  unittest.main()
