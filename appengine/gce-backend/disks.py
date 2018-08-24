# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for operating on persistent disks."""

import logging

from components import gce
from components import net

import instances
import models
import utilities


def create(key):
  """Creates and attaches a disk for the given Instance.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  instance = key.get()
  if not instance:
    logging.warning('Instance does not exist: %s', key)
    return

  igm_key = instances.get_instance_group_manager_key(key)
  itr_key = igm_key.parent()
  itr = itr_key.get()
  if not itr:
    logging.warning('InstanceTemplateRevision does not exist: %s', itr_key)
    return

  if not itr.project:
    logging.warning('InstanceTemplateRevision project unspecified: %s', itr_key)
    return

  if not itr.snapshot_url:
    logging.warning(
        'InstanceTemplateRevision snapshot unspecified: %s', itr_key)
    return

  name = instances.get_name(key)
  disk = '%s-disk' % name
  api = gce.Project(itr.project)

  # Create the disk from the snapshot.
  # Returns 200 if the operation has started. Returns 409 if the disk has
  # already been created.
  try:
    api.create_disk(disk, itr.snapshot_url, igm_key.id())
  except net.Error as e:
    if e.status_code != 409:
      # 409 means the disk already exists.
      raise

  # TODO(smut): Attach the disk.


def _schedule_creation(keys):
  """Enqueues tasks to create disks.

  Args:
    keys: ndb.Key for models.InstanceGroupManager entities.
  """
  for igm_key in keys:
    igm = igm_key.get()
    if igm:
      for key in igm.instances:
        instance = key.get()
        if instance:
          utilities.enqueue_task('create-disk', key)


def schedule_creation():
  """Enqueues tasks to create disks."""
  for itr in models.InstanceTemplateRevision.query():
    if itr.snapshot_url:
      _schedule_creation(itr.active)
      _schedule_creation(itr.drained)
