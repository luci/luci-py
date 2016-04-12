# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for operating on instance group managers."""

import logging

from google.appengine.ext import ndb

from components import gce
from components import net
from components import utils

import instance_templates
import models


def get_instance_group_manager_key(base_name, revision, zone):
  """Returns a key for an InstanceTemplateGroupManager.

  Args:
    base_name: Base name for the models.InstanceTemplate.

  Returns:
    ndb.Key for a models.InstanceTemplate entity.
  """
  return ndb.Key(
      models.InstanceGroupManager,
      zone,
      parent=instance_templates.get_instance_template_revision_key(
          base_name, revision),
  )


def get_name(instance_group_manager):
  """Returns the name to use when creating an instance group manager.

  Args:
    instance_group_manager: models.InstanceGroupManager.

  Returns:
    A string.
  """
  # <base-name>-<revision>
  return '%s-%s' % (
      instance_group_manager.key.parent().parent().id(),
      instance_group_manager.key.parent().id(),
  )


def get_base_name(instance_group_manager):
  """Returns the base name to use when creating an instance group manager.

  The base name is suffixed randomly by GCE when naming instances.

  Args:
    instance_group_manager: models.InstanceGroupManager.

  Returns:
    A string.
  """
  # <base-name>-<abbreviated-revision>
  return '%s-%s' % (
      instance_group_manager.key.parent().parent().id(),
      instance_group_manager.key.parent().id()[:8],
  )


@ndb.transactional
def update_url(key, url):
  """Updates the given InstanceGroupManager with the instance group manager URL.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
    url: URL string for the instance group manager.
  """
  entity = key.get()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return

  if entity.url:
    if entity.url == url:
      return
    # Sometimes the URLs get updated. For example, they used to be
    # https://www.googleapis.com/compute/v1/projects/... but they
    # changed to https://content.googleapis.com/compute/v1/projects/...
    logging.warning(
        'Updating URL for InstanceGroupManager: %s\nOld: %s\nNew: %s',
        key,
        entity.url,
        url,
    )

  entity.url = url
  entity.put()


def create(key):
  """Creates an instance group manager from the given InstanceGroupManager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.

  Raises:
    net.Error: HTTP status code is not 200 (created) or 409 (already created).
  """
  entity = key.get()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return

  parent = key.parent().get()
  if not parent:
    logging.warning('InstanceTemplateRevision does not exist: %s', key.parent())
    return

  if not parent.project:
    logging.warning(
        'InstanceTemplateRevision project unspecified: %s', key.parent())
    return

  if not parent.url:
    logging.warning(
        'InstanceTemplateRevision URL unspecified: %s', key.parent())
    return

  api = gce.Project(parent.project)
  try:
    result = api.create_instance_group_manager(
        get_name(entity),
        parent.url,
        entity.minimum_size,
        entity.key.id(),
        base_name=get_base_name(entity),
    )
  except net.Error as e:
    if e.status_code == 409:
      # If the instance template already exists, just record the URL.
      result = api.get_instance_group_manager(get_name(entity), entity.key.id())
      update_url(entity.key, result['selfLink'])
      return
    else:
      raise

  update_url(entity.key, result['targetLink'])


def schedule_creation():
  """Enqueues tasks to create missing instance group managers."""
  # For each active InstanceGroupManager without a URL, schedule creation
  # of its instance group manager. Since we are outside a transaction the
  # InstanceGroupManager could be out of date and may already have a task
  # scheduled/completed. In either case it doesn't matter since we make
  # creating an instance group manager and updating the URL idempotent.
  for instance_template in models.InstanceTemplate.query():
    if instance_template.active:
      instance_template_revision = instance_template.active.get()
      if instance_template_revision and instance_template_revision.url:
        for instance_group_manager_key in instance_template_revision.active:
          instance_group_manager = instance_group_manager_key.get()
          if instance_group_manager and not instance_group_manager.url:
            if not utils.enqueue_task(
                '/internal/queues/create-instance-group-manager',
                'create-instance-group-manager',
                params={
                    'key': instance_group_manager_key.urlsafe(),
                },
            ):
              logging.warning(
                  'Failed to enqueue task for InstanceGroupManager: %s',
                  instance_group_manager_key,
              )
