# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for cleaning up GCE Backend."""

import logging

from google.appengine.ext import ndb

from components import gce
from components import net
from components import utils

import instance_group_managers
import instance_templates
import instances
import models
import utilities


def exists(instance_url):
  """Returns whether the given instance exists or not.

  Args:
    instance_url: URL of the instance.

  Returns:
    True if the instance exists, False otherwise.

  Raises:
    net.Error: If GCE responds with an error.
  """
  try:
    net.json_request(instance_url, method='GET', scopes=gce.AUTH_SCOPES)
    return True
  except net.Error as e:
    if e.status_code == 404:
      return False
    raise


def _delete_instance(instance_key, instance_group_manager):
  """Attempts to delete the given Instance.

  Args:
    instance_key: ndb.Key for a models.Instance entity.
    instance_group_manager: models.InstanceGroupManager.
  """
  logging.info('Deleting Instance: %s', instance_key)
  assert ndb.in_transaction()
  instance_key.delete()
  for i, key in enumerate(instance_group_manager.instances):
    if key.id() == instance_key.id():
      instance_group_manager.instances.pop(i)
      instance_group_manager.put()
      return
  logging.warning('Instance not found: %s', instance_key)


@ndb.transactional
def delete_instance_pending_deletion(key):
  """Attempts to delete the given Instance pending deletion.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.info('Instance does not exist: %s', key)
    return

  if not entity.pending_deletion:
    logging.warning('Instance not pending deletion: %s', key)
    return

  parent = key.parent().get()
  if not parent:
    logging.warning('InstanceGroupManager does not exist: %s', key.parent())
    return

  _delete_instance(key, parent)


@ndb.transactional
def delete_drained_instance(key):
  """Deletes the given drained Instance.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if entity.cataloged:
    logging.warning('Instance is cataloged: %s', key)
    return

  parent = key.parent().get()
  if not parent:
    logging.warning('InstanceGroupManager does not exist: %s', key.parent())
    return

  grandparent = parent.key.parent().get()
  if not grandparent:
    logging.warning(
        'InstanceTemplateRevision does not exist: %s', parent.key.parent())
    return

  root = grandparent.key.parent().get()
  if not root:
    logging.warning(
        'InstanceTemplate does not exist: %s', grandparent.key.parent())
    return

  if parent.key not in grandparent.drained:
    if grandparent.key not in root.drained:
      logging.warning('Instance is not drained: %s', key)
      return

  _delete_instance(key, parent)


@ndb.transactional_tasklet
def delete_instance_group_manager(key):
  """Attempts to delete the given InstanceGroupManager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return

  if entity.url or entity.instances:
    return

  parent = yield key.parent().get_async()
  if not parent:
    logging.warning('InstanceTemplateRevision does not exist: %s', key.parent())
    return

  root = yield parent.key.parent().get_async()
  if not root:
    logging.warning('InstanceTemplate does not exist: %s', parent.key.parent())
    return

  # If the InstanceGroupManager is drained, we can delete it now.
  for i, drained_key in enumerate(parent.drained):
    if key.id() == drained_key.id():
      parent.drained.pop(i)
      yield parent.put_async()
      yield key.delete_async()
      return

  # If the InstanceGroupManager is implicitly drained, we can still delete it.
  if parent.key in root.drained:
    for i, drained_key in enumerate(parent.active):
      if key.id() == drained_key.id():
        parent.active.pop(i)
        yield parent.put_async()
        yield key.delete_async()


@ndb.transactional_tasklet
def delete_instance_template_revision(key):
  """Attempts to delete the given InstanceTemplateRevision.

  Args:
    key: ndb.Key for a models.InstanceTemplateRevision entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceTemplateRevision does not exist: %s', key)
    return

  if entity.url or entity.active or entity.drained:
    return

  parent = yield key.parent().get_async()
  if not parent:
    logging.warning('InstanceTemplate does not exist: %s', key.parent())
    return

  for i, drained_key in enumerate(parent.drained):
    if key.id() == drained_key.id():
      parent.drained.pop(i)
      yield parent.put_async()
      yield key.delete_async()


@ndb.transactional_tasklet
def delete_instance_template(key):
  """Attempts to delete the given InstanceTemplate.

  Args:
    key: ndb.Key for a models.InstanceTemplate entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceTemplate does not exist: %s', key)
    return

  if entity.active or entity.drained:
    return

  yield key.delete_async()


def cleanup_instance_group_managers(max_concurrent=50):
  """Deletes drained InstanceGroupManagers.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utilities.batch_process_async(
      instance_group_managers.get_drained_instance_group_managers(),
      delete_instance_group_manager,
      max_concurrent=max_concurrent,
  )


def cleanup_instance_template_revisions(max_concurrent=50):
  """Deletes drained InstanceTemplateRevisions.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utilities.batch_process_async(
      instance_templates.get_drained_instance_template_revisions(),
      delete_instance_template_revision,
      max_concurrent=max_concurrent,
  )


def cleanup_instance_templates(max_concurrent=50):
  """Deletes InstanceTemplates.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utilities.batch_process_async(
      models.InstanceTemplate.query().fetch(keys_only=True),
      delete_instance_template,
      max_concurrent=max_concurrent,
  )


def cleanup_deleted_instance(key):
  """Deletes the given Instance.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    return

  if not entity.pending_deletion:
    logging.warning('Instance not pending deletion: %s', key)
    return

  if not entity.url:
    logging.warning('Instance URL unspecified: %s', key)
    return

  if not exists(entity.url):
    # When the instance isn't found, assume it's deleted.
    delete_instance_pending_deletion(key)


def schedule_deleted_instance_cleanup():
  """Enqueues tasks to clean up deleted instances."""
  for instance in models.Instance.query():
    if instance.pending_deletion:
      if not utils.enqueue_task(
          '/internal/queues/cleanup-deleted-instance',
          'cleanup-deleted-instance',
          params={
              'key': instance.key.urlsafe(),
          },
      ):
        logging.warning('Failed to enqueue task for Instance: %s', instance.key)


def cleanup_drained_instance(key):
  """Deletes the given drained Instance.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    return

  if not entity.url:
    logging.warning('Instance URL unspecified: %s', key)
    return

  parent = key.parent().get()
  if not parent:
    logging.warning('InstanceGroupManager does not exist: %s', key.parent())
    return

  grandparent = parent.key.parent().get()
  if not grandparent:
    logging.warning(
        'InstanceTemplateRevision does not exist: %s', parent.key.parent())
    return

  root = grandparent.key.parent().get()
  if not root:
    logging.warning(
        'InstanceTemplate does not exist: %s', grandparent.key.parent())
    return

  if parent.key not in grandparent.drained:
    if grandparent.key not in root.drained:
      logging.warning('Instance is not drained: %s', key)
      return

  if not exists(entity.url):
    # When the instance isn't found, assume it's deleted.
    delete_drained_instance(key)


def schedule_drained_instance_cleanup():
  """Enqueues tasks to clean up drained instances."""
  for instance_group_manager_key in (
      instance_group_managers.get_drained_instance_group_managers()):
    instance_group_manager = instance_group_manager_key.get()
    if instance_group_manager:
      for instance_key in instance_group_manager.instances:
        instance = instance_key.get()
        if instance and not instance.cataloged:
          if not utils.enqueue_task(
              '/internal/queues/cleanup-drained-instance',
              'cleanup-drained-instance',
              params={
                  'key': instance.key.urlsafe(),
              },
          ):
            logging.warning(
              'Failed to enqueue task for Instance: %s', instance.key)
