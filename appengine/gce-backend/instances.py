# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for operating on instances."""

import json
import logging

from google.appengine.ext import ndb

from components import gce
from components import net
from components import pubsub
from components import utils

import instance_group_managers
import metrics
import models
import utilities


def get_instance_key(base_name, revision, zone, instance_name):
  """Returns a key for an Instance.

  Args:
    base_name: Base name for the models.InstanceTemplate.
    revision: Revision string for the models.InstanceTemplateRevision.
    zone: Zone for the models.InstanceGroupManager.
    instance_name: Name of the models.Instance.

  Returns:
    ndb.Key for a models.Instance entity.
  """
  return ndb.Key(
    models.Instance, '%s %s %s %s' % (base_name, revision, zone, instance_name))


def get_instance_group_manager_key(key):
  """Returns a key for the InstanceGroupManager the given Instance belongs to.

  Args:
    key: ndb.Key for a models.Instance.

  Returns:
    ndb.Key for a models.InstanceGroupManager entity.
  """
  return instance_group_managers.get_instance_group_manager_key(
      *key.id().split()[:-1])


@ndb.transactional
def mark_for_deletion(key):
  """Marks the given instance for deletion.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.pending_deletion:
    logging.info('Marking Instance for deletion: %s', key)
    entity.pending_deletion = True
    entity.put()
    metrics.send_machine_event('DELETION_PROPOSED', entity.hostname)


@ndb.transactional
def add_subscription_metadata(
      key, subscription_project, subscription, service_account):
  """Queues the addition of subscription metadata.

  Args:
    key: ndb.Key for a models.Instance entity.
    subscription_project: Project containing the Pub/Sub subscription.
    subscription: Name of the Pub/Sub subscription that Machine Provider will
      communicate with the instance on.
    service_account: Service account authorized to read the Pub/Sub
      subscription.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if entity.pubsub_subscription:
    return

  logging.info('Instance Pub/Sub subscription received: %s', key)
  entity.pending_metadata_updates.append(models.MetadataUpdate(
      metadata={
          'pubsub_service_account': service_account,
          'pubsub_subscription': subscription,
          'pubsub_subscription_project': subscription_project,
      },
  ))
  entity.pubsub_service_account = service_account
  entity.pubsub_subscription = pubsub.full_subscription_name(
      subscription_project, subscription)
  entity.put()


def fetch(key):
  """Gets instances created by the given instance group manager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.

  Returns:
    A list of instance URLs.
  """
  entity = key.get()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return []

  if not entity.url:
    logging.warning('InstanceGroupManager URL unspecified: %s', key)
    return []

  parent = key.parent().get()
  if not parent:
    logging.warning('InstanceTemplateRevision does not exist: %s', key.parent())
    return []

  if not parent.project:
    logging.warning(
        'InstanceTemplateRevision project unspecified: %s', key.parent())
    return []

  api = gce.Project(parent.project)
  result = api.get_instances_in_instance_group(
      instance_group_managers.get_name(entity),
      entity.key.id(),
      max_results=500,
  )
  instance_urls = [instance['instance'] for instance in result.get('items', [])]
  while result.get('nextPageToken'):
    result = api.get_instances_in_instance_group(
        instance_group_managers.get_name(entity),
        entity.key.id(),
        max_results=500,
        page_token=result['nextPageToken'],
    )
    instance_urls.extend([instance['instance'] for instance in result['items']])

  return instance_urls


@ndb.transactional_tasklet
def ensure_entity_exists(key, url, instance_group_manager):
  """Ensures an Instance entity exists.

  Args:
    key: ndb.Key for a models.Instance entity.
    url: URL for the instance.
    instance_group_manager: ndb.Key for the models.InstanceGroupManager the
      instance was created from.
  """
  entity = yield key.get_async()
  if entity:
    logging.info('Instance entity already exists: %s', key)
    return

  logging.info('Creating Instance entity: %s', key)
  yield models.Instance(
      key=key,
      instance_group_manager=instance_group_manager,
      url=url,
  ).put_async()
  metrics.send_machine_event('CREATED', gce.extract_instance_name(url))


def ensure_entities_exist(key, max_concurrent=50):
  """Ensures Instance entities exist for the given instance group manager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
    max_concurrent: Maximun number of entities to create concurrently.
  """
  urls = fetch(key)
  if not urls:
    instance_group_managers.set_instances(key, [])
    return

  base_name = key.parent().parent().id()
  revision = key.parent().id()
  zone = key.id()

  keys = {
      url: get_instance_key(
          base_name, revision, zone, gce.extract_instance_name(url))
      for url in urls
  }

  utilities.batch_process_async(
      urls,
      lambda url: ensure_entity_exists(keys[url], url, key),
      max_concurrent=max_concurrent,
  )

  instance_group_managers.set_instances(key, keys.values())


def schedule_fetch():
  """Enqueues tasks to fetch instances."""
  for instance_group_manager in models.InstanceGroupManager.query():
    if instance_group_manager.url:
      if not utils.enqueue_task(
          '/internal/queues/fetch-instances',
          'fetch-instances',
          params={
              'key': instance_group_manager.key.urlsafe(),
          },
      ):
        logging.warning(
            'Failed to enqueue task for InstanceGroupManager: %s',
            instance_group_manager.key,
        )


def _delete(instance_template_revision, instance_group_manager, instance):
  """Deletes the given instance.

  Args:
    instance_template_revision: models.InstanceTemplateRevision.
    instance_group_manager: models.InstanceGroupManager.
    instance: models.Instance
  """
  api = gce.Project(instance_template_revision.project)
  try:
    result = api.delete_instances(
        instance_group_managers.get_name(instance_group_manager),
        instance_group_manager.key.id(),
        [instance.url],
    )
    if result['status'] != 'DONE':
      logging.warning(
          'Instance group manager operation failed: %s\n%s',
          parent.key,
          json.dumps(result, indent=2),
      )
    else:
      metrics.send_machine_event('DELETION_SCHEDULED', instance.hostname)
  except net.Error as e:
    if e.status_code == 400:
      metrics.send_machine_event('DELETION_SUCCEEDED', instance.hostname)
    else:
      raise


def delete_pending(key):
  """Deletes the given instance pending deletion.

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

  parent = entity.instance_group_manager.get()
  if not parent:
    logging.warning(
        'InstanceGroupManager does not exist: %s',
        entity.instance_group_manager,
    )
    return

  grandparent = parent.key.parent().get()
  if not grandparent:
    logging.warning(
        'InstanceTemplateRevision does not exist: %s', parent.key.parent())
    return

  if not grandparent.project:
    logging.warning(
        'InstanceTemplateRevision project unspecified: %s', grandparent.key)
    return

  _delete(grandparent, parent, entity)


def schedule_pending_deletion():
  """Enqueues tasks to delete instances."""
  for instance in models.Instance.query():
    if instance.pending_deletion and not instance.deleted:
      if not utils.enqueue_task(
          '/internal/queues/delete-instance-pending-deletion',
          'delete-instance-pending-deletion',
          params={
              'key': instance.key.urlsafe(),
          },
      ):
        logging.warning('Failed to enqueue task for Instance: %s', instance.key)


def delete_drained(key):
  """Deletes the given drained instance.

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

  if not entity.url:
    logging.warning('Instance URL unspecified: %s', key)
    return

  parent = entity.instance_group_manager.get()
  if not parent:
    logging.warning(
        'InstanceGroupManager does not exist: %s',
        entity.instance_group_manager,
    )
    return

  grandparent = parent.key.parent().get()
  if not grandparent:
    logging.warning(
        'InstanceTemplateRevision does not exist: %s', parent.key.parent())
    return

  if not grandparent.project:
    logging.warning(
        'InstanceTemplateRevision project unspecified: %s', grandparent.key)
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

  _delete(grandparent, parent, entity)


def schedule_drained_deletion():
  """Enqueues tasks to delete drained instances."""
  for instance_group_manager_key in (
      instance_group_managers.get_drained_instance_group_managers()):
    instance_group_manager = instance_group_manager_key.get()
    if instance_group_manager:
      for instance_key in instance_group_manager.instances:
        instance = instance_key.get()
        if instance and not instance.cataloged:
          if not utils.enqueue_task(
              '/internal/queues/delete-drained-instance',
              'delete-drained-instance',
              params={
                  'key': instance.key.urlsafe(),
              },
          ):
            logging.warning(
              'Failed to enqueue task for Instance: %s', instance.key)
