# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for operating on instances."""

import logging

from google.appengine.ext import ndb

from components import gce
from components import net
from components import utils

import instance_group_managers
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
    ndb.Key for a models.InstanceTemplate entity.
  """
  return ndb.Key(
      models.Instance,
      instance_name,
      parent=instance_group_managers.get_instance_group_manager_key(
          base_name, revision, zone),
  )


@ndb.transactional_tasklet
def mark_for_deletion(key):
  """Marks the given instance for deletion.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.pending_deletion:
    entity.pending_deletion = True
    yield entity.put_async()


@ndb.transactional_tasklet
def add_subscription_metadata(key, subscription_project, subscription):
  """Queues the addition of subscription metadata.

  Args:
    key: ndb.Key for a models.Instance entity.
    subscription_project: Project containing the Pub/Sub subscription.
    subscription: Name of the Pub/Sub subscription that Machine Provider will
      communicate with the instance on.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  parent = yield key.parent().get_async()
  if not parent:
    logging.warning('InstanceGroupManager does not exist: %s', key.parent())
    return

  grandparent = yield parent.key.parent().get_async()
  if not grandparent:
    logging.warning(
        'InstanceTemplateRevision does not exist: %s', parent.key.parent())
    return

  if not grandparent.service_accounts:
    logging.warning(
        'InstanceTemplateRevision service account unspecified: %s',
        parent.key.parent(),
    )
    return

  entity.pending_metadata_updates.append(models.MetadataUpdate(
      metadata={
          'pubsub_service_account': grandparent.service_accounts[0].name,
          'pubsub_subscription': subscription,
          'pubsub_subscription_project': subscription_project,
      },
  ))
  yield entity.put_async()


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
def ensure_entity_exists(key, url):
  """Ensures an Instance entity exists.

  Args:
    key: ndb.Key for a models.Instance entity.
    url: URL for the instance.
  """
  entity = yield key.get_async()
  if entity:
    return

  yield models.Instance(key=key, url=url).put_async()


@ndb.transactional
def add_instances(key, keys):
  """Adds the given Instances to the given InstanceGroupManager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
    keys: List of ndb.Keys for models.Instance entities.
  """
  entity = key.get()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return

  instances = set(entity.instances)
  instances.update(keys)

  if len(instances) != entity.current_size:
    entity.instances = sorted(instances)
    entity.put()


def ensure_entities_exist(key, max_concurrent=50):
  """Ensures Instance entities exist for the given instance group manager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
    max_concurrent: Maximun number of entities to create concurrently.
  """
  urls = fetch(key)
  if not urls:
    return

  keys = {
      url: ndb.Key(models.Instance, gce.extract_instance_name(url), parent=key)
      for url in urls
  }

  utilities.batch_process_async(
      urls,
      lambda url: ensure_entity_exists(keys[url], url),
      max_concurrent=max_concurrent,
  )

  add_instances(key, keys.values())


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
