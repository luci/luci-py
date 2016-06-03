# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for operating on instance metadata."""

import json
import logging

from google.appengine.ext import ndb

from components import gce
from components import net
from components import utils

import models
import utilities


def apply_metadata_update(items, metadata):
  """Returns the result of applying the given metadata update.

  Args:
    items: List of {'key': ..., 'value': ...} dicts specifying existing
      metadata.
    metadata: Dict of metadata to update. A None-value indicates the key
      should be removed.

  Returns:
    A list of {'key': ..., 'value': ...} dicts.
  """
  metadata = metadata.copy()
  result = []

  # Update existing metadata.
  for item in items:
    if item['key'] not in metadata:
      # Not referenced at all, just keep as is.
      result.append({'key': item['key'], 'value': item['value']})
    else:
      value = metadata.pop(item['key'])
      if value is not None:
        # Referenced non-None value, use the newer one.
        result.append({'key': item['key'], 'value': value})
      else:
        # Referenced None value, omit.
        pass

  # Add new metadata.
  result.extend(
      [{'key': key, 'value': value} for key, value in metadata.iteritems()])
  return result


def compress_metadata_updates(metadata_updates):
  """Compresses metadata updates into a single update.

  Args:
    instance: A list of models.MetadataUpdate instances.

  Retuns:
    A models.MetadataUpdate instance.
  """
  metadata = {}
  for update in metadata_updates:
    metadata.update(update.metadata)

  return models.MetadataUpdate(metadata=metadata)


@ndb.transactional
def compress_pending_metadata_updates(key):
  """Compresses pending metadata updates into a single active update.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if entity.active_metadata_update:
    logging.warning('Instance already has active metadata update: %s', key)
    return

  if not entity.pending_metadata_updates:
    return

  entity.active_metadata_update = compress_metadata_updates(
      entity.pending_metadata_updates)
  entity.pending_metadata_updates = []
  entity.put()


def compress(key):
  """Sets active instance metadata update.

  Args:
    key: ndb.Key for a models.instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if entity.active_metadata_update:
    logging.warning('Instance already has active metadata update: %s', key)
    return

  if not entity.pending_metadata_updates:
    return

  compress_pending_metadata_updates(key)


def schedule_metadata_compressions():
  """Enqueues tasks to compress instance metadata."""
  for instance in models.Instance.query():
    if instance.pending_metadata_updates:
      if not utils.enqueue_task(
          '/internal/queues/compress-instance-metadata-updates',
          'compress-instance-metadata-updates',
          params={
              'key': instance.key.urlsafe(),
          },
      ):
        logging.warning('Failed to enqueue task for Instance: %s', instance.key)


@ndb.transactional
def associate_metadata_operation(key, checksum, url):
  """Associates the metadata operation with the active metadata update.

  Args:
    key: ndb.Key for a models.Instance entity.
    checksum: Metadata checksum the operation is associated with.
    url: URL for the the zone operation.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.active_metadata_update:
    logging.warning('Instance active metadata update unspecified: %s', key)
    return

  if entity.active_metadata_update.checksum != checksum:
    logging.warning('Instance has unexpected active metadata update: %s', key)
    return

  if entity.active_metadata_update.url:
    if entity.active_metadata_update.url == url:
      return
    logging.warning('Instance has associated metadata operation: %s', key)
    return

  entity.active_metadata_update.url = url
  entity.put()


def update(key):
  """Updates instance metadata.

  Args:
    key: ndb.Key for a models.instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.active_metadata_update:
    logging.warning('Instance active metadata update unspecified: %s', key)
    return

  if entity.active_metadata_update.url:
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

  if not grandparent.project:
    logging.warning(
        'InstanceTemplateRevision project unspecified: %s', grandparent.key)
    return

  result = net.json_request(entity.url, scopes=gce.AUTH_SCOPES)
  api = gce.Project(grandparent.project)
  operation = api.set_metadata(
      parent.key.id(),
      key.id(),
      result['metadata']['fingerprint'],
      apply_metadata_update(
          result['metadata']['items'], entity.active_metadata_update.metadata),
  )

  associate_metadata_operation(
      key,
      utilities.compute_checksum(entity.active_metadata_update.metadata),
      operation.url,
  )


def schedule_metadata_updates():
  """Enqueues tasks to update instance metadata."""
  for instance in models.Instance.query():
    if instance.active_metadata_update:
      if not instance.active_metadata_update.url:
        if not utils.enqueue_task(
            '/internal/queues/update-instance-metadata',
            'update-instance-metadata',
            params={
                'key': instance.key.urlsafe(),
            },
        ):
          logging.warning(
            'Failed to enqueue task for Instance: %s', instance.key)


@ndb.transactional
def reschedule_active_metadata_update(key, url):
  """Reschedules the active metadata update.

  Args:
    key: ndb.Key for a models.Instance entity.
    url: URL for the zone operation to reschedule.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.active_metadata_update:
    logging.warning('Instance active metadata operation unspecified: %s', key)
    return

  if entity.active_metadata_update.url != url:
    logging.warning(
        'Instance has unexpected active metadata operation: %s', key)
    return

  metadata_updates = [entity.active_metadata_update]
  metadata_updates.extend(entity.pending_metadata_updates)
  entity.active_metadata_update = compress_metadata_updates(metadata_updates)
  entity.pending_metadata_updates = []
  entity.put()


@ndb.transactional
def clear_active_metadata_update(key, url):
  """Clears the active metadata update.

  Args:
    key: ndb.Key for a models.Instance entity.
    url: URL for the zone operation to clear.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.active_metadata_update:
    return

  if entity.active_metadata_update.url != url:
    logging.warning(
        'Instance has unexpected active metadata operation: %s', key)
    return

  entity.active_metadata_update = None
  entity.put()


def check(key):
  """Checks the active metadata update operation.

  Reschedules the active metadata update if the operation failed.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.active_metadata_update:
    logging.warning('Instance active metadata operation unspecified: %s', key)
    return

  if not entity.active_metadata_update.url:
    logging.warning(
        'Instance active metadata operation URL unspecified: %s', key)
    return

  result = net.json_request(
      entity.active_metadata_update.url, scopes=gce.AUTH_SCOPES)
  if result['status'] != 'DONE':
    return

  if result.get('error'):
    logging.warning(
        'Instance metadata operation failed: %s\n%s',
        key,
        json.dumps(result, indent=2),
    )
    reschedule_active_metadata_update(key, entity.active_metadata_update.url)
  else:
    clear_active_metadata_update(key, entity.active_metadata_update.url)


def schedule_metadata_operations_check():
  """Enqueues tasks to check on metadata operations."""
  for instance in models.Instance.query():
    if instance.active_metadata_update and instance.active_metadata_update.url:
      if not utils.enqueue_task(
          '/internal/queues/check-instance-metadata-operation',
          'check-instance-metadata-operation',
          params={
              'key': instance.key.urlsafe(),
          },
      ):
        logging.warning('Failed to enqueue task for Instance: %s', instance.key)
