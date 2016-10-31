# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for interacting with the Machine Provider catalog."""

import json
import logging

from google.appengine.ext import ndb
from protorpc.remote import protojson

from components import gce
from components import machine_provider
from components import net
from components import utils

import instances
import instance_group_managers
import metrics
import models
import pubsub


def get_policies(key, service_account):
  """Returns Machine Provider policies governing the given instance.

  Args:
    key: ndb.Key for a models.Instance entity.
    service_account: Name of the service account the instance will use to
      talk to Machine Provider.
  """
  return {
      'backend_attributes': {
          'key': 'key',
          'value': key.urlsafe(),
      },
      'backend_project': pubsub.get_machine_provider_topic_project(),
      'backend_topic': pubsub.get_machine_provider_topic(),
      'machine_service_account': service_account,
      'on_reclamation': 'DELETE',
  }


def extract_dimensions(instance, instance_template_revision):
  """Extracts Machine Provider dimensions.

  Args:
    instance: models.Instance entity.
    instance_template_revision: models.InstanceTemplateRevision entity.

  Returns:
    A dict of dimensions.
  """
  if instance_template_revision.dimensions:
    dimensions = json.loads(protojson.encode_message(
        instance_template_revision.dimensions))
  else:
    dimensions = {}

  dimensions['backend'] = 'GCE'

  if instance_template_revision.disk_size_gb:
    dimensions['disk_gb'] = instance_template_revision.disk_size_gb

  if instance_template_revision.machine_type:
    dimensions['memory_gb'] = gce.machine_type_to_memory(
        instance_template_revision.machine_type)
    dimensions['num_cpus'] = gce.machine_type_to_num_cpus(
        instance_template_revision.machine_type)

  dimensions['hostname'] = instance.key.id()

  return dimensions


@ndb.transactional
def set_cataloged(key, cataloged):
  """Sets the cataloged field of the given instance.

  Args:
    key: ndb.Key for a models.Instance entity.
    cataloged: True or False.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', instance)
    return

  if entity.cataloged != cataloged:
    entity.cataloged = cataloged
    entity.put()


def catalog(key):
  """Catalogs the given instance.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', instance)
    return

  if entity.cataloged:
    return

  if entity.pending_deletion:
    logging.warning('Instance pending deletion: %s', instance)
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

  if not grandparent.service_accounts:
    logging.warning(
        'InstanceTemplateRevision service account unspecified: %s',
        parent.key.parent(),
    )
    return

  logging.info('Cataloging Instance: %s', key)
  response = machine_provider.add_machine(
      extract_dimensions(entity, grandparent),
      get_policies(key, grandparent.service_accounts[0].name),
  )

  if response.get('error') and response['error'] != 'HOSTNAME_REUSE':
    # Assume HOSTNAME_REUSE implies a duplicate request.
    logging.warning(
        'Error adding Instance to catalog: %s\nError: %s',
        key,
        response['error'],
    )
    return

  set_cataloged(key, True)
  metrics.send_machine_event('CATALOGED', key.id())


def schedule_catalog():
  """Enqueues tasks to catalog instances."""
  # Only enqueue tasks for uncataloged instances not pending deletion which
  # are part of active instance group managers which are part of active
  # instance templates.
  for instance_template in models.InstanceTemplate.query():
    if instance_template.active:
      instance_template_revision = instance_template.active.get()
      if instance_template_revision:
        for instance_group_manager_key in instance_template_revision.active:
          instance_group_manager = instance_group_manager_key.get()
          if instance_group_manager:
            for instance_key in instance_group_manager.instances:
              instance = instance_key.get()
              if instance:
                if not instance.cataloged and not instance.pending_deletion:
                  if not utils.enqueue_task(
                      '/internal/queues/catalog-instance',
                      'catalog-instance',
                      params={
                          'key': instance.key.urlsafe(),
                      },
                  ):
                    logging.warning(
                        'Failed to enqueue task for Instance: %s', instance.key)


def remove(key):
  """Removes the given instance from the catalog.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.cataloged:
    return

  response = machine_provider.delete_machine({'hostname': key.id()})
  if response.get('error') and response['error'] != 'ENTRY_NOT_FOUND':
    # Assume ENTRY_NOT_FOUND implies a duplicate request.
    logging.warning(
        'Error removing Instance from catalog: %s\nError: %s',
        key,
        response['error'],
    )
    return

  set_cataloged(key, False)


def schedule_removal():
  """Enqueues tasks to remove drained instances from the catalog."""
  for instance_group_manager_key in (
      instance_group_managers.get_drained_instance_group_managers()):
    instance_group_manager = instance_group_manager_key.get()
    if instance_group_manager:
      for instance_key in instance_group_manager.instances:
        instance = instance_key.get()
        if instance and instance.cataloged:
          if not utils.enqueue_task(
              '/internal/queues/remove-cataloged-instance',
              'remove-cataloged-instance',
              params={
                  'key': instance.key.urlsafe(),
              },
          ):
            logging.warning(
                'Failed to enqueue task for Instance: %s', instance.key)


def update_cataloged_instance(key):
  """Updates an Instance based on its state in Machine Provider's catalog.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', key)
    return

  if not entity.cataloged:
    return

  try:
    response = machine_provider.retrieve_machine(key.id())
    if response.get('pubsub_subscription') and not entity.pubsub_subscription:
      metrics.send_machine_event('SUBSCRIPTION_RECEIVED', key.id())
      instances.add_subscription_metadata(
          key,
          response['pubsub_subscription_project'],
          response['pubsub_subscription'],
      )
      metrics.send_machine_event('METADATA_UPDATE_PROPOSED', key.id())
  except net.NotFoundError:
    instances.mark_for_deletion(key)


def schedule_cataloged_instance_update():
  """Enqueues tasks to update information about cataloged instances."""
  for instance in models.Instance.query():
    if instance.cataloged:
      if not utils.enqueue_task(
          '/internal/queues/update-cataloged-instance',
          'update-cataloged-instance',
          params={
              'key': instance.key.urlsafe(),
          },
      ):
        logging.warning('Failed to enqueue task for Instance: %s', instance.key)
