# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for interacting with the Machine Provider catalog."""

import json
import logging

from google.appengine.ext import ndb
from protorpc.remote import protojson

from components import gce
from components import machine_provider
from components import utils

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
    dimensions['disk_size_gb'] = instance_template_revision.disk_size_gb

  if instance_template_revision.machine_type:
    dimensions['memory_gb'] = gce.machine_type_to_memory(
        instance_template_revision.machine_type)
    dimensions['num_cpus'] = gce.machine_type_to_num_cpus(
        instance_template_revision.machine_type)

  dimensions['hostname'] = instance.key.id()

  return dimensions


@ndb.transactional
def set_cataloged(key):
  """Sets the given instance as cataloged.

  Args:
    key: ndb.Key for a models.Instance entity.
  """
  entity = key.get()
  if not entity:
    logging.warning('Instance does not exist: %s', instance)
    return

  if not entity.cataloged:
    entity.cataloged = True
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

  response = machine_provider.add_machine(
      extract_dimensions(entity, grandparent),
      get_policies(key, grandparent.service_accounts[0].name),
  )

  if response.get('error') and response['error'] != 'HOSTNAME_REUSE':
    # Ignore duplicate requests.
    logging.warning(
        'Error adding Instance to catalog: %s\nError: %s',
        key,
        response['error'],
    )
    return

  set_cataloged(key)


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
