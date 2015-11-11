# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import collections
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import gce
from components import machine_provider
from components import net
from components import utils

import handlers_pubsub
import models


class InstanceTemplateProcessor(webapp2.RequestHandler):
  """Worker for processing instance templates."""

  @decorators.require_cronjob
  def get(self):
    # For each template entry in the datastore, create a group manager.
    for template in models.InstanceTemplate.query():
      logging.info(
          'Retrieving instance template %s from project %s',
          template.template_name,
          template.template_project,
      )
      api = gce.Project(template.template_project)
      try:
        instance_template = api.get_instance_template(template.template_name)
      except net.NotFoundError:
        logging.error(
            'Instance template does not exist: %s',
            template.template_name,
        )
        continue
      api = gce.Project(template.instance_group_project)
      try:
        api.create_instance_group_manager(
            template.instance_group_name,
            instance_template,
            template.initial_size,
            template.zone,
        )
      except net.Error as e:
        if e.status_code == 409:
          logging.info(
              'Instance group manager already exists: %s',
              template.template_name,
          )
        else:
          logging.error(
              'Could not create instance group manager: %s\n%s',
              template.template_name,
              e,
          )


@ndb.transactional
def process_instance_group(
    name, dimensions, policies, instances, zone, project):
  """Processes an InstanceGroup entity.

  We store an InstanceGroup entity in the datastore if one doesn't already
  exist, then we look at instances themselves which need to be processed.

  Args:
    name: Name of this instance group.
    dimensions: machine_provider.Dimensions describing members of this instance
      group.
    policies: machine_provider.Policies governing members of this instance
      group.
    instances: Return value of gce.get_managed_instances listing instances in
      this instance group.
    zone: Zone these instances exist in. e.g. us-central1-f.
    project: Project these instances exist in.
  """
  # Mapping of instance names to catalog to service accounts to use for Cloud
  # Pub/Sub communication with the Machine Provider.
  instances_to_catalog = {}
  # Mapping of instance names to instance URLs to delete.
  instances_to_delete = {}
  # List of instances to prepare.
  instances_to_prepare = []
  # Mapping of instances whose metadata should be updated to a dict of metadata
  # to update.
  instances_to_update = {}

  instance_group_key = models.InstanceGroup.generate_key(name)
  instance_group = instance_group_key.get()

  old_instance_map = {}
  new_instance_map = {}

  if instance_group and instance_group.members:
    for instance in instance_group.members:
      old_instance_map[instance.name] = instance

  for instance_name, instance in instances.iteritems():
    logging.info('Processing instance: %s', instance_name)
    if instance.get('instanceStatus') == 'RUNNING':
      existing_instance = old_instance_map.get(instance_name)
      if existing_instance:
        new_instance_map[instance_name] = existing_instance
        if existing_instance.state == models.InstanceStates.PENDING_CATALOG:
          logging.info('Attempting to catalog instance: %s', instance_name)
          instances_to_catalog[instance_name] = (
              existing_instance.pubsub_service_account)
        elif existing_instance.state == models.InstanceStates.CATALOGED:
          logging.info('Skipping already cataloged instance: %s', instance_name)
        elif existing_instance.state == models.InstanceStates.PENDING_DELETION:
          logging.info('Scheduling instance for deletion: %s', instance_name)
          instances_to_delete[instance_name] = instance['instance']
        elif existing_instance.state == models.InstanceStates.NEW:
          logging.info('Preparing new instance: %s', instance_name)
          instances_to_prepare.append(instance_name)
        elif existing_instance.state == \
            models.InstanceStates.PENDING_METADATA_UPDATE:
          logging.info(
              'Scheduling instance for metadata update: %s', instance_name)
          instances_to_update[instance_name] = {
              'pubsub_service_account':
                  existing_instance.pubsub_service_account,
              'pubsub_subscription': existing_instance.pubsub_subscription,
              'pubsub_subscription_project':
                  existing_instance.pubsub_subscription_project,
              'pubsub_topic': existing_instance.pubsub_topic,
              'pubsub_topic_project': existing_instance.pubsub_topic_project,
          }
      else:
        new_instance_map[instance_name] = models.Instance(
            name=instance_name,
            state=models.InstanceStates.NEW,
            url=instance['instance'],
        )
        logging.info('Storing new instance: %s', instance_name)
    else:
      logging.warning(
          'Instance not running: %s\ncurrentAction: %s\ninstanceStatus: %s',
          instance_name,
          instance['currentAction'],
          instance.get('instanceStatus'),
      )

  if instances_to_catalog:
    # If we fail to enqueue the task to add these instances to the Machine
    # Provider, set them back to PENDING_CATALOG in order to try again later.
    if utils.enqueue_task(
        '/internal/queues/catalog-instance-group',
        'catalog-instance-group',
        params={
            'dimensions': utils.encode_to_json(dimensions),
            'instance_map': utils.encode_to_json(instances_to_catalog),
            'name': name,
            'policies': utils.encode_to_json(policies),
        },
        transactional=True,
    ):
      for instance_name in instances_to_catalog:
        new_instance_map[instance_name].state = models.InstanceStates.CATALOGED
    else:
      for instance_name in instances_to_catalog:
        new_instance_map[instance_name].state = (
            models.InstanceStates.PENDING_CATALOG)
  else:
    logging.info('Nothing to catalog')

  if instances_to_delete:
    # If we fail to enqueue the task to delete these instances,
    # set them back to PENDING_DELETION in order to try again later.
    if utils.enqueue_task(
        '/internal/queues/delete-instances',
        'delete-instances',
        params={
            'group': name,
            'instance_map': utils.encode_to_json(instances_to_delete),
            'project': project,
            'zone': zone,
        },
        transactional=True,
    ):
      for instance_name in instances_to_delete:
        new_instance_map[instance_name].state = models.InstanceStates.DELETING
    else:
      for instance_name in instances_to_delete:
        new_instance_map[instance_name].state = (
            models.InstanceStates.PENDING_DELETION)
  else:
    logging.info('Nothing to delete')

  if instances_to_prepare:
    # If we fail to enqueue the task to prepare these instances,
    # set them back to NEW in order to try again later.
    if utils.enqueue_task(
        '/internal/queues/prepare-instances',
        'prepare-instances',
        params={
            'group': name,
            'instances': utils.encode_to_json(instances_to_prepare),
            'project': project,
            'zone': zone,
        },
        transactional=True,
    ):
      for instance_name in instances_to_prepare:
        new_instance_map[instance_name].state = models.InstanceStates.PREPARING
    else:
      for instance_name in instances_to_prepare:
        new_instance_map[instance_name].state = models.InstanceStates.NEW
  else:
    logging.info('Nothing to prepare')

  if instances_to_update:
    # If we fail to enqueue the task to update these instances, set
    # them back to PENDING_METADATA_UPDATE in order to try again later.
    if utils.enqueue_task(
        '/internal/queues/update-instance-metadata',
        'update-instance-metadata',
        params={
            'group': name,
            'instance_map': utils.encode_to_json(instances_to_update),
            'project': project,
            'zone': zone,
        },
        transactional=True,
    ):
      for instance_name in instances_to_update:
        new_instance_map[instance_name].state = (
            models.InstanceStates.UPDATING_METADATA)
    else:
      for instance_name in instances_to_update:
        new_instance_map[instance_name].state = (
            models.InstanceStates.PENDING_METADATA_UPDATE)
  else:
    logging.info('Nothing to update')

  models.InstanceGroup(
      key=models.InstanceGroup.generate_key(name),
      dimensions=dimensions,
      members=sorted(
          new_instance_map.values(), key=lambda instance: instance.name),
      name=name,
      policies=policies,
      project=project,
      zone=zone,
  ).put()


class InstanceGroupProcessor(webapp2.RequestHandler):
  """Worker for processing instance group managers."""

  @decorators.require_cronjob
  def get(self):
    pubsub_handler = handlers_pubsub.MachineProviderSubscriptionHandler
    if not pubsub_handler.is_subscribed():
      logging.error(
          'Pub/Sub subscription not created:\n%s',
          pubsub_handler.get_subscription_name(),
      )
      return

    # For each group manager, tell the Machine Provider about its instances.
    for template in models.InstanceTemplate.query():
      logging.info(
          'Retrieving instance template %s from project %s',
          template.template_name,
          template.template_project,
      )
      api = gce.Project(template.template_project)
      try:
        instance_template = api.get_instance_template(template.template_name)
      except net.NotFoundError:
        logging.error(
            'Instance template does not exist: %s',
            template.template_name,
        )
        continue
      api = gce.Project(template.instance_group_project)
      properties = instance_template['properties']
      disk_gb = int(properties['disks'][0]['initializeParams']['diskSizeGb'])
      memory_gb = float(gce.machine_type_to_memory(properties['machineType']))
      num_cpus = gce.machine_type_to_num_cpus(properties['machineType'])
      os_family = machine_provider.OSFamily.lookup_by_name(template.os_family)
      dimensions = machine_provider.Dimensions(
          backend=machine_provider.Backend.GCE,
          disk_gb=disk_gb,
          memory_gb=memory_gb,
          num_cpus=num_cpus,
          os_family=os_family,
      )
      try:
        instances = api.get_managed_instances(
            template.instance_group_name, template.zone)
      except net.NotFoundError:
        logging.warning(
            'Instance group manager does not exist: %s',
            template.instance_group_name,
        )
        continue
      policies = machine_provider.Policies(
          backend_attributes=[
              machine_provider.KeyValuePair(
                  key='group', value=template.instance_group_name),
          ],
          backend_project=
              handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC_PROJECT,
          backend_topic=
              handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC,
          on_reclamation=machine_provider.MachineReclamationPolicy.DELETE,
      )

      process_instance_group(
          template.instance_group_name,
          dimensions,
          policies,
          instances,
          template.zone,
          template.instance_group_project,
      )


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-instance-groups', InstanceGroupProcessor),
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
  ])
