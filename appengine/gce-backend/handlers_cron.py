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


@ndb.transactional
def delete_instances(instance_keys):
  """Sets the given Instances as DELETED.

  Args:
    instance_keys: List of ndb.Keys for Instances to set as DELETED.
  """
  get_futures = [instance_key.get_async() for instance_key in instance_keys]
  put_futures = []
  while get_futures:
    ndb.Future.wait_any(get_futures)
    incomplete_futures = []
    for future in get_futures:
      if future.done():
        instance = future.get_result()
        if instance.state == models.InstanceStates.PENDING_DELETION:
          logging.info('Deleting instance: %s', instance.name)
          # TODO(smut): Remove from the datastore.
          # The deletion operation returns success as soon as it's scheduled.
          # We also need to list the instances to check which ones have been
          # deleted.
          instance.state = models.InstanceStates.DELETED
          put_futures.append(instance.put_async())
        else:
          logging.warning(
              'Instance no longer pending deletion: %s', instance.name)
      else:
        incomplete_futures.append(future)
    get_futures = incomplete_futures
  if put_futures:
    ndb.Future.wait_all(put_futures)


class InstanceDeletionProcessor(webapp2.RequestHandler):
  """Worker for processing pending instance deletions."""

  @decorators.require_cronjob
  def get(self):
    # Aggregate instances because the API lets us batch instance deletions
    # for a common instance group manager.
    instance_map = collections.defaultdict(list)
    for instance in models.Instance.query(
        models.Instance.state == models.InstanceStates.PENDING_DELETION
    ):
      instance_map[instance.group].append(instance)

    # TODO(smut): Delete instances in different instance groups concurrently.
    for group, instances in instance_map.iteritems():
      # TODO(smut): Resize the instance group.
      # When instances are deleted from an instance group, the instance group's
      # size is decreased by the number of deleted instances. We need to resize
      # the group back to its original size in order to replace those deleted
      # instances.
      logging.info(
          'Deleting instances from instance group: %s\n%s',
          group,
          [instance.name for instance in instances],
      )
      api = gce.Project(instance.project)
      # Try to delete the instances. If the operation succeeds, update the
      # datastore. If it fails, just move on to the next set of instances
      # and try again later.
      try:
        response = api.delete_instances(
            group, instance.zone, [instance.url for instance in instances])
        if response.get('status') == 'DONE':
          delete_instances([instance.key for instance in instances])
      except net.Error as e:
        logging.warning('%s', e)


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


@ndb.transactional(xg=True)
def create_instance_group(name, dimensions, policies, instances, zone, project):
  """Stores an InstanceGroup and Instance entities in the datastore.

  Also attempts to catalog each running Instance in the Machine Provider.

  Operates on two root entities: model.Instance and model.InstanceGroup.

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
  instance_map = {}
  instances_to_catalog = []

  for instance_name, instance in instances.iteritems():
    logging.info('Processing instance: %s', instance_name)
    instance_key = models.Instance.generate_key(instance_name)
    instance_map[instance_name] = models.Instance(
        key=instance_key,
        group=name,
        name=instance_name,
        project=project,
        state=models.InstanceStates.UNCATALOGED,
        url=instance['instance'],
        zone=zone,
    )
    if instance.get('instanceStatus') == 'RUNNING':
      existing_instance = instance_key.get()
      if existing_instance:
        if existing_instance.state == models.InstanceStates.UNCATALOGED:
          logging.info('Attempting to catalog instance: %s', instance_name)
          instances_to_catalog.append(instance_name)
        else:
          logging.info('Skipping already cataloged instance: %s', instance_name)
          instance_map[instance_name].state = existing_instance.state
    else:
      logging.warning(
          'Instance not running: %s\ncurrentAction: %s\ninstanceStatus: %s',
          instance_name,
          instance['currentAction'],
          instance.get('instanceStatus'),
      )

  if instances_to_catalog:
    # Above we defaulted each instance to UNCATALOGED. Here, try to enqueue a
    # task to catalog them in the Machine Provider, setting CATALOGED if
    # successful.
    if utils.enqueue_task(
        '/internal/queues/catalog-instance-group',
        'catalog-instance-group',
        params={
            'dimensions': utils.encode_to_json(dimensions),
            'instances': utils.encode_to_json(instances_to_catalog),
            'policies': utils.encode_to_json(policies),
        },
        transactional=True,
    ):
      for instance_name in instances_to_catalog:
        instance_map[instance_name].state = models.InstanceStates.CATALOGED
  else:
    logging.info('Nothing to catalog')

  ndb.put_multi(instance_map.values())
  models.InstanceGroup.create_and_put(
      name, dimensions, policies, sorted(instance_map.keys()))


class InstanceProcessor(webapp2.RequestHandler):
  """Worker for processing instances."""

  @decorators.require_cronjob
  def get(self):
    pubsub_handler = handlers_pubsub.MachineProviderSubscriptionHandler
    if not pubsub_handler.is_subscribed():
      logging.error(
          'Pub/Sub subscription not created:\n%s',
          pubsub_handler.get_subscription_url(),
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
          on_reclamation=machine_provider.MachineReclamationPolicy.DELETE,
          pubsub_project=
              handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC_PROJECT,
          pubsub_topic=handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC,
      )

      create_instance_group(
          template.instance_group_name,
          dimensions,
          policies,
          instances,
          template.zone,
          template.instance_group_project,
      )


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/delete-instances', InstanceDeletionProcessor),
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
      ('/internal/cron/process-instances', InstanceProcessor),
  ])
