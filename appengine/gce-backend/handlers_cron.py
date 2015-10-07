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
from components import pubsub
from components import utils

import handlers_pubsub
import models


# TODO(smut): Make this modifiable at runtime (keep in datastore).
GCE_PROJECT_ID = app_identity.get_application_id()

# TODO(smut): Make this modifiable at runtime (keep in datastore).
# Minimum number of instances to keep in each instance group.
MIN_INSTANCE_GROUP_SIZE = 4

# TODO(smut): Support other zones.
ZONE = 'us-central1-f'


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
    api = gce.Project(GCE_PROJECT_ID)

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
      # Try to delete the instances. If the operation succeeds, update the
      # datastore. If it fails, just move on to the next set of instances
      # and try again later.
      try:
        response = api.delete_instances(
            group, ZONE, [instance.url for instance in instances])
        if response.get('status') == 'DONE':
          delete_instances([instance.key for instance in instances])
      except net.Error as e:
        logging.warning('%s', e)


def filter_templates(templates):
  """Filters out misconfigured templates.

  Args:
    templates: A dict mapping instance template names to
      compute#instanceTemplate dicts.

  Yields:
    (instance template name, compute#instanceTemplate dict) pairs.
  """
  for template_name, template in templates.iteritems():
    logging.info('Processing instance template: %s', template_name)
    properties = template.get('properties', {})
    # For now, enforce only one disk.
    if len(properties.get('disks', [])) == 1:
      # For now, require the template to give the OS family in its metadata.
      os_family_values = [
          metadatum['value'] for metadatum in properties['metadata'].get(
              'items', [],
          )
          if metadatum['key'] == 'os_family'
      ]
      if len(os_family_values) == 1:
        if os_family_values[0] in machine_provider.OSFamily.names():
          yield template_name, template
        else:
          logging.warning(
              'Skipping %s due to invalid os_family: %s',
              template_name,
              os_family_values[0],
          )
      else:
        logging.warning(
            'Skipping %s due to metadata errors:\n%s',
            template_name,
            json.dumps(properties.get('metadata', {}), indent=2),
        )
    else:
      logging.warning(
          'Skipping %s due to unsupported number of disks:\n%s',
          template_name,
          json.dumps(properties.get('disks', []), indent=2),
      )


class InstanceTemplateProcessor(webapp2.RequestHandler):
  """Worker for processing instance templates."""

  @decorators.require_cronjob
  def get(self):
    api = gce.Project(GCE_PROJECT_ID)
    logging.info('Retrieving instance templates')
    templates = api.get_instance_templates()
    logging.info('Retrieving instance group managers')
    managers = api.get_instance_group_managers(ZONE)

    # For each template, ensure there exists a group manager managing it.
    for template_name, template in filter_templates(templates):
      if template_name not in managers:
        logging.info(
          'Creating instance group manager from instance template: %s',
          template_name,
        )
        api.create_instance_group_manager(
            template, MIN_INSTANCE_GROUP_SIZE, ZONE,
        )
      else:
        logging.info('Instance group manager already exists: %s', template_name)


@ndb.transactional(xg=True)
def create_instance_group(name, dimensions, policies, instances):
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
        state=models.InstanceStates.UNCATALOGED,
        url=instance['instance'],
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
    api = gce.Project(GCE_PROJECT_ID)
    logging.info('Retrieving instance templates')
    templates = api.get_instance_templates()
    logging.info('Retrieving instance group managers')
    managers = api.get_instance_group_managers(ZONE)

    handlers_pubsub.MachineProviderSubscriptionHandler.ensure_subscribed()

    requests = []

    # TODO(smut): Process instance group managers concurrently with a taskqueue.
    # For each group manager, tell the Machine Provider about its instances.
    for manager_name, manager in managers.iteritems():
      logging.info('Processing instance group manager: %s', manager_name)
      # Extract template name from a link to the template.
      template_name = manager['instanceTemplate'].split('/')[-1]
      # Property-related verification was done by InstanceTemplateProcessor,
      # so we can be sure all the properties we need have been supplied.
      properties = templates[template_name]['properties']
      disk_gb = int(properties['disks'][0]['initializeParams']['diskSizeGb'])
      memory_gb = float(gce.machine_type_to_memory(properties['machineType']))
      num_cpus = gce.machine_type_to_num_cpus(properties['machineType'])
      os_family = machine_provider.OSFamily.lookup_by_name([
          metadatum['value'] for metadatum in properties['metadata']['items']
          if metadatum['key'] == 'os_family'
      ][0])
      dimensions = machine_provider.Dimensions(
          backend=machine_provider.Backend.GCE,
          disk_gb=disk_gb,
          memory_gb=memory_gb,
          num_cpus=num_cpus,
          os_family=os_family,
      )
      instances = api.get_managed_instances(manager_name, ZONE)
      policies = machine_provider.Policies(
          on_reclamation=machine_provider.MachineReclamationPolicy.DELETE,
          pubsub_project=
              handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC_PROJECT,
          pubsub_topic=handlers_pubsub.MachineProviderSubscriptionHandler.TOPIC,
      )

      create_instance_group(manager_name, dimensions, policies, instances)


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/delete-instances', InstanceDeletionProcessor),
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
      ('/internal/cron/process-instances', InstanceProcessor),
  ])
