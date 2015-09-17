# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import gce
from components import machine_provider

import models


# TODO(smut): Make this modifiable at runtime (keep in datastore).
GCE_PROJECT_ID = app_identity.get_application_id()

# TODO(smut): Make this modifiable at runtime (keep in datastore).
# Minimum number of instances to keep in each instance group.
MIN_INSTANCE_GROUP_SIZE = 4

# TODO(smut): Support other zones.
ZONE = 'us-central1-f'


def filter_templates(templates):
  """Filters out misconfigured templates.

  Args:
    templates: A dict mapping instance template names to
      compute#instanceTemplate dicts.

  Yields:
    (instance template name, compute#instanceTemplate dict) pairs.
  """
  for template_name, template in templates.iteritems():
    logging.info('Proessing instance template: %s', template_name)
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


@ndb.transactional
def create_instance_group(name, dimensions, policies, members):
  """Stores an InstanceGroup and Instance entities in the datastore.

  Transactionally operates on model.Instance entities which share a common
  ancestor model.InstanceGroup (which is the root entity operated on).

  Args:
    name: Name of this instance group.
    dimensions: rpc_messages.Dimensions describing members of this instance
      group.
    policies: rpc_messages.Policies governing members of this instance group.
    members: List of models.Instances representing members of this instance
      group.
  """
  member_names = []
  for instance in members:
    member_names.append(instance.name)
    instance.key = instance.generate_key(instance.name, name)
  ndb.put_multi(members)
  models.InstanceGroup.create_and_put(name, dimensions, policies, member_names)


class InstanceProcessor(webapp2.RequestHandler):
  """Worker for processing instances."""

  @decorators.require_cronjob
  def get(self):
    api = gce.Project(GCE_PROJECT_ID)
    logging.info('Retrieving instance templates')
    templates = api.get_instance_templates()
    logging.info('Retrieving instance group managers')
    managers = api.get_instance_group_managers(ZONE)

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

      instances = api.get_managed_instances(manager_name, ZONE)
      members = []
      for instance_name, instance in instances.iteritems():
        logging.info('Processing instance: %s', instance_name)
        members.append(models.Instance(name=instance_name, group=manager_name))
        if instance['instanceStatus'] == 'RUNNING':
          requests.append(
              machine_provider.CatalogMachineAdditionRequest(
                  dimensions=machine_provider.Dimensions(
                      backend=machine_provider.Backend.GCE,
                      disk_gb=disk_gb,
                      hostname=instance_name,
                      memory_gb=memory_gb,
                      num_cpus=num_cpus,
                      os_family=os_family,
                  ),
                  policies=machine_provider.Policies(
                      on_reclamation=
                          machine_provider.MachineReclamationPolicy.DELETE,
                  ),
              )
          )
          members[-1].state = models.InstanceStates.CATALOGED
        else:
          members[-1].state = models.InstanceStates.UNCATALOGED
          # TODO(smut): Static IP assignment.

      dimensions = machine_provider.Dimensions(
          backend=machine_provider.Backend.GCE,
          disk_gb=disk_gb,
          memory_gb=memory_gb,
          num_cpus=num_cpus,
          os_family=os_family,
      )
      policies = machine_provider.Policies(
          on_reclamation=machine_provider.MachineReclamationPolicy.DELETE)
      create_instance_group(manager_name, dimensions, policies, members)

    machine_provider.add_machines(requests)


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
      ('/internal/cron/process-instances', InstanceProcessor),
  ])
