# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import json
import logging

from google.appengine.api import app_identity
import webapp2

from components import decorators
from components import gce
from components import machine_provider


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


class InstanceProcessor(webapp2.RequestHandler):
  """Worker for processing instances."""

  @decorators.require_cronjob
  def get(self):
    api = gce.Project(GCE_PROJECT_ID)
    logging.info('Retrieving instance templates')
    templates = api.get_instance_templates()
    logging.info('Retrieving instance group managers')
    managers = api.get_instance_group_managers(ZONE)

    instances = {}

    # For each group manager, tell the Machine Provider about its instances.
    for manager_name, manager in managers.iteritems():
      logging.info('Processing instance group manager: %s', manager_name)
      # Extract template name from a link to the template.
      template_name = manager['instanceTemplate'].split('/')[-1]
      # Property-related verification was done by InstanceTemplateProcessor,
      # so we can be sure all the properties we need have been supplied.
      properties = templates[template_name]['properties']
      os_family = [
          metadatum['value'] for metadatum in properties['metadata']['items']
          if metadatum['key'] == 'os_family'
      ][0]
      group_dimensions = {
          'backend': machine_provider.Backend.GCE.name,
          'disk_gb': properties['disks'][0]['initializeParams']['diskSizeGb'],
          'memory_gb': gce.machine_type_to_memory(properties['machineType']),
          'num_cpus': gce.machine_type_to_num_cpus(properties['machineType']),
          'os_family': os_family,
      }

      instances = api.get_managed_instances(manager_name, ZONE)
      for instance_name, instance in instances.iteritems():
        logging.info('Processing instance: %s', instance_name)
        if instance['instanceStatus'] == 'RUNNING':
          instances[instance_name] = group_dimensions.copy()
          instances[instance_name]['hostname'] = instance_name
          # TODO(smut): Static IP assignment.

    machine_provider.add_machines(instances.values())


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
      ('/internal/cron/process-instances', InstanceProcessor),
  ])
