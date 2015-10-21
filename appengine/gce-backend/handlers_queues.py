# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for the GCE Backend."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import gce
from components import machine_provider
from components import net

import models


@ndb.transactional
def uncatalog_instances(instance_group_key, instances):
  """Uncatalogs cataloged instances.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    instances: Set of instance names to uncatalog.
  """
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  updated = False
  for instance in instance_group.members:
    if instance.name in instances:
      if instance.state == models.InstanceStates.CATALOGED:
        # handlers_cron.py sets each instance's state to CATALOGED
        # before triggering the InstanceGroupCataloger task queue.
        # Since cataloging failed, revert to PENDING_CATALOG to
        # try again later.
        logging.info('Uncataloging instance: %s', instance.name)
        instance.state = models.InstanceStates.PENDING_CATALOG
        updated = True
      elif instance.state == models.InstanceStates.PENDING_CATALOG:
        logging.info('Ignoring already uncataloged instance: %s', instance.name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)
  if updated:
    instance_group.put()


class InstanceGroupCataloger(webapp2.RequestHandler):
  """Worker for cataloging instance groups."""

  @decorators.require_taskqueue('catalog-instance-group')
  def post(self):
    """Catalog instances in the Machine Provider.

    Params:
      dimensions: JSON-encoded string representation of
        machine_provider.Dimensions describing the members of the instance
        group.
      instances: JSON-encoded list of instances in the instance group to
        catalog.
      name: Name of the instance group whose instances are being cataloged.
      policies: JSON-encoded string representation of machine_provider.Policies
        governing the members of the instance group.
    """
    dimensions = json.loads(self.request.get('dimensions'))
    instances = json.loads(self.request.get('instances'))
    name = self.request.get('name')
    policies = json.loads(self.request.get('policies'))

    requests = []
    instances_to_uncatalog = set()

    for instance_name in instances:
      instances_to_uncatalog.add(instance_name)
      requests.append({
          'dimensions': dimensions.copy(), 'policies': policies})
      requests[-1]['dimensions']['hostname'] = instance_name

    try:
      responses = machine_provider.add_machines(requests).get('responses', {})
    except net.Error as e:
      logging.warning(e)
      responses = {}

    for response in responses:
      request = response.get('machine_addition_request', {})
      error = response.get('error')
      instance_name = request.get('dimensions', {}).get('hostname')
      if instance_name in instances:
        if not error:
          logging.info('Instance added to Catalog: %s', instance_name)
          instances_to_uncatalog.discard(instance_name)
        elif error == 'HOSTNAME_REUSE':
          logging.warning('Hostname reuse in Catalog: %s', instance_name)
          instances_to_uncatalog.discard(instance_name)
        else:
          logging.warning('Instance not added to Catalog: %s', instance_name)
      else:
        logging.info('Unknown instance: %s', instance_name)

    uncatalog_instances(
        models.InstanceGroup.generate_key(name), instances_to_uncatalog)


@ndb.transactional
def delete_instances(instance_group_key, instances):
  """Deletes instances from the datastore.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    instances: Set of instance names to delete.
  """
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  members = []
  updated = False
  for instance in instance_group.members:
    members.append(instance)
    if instance.name in instances:
      if instance.state == models.InstanceStates.PENDING_DELETION:
        logging.info('Deleting instance: %s', instance.name)
        instances.discard(instance.name)
        members.pop()
        updated = True
      else:
        logging.error('Instance in unexpected state:\n%s', instance)

  if instances:
    logging.info('Instances already deleted: %s', ', '.join(instances))

  if updated:
    instance_group.members = members
    instance_group.put()


class InstanceDeleter(webapp2.RequestHandler):
  """Worker for deleting instances."""

  @decorators.require_taskqueue('delete-instances')
  def post(self):
    """Deletes GCE instances from an instance group.

    Params:
      group: Name of the instance group containing the instances to delete.
      instance_map: JSON-encoded dict mapping instance names to instance URLs
        in the instance group which should be deleted.
      project: Name of the project the instance group exists in.
      zone: Zone the instances exist in. e.g. us-central1-f.
    """
    group = self.request.get('group')
    instance_map = json.loads(self.request.get('instance_map'))
    project = self.request.get('project')
    zone = self.request.get('zone')

    logging.info(
        'Deleting instances from instance group: %s\n%s',
        group,
        ', '.join(instance_map.keys()),
    )
    api = gce.Project(project)
    # Try to delete the instances. If the operation succeeds, update the
    # datastore. If it fails, don't update the datastore which will make us
    # try again later.
    # TODO(smut): Resize the instance group.
    # When instances are deleted from an instance group, the instance group's
    # size is decreased by the number of deleted instances. We need to resize
    # the group back to its original size in order to replace those deleted
    # instances.
    try:
      response = api.delete_instances(group, zone, instance_map.values())
      if response.get('status') == 'DONE':
        delete_instances(
            models.InstanceGroup.generate_key(group), set(instance_map.keys()))
    except net.Error as e:
      logging.warning('%s', e)


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/catalog-instance-group', InstanceGroupCataloger),
      ('/internal/queues/delete-instances', InstanceDeleter),
  ])
