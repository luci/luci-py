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
def reschedule_instance_cataloging(instance_group_key, instances):
  """Reschedules the given instances for cataloging.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    instances: List of instance names to reschedule for cataloging.
  """
  instances = set(instances)
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  updated = False
  for instance in instance_group.members:
    if instance.name in instances:
      instances.discard(instance.name)
      if instance.state == models.InstanceStates.CATALOGED:
        # handlers_cron.py sets each instance's state to CATALOGED
        # before triggering the InstanceGroupCataloger task queue.
        # Since cataloging failed, revert to PENDING_CATALOG to
        # try again later.
        logging.info('Uncataloging instance: %s', instance.name)
        instance.state = models.InstanceStates.PENDING_CATALOG
        updated = True
      elif instance.state == models.InstanceStates.PENDING_CATALOG:
        logging.info('Ignoring already rescheduled instance: %s', instance.name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)

  if instances:
    logging.warning('Instances not found: %s', ', '.join(sorted(instances)))

  if updated:
    instance_group.put()


class InstanceGroupCataloger(webapp2.RequestHandler):
  """Worker for cataloging instance groups."""

  @decorators.require_taskqueue('catalog-instance-group')
  def post(self):
    """Catalogs instances in the Machine Provider.

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

    requests = {}

    for instance_name in instances:
      requests[instance_name] = {
          'dimensions': dimensions.copy(), 'policies': policies}
      requests[instance_name]['dimensions']['hostname'] = instance_name

    try:
      responses = machine_provider.add_machines(
          requests.values()).get('responses', {})
    except net.Error as e:
      logging.warning(e)
      responses = {}

    for response in responses:
      request = response.get('machine_addition_request', {})
      error = response.get('error')
      instance_name = request.get('dimensions', {}).get('hostname')
      if instance_name in requests.keys():
        if not error:
          logging.info('Instance added to Catalog: %s', instance_name)
          requests.pop(instance_name)
        elif error == 'HOSTNAME_REUSE':
          logging.warning('Hostname reuse in Catalog: %s', instance_name)
          requests.pop(instance_name)
        else:
          logging.warning('Instance not added to Catalog: %s', instance_name)
      else:
        logging.info('Unknown instance: %s', instance_name)

    reschedule_instance_cataloging(
        models.InstanceGroup.generate_key(name), requests.keys())


@ndb.transactional
def delete_instances(instance_group_key, instances):
  """Deletes instances from the datastore.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    instances: List of instance names to delete.
  """
  instances = set(instances)
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  members = []
  updated = False
  for instance in instance_group.members:
    members.append(instance)
    if instance.name in instances:
      instances.discard(instance.name)
      if instance.state == models.InstanceStates.DELETING:
        logging.info('Deleting instance: %s', instance.name)
        members.pop()
        updated = True
      else:
        logging.error('Instance in unexpected state:\n%s', instance)

  if instances:
    logging.warning('Instances not found: %s', ', '.join(sorted(instances)))

  if updated:
    instance_group.members = members
    instance_group.put()


@ndb.transactional
def reschedule_instance_deletion(instance_group_key, instances):
  """Reschedules the given instances for deletion.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    instances: List of instance names to reschedule for deletion.
  """
  instances = set(instances)
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  updated = False
  for instance in instance_group.members:
    if instance.name in instances:
      instances.discard(instance.name)
      if instance.state == models.InstanceStates.DELETING:
        logging.info('Rescheduling deletion of instance: %s', instance.name)
        instance.state = models.InstanceStates.PENDING_DELETION
        updated = True
      elif instance.state == models.InstanceStates.PENDING_DELETION:
        logging.info('Ignoring already rescheduled instance: %s', instance.name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)

  if instances:
    logging.warning('Instances not found: %s', ', '.join(sorted(instances)))

  if updated:
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

    instance_group_key = models.InstanceGroup.generate_key(group)
    instances = sorted(instance_map.keys())

    logging.info(
        'Deleting instances from instance group: %s\n%s',
        group,
        ', '.join(instances),
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
        # Either they all succeed or they all fail. If they all succeeded,
        # remove them from the datastore and return. In all other cases,
        # set them back to PENDING_DELETION to try again later.
        delete_instances(instance_group_key, instances)
        return
    except net.Error as e:
      logging.warning('%s', e)
    reschedule_instance_deletion(instance_group_key, instances)


@ndb.transactional
def set_prepared_instance_states(instance_group_key, succeeded, failed):
  """Sets the states of prepared instances.

  Args:
    instance_group_key: ndb.Key for the instance group containing the instances.
    succeeded: List of instance names to schedule for cataloging.
    failed: List of instance names to reschedule for preparation.
  """
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  updated = False
  for instance in instance_group.members:
    if instance.name in succeeded:
      succeeded.remove(instance.name)
      if instance.state == models.InstanceStates.PREPARING:
        logging.info('Scheduling catalog of instance: %s', instance.name)
        instance.state = models.InstanceStates.PENDING_CATALOG
        updated = True
      elif instance.state == models.InstanceStates.PENDING_CATALOG:
        logging.info('Ignoring already scheduled instance: %s', instance.name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)
    elif instance.name in failed:
      failed.remove(instance.name)
      if instance.state == models.InstanceStates.PREPARING:
        logging.info('Rescheduling preparation of instance: %s', instance.name)
        instance.state = models.InstanceStates.NEW
        updated = True
      elif instance.state == models.InstanceStates.NEW:
        logging.info('Ignoring already rescheduled instance: %s', instance.name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)

  if succeeded:
    logging.warning('Instances not found: %s', ', '.join(sorted(succeeded)))
  if failed:
    logging.warning('Instances not found: %s', ', '.join(sorted(failed)))

  if updated:
    instance_group.put()


class InstancePreparer(webapp2.RequestHandler):
  """Worker for preparing instances."""

  @decorators.require_taskqueue('prepare-instances')
  def post(self):
    """Prepares GCE instances for use.

    Params:
      group: Name of the instance group containing the instances to prepare.
      instances: JSON-encoded list of instances to prepare.
      project: Name of the project the instance group exists in.
      zone: Zone the instances exist in. e.g. us-central1-f.
    """
    group = self.request.get('group')
    instances = json.loads(self.request.get('instances'))
    project = self.request.get('project')
    zone = self.request.get('zone')

    # TODO(smut): Prepare instances.
    set_prepared_instance_states(
        models.InstanceGroup.generate_key(group), instances, [])


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/catalog-instance-group', InstanceGroupCataloger),
      ('/internal/queues/delete-instances', InstanceDeleter),
      ('/internal/queues/prepare-instances', InstancePreparer),
  ])
