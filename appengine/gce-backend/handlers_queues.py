# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for the GCE Backend."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import machine_provider
from components import net

import models


@ndb.transactional
def uncatalog_instances(instances):
  """Uncatalogs cataloged instances.

  Args:
    instances: List of instance names to uncatalog.
  """
  put_futures = []
  get_futures = [
      models.Instance.generate_key(instance_name).get_async()
      for instance_name in instances
  ]
  while get_futures:
    ndb.Future.wait_any(get_futures)
    instances = [future.get_result() for future in get_futures if future.done()]
    get_futures = [future for future in get_futures if not future.done()]
    for instance in instances:
      if instance.state == models.InstanceStates.CATALOGED:
        # handlers_cron.py sets each Instance's state to
        # CATALOGED before triggering InstanceGroupCataloger.
        logging.info('Uncataloging instance: %s', instance.name)
        instance.state = models.InstanceStates.UNCATALOGED
        put_futures.append(instance.put_async())
      else:
        logging.info('Ignoring already uncataloged instance: %s', instance.name)
  if put_futures:
    ndb.Future.wait_all(put_futures)
  else:
    logging.info('Nothing to uncatalog')


class InstanceGroupCataloger(webapp2.RequestHandler):
  """Worker for cataloging instance groups."""

  @decorators.require_taskqueue('catalog-instance-group')
  def post(self):
    """Reclaim a machine.

    Params:
      dimensions: JSON-encoded string representation of
        machine_provider.Dimensions describing the members of the instance
        group.
      instances: JSON-encoded list of instances in the instance group to
        catalog:
      policies: JSON-encoded string representation of machine_provider.Policies
        governing the members of the instance group.
    """
    dimensions = json.loads(self.request.get('dimensions'))
    instances = json.loads(self.request.get('instances'))
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

    uncatalog_instances(instances_to_uncatalog)


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/catalog-instance-group', InstanceGroupCataloger),
  ])
