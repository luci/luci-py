# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for fulfilling lease requests."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import net
from components import pubsub
from components.machine_provider import rpc_messages

import models


class LeaseRequestFulfiller(webapp2.RequestHandler):
  """Worker for fulfilling lease requests."""

  @decorators.require_taskqueue('fulfill-lease-request')
  def post(self):
    """Fulfill a lease request.

    Params:
      lease_id: ID of the LeaseRequest being fulfilled.
      lease_json: JSON-encoded string representation of the
        rpc_messages.LeaseRequest being fulfilled.
      machine_id: ID of the CatalogMachineEntry fulfilling the LeaseRequest.
      machine_project: Project that the machine communication topic is contained
        in.
      machine_topic: Topic that the machine communication should occur on.
    """
    lease_id = self.request.get('lease_id')
    lease_json = json.loads(self.request.get('lease_json'))
    machine_id = self.request.get('machine_id')
    machine_project = self.request.get('machine_project')
    machine_topic = self.request.get('machine_topic')

    if lease_json.get('pubsub_topic'):
      pubsub.publish(
          pubsub.full_topic_name(
              lease_json['pubsub_project'],
              lease_json['pubsub_topic'],
          ),
          'FULFILLED',
          {
              'machine_id': machine_id,
              'request_hash': lease_id,
          },
      )

    if lease_json.get('on_lease', {}).get('swarming_server'):
      pubsub.publish(
          pubsub.full_topic_name(machine_project, machine_topic),
          'CONNECT',
          {
              'swarming_server': lease_json['on_lease']['swarming_server'],
          },
      )


class MachineReclaimer(webapp2.RequestHandler):
  """Worker for reclaiming machines."""

  @decorators.require_taskqueue('reclaim-machine')
  def post(self):
    """Reclaim a machine.

    Params:
      backend_project: If specified, project that the machine reclamation
        topic is contained in for the backend.
      backend_attributes: If specified, JSON-encoded dict of attributes to
        include in the machine reclamation message for the backend.
      backend_topic: If specified, topic that the machine reclamation should
        be published to for the backend.
      hostname: Hostname being reclaimed.
      lease_id: ID of the LeaseRequest the machine was leased for.
      lessee_project: If specified, project that the machine reclamation and
        lease expiration topic is contained in.
      lessee_topic: If specified, topic that the machine reclamation and lease
        expiration should be published to for the lessee.
      machine_id: ID of the CatalogMachineEntry being reclaimed.
    """
    backend_attributes = json.loads(self.request.get('backend_attributes', {}))
    backend_project = self.request.get('backend_project')
    backend_topic = self.request.get('backend_topic')
    hostname = self.request.get('hostname')
    lease_id = self.request.get('lease_id')
    lessee_project = self.request.get('lessee_project')
    lessee_topic = self.request.get('lessee_topic')
    machine_id = self.request.get('machine_id')

    if lessee_topic:
      pubsub.publish(
          pubsub.full_topic_name(lessee_project, lessee_topic),
          'RECLAIMED',
          {
              'machine_id': machine_id,
              'request_hash': lease_id,
          },
    )

    if backend_topic:
      attributes = backend_attributes.copy()
      attributes['hostname'] = hostname
      pubsub.publish(
          pubsub.full_topic_name(backend_project, backend_topic),
          'RECLAIMED',
          attributes,
    )


@ndb.transactional
def set_available(machine_key):
  """Sets a machine as AVAILABLE.

  Args:
    machine_key: ndb.Key for a models.CatalogMachineEntry instance.
  """
  machine = machine_key.get()
  if not machine:
    logging.error('CatalogMachineEntry does not exist: %s', machine_key)
    return

  if machine.state == models.CatalogMachineEntryStates.SUBSCRIBING:
    machine.state = models.CatalogMachineEntryStates.AVAILABLE
    machine.put()
  elif machine.state == models.CatalogMachineEntryStates.AVAILABLE:
    logging.info('CatalogMachineEntry already AVAILABLE:\n%s', machine)
  else:
    logging.error('CatalogMachineEntry in unexpected state:\n%s', machine)


class MachineSubscriber(webapp2.RequestHandler):
  """Worker for subscribing machines to a Cloud Pub/Sub topic."""

  @decorators.require_taskqueue('subscribe-machine')
  def post(self):
    """Subscribe a machine to a Cloud Pub/Sub topic.

    Params:
      backend_project: If specified, project that the machine subscription
        topic is contained in for the backend.
      backend_attributes: If specified, JSON-encoded dict of attributes to
        include in the machine subscription message for the backend.
      backend_topic: If specified, topic that the machine subscription should
        be published to for the backend.
      hostname: Hostname being reclaimed.
      machine_id: ID of the CatalogMachineEntry being reclaimed.
      machine_service_account: Service account to authorize to consume the
        subscription.
      machine_subscription: Cloud Pub/Sub subscription to create for the
        machine.
      machine_subscription_project: Project the Cloud Pub/Sub subscription
        should live in.
      machine_topic: Cloud Pub/Sub topic to create for the machine.
      machine_topic_project: Project the Cloud Pub/Sub topic should live in.
    """
    backend_attributes = json.loads(self.request.get('backend_attributes', {}))
    backend_project = self.request.get('backend_project')
    backend_topic = self.request.get('backend_topic')
    hostname = self.request.get('hostname')
    machine_id = self.request.get('machine_id')
    machine_service_account = self.request.get('machine_service_account')
    machine_subscription = self.request.get('machine_subscription')
    machine_subscription_project = self.request.get(
        'machine_subscription_project')
    machine_topic = self.request.get('machine_topic')
    machine_topic_project = self.request.get('machine_topic_project')

    topic = pubsub.full_topic_name(machine_topic_project, machine_topic)
    subscription = pubsub.full_subscription_name(
        machine_subscription_project, machine_subscription)
    pubsub.ensure_subscription_deleted(subscription)
    pubsub.ensure_subscription_exists(subscription, topic)

    with pubsub.iam_policy(subscription) as policy:
      policy.add_member(
          'roles/pubsub.subscriber',
          'serviceAccount:%s' % machine_service_account,
      )

    if backend_topic:
      attributes = backend_attributes.copy()
      attributes['hostname'] = hostname
      attributes['subscription'] = machine_subscription
      attributes['subscription_project'] = machine_subscription_project
      attributes['topic'] = machine_topic
      attributes['topic_project'] = machine_topic_project
      pubsub.publish(
          pubsub.full_topic_name(backend_project, backend_topic),
          'SUBSCRIBED',
          attributes,
    )

    set_available(ndb.Key(models.CatalogMachineEntry, machine_id))


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/fulfill-lease-request', LeaseRequestFulfiller),
      ('/internal/queues/reclaim-machine', MachineReclaimer),
      ('/internal/queues/subscribe-machine', MachineSubscriber),
  ])
