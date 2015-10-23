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
      machine_id: ID of the CatalogMachineEntry fulfilling the LeaseRequest.
      pubsub_topic: If specified, topic that the lease fulfillment should be
        published to.
      pubsub_project: If specified, project that the lease fulfillment topic is
        contained in.
    """
    lease_id = self.request.get('lease_id')
    machine_id = self.request.get('machine_id')
    pubsub_project = self.request.get('pubsub_project')
    pubsub_topic = self.request.get('pubsub_topic')

    if pubsub_topic:
      pubsub.publish(
          pubsub_topic,
          pubsub_project,
          'FULFILLED',
          machine_id=machine_id,
          request_hash=lease_id,
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
          lessee_topic,
          lessee_project,
          'RECLAIMED',
          machine_id=machine_id,
          request_hash=lease_id,
    )

    if backend_topic:
      attributes = backend_attributes.copy()
      attributes['hostname'] = hostname
      pubsub.publish(
          backend_topic,
          backend_project,
          'RECLAIMED',
          **attributes
    )


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/fulfill-lease-request', LeaseRequestFulfiller),
      ('/internal/queues/reclaim-machine', MachineReclaimer),
  ])
