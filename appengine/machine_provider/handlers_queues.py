# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for fulfilling lease requests."""

import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import net
from components.machine_provider import rpc_messages

import models
import pubsub


def publish(topic, message, lease_id, machine_id):
  """Publish a message about a lease and machine.

  Args:
    topic: Topic that the message should be published to.
    message: Content of the message to publish.
    lease_id: ID of the LeaseRequest associated with this message.
    machine_id: ID of the CatalogMachineEntry associated with this message.
  """
  pubsub.publish(
      topic,
      message,
      request_hash=lease_id,
      machine_id=machine_id,
  )


class LeaseRequestFulfiller(webapp2.RequestHandler):
  """Worker for fulfilling lease requests."""

  @decorators.require_taskqueue('fulfill-lease-request')
  def post(self):
    """Fulfill a lease request.

    Params:
      lease_id: ID of the LeaseRequest being fulfilled.
      machine_id: ID of the CatalogMachineEntry fulfilling the LeaseRequest.
      topic: If specified, topic that the lease fulfillment should be published
        to.
    """
    lease_id = self.request.get('lease_id')
    machine_id = self.request.get('machine_id')
    topic = self.request.get('topic')

    if topic:
      publish(topic, 'FULFILLED', lease_id, machine_id)


class MachineReclaimer(webapp2.RequestHandler):
  """Worker for reclaiming machines."""

  @decorators.require_taskqueue('reclaim-machine')
  def post(self):
    """Reclaim a machine.

    Params:
      backend_topic: If specified, topic that the machine reclamation should
        be published to for the backend.
      backend_project: If specified, project that the machine reclamation
        topic is contained in.
      lease_id: ID of the LeaseRequest the machine was leased for.
      machine_id: ID of the CatalogMachineEntry being reclaimed.
      topic: If specified, topic that the machine reclamation and lease
        expiration should be published to for the lessee.
    """
    lease_id = self.request.get('lease_id')
    machine_id = self.request.get('machine_id')
    topic = self.request.get('topic')

    if topic:
      publish(topic, 'RECLAIMED', lease_id, machine_id)


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/fulfill-lease-request', LeaseRequestFulfiller),
      ('/internal/queues/reclaim-machine', MachineReclaimer),
  ])
