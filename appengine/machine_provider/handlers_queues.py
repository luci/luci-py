# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for fulfilling lease requests."""

import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators
from components import net

import models
import pubsub
import rpc_messages


def publish_fulfillment(topic, lease_id, machine_id):
  """Publish a message announcing that a lease has been fulfilled.

  Args:
    topic: Topic that the lease fulfillment should be published to.
    lease_id: ID of the LeaseRequest being uflfilled.
    machine_id: ID of the CatalogMachineEntry fulfilling the LeaseRequest.
  """
  pubsub.publish(
      topic,
      'FULFILLED',
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
      publish_fulfillment(topic, lease_id, machine_id)


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/fulfill-lease-request', LeaseRequestFulfiller),
  ])
