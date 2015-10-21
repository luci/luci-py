# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend Pub/Sub push subscription receiver."""

import base64
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb
import webapp2

from components import pubsub
from components import utils

import models


APP_BASE_URL = 'https://%s' % app_identity.get_default_version_hostname()
APP_ID = app_identity.get_application_id()
MACHINE_PROVIDER_ENDPOINT = '/pubsub/machine-provider'


@ndb.transactional
def schedule_deletion(instance_name):
  """Marks a cataloged instance for deletion.

  Args:
    instance_name: Name of the instance to schedule for deletion.
  """
  models.InstanceDeletions.add_instance(instance_name)


class MachineProviderSubscriptionHandler(pubsub.SubscriptionHandler):
  """Worker for receiving Pub/Sub push messages from the Machine Provider."""
  # How the Machine Provider should talk to the GCE Backend.
  ENDPOINT = '%s/%s' % (APP_BASE_URL, MACHINE_PROVIDER_ENDPOINT)
  SUBSCRIPTION = 'machine-provider-subscription'
  SUBSCRIPTION_PROJECT = APP_ID
  TOPIC = 'machine-provider'
  TOPIC_PROJECT = APP_ID

  def process_message(self, message, attributes):
    """Process a Pub/Sub message.

    Args:
      message: The message string.
      attributes: A dict of key/value pairs representing attributes associated
        with this message.

    Returns:
      A webapp2.Response instance, or None.
    """
    hostname = attributes.get('hostname')
    message = base64.b64decode(message)

    if hostname and message == 'RECLAIMED':
      # Per the policies we set on the instance when adding it to the Machine
      # Provider, a reclaimed machine is deleted from the Catalog. Therefore
      # we are safe to manipulate it. Here we schedule it for deletion.
      schedule_deletion(hostname)


def create_pubsub_app():
  return webapp2.WSGIApplication([
      (MACHINE_PROVIDER_ENDPOINT, MachineProviderSubscriptionHandler),
  ])
