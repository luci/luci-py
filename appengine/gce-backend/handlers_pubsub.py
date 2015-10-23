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
def schedule_deletion(instance_group_key, instance_name):
  """Marks a cataloged instance for deletion.

  Args:
    instance_group_key: ndb.Key for the instance group that owns the instance.
    instance_name: Name of the instance to schedule for deletion.
  """
  instance_group = instance_group_key.get()
  if not instance_group:
    logging.error('Instance group does not exist: %s', instance_group_key)
    return

  found = False
  for instance in instance_group.members:
    if instance.name == instance_name:
      if instance.state == models.InstanceStates.CATALOGED:
        logging.info('Scheduling instance for deletion: %s', instance_name)
        instance.state = models.InstanceStates.PENDING_DELETION
        found = True
      elif instance.state == models.InstanceStates.PENDING_DELETION:
        logging.info('Instance already pending deletion: %s', instance_name)
      else:
        logging.error('Instance in unexpected state:\n%s', instance)
      break

  if found:
    instance_group.put()
  else:
    # We may have deleted it already.
    logging.warning(
        'Instance %s not found in instance group %s',
        instance_name,
        instance_group.name,
    )


class MachineProviderSubscriptionHandler(pubsub.SubscriptionHandler):
  """Worker for receiving Pub/Sub push messages from the Machine Provider."""
  # How the Machine Provider should talk to the GCE Backend.
  ENDPOINT = '%s/%s' % (APP_BASE_URL, MACHINE_PROVIDER_ENDPOINT)
  MAX_MESSAGES = 100
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
    group = attributes.get('group')
    hostname = attributes.get('hostname')
    message = base64.b64decode(message)

    if group and hostname and message == 'RECLAIMED':
      # Per the policies we set on the instance when adding it to the Machine
      # Provider, a reclaimed machine is deleted from the Catalog. Therefore
      # we are safe to manipulate it. Here we schedule it for deletion.
      schedule_deletion(models.InstanceGroup.generate_key(group), hostname)


def create_pubsub_app():
  return webapp2.WSGIApplication([
      (MACHINE_PROVIDER_ENDPOINT, MachineProviderSubscriptionHandler),
  ])
