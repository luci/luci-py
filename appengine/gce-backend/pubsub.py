# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for interacting with Pub/Sub."""

import base64
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from components import gce
from components import machine_provider
from components import pubsub
from components import utils

import instances
import models
import utilities


@utils.cache
def get_machine_provider_topic_project():
  """Returns the project the Machine Provider topic is contained in."""
  return app_identity.get_application_id()


def get_machine_provider_topic():
  """Returns the name of the topic Machine Provider communication occurs on."""
  return 'machine-provider'


def get_machine_provider_subscription():
  """Returns the subscription to the Machine Provider topic."""
  return 'machine-provider'


@ndb.tasklet
def process(message):
  """Processes the given Pub/Sub message.

  Args:
    message: A message dict.
  """
  attributes = message.get('message', {}).get('attributes', {})
  data = base64.b64decode(message.get('message', {}).get('data', ''))
  logging.info(
      'Received Pub/Sub message: %s\nMessage: %s\nAttributes: %s',
      message['ackId'],
      data,
      json.dumps(attributes, indent=2),
  )

  key = ndb.Key(urlsafe=attributes['key'])

  if key.kind() == 'Instance':
    if data == 'LEASED':
      logging.info('Instance leased: %s', key)
    elif data == 'RECLAIMED':
      yield instances.mark_for_deletion(key)
    elif data == 'SUBSCRIBED':
      yield instances.add_subscription_metadata(
          key,
          attributes['subscription_project'],
          attributes['subscription'],
      )
  else:
    logging.error('Unexpected key: %s', key)

  yield pubsub.ack_async(
      pubsub.full_subscription_name(
          get_machine_provider_topic_project(), get_machine_provider_topic()),
      message['ackId'],
  )


def poll():
  """Polls and processes Pub/Sub messages."""
  response = pubsub.pull(pubsub.full_subscription_name(
      get_machine_provider_topic_project(),
      get_machine_provider_subscription(),
  ))

  utilities.batch_process_async(response.get('receivedMessages', []), process)


def schedule_poll():
  """Enqueues tasks to poll for Pub/Sub messages."""
  if not utils.enqueue_task(
      '/internal/queues/process-pubsub-messages',
      'process-pubsub-messages',
  ):
    logging.warning('Failed to enqueue task for Pub/Sub')
