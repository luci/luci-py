# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utilities for interacting with Pub/Sub."""

import base64
import collections
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
def _process_message(key, data, attributes):
  """Processes a single Pub/Sub message.

  Args:
    key: ndb.Key for the models.Instance this message refers to.
    data: Text of the message.
    attributes: Any attributes associated with the message.
  """
  assert ndb.in_transaction()

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


@ndb.transactional_tasklet
def _process(messages):
  """Processes the given Pub/Sub messages.

  Args:
    messages: A list of message dicts where each message refers to an Instance
      with the same parent InstanceGroupManager.
  """
  for message in messages:
    attributes = message.get('message', {}).get('attributes', {})
    data = base64.b64decode(message.get('message', {}).get('data', ''))
    logging.info(
        'Received Pub/Sub message: %s\nAt: %s\nMessage: %s\nAttributes: %s\n',
        message['ackId'],
        message.get('message', {}).get('publishTime'),
        data,
        json.dumps(attributes, indent=2),
    )
    key = ndb.Key(urlsafe=attributes['key'])
    yield _process_message(key, data, attributes)


@ndb.tasklet
def process(messages):
  """Processes the given Pub/Sub messages.

  Args:
    messages: A list of message dicts where each message refers to an Instance
      with the same parent InstanceGroupManager.
  """
  logging.info('Processing messages: %s', len(messages))
  ack_ids = [message['ackId'] for message in messages]
  # Since it can take some time to handle the Pub/Sub messages and commit the
  # large transaction, extend the ack deadline to match the cron interval with
  # which we pull Pub/Sub messages. We should not extend the deadline longer
  # than that, otherwise if the transaction fails and the messages are not
  # acknowledged then we won't receive those messages again during the next
  # run of the cron job.
  yield pubsub.modify_ack_deadline_async(
      pubsub.full_subscription_name(
          get_machine_provider_topic_project(),
          get_machine_provider_subscription(),
      ),
      60,
      *ack_ids
  )
  yield _process(messages)
  yield pubsub.ack_async(
      pubsub.full_subscription_name(
          get_machine_provider_topic_project(),
          get_machine_provider_subscription(),
      ),
      *ack_ids
  )


def split_by_entity_group(messages):
  """Returns a list of lists of Pub/Sub messages in the same entity group.

  Each list contains Pub/Sub messages that refer to Instance entities with
  a common parent InstanceGroupManager.

  Args:
    messages: A list of message dicts.
  """
  filtered = collections.defaultdict(list)

  for message in messages:
    attributes = message.get('message', {}).get('attributes', {})
    key = ndb.Key(urlsafe=attributes['key'])
    if key.kind() != 'Instance':
      filtered['erroneous'].append(message)
    else:
      filtered[key.parent()].append(message)

  return filtered.values()


def poll():
  """Polls and processes Pub/Sub messages."""
  response = pubsub.pull(
      pubsub.full_subscription_name(
          get_machine_provider_topic_project(),
          get_machine_provider_subscription(),
      ),
      max_messages=400,
  )

  if response.get('receivedMessages', []):
    logging.info('Messages received: %s', len(response['receivedMessages']))
    messages = split_by_entity_group(response['receivedMessages'])
    utilities.batch_process_async(messages, process)


def schedule_poll():
  """Enqueues tasks to poll for Pub/Sub messages."""
  if not utils.enqueue_task(
      '/internal/queues/process-pubsub-messages',
      'process-pubsub-messages',
  ):
    logging.warning('Failed to enqueue task for Pub/Sub')
