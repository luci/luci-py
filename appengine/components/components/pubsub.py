# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with Cloud Pub/Sub."""

import base64
import logging
import re

from google.appengine.ext import ndb
import webapp2

from components import net


PUBSUB_BASE_URL = 'https://pubsub.googleapis.com/v1/projects'
PUBSUB_SCOPES = (
    'https://www.googleapis.com/auth/pubsub',
)


def validate_topic(topic):
  """Ensures the given topic is valid for Cloud Pub/Sub."""
  # Technically, there are more restrictions for topic names than we check here,
  # but the API will reject anything that doesn't match. We only check / in case
  # the user is trying to manipulate the topic into posting somewhere else (e.g.
  # by setting the topic as ../../<some other project>/topics/<topic>.
  return '/' not in topic


def validate_project(project):
  """Ensures the given project is valid for Cloud Pub/Sub."""
  return validate_topic(project)


def ensure_topic_exists(topic, project):
  """Ensures the given Cloud Pub/Sub topic exists in the given project.

  Args:
    topic: Name of the topic which should exist.
    project: Name of the project the topic should exist in.
  """
  try:
    net.json_request(
        '%s/%s/topics/%s' % (PUBSUB_BASE_URL, project, topic),
        method='PUT',
        scopes=PUBSUB_SCOPES,
    )
  except net.Error as e:
    if e.status_code != 409:
      # 409 is the status code when the topic already exists.
      # Ignore 409, but raise any other error.
      raise


def _publish(topic, project, message, **attributes):
  """Publish messages to Cloud Pub/Sub.

  Args:
    topic: Name of the topic to publish to.
    project: Name of the project the topic exists in.
    message: Content of the message to publish.
    **attributes: Any attributes to send with the message.
  """
  net.json_request(
      '%s/%s/topics/%s:publish' % (PUBSUB_BASE_URL, project, topic),
      method='POST',
      payload={
          'messages': [
              {
                  'attributes': attributes,
                  'data': base64.b64encode(message),
              },
          ],
      },
      scopes=PUBSUB_SCOPES,
  )


def publish(topic, project, message, **attributes):
  """Publish messages to Cloud Pub/Sub. Creates the topic if it doesn't exist.

  Args:
    topic: Name of the topic to publish to.
    project: Name of the project the topic should exist in.
    message: Content of the message to publish.
    **attributes: Any attributes to send with the message.
  """
  try:
    _publish(topic, project, message, **attributes)
  except net.Error as e:
    if e.status_code == 404:
      # Topic does not exist. Try to create it.
      ensure_topic_exists(topic, project)
      try:
        net.json_request(
            '%s/%s/topics/%s' % (PUBSUB_BASE_URL, project, topic),
            method='PUT',
            scopes=PUBSUB_SCOPES,
        )
      except net.Error as e:
        if e.status_code != 409:
          # 409 is the status code when the topic already exists (maybe someone
          # else created it just now). Ignore 409, but raise any other error.
          raise
      # Retransmit now that the topic is created.
      _publish(topic, project, message, **attributes)
    else:
      # Unknown error.
      raise


class SubscriptionHandler(webapp2.RequestHandler):
  """Base class for defining Pub/Sub subscription handlers."""
  # TODO(smut): Keep in datastore. See components/datastore_utils.
  ENDPOINT = None
  SUBSCRIPTION = None
  SUBSCRIPTION_PROJECT = None
  TOPIC = None
  TOPIC_PROJECT = None

  @classmethod
  def get_subscription_url(cls):
    return '%s/%s/subscriptions/%s' % (
        PUBSUB_BASE_URL,
        cls.SUBSCRIPTION_PROJECT,
        cls.SUBSCRIPTION,
    )

  @classmethod
  def unsubscribe(cls):
    """Unsubscribes from a Cloud Pub/Sub project."""
    net.json_request(
        cls.get_subscription_url(),
        method='DELETE',
        scopes=PUBSUB_SCOPES,
    )

  @classmethod
  def _subscribe(cls, push=False):
    """Subscribes to a Cloud Pub/Sub project."""
    payload = {
        'topic': 'projects/%s/topics/%s' % (cls.TOPIC_PROJECT, cls.TOPIC),
    }
    if push:
      payload['pushConfig'] = {'pushEndpoint': cls.ENDPOINT}
    net.json_request(
        cls.get_subscription_url(),
        method='PUT',
        payload=payload,
        scopes=PUBSUB_SCOPES,
    )

  @classmethod
  def ensure_subscribed(cls, push=False):
    """Ensures a Cloud Pub/Sub subscription exists.

    Can also be used to change subscription type between push and pull.

    Args:
      push: Whether or not to create a push subscription. Defaults to pull.
    """
    try:
      cls._subscribe(push=push)
    except net.NotFoundError:
      # Topic does not exist. Try to create it.
      ensure_topic_exists(cls.TOPIC, cls.TOPIC_PROJECT)
      # Retransmit now that the topic is created.
      cls._subscribe(push=push)
    except net.Error as e:
      if e.status_code == 409:
        # Subscription already exists. When the subscription already exists,
        # it won't change between pull and push if the create request was for
        # a different type of subscription than what currently existed. Since
        # we get no information about whether the existing subscription is a
        # push or pull subscription, just blindly send a request to change the
        # subscription type.
        payload = None
        if push:
          payload = {'pushConfig': {'pushEndpoint': cls.ENDPOINT}}
        net.json_request(
          cls.get_subscription_url(),
          method='POST',
          payload=payload,
          scopes=PUBSUB_SCOPES,
        )
      else:
        raise

  @classmethod
  def is_subscribed(cls):
    """Returns whether or not a Cloud Pub/Sub subscription exists.

    Returns:
      True if the subscription exists, False otherwise.
    """
    try:
      net.json_request(
          cls.get_subscription_url(),
          method='GET',
          scopes=PUBSUB_SCOPES,
      )
      return True
    except net.NotFoundError:
      return False

  def get(self):
    """Queries for Pub/Sub messages."""
    response = net.json_request(
        '%s:pull' % self.get_subscription_url(),
        method='POST',
        payload={
            'maxMessages': 1,
            'returnImmediately': True,
        },
        scopes=PUBSUB_SCOPES,
    )
    message_ids = []
    for received_message in response.get('receivedMessages', []):
      attributes = received_message.get('message', {}).get('attributes', {})
      message = received_message.get('message', {}).get('data', '')
      logging.info(
          'Received Pub/Sub message:\n%s\nAttributes:\n%s', message, attributes)
      # TODO(smut): Process messages in parallel.
      self.process_message(message, attributes)
      message_ids.append(received_message['ackId'])
    if message_ids:
      net.json_request(
          '%s:acknowledge' % self.get_subscription_url(),
          method='POST',
          payload={'ackIds': message_ids},
          scopes=PUBSUB_SCOPES,
      )

  def post(self):
    """Handles a Pub/Sub push message."""
    # TODO(smut): Ensure message came from Cloud Pub/Sub.
    # Since anyone can post to this endpoint, we need to ensure the message
    # actually came from Cloud Pub/Sub. Unfortunately, there aren't any
    # useful headers set that can guarantee this.
    attributes = self.request.json.get('message', {}).get('attributes', {})
    message = self.request.json.get('message', {}).get('data', '')
    subscription = self.request.json.get('subscription')

    if subscription != 'projects/%s/subscriptions/%s' % (
        self.SUBSCRIPTION_PROJECT, self.SUBSCRIPTION):
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      logging.error('Ignoring unexpected subscription: %s', subscription)
      self.abort(403, 'Unexpected subscription: %s' % subscription)
      return

    logging.info(
        'Received Pub/Sub message:\n%s\nAttributes:\n%s', message, attributes)
    return self.process_message(message, attributes)

  def process_message(self, message, attributes):
    """Process a Pub/Sub message.

    Args:
      message: The message string.
      attributes: A dict of key/value pairs representing attributes associated
        with this message.

    Returns:
      A webapp2.Response instance, or None.
    """
    raise NotImplementedError()
