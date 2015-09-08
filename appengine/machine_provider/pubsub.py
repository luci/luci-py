# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with Cloud Pub/Sub."""

import base64
import logging
import re

from google.appengine.api import app_identity

from components import net


PUBSUB_BASE_URL = 'https://pubsub.googleapis.com/v1/projects/%s' % (
    app_identity.get_application_id(),
)
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


def _publish(topic, message, **attributes):
  """Publish messages to Cloud Pub/Sub.

  Args:
    topic: Name of the topic to publish to.
    message: Content of the message to publish.
    **attributes: Any attributes to send with the message.
  """
  net.json_request(
      '%s/topics/%s:publish' % (PUBSUB_BASE_URL, topic),
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


def publish(topic, message, **attributes):
  """Publish messages to Cloud Pub/Sub. Creates the topic if it doesn't exist.

  Args:
    topic: Name of the topic to publish to.
    message: Content of the message to publish.
    **attributes: Any attributes to send with the message.
  """
  try:
    _publish(topic, message, **attributes)
  except net.Error as e:
    if e.status_code == 404:
      # Topic does not exist. Try to create it.
      try:
        net.json_request(
            '%s/topics/%s' % (PUBSUB_BASE_URL, topic),
            method='PUT',
            scopes=PUBSUB_SCOPES,
        )
      except net.Error as e:
        if e.status_code != 409:
          # 409 is the status code when the topic already exists (maybe someone
          # else created it just now). Ignore 409, but raise any other error.
          raise
      # Retransmit now that the topic is created.
      _publish(topic, message, **attributes)
    else:
      # Unknown error.
      raise
