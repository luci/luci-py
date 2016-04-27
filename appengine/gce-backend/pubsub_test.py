#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for pubsub.py."""

import base64
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import datastore_utils
from test_support import test_case

import instances
import models
import pubsub


class ProcessTest(test_case.TestCase):
  """Tests for pubsub.process."""

  def setUp(self):
    super(ProcessTest, self).setUp()

    def ack_async(*args, **kwargs):
      return ndb.Future()
    self.mock(pubsub.pubsub, 'ack_async', ack_async)

  def test_leased(self):
    """Ensures nothing happens when an instance is leased."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()
    message = {
        'ackId': 'id',
        'message': {
            'attributes': {
                'key': key.urlsafe(),
            },
            'data': base64.b64encode('LEASED'),
        },
    }

    pubsub.process(message).wait()
    self.failUnless(key.get())

  def test_reclaimed(self):
    """Ensures reclaimed instance is marked for deletion."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()
    message = {
        'ackId': 'id',
        'message': {
            'attributes': {
                'key': key.urlsafe(),
            },
            'data': base64.b64encode('RECLAIMED'),
        },
    }

    pubsub.process(message).wait()
    self.failUnless(key.get().pending_deletion)

  def test_subscribed(self):
    """Ensures subscribed instance has pending metadata update."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()
    models.InstanceGroupManager(
        key=key.parent(),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent().parent(),
        service_accounts=[
            models.ServiceAccount(
                name='name',
                scopes=[
                    'scope',
                ],
            ),
        ],
    ).put()
    message = {
        'ackId': 'id',
        'message': {
            'attributes': {
                'key': key.urlsafe(),
                'subscription': 'subscription',
                'subscription_project': 'subscription-project',
            },
            'data': base64.b64encode('SUBSCRIBED'),
        },
    }
    expected_pending_metadata_updates = [
        models.MetadataUpdate(
            metadata={
                'pubsub_service_account': 'name',
                'pubsub_subscription': 'subscription',
                'pubsub_subscription_project': 'subscription-project',
            },
        )
    ]

    pubsub.process(message).wait()
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_unexpected_key(self):
    """Ensures nothing happens when key has unexpected kind."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()
    models.InstanceGroupManager(
        key=key.parent(),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent().parent(),
        service_accounts=[
            models.ServiceAccount(
                name='name',
                scopes=[
                    'scope',
                ],
            ),
        ],
    ).put()
    message = {
        'ackId': 'id',
        'message': {
            'attributes': {
                'key': key.parent().urlsafe(),
                'subscription': 'subscription',
                'subscription_project': 'subscription-project',
            },
            'data': base64.b64encode('SUBSCRIBED'),
        },
    }

    pubsub.process(message).wait()
    self.failIf(key.get().pending_metadata_updates)


if __name__ == '__main__':
  unittest.main()
