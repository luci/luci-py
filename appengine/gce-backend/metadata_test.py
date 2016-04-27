#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for metadata.py."""

import collections
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import datastore_utils
from test_support import test_case

import instances
import metadata
import models


class ApplyMetadataUpdateTest(test_case.TestCase):
  """Tests for metadata.apply_metadata_update."""

  def test_empty(self):
    """Ensures nothing happens when empty values are passed."""
    items = [
    ]
    updates = {
    }
    expected = [
    ]

    self.assertItemsEqual(
        metadata.apply_metadata_update(items, updates), expected)

  def test_no_metadata_to_apply(self):
    """Ensures nothing happens when there is no metadata to apply."""
    items = [
        {'key': 'key', 'value': 'value'},
    ]
    updates = {
    }
    expected = [
        {'key': 'key', 'value': 'value'},
    ]

    self.assertItemsEqual(
        metadata.apply_metadata_update(items, updates), expected)

  def test_new_key(self):
    """Ensures new metadata is added."""
    items = [
        {'key': 'key1', 'value': 'value1'},
    ]
    updates = {
        'key2': 'value2',
    }
    expected = [
        {'key': 'key1', 'value': 'value1'},
        {'key': 'key2', 'value': 'value2'},
    ]

    self.assertItemsEqual(
        metadata.apply_metadata_update(items, updates), expected)

  def test_new_value(self):
    """Ensures existing metadata value is updated."""
    items = [
        {'key': 'key', 'value': 'value1'},
    ]
    updates = {
        'key': 'value2',
    }
    expected = [
        {'key': 'key', 'value': 'value2'},
    ]

    self.assertItemsEqual(
        metadata.apply_metadata_update(items, updates), expected)

  def test_delete(self):
    """Ensures existing metadata key is deleted."""
    items = [
        {'key': 'key', 'value': 'value'},
    ]
    updates = {
        'key': None,
    }
    expected = [
    ]

    self.assertItemsEqual(
        metadata.apply_metadata_update(items, updates), expected)


class AssociateMetadataOperationTest(test_case.TestCase):
  """Tests for metadata.associate_metadata_operation."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.associate_metadata_operation(key, 'checksum', 'url')
    self.failIf(key.get())

  def test_no_active_metadata_update(self):
    """Ensures nothing happens when active metadata update is unspecified."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()

    metadata.associate_metadata_operation(key, 'checksum', 'url')
    self.failIf(key.get().active_metadata_update)

  def test_checksum_mismatch(self):
    """Ensures nothing happens when the metadata checksum doesn't match."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()

    metadata.associate_metadata_operation(key, 'checksum', 'url')
    self.failIf(key.get().active_metadata_update.url)

  def test_already_has_url(self):
    """Ensures nothing happens when a URL is already associated."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
            url='url',
        ),
    ).put()
    checksum = key.get().active_metadata_update.checksum
    expected_url = 'url'

    metadata.associate_metadata_operation(key, checksum, 'new-url')
    self.assertEqual(key.get().active_metadata_update.url, expected_url)

  def test_associates_url(self):
    """Ensures a URL is associated."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()
    checksum = key.get().active_metadata_update.checksum
    expected_url = 'url'

    metadata.associate_metadata_operation(key, checksum, 'url')
    self.assertEqual(key.get().active_metadata_update.url, expected_url)


class CompressPendingMetadataUpdatesTest(test_case.TestCase):
  """Tests for metadata.compress_pending_metadata_updates."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.compress_pending_metadata_updates(key)
    self.failIf(key.get())

  def test_active_metadata_update(self):
    """Ensures nothing happens when a metadata update is already active."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key1': 'value1'},
    )
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.compress_pending_metadata_updates(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_no_pending_metadata_updates(self):
    """Ensures nothing happens when there are no pending metadata updates."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()

    metadata.compress_pending_metadata_updates(key)
    self.failIf(key.get().active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)

  def test_pending_metadata_update(self):
    """Ensures a pending metadata update is activated."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key': 'value'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={
            'key': 'value',
        },
    )

    metadata.compress_pending_metadata_updates(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)

  def test_pending_metadata_updates(self):
    """Ensures pending metadata updates are compressed and activated."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key1': 'value1'}),
            models.MetadataUpdate(metadata={'key2': 'value2'}),
            models.MetadataUpdate(metadata={'key3': 'value3', 'key1': 'value'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={
            'key1': 'value',
            'key2': 'value2',
            'key3': 'value3',
        }
    )

    metadata.compress_pending_metadata_updates(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)


class UpdateTest(test_case.TestCase):
  """Tests for metadata.update."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.update(key)
    self.failIf(key.get())

  def test_parent_not_found(self):
    """Ensures nothing happens when the parent doesn't exist."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_grandparent_not_found(self):
    """Ensures nothing happens when the grandparent doesn't exist."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()
    models.InstanceGroupManager(
        key=key.parent(),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_project_unspecified(self):
    """Ensures nothing happens when project is unspecified."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()
    models.InstanceGroupManager(
        key=key.parent(),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent().parent(),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_metadata_updated(self):
    """Ensures metadata is updated."""
    def json_request(*args, **kwargs):
      return {'metadata': {'fingerprint': 'fingerprint', 'items': []}}
    def set_metadata(*args, **kwargs):
      return collections.namedtuple('operation', ['url'])(url='url')
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.gce.Project, 'set_metadata', set_metadata)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
    ).put()
    models.InstanceGroupManager(
        key=key.parent(),
    ).put()
    models.InstanceTemplateRevision(
        key=key.parent().parent(),
        project='project',
    ).put()
    expected_url = 'url'

    metadata.update(key)
    self.assertEqual(key.get().active_metadata_update.url, expected_url)


if __name__ == '__main__':
  unittest.main()
