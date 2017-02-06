#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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

    metadata.associate_metadata_operation(key, checksum, 'url')
    self.assertEqual(key.get().active_metadata_update.url, expected_url)

  def test_already_has_mismatched_url(self):
    """Ensures nothing happens when an unexpected URL is already associated."""
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


class ClearActiveMetadataUpdateTest(test_case.TestCase):
  """Tests for metadata.clear_active_metadata_update."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.clear_active_metadata_update(key, 'url')
    self.failIf(key.get())

  def test_active_metadata_update_unspecified(self):
    """Ensures nothing happens when an active metadata update is unspecified."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.clear_active_metadata_update(key, 'url')
    self.failIf(key.get().active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_url_mismatch(self):
    """Ensures nothing happens when the URL is unexpected."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='expected-url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key1': 'value1'},
        url='expected-url',
    )
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.clear_active_metadata_update(key, 'url')
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.assertEqual(
        key.get().active_metadata_update.url,
        expected_active_metadata_update.url,
    )
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_cleared(self):
    """Ensures an active metadata update is cleared."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.clear_active_metadata_update(key, 'url')
    self.failIf(key.get().active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)


class CheckTest(test_case.TestCase):
  """Tests for metadata.check."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    def json_request(*args, **kwargs):
      self.fail('json_request called')
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = ndb.Key(models.Instance, 'fake-key')
    metadata.check(key)
    self.failIf(key.get())

  def test_active_metadata_update_unspecified(self):
    """Ensures nothing happens when a metadata update doesn't exist."""
    def json_request(*args, **kwargs):
      self.fail('json_request called')
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()

    metadata.check(key)
    self.failIf(key.get().active_metadata_update)

  def test_url_unspecified(self):
    """Ensures nothing happens when the active metadata update has no URL."""
    def json_request(*args, **kwargs):
      self.fail('json_request called')
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key': 'value'},
        ),
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key': 'value'},
    )

    metadata.check(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)

  def test_not_done(self):
    """Ensures nothing happens when the active metadata operation isn't done."""
    def json_request(*args, **kwargs):
      return {'status': 'not done'}
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key': 'value'},
            url='url',
        ),
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key': 'value'},
        url='url',
    )

    metadata.check(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)

  def test_done(self):
    """Ensures the active metadata operation is cleared when done."""
    def json_request(*args, **kwargs):
      return {'status': 'DONE'}
    def send_machine_event(*args, **kwargs):
      pass
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.check(key)
    self.failIf(key.get().active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_error(self):
    """Ensures the active metadata operation is rescheduled on error."""
    def json_request(*args, **kwargs):
      return {'error': 'error', 'status': 'DONE'}
    def send_machine_event(*args, **kwargs):
      pass
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key1': 'value1', 'key2': 'value2'},
    )

    metadata.check(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)


class CompressTest(test_case.TestCase):
  """Tests for metadata.compress."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = ndb.Key(models.Instance, 'fake-key')
    metadata.compress(key)
    self.failIf(key.get())

  def test_active_metadata_update(self):
    """Ensures nothing happens when a metadata update is already active."""
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

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

    metadata.compress(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_no_pending_metadata_updates(self):
    """Ensures nothing happens when there are no pending metadata updates."""
    def send_machine_event(*args, **kwargs):
      self.fail('send_machine_event called')
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    ).put()

    metadata.compress(key)
    self.failIf(key.get().active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)

  def test_pending_metadata_update(self):
    """Ensures a pending metadata update is activated."""
    def send_machine_event(*args, **kwargs):
      pass
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

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

    metadata.compress(key)
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.failIf(key.get().pending_metadata_updates)


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


class RescheduleActiveMetadataUpdateTest(test_case.TestCase):
  """Tests for metadata.reschedule_active_metadata_update."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.reschedule_active_metadata_update(key, 'url')
    self.failIf(key.get())

  def test_active_metadata_update_unspecified(self):
    """Ensures nothing happens when an active metadata update is unspecified."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.reschedule_active_metadata_update(key, 'url')
    self.failIf(key.get().active_metadata_update)
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_url_mismatch(self):
    """Ensures nothing happens when the URL is unexpected."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='expected-url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key1': 'value1'},
        url='expected-url',
    )
    expected_pending_metadata_updates = [
        models.MetadataUpdate(metadata={'key2': 'value2'}),
    ]

    metadata.reschedule_active_metadata_update(key, 'url')
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.assertEqual(
        key.get().active_metadata_update.url,
        expected_active_metadata_update.url,
    )
    self.assertEqual(
        key.get().pending_metadata_updates, expected_pending_metadata_updates)

  def test_rescheduled(self):
    """Ensures an active metadata update is rescheduled."""
    key = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
        active_metadata_update=models.MetadataUpdate(
            metadata={'key1': 'value1'},
            url='url',
        ),
        pending_metadata_updates=[
            models.MetadataUpdate(metadata={'key2': 'value2'}),
        ],
    ).put()
    expected_active_metadata_update = models.MetadataUpdate(
        metadata={'key1': 'value1', 'key2': 'value2'},
    )

    metadata.reschedule_active_metadata_update(key, 'url')
    self.assertEqual(
        key.get().active_metadata_update, expected_active_metadata_update)
    self.failIf(key.get().active_metadata_update.url)
    self.failIf(key.get().pending_metadata_updates)


class UpdateTest(test_case.TestCase):
  """Tests for metadata.update."""

  def test_not_found(self):
    """Ensures nothing happens when the entity doesn't exist."""
    key = ndb.Key(models.Instance, 'fake-key')
    metadata.update(key)
    self.failIf(key.get())

  def test_no_active_metadata_update(self):
    """Ensures nothing happens when there is no active metadata update."""
    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceGroupManager(
        key=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceTemplateRevision(
        key=instances.get_instance_group_manager_key(key).parent(),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update)

  def test_has_url(self):
    """Ensures nothing happens when the active metadata update has a URL."""
    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
            url='url',
        ),
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceGroupManager(
        key=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceTemplateRevision(
        key=instances.get_instance_group_manager_key(key).parent(),
    ).put()

    metadata.update(key)
    self.assertEqual(key.get().active_metadata_update.url, 'url')

  def test_parent_not_found(self):
    """Ensures nothing happens when the parent doesn't exist."""
    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_grandparent_not_found(self):
    """Ensures nothing happens when the grandparent doesn't exist."""
    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceGroupManager(
        key=instances.get_instance_group_manager_key(key),
        instances=[
            key,
        ],
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_project_unspecified(self):
    """Ensures nothing happens when project is unspecified."""
    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceGroupManager(
        key=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceTemplateRevision(
        key=instances.get_instance_group_manager_key(key).parent(),
    ).put()

    metadata.update(key)
    self.failIf(key.get().active_metadata_update.url)

  def test_metadata_updated(self):
    """Ensures metadata is updated."""
    def json_request(*args, **kwargs):
      return {'metadata': {'fingerprint': 'fingerprint', 'items': []}}
    def set_metadata(*args, **kwargs):
      return collections.namedtuple('operation', ['url'])(url='url')
    def send_machine_event(*args, **kwargs):
      pass
    self.mock(metadata.net, 'json_request', json_request)
    self.mock(metadata.gce.Project, 'set_metadata', set_metadata)
    self.mock(metadata.metrics, 'send_machine_event', send_machine_event)

    key = instances.get_instance_key(
        'base-name',
        'revision',
        'zone',
        'instance-name',
    )
    models.Instance(
        key=key,
        active_metadata_update=models.MetadataUpdate(
            metadata={
                'key': 'value',
            },
        ),
        instance_group_manager=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceGroupManager(
        key=instances.get_instance_group_manager_key(key),
    ).put()
    models.InstanceTemplateRevision(
        key=instances.get_instance_group_manager_key(key).parent(),
        project='project',
    ).put()
    expected_url = 'url'

    metadata.update(key)
    self.assertEqual(key.get().active_metadata_update.url, expected_url)


if __name__ == '__main__':
  unittest.main()
