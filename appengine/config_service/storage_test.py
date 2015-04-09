#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import test_env
test_env.setup_test_env()

from test_support import test_case
import mock

from proto import service_config_pb2
import storage


class StorageTestCase(test_case.TestCase):
  def put_file(self, config_set, revision, path, content):
    confg_set_key = storage.ConfigSet(
        id=config_set, latest_revision=revision).put()
    rev_key = storage.Revision(id=revision, parent=confg_set_key).put()

    content_hash = storage.compute_hash(content)
    storage.File(id=path, parent=rev_key, content_hash=content_hash).put()
    storage.Blob(id=content_hash, content=content).put()

  def test_get_config(self):
    self.put_file('foo', 'deadbeef', 'config.cfg', 'content')
    revision, content_hash = storage.get_config_hash(
        'foo', 'config.cfg', revision='deadbeef')
    self.assertEqual(revision, 'deadbeef')
    self.assertEqual(
        content_hash, 'v1:6b584e8ece562ebffc15d38808cd6b98fc3d97ea')

  def test_get_non_existing_config(self):
    revision, content_hash = storage.get_config_hash(
        'foo', 'config.cfg', revision='deadbeef')
    self.assertEqual(revision, None)
    self.assertEqual(content_hash, None)

  def test_get_latest_config(self):
    self.put_file('foo', 'deadbeef', 'config.cfg', 'content')
    revision, content_hash = storage.get_config_hash('foo', 'config.cfg')
    self.assertEqual(revision, 'deadbeef')
    self.assertEqual(
        content_hash, 'v1:6b584e8ece562ebffc15d38808cd6b98fc3d97ea')

  def test_get_latest_non_existing_config_set(self):
    revision, content_hash = storage.get_config_hash('foo', 'config.yaml')
    self.assertEqual(revision, None)
    self.assertEqual(content_hash, None)

  def test_get_config_by_hash(self):
    self.assertIsNone(storage.get_config_by_hash('deadbeef'))
    storage.Blob(id='deadbeef', content='content').put()
    self.assertEqual(storage.get_config_by_hash('deadbeef'), 'content')

  def test_compute_hash(self):
    content = 'some content\n'
    # echo some content | git hash-object --stdin
    expected = 'v1:2ef267e25bd6c6a300bb473e604b092b6a48523b'
    self.assertEqual(expected, storage.compute_hash(content))

  def test_import_blob(self):
    content = 'some content'
    storage.import_blob(content)
    storage.import_blob(content)  # Coverage.
    blob = storage.Blob.get_by_id(storage.compute_hash(content))
    self.assertIsNotNone(blob)
    self.assertEqual(blob.content, content)

  def test_message_field_merge(self):
    default_msg = service_config_pb2.AclCfg(service_access_group='def-group')
    self.mock(storage, 'get_latest', mock.Mock())
    storage.get_latest.return_value = ''

    msg = storage.get_self_config('acl.cfg', lambda: default_msg)
    self.assertEqual(msg.service_access_group, 'def-group')

  def test_get_self_config(self):
    expected = service_config_pb2.AclCfg(service_access_group='group')

    self.mock(storage, 'get_config_hash', mock.Mock())
    self.mock(storage, 'get_config_by_hash', mock.Mock())

    storage.get_config_hash.return_value = 'deadbeef', 'beefdead'
    storage.get_config_by_hash.return_value = 'service_access_group: "group"'

    actual = storage.get_self_config('acl.cfg', service_config_pb2.AclCfg)
    self.assertEqual(expected, actual)

    storage.get_config_hash.assert_called_once_with(
        'services/sample-app', 'acl.cfg')
    storage.get_config_by_hash.assert_called_once_with('beefdead')

    # memcached:
    storage.get_config_hash.reset_mock()
    storage.get_config_by_hash.reset_mock()
    actual = storage.get_latest_as_message(
        'services/sample-app', 'acl.cfg',
        service_config_pb2.AclCfg)
    self.assertEqual(expected, actual)
    self.assertFalse(storage.get_config_hash.called)
    self.assertFalse(storage.get_config_by_hash.called)


if __name__ == '__main__':
  test_env.main()
