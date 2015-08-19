#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from test_env import future
import test_env
test_env.setup_test_env()

from test_support import test_case
import mock

from components.config.proto import service_config_pb2

import storage


class StorageTestCase(test_case.TestCase):

  def put_file(self, config_set, revision, path, content):
    confg_set_key = storage.ConfigSet(
        id=config_set, latest_revision=revision, location='http://x.com').put()
    rev_key = storage.Revision(id=revision, parent=confg_set_key).put()

    content_hash = storage.compute_hash(content)
    storage.File(id=path, parent=rev_key, content_hash=content_hash).put()
    storage.Blob(id=content_hash, content=content).put()

  def test_get_mapping(self):
    storage.ConfigSet(
        id='services/x', latest_revision='deadbeef', location='http://x').put()
    storage.ConfigSet(
        id='services/y', latest_revision='deadbeef', location='http://y').put()

    self.assertEqual(
        storage.get_mapping_async().get_result(),
        {'services/x':'http://x', 'services/y':'http://y'}
    )
    self.assertEqual(
        storage.get_mapping_async('services/x').get_result(),
        {'services/x':'http://x'}
    )
    self.assertEqual(
        storage.get_mapping_async('services/z').get_result(),
        {'services/z':None}
    )

  def test_get_config(self):
    self.put_file('foo', 'deadbeef', 'config.cfg', 'content')
    revision, content_hash = storage.get_config_hash_async(
        'foo', 'config.cfg', revision='deadbeef').get_result()
    self.assertEqual(revision, 'deadbeef')
    self.assertEqual(
        content_hash, 'v1:6b584e8ece562ebffc15d38808cd6b98fc3d97ea')

  def test_get_non_existing_config(self):
    revision, content_hash = storage.get_config_hash_async(
        'foo', 'config.cfg', revision='deadbeef').get_result()
    self.assertEqual(revision, None)
    self.assertEqual(content_hash, None)

  def test_get_latest_config(self):
    self.put_file('foo', 'deadbeef', 'config.cfg', 'content')
    revision, content_hash = storage.get_config_hash_async(
        'foo', 'config.cfg').get_result()
    self.assertEqual(revision, 'deadbeef')
    self.assertEqual(
        content_hash, 'v1:6b584e8ece562ebffc15d38808cd6b98fc3d97ea')

  def test_get_latest_multi(self):
    self.put_file('foo', 'deadbeef', 'a.cfg', 'fooo')
    self.put_file('bar', 'beefdead', 'a.cfg', 'barr')

    expected = [
      {
        'config_set': 'foo',
        'revision': 'deadbeef',
        'content_hash': 'v1:551f4d3376caed56a600e02dfaa733b68898dc2b',
        'content': 'fooo',
      },
      {
        'config_set': 'bar',
        'revision': 'beefdead',
        'content_hash': 'v1:f89094690273aed90f20da47629315b54e494eb8',
        'content': 'barr',
      },
    ]
    actual = storage.get_latest_multi_async(
        ['foo', 'bar'], 'a.cfg').get_result()
    self.assertEqual(expected, actual)

  def test_get_latest_multi_hashes_only(self):
    self.put_file('foo', 'deadbeef', 'a.cfg', 'fooo')
    self.put_file('bar', 'beefdead', 'a.cfg', 'barr')

    expected = [
      {
        'config_set': 'foo',
        'revision': 'deadbeef',
        'content_hash': 'v1:551f4d3376caed56a600e02dfaa733b68898dc2b',
        'content': None,
      },
      {
        'config_set': 'bar',
        'revision': 'beefdead',
        'content_hash': 'v1:f89094690273aed90f20da47629315b54e494eb8',
        'content': None,
      },
    ]
    actual = storage.get_latest_multi_async(
        ['foo', 'bar'], 'a.cfg', hashes_only=True).get_result()
    self.assertEqual(expected, actual)

  def test_get_latest_non_existing_config_set(self):
    revision, content_hash = storage.get_config_hash_async(
        'foo', 'config.yaml').get_result()
    self.assertEqual(revision, None)
    self.assertEqual(content_hash, None)

  def test_get_config_by_hash(self):
    self.assertIsNone(storage.get_config_by_hash_async('deadbeef').get_result())
    storage.Blob(id='deadbeef', content='content').put()
    self.assertEqual(
        storage.get_config_by_hash_async('deadbeef').get_result(), 'content')

  def test_compute_hash(self):
    content = 'some content\n'
    # echo some content | git hash-object --stdin
    expected = 'v1:2ef267e25bd6c6a300bb473e604b092b6a48523b'
    self.assertEqual(expected, storage.compute_hash(content))

  def test_import_blob(self):
    content = 'some content'
    storage.import_blob(content)
    storage.import_blob(content)  # Coverage.
    blob = storage.Blob.get_by_id_async(
        storage.compute_hash(content)).get_result()
    self.assertIsNotNone(blob)
    self.assertEqual(blob.content, content)

  def test_message_field_merge(self):
    default_msg = service_config_pb2.ImportCfg(
        gitiles=service_config_pb2.ImportCfg.Gitiles(fetch_log_deadline=42))
    self.mock(storage, 'get_latest_async', mock.Mock())
    storage.get_latest_async.return_value = future(
        'gitiles { fetch_archive_deadline: 10 }')
    msg = storage.get_self_config_async(
        'import.cfg', lambda: default_msg).get_result()
    self.assertEqual(msg.gitiles.fetch_log_deadline, 42)

  def test_get_self_config(self):
    expected = service_config_pb2.AclCfg(project_access_group='group')

    self.mock(storage, 'get_config_hash_async', mock.Mock())
    self.mock(storage, 'get_config_by_hash_async', mock.Mock())
    storage.get_config_hash_async.return_value = future(
        ('deadbeef', 'beefdead'))
    storage.get_config_by_hash_async.return_value = future(
        'project_access_group: "group"')

    actual = storage.get_self_config_async(
        'acl.cfg', service_config_pb2.AclCfg).get_result()
    self.assertEqual(expected, actual)

    storage.get_config_hash_async.assert_called_once_with(
        'services/sample-app', 'acl.cfg')
    storage.get_config_by_hash_async.assert_called_once_with('beefdead')

    # memcached:
    storage.get_config_hash_async.reset_mock()
    storage.get_config_by_hash_async.reset_mock()
    actual = storage.get_latest_as_message_async(
        'services/sample-app', 'acl.cfg',
        service_config_pb2.AclCfg).get_result()
    self.assertEqual(expected, actual)
    self.assertFalse(storage.get_config_hash_async.called)
    self.assertFalse(storage.get_config_by_hash_async.called)


if __name__ == '__main__':
  test_env.main()
