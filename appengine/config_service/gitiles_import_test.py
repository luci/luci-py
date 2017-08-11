#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import os

from test_env import future
import test_env
test_env.setup_test_env()

from google.appengine.api import urlfetch_errors
from google.appengine.ext import ndb

import mock

from components import config
from components import gitiles
from components import net
from components.config.proto import project_config_pb2
from components.config.proto import service_config_pb2
from test_support import test_case

import admin
import gitiles_import
import notifications
import projects
import storage
import validation


TEST_ARCHIVE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'test_archive.tar.gz')


class GitilesImportTestCase(test_case.TestCase):
  john = gitiles.Contribution(
      'John Doe', 'john@doe.com', datetime.datetime(2016, 1, 1))
  test_commit = gitiles.Commit(
      sha='a1841f40264376d170269ee9473ce924b7c2c4e9',
      tree='deadbeef',
      parents=['beefdead'],
      author=john,
      committer=john,
      message=None,
      tree_diff=None)

  def assert_attempt(self, success, msg, config_set=None, no_revision=False):
    config_set = config_set or 'config_set'
    attempt = storage.last_import_attempt_key(config_set).get()
    self.assertIsNotNone(attempt)
    if no_revision:
      self.assertIsNone(attempt.revision)
    else:
      self.assertEqual(attempt.revision.id, self.test_commit.sha)
      self.assertEqual(attempt.revision.time, self.test_commit.committer.time)
      self.assertEqual(
          attempt.revision.url,
          'https://localhost/project/+/a1841f40264376d170269ee9473ce924b7c2c4e9'
      )
      self.assertEqual(attempt.revision.committer_email, 'john@doe.com')
    self.assertEqual(attempt.success, success)
    self.assertEqual(attempt.message, msg)
    return attempt

  def test_get_gitiles_config_corrupted(self):
    self.mock(storage, 'get_latest_configs_async', mock.Mock())
    storage.get_latest_configs_async.return_value = future({
      storage.get_self_config_set(): (
          'rev', 'file://config', 'content_hash', 'garbage'),
    })
    gitiles_import.get_gitiles_config()

  def mock_get_archive(self):
    self.mock(gitiles, 'get_archive', mock.Mock())
    with open(TEST_ARCHIVE_PATH, 'r') as test_archive_file:
      gitiles.get_archive.return_value = test_archive_file.read()

  def test_import_revision(self):
    self.mock_get_archive()

    gitiles_import._import_revision(
        'config_set',
        gitiles.Location(
            hostname='localhost',
            project='project',
            treeish='luci/config',
            path='/',
        ),
        self.test_commit)

    gitiles.get_archive.assert_called_once_with(
        'localhost', 'project', 'a1841f40264376d170269ee9473ce924b7c2c4e9', '/',
        deadline=15)
    saved_config_set = storage.ConfigSet.get_by_id('config_set')
    self.assertIsNotNone(saved_config_set)
    self.assertEqual(saved_config_set.latest_revision, self.test_commit.sha)
    self.assertEqual(
        saved_config_set.location,
        'https://localhost/project/+/luci/config')

    saved_revision = storage.Revision.get_by_id(
        self.test_commit.sha, parent=saved_config_set.key)
    self.assertIsNotNone(saved_revision)

    saved_file = storage.File.get_by_id(
        'test_archive/x', parent=saved_revision.key)
    self.assertIsNotNone(saved_file)
    self.assertEqual(
        saved_file.content_hash, 'v1:587be6b4c3f93f93c489c0111bba5596147a26cb')

    saved_blob = storage.Blob.get_by_id(saved_file.content_hash)
    self.assertIsNotNone(saved_blob)
    self.assertEqual(saved_blob.content, 'x\n')
    self.assert_attempt(True, 'Imported')

    # Run second time, assert nothing is fetched from gitiles.
    ndb.Key(storage.ConfigSet, 'config_set').delete()
    gitiles.get_archive.reset_mock()
    gitiles_import._import_revision(
        'config_set',
        gitiles.Location(
            hostname='localhost',
            project='project',
            treeish='master',
            path='/'),
        self.test_commit)
    self.assertFalse(gitiles.get_archive.called)
    self.assert_attempt(True, 'Up-to-date')

  def test_import_revision_no_archive(self):
    self.mock_get_log()
    self.mock(gitiles, 'get_archive', mock.Mock(return_value=None))

    gitiles_import._import_revision(
        'config_set',
        gitiles.Location(
          hostname='localhost',
          project='project',
          treeish='master',
          path='/'),
        self.test_commit)
    self.assert_attempt(True, 'Config directory not found. Imported as empty')

  def test_import_invalid_revision(self):
    self.mock_get_archive()
    self.mock(notifications, 'notify_gitiles_rejection', mock.Mock())

    def validate_config(config_set, filename, content, ctx):
      if filename == 'test_archive/x':
        ctx.error('bad config!')
    self.mock(validation, 'validate_config', validate_config)

    gitiles_import._import_revision(
        'config_set',
        gitiles.Location(
          hostname='localhost',
          project='project',
          treeish='master',
          path='/'),
        self.test_commit)
    # Assert not saved.
    self.assertIsNone(storage.ConfigSet.get_by_id('config_set'))

    saved_attempt = self.assert_attempt(False, 'Validation errors')
    self.assertEqual(len(saved_attempt.validation_messages), 1)
    val_msg = saved_attempt.validation_messages[0]
    self.assertEqual(val_msg.severity, config.Severity.ERROR)
    self.assertEqual(val_msg.text, 'test_archive/x: bad config!')

  def mock_get_log(self):
    self.mock(gitiles, 'get_log', mock.Mock())
    gitiles.get_log.return_value = gitiles.Log(
        commits=[self.test_commit],
        next_cursor=None,
    )

  def test_import_config_set(self):
    self.mock_get_log()
    self.mock_get_archive()

    gitiles_import._import_config_set(
        'config_set', gitiles.Location.parse('https://localhost/project'))

    gitiles.get_log.assert_called_once_with(
        'localhost', 'project', 'HEAD', '/', limit=1,
        deadline=15)

    saved_config_set = storage.ConfigSet.get_by_id('config_set')
    self.assertIsNotNone(saved_config_set)
    self.assertEqual(
        saved_config_set.latest_revision,
        'a1841f40264376d170269ee9473ce924b7c2c4e9')
    self.assertTrue(storage.Revision.get_by_id(
        'a1841f40264376d170269ee9473ce924b7c2c4e9',
        parent=saved_config_set.key))
    self.assert_attempt(True, 'Imported')

    # Import second time, import_revision should not be called.
    self.mock(gitiles_import, '_import_revision', mock.Mock())
    gitiles_import._import_config_set(
        'config_set', gitiles.Location.parse('https://localhost/project'))
    self.assertFalse(gitiles_import._import_revision.called)
    self.assert_attempt(True, 'Up-to-date')

  def test_import_config_set_with_log_failed(self):
    self.mock(gitiles_import, '_import_revision', mock.Mock())
    self.mock(gitiles, 'get_log', mock.Mock(return_value = None))
    with self.assertRaises(gitiles_import.NotFoundError):
      gitiles_import._import_config_set(
          'config_set',
          gitiles.Location.parse('https://localhost/project'))

    self.assert_attempt(False, 'Could not load commit log', no_revision=True)

  def test_import_existing_config_set_with_log_failed(self):
    self.mock(gitiles_import, '_import_revision', mock.Mock())
    self.mock(gitiles, 'get_log', mock.Mock(return_value = None))

    cs = storage.ConfigSet(
        id='config_set',
        latest_revision='deadbeef',
        latest_revision_url='https://localhost/project/+/deadbeef/x',
        latest_revision_committer_email=self.john.email,
        latest_revision_time=self.john.time,
        location='https://localhost/project/+/master/x',
    )
    cs.put()

    with self.assertRaises(gitiles_import.NotFoundError):
      gitiles_import._import_config_set(
          'config_set',
          gitiles.Location.parse('https://localhost/project'))

    self.assert_attempt(False, 'Could not load commit log', no_revision=True)

    cs = cs.key.get()
    self.assertIsNone(cs.latest_revision)

  def test_import_config_set_with_auth_error(self):
    self.mock(gitiles, 'get_log', mock.Mock())
    gitiles.get_log.side_effect = net.AuthError('Denied', 500, 'Denied')

    with self.assertRaises(gitiles_import.Error):
      gitiles_import._import_config_set(
          'config_set',
          gitiles.Location.parse('https://localhost/project'))
    self.assert_attempt(
        False, 'Could not import: permission denied', no_revision=True)

  def test_deadline_exceeded(self):
    self.mock_get_log()
    self.mock(gitiles, 'get_archive', mock.Mock())
    gitiles.get_archive.side_effect = urlfetch_errors.DeadlineExceededError

    with self.assertRaises(gitiles_import.Error):
      gitiles_import._import_config_set(
          'config_set',
          gitiles.Location.parse('https://localhost/project'))
    self.assert_attempt(False, 'Could not import: deadline exceeded')

  def test_import_services(self):
    self.mock(gitiles_import, '_import_config_set', mock.Mock())
    self.mock(gitiles, 'get_tree', mock.Mock())
    gitiles.get_tree.return_value = gitiles.Tree(
        id='abc',
        entries=[
          gitiles.TreeEntry(
              id='deadbeef',
              name='luci-config',
              type='tree',
              mode=0,
          ),
          gitiles.TreeEntry(
              id='deadbeef1',
              name='malformed service id',
              type='tree',
              mode=0,
          ),
          gitiles.TreeEntry(
              id='deadbeef1',
              name='a-file',
              type='blob',
              mode=0,
          ),
        ],
    )

    gitiles_import.import_services(
        gitiles.Location.parse('https://localhost/config'))

    gitiles.get_tree.assert_called_once_with(
        'localhost', 'config', 'HEAD', '/')
    gitiles_import._import_config_set.assert_called_once_with(
        'services/luci-config', 'https://localhost/config/+/HEAD/luci-config')

  def test_import_service(self):
    self.mock(gitiles_import, '_import_config_set', mock.Mock())

    conf = admin.GlobalConfig(
        services_config_storage_type=admin.ServiceConfigStorageType.GITILES,
        services_config_location='https://localhost/config'
    )
    gitiles_import.import_service('luci-config', conf)

    gitiles_import._import_config_set.assert_called_once_with(
        'services/luci-config',
        'https://localhost/config/+/HEAD/luci-config')

  def test_import_projects_and_refs(self):
    self.mock(gitiles_import, '_import_config_set', mock.Mock())
    self.mock(projects, 'get_projects', mock.Mock())
    self.mock(projects, 'get_refs', mock.Mock())
    projects.get_projects.return_value = [
      service_config_pb2.Project(
          id='chromium',
          config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
          )
      ),
      service_config_pb2.Project(
          id='bad_location',
          config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
          ),
      ),
      service_config_pb2.Project(
          id='non-gitiles',
      ),
    ]
    RefType = project_config_pb2.RefsCfg.Ref
    projects.get_refs.return_value = {
      'chromium': [
        RefType(name='refs/heads/master'),
        RefType(name='refs/heads/release42', config_path='/my-configs'),
      ],
    }

    gitiles_import.import_projects()

    self.assertEqual(gitiles_import._import_config_set.call_count, 3)
    gitiles_import._import_config_set.assert_any_call(
        'projects/chromium', 'https://localhost/chromium/src/+/refs/heads/luci')
    gitiles_import._import_config_set.assert_any_call(
        'projects/chromium/refs/heads/master',
        'https://localhost/chromium/src/+/refs/heads/master/luci')
    gitiles_import._import_config_set.assert_any_call(
        'projects/chromium/refs/heads/release42',
        'https://localhost/chromium/src/+/refs/heads/release42/my-configs')

  def test_import_project(self):
    self.mock(gitiles_import, '_import_config_set', mock.Mock())
    self.mock(projects, 'get_project', mock.Mock())
    projects.get_project.return_value = service_config_pb2.Project(
        id='chromium',
        config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
        ),
    )

    gitiles_import.import_project('chromium')

    gitiles_import._import_config_set.assert_called_once_with(
        'projects/chromium', 'https://localhost/chromium/src/+/refs/heads/luci')

  def test_import_project_not_found(self):
    self.mock(projects, 'get_project', mock.Mock(return_value=None))
    with self.assertRaises(gitiles_import.NotFoundError):
      gitiles_import.import_project('chromium')

  def test_import_project_invalid_id(self):
    with self.assertRaises(ValueError):
      gitiles_import.import_project(')))')

  def test_import_ref(self):
    self.mock(gitiles_import, '_import_config_set', mock.Mock())
    self.mock(projects, 'get_project', mock.Mock())
    self.mock(projects, 'get_refs', mock.Mock())
    projects.get_project.return_value = service_config_pb2.Project(
        id='chromium',
        config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
        ),
    )
    projects.get_refs.return_value = {
      'chromium': [
          project_config_pb2.RefsCfg.Ref(
              name='refs/heads/release42',
              config_path='/my-configs'),
      ],
    }

    gitiles_import.import_ref('chromium', 'refs/heads/release42')

    gitiles_import._import_config_set.assert_called_once_with(
        'projects/chromium/refs/heads/release42',
        'https://localhost/chromium/src/+/refs/heads/release42/my-configs')

  def test_import_ref_project_not_found(self):
    self.mock(projects, 'get_project', mock.Mock(return_value=None))
    with self.assertRaises(gitiles_import.NotFoundError):
      gitiles_import.import_ref('chromium', 'refs/heads/release42')

  def test_import_ref_not_found(self):
    self.mock(projects, 'get_project', mock.Mock())
    projects.get_project.return_value = service_config_pb2.Project(
        id='chromium',
        config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
        ),
    )
    self.mock(projects, 'get_refs', mock.Mock(return_value={
      'chromium': [],
    }))
    with self.assertRaises(gitiles_import.NotFoundError):
      gitiles_import.import_ref('chromium', 'refs/heads/release42')

  def test_import_projects_exception(self):
    self.mock(gitiles_import, 'import_project', mock.Mock())
    gitiles_import.import_project.side_effect = Exception
    self.mock(projects, 'get_refs', mock.Mock(return_value={
      'chromium': [],
      'will-fail': [],
    }))

    self.mock(projects, 'get_projects', mock.Mock())
    projects.get_projects.return_value = [
      service_config_pb2.Project(
          id='chromium',
          config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
          ),
      ),
      service_config_pb2.Project(
          id='will-fail',
          config_location=service_config_pb2.ConfigSetLocation(
            url='https://localhost/chromium/src/',
            storage_type=service_config_pb2.ConfigSetLocation.GITILES,
          ),
      )
    ]

    gitiles_import.import_projects()
    self.assertEqual(gitiles_import.import_project.call_count, 2)


if __name__ == '__main__':
  test_env.main()
