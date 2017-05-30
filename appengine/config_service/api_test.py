#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import datetime
import httplib

import test_env
from test_env import future

test_env.setup_test_env()

from components import auth
from components import config
from components.config.proto import project_config_pb2
from components.config.proto import service_config_pb2
from test_support import test_case
import mock

import acl
import api
import gitiles_import
import projects
import storage


class ApiTest(test_case.EndpointsTestCase):
  api_service_cls = api.ConfigApi

  def setUp(self):
    super(ApiTest, self).setUp()
    self.mock(acl, 'has_project_access', mock.Mock())
    acl.has_project_access.side_effect = (
        lambda pid: pid != 'secret'
    )
    self.mock(acl, 'has_service_access', mock.Mock(return_value=True))
    self.mock(projects, 'get_projects', mock.Mock())
    projects.get_projects.return_value = [
      service_config_pb2.Project(id='chromium'),
      service_config_pb2.Project(id='v8'),
    ]
    self.mock(projects, 'get_metadata', mock.Mock())
    projects.get_metadata.return_value = project_config_pb2.ProjectCfg()
    self.mock(projects, 'get_repos', mock.Mock())
    projects.get_repos.return_value = [
      (projects.RepositoryType.GITILES, 'https://localhost/project'),
    ]

  def mock_config(self):
    self.mock(storage, 'get_config_hash_async', mock.Mock())
    self.mock(storage, 'get_config_by_hash_async', mock.Mock())
    storage.get_config_hash_async.return_value = future(('deadbeef', 'abc0123'))
    storage.get_config_by_hash_async.return_value = future('config text')

  def mock_refs(self):
    self.mock(projects, 'get_refs', mock.Mock())
    projects.get_refs.return_value = [
      project_config_pb2.RefsCfg.Ref(name='refs/heads/master'),
      project_config_pb2.RefsCfg.Ref(name='refs/heads/release42'),
    ]

  ##############################################################################
  # get_mapping

  def test_get_mapping_one(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(id='services/x', location='http://x'),
    ])

    req = {
      'config_set': 'services/x',
    }
    resp = self.call_api('get_mapping', req).json_body

    storage.get_config_sets_async.assert_called_once_with(
        config_set='services/x')

    self.assertEqual(resp, {
      'mappings': [
        {
          'config_set': 'services/x',
          'location': 'http://x',
        },
      ],
    })

  def test_get_mapping_one_forbidden(self):
    self.mock(acl, 'can_read_config_set', mock.Mock(return_value=False))
    with self.call_should_fail(httplib.FORBIDDEN):
      req = {
        'config_set': 'services/x',
      }
      self.call_api('get_mapping', req)

  def test_get_mapping_all(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(id='services/x', location='http://x'),
      storage.ConfigSet(id='services/y', location='http://y'),
    ])
    resp = self.call_api('get_mapping', {}).json_body

    self.assertEqual(resp, {
      'mappings': [
        {
          'config_set': 'services/x',
          'location': 'http://x',
        },
        {
          'config_set': 'services/y',
          'location': 'http://y',
        },
      ],
    })

  def test_get_mapping_all_partially_forbidden(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(id='services/x', location='http://x'),
      storage.ConfigSet(id='services/y', location='http://y'),
    ])
    self.mock(acl, 'can_read_config_set', mock.Mock(side_effect=[True, False]))

    resp = self.call_api('get_mapping', {}).json_body

    self.assertEqual(resp, {
      'mappings': [
        {
          'config_set': 'services/x',
          'location': 'http://x',
        },
      ],
    })

  ##############################################################################
  # get_config_sets

  def test_get_config_one(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(
          id='services/x',
          location='https://x.googlesource.com/x',
          latest_revision='deadbeef',
          latest_revision_url='https://x.googlesource.com/x/+/deadbeef',
          latest_revision_time=datetime.datetime(2016, 1, 1),
          latest_revision_committer_email='john@doe.com',
      ),
    ])

    req = {
      'config_set': 'services/x',
    }
    resp = self.call_api('get_config_sets', req).json_body

    storage.get_config_sets_async.assert_called_once_with(
        config_set='services/x')

    self.assertEqual(resp, {
      'config_sets': [
        {
          'config_set': 'services/x',
          'location': 'https://x.googlesource.com/x',
          'revision': {
            'id': 'deadbeef',
            'url': 'https://x.googlesource.com/x/+/deadbeef',
            'timestamp': '1451606400000000',
            'committer_email': 'john@doe.com',
          },
        },
      ],
    })

  def test_get_config_one_with_last_attempt(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(
          id='services/x',
          location='https://x.googlesource.com/x',
          latest_revision='deadbeef',
          latest_revision_url='https://x.googlesource.com/x/+/deadbeef',
          latest_revision_time=datetime.datetime(2016, 1, 1),
          latest_revision_committer_email='john@doe.com',
      ),
    ])

    storage.ImportAttempt(
        key=storage.last_import_attempt_key('services/x'),
        time=datetime.datetime(2016, 1, 2),
        revision=storage.RevisionInfo(
          id='badcoffee',
          url='https://x.googlesource.com/x/+/badcoffee',
          time=datetime.datetime(2016, 1, 1),
          committer_email='john@doe.com',
        ),
        success=False,
        message='Validation errors',
        validation_messages=[
          storage.ImportAttempt.ValidationMessage(
              severity=config.Severity.ERROR,
              text='error!',
          ),
          storage.ImportAttempt.ValidationMessage(
              severity=config.Severity.WARNING,
              text='warning!',
          ),
        ],
    ).put()

    req = {
      'config_set': 'services/x',
    }
    resp = self.call_api('get_config_sets', req).json_body

    storage.get_config_sets_async.assert_called_once_with(
        config_set='services/x')

    expected_resp = {
      'config_sets': [
        {
          'config_set': 'services/x',
          'location': 'https://x.googlesource.com/x',
          'revision': {
            'id': 'deadbeef',
            'url': 'https://x.googlesource.com/x/+/deadbeef',
            'timestamp': '1451606400000000',
            'committer_email': 'john@doe.com',
          },
        },
      ],
    }
    self.assertEqual(resp, expected_resp)

    req['include_last_import_attempt'] = True
    resp = self.call_api('get_config_sets', req).json_body
    expected_resp['config_sets'][0]['last_import_attempt'] = {
      'timestamp': '1451692800000000',
      'revision': {
        'id': 'badcoffee',
        'url': 'https://x.googlesource.com/x/+/badcoffee',
        'timestamp': '1451606400000000',
        'committer_email': 'john@doe.com',
      },
      'success': False,
      'message': 'Validation errors',
      'validation_messages': [
        {
          'severity': 'ERROR',
          'text': 'error!',
        },
        {
          'severity': 'WARNING',
          'text': 'warning!',
        },
      ]
    }
    self.assertEqual(resp, expected_resp)

  def test_get_config_one_forbidden(self):
    self.mock(acl, 'can_read_config_set', mock.Mock(return_value=False))
    with self.call_should_fail(httplib.FORBIDDEN):
      req = {
        'config_set': 'services/x',
      }
      self.call_api('get_config_sets', req)

  def test_get_config_all(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(
          id='services/x',
          location='https://x.googlesource.com/x',
          latest_revision='deadbeef',
          latest_revision_url='https://x.googlesource.com/x/+/deadbeef',
          latest_revision_time=datetime.datetime(2016, 1, 1),
          latest_revision_committer_email='john@doe.com',
      ),
      storage.ConfigSet(
          id='projects/y',
          location='https://y.googlesource.com/y',
          latest_revision='badcoffee',
          latest_revision_url='https://y.googlesource.com/y/+/badcoffee',
          latest_revision_time=datetime.datetime(2016, 1, 2),
          latest_revision_committer_email='john@doe.com',
      ),
    ])

    resp = self.call_api('get_config_sets', {}).json_body

    storage.get_config_sets_async.assert_called_once_with(config_set=None)

    self.assertEqual(resp, {
      'config_sets': [
        {
          'config_set': 'services/x',
          'location': 'https://x.googlesource.com/x',
          'revision': {
            'id': 'deadbeef',
            'url': 'https://x.googlesource.com/x/+/deadbeef',
            'timestamp': '1451606400000000',
            'committer_email': 'john@doe.com',
          },
        },
        {
          'config_set': 'projects/y',
          'location': 'https://y.googlesource.com/y',
          'revision': {
            'id': 'badcoffee',
            'url': 'https://y.googlesource.com/y/+/badcoffee',
            'timestamp': '1451692800000000',
            'committer_email': 'john@doe.com',
          },
        },
      ],
    })

  def test_get_config_all_partially_forbidden(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(
          id='services/x',
          location='https://x.googlesource.com/x',
          latest_revision='deadbeef',
      ),
      storage.ConfigSet(
          id='projects/y',
          location='https://y.googlesource.com/y',
          latest_revision='badcoffee',
      ),
    ])
    self.mock(acl, 'can_read_config_set', mock.Mock(side_effect=[True, False]))

    resp = self.call_api('get_config_sets', {}).json_body

    self.assertEqual(resp, {
      'config_sets': [
        {
          'config_set': 'services/x',
          'location': 'https://x.googlesource.com/x',
          'revision': {
            'id': 'deadbeef',
          }
        },
      ],
    })

  def test_get_config_one(self):
    self.mock(storage, 'get_config_sets_async', mock.Mock())
    storage.get_config_sets_async.return_value = future([
      storage.ConfigSet(
          id='services/x',
          location='https://x.googlesource.com/x',
          latest_revision='deadbeef',
          latest_revision_url='https://x.googlesource.com/x/+/deadbeef',
          latest_revision_time=datetime.datetime(2016, 1, 1),
          latest_revision_committer_email='john@doe.com',
      ),
    ])

    req = {
      'config_set': 'services/x',
    }
    resp = self.call_api('get_config_sets', req).json_body

    storage.get_config_sets_async.assert_called_once_with(
        config_set='services/x')

    self.assertEqual(resp, {
      'config_sets': [
        {
          'config_set': 'services/x',
          'location': 'https://x.googlesource.com/x',
          'revision': {
            'id': 'deadbeef',
            'url': 'https://x.googlesource.com/x/+/deadbeef',
            'timestamp': '1451606400000000',
            'committer_email': 'john@doe.com',
          },
        },
      ],
    })

  ##############################################################################
  # get_config

  def test_get_config(self):
    self.mock_config()

    req = {
      'config_set': 'services/luci-config',
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {
      'content': base64.b64encode('config text'),
      'content_hash': 'abc0123',
      'revision': 'deadbeef',
    })
    storage.get_config_hash_async.assert_called_once_with(
      'services/luci-config', 'my.cfg', revision='deadbeef')
    storage.get_config_by_hash_async.assert_called_once_with('abc0123')

  def test_get_config_hash_only(self):
    self.mock_config()

    req = {
      'config_set': 'services/luci-config',
      'hash_only': True,
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {
      'content_hash': 'abc0123',
      'revision': 'deadbeef',
    })
    self.assertFalse(storage.get_config_by_hash_async.called)

  def test_get_config_blob_not_found(self):
    self.mock_config()
    storage.get_config_by_hash_async.return_value = future(None)

    req = {
      'config_set': 'services/luci-config',
      'path': 'my.cfg',
    }
    with self.call_should_fail(httplib.NOT_FOUND):
      self.call_api('get_config', req)

  def test_get_config_not_found(self):
    self.mock(
        storage, 'get_config_hash_async', lambda *_, **__: future((None, None)))

    req = {
      'config_set': 'services/x',
      'path': 'a.cfg',
    }
    with self.call_should_fail(httplib.NOT_FOUND):
      self.call_api('get_config', req)

  def test_get_wrong_config_set(self):
    self.mock(acl, 'can_read_config_set', mock.Mock(side_effect=ValueError))

    req = {
      'config_set': 'xxx',
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    with self.call_should_fail(httplib.BAD_REQUEST):
      self.call_api('get_config', req).json_body

  def test_get_config_without_permissions(self):
    self.mock(acl, 'can_read_config_set', mock.Mock(return_value=False))
    self.mock(storage, 'get_config_hash_async', mock.Mock())

    req = {
      'config_set': 'services/luci-config',
      'path': 'projects.cfg',
    }
    with self.call_should_fail(404):
      self.call_api('get_config', req)
    self.assertFalse(storage.get_config_hash_async.called)

  ##############################################################################
  # get_config_by_hash

  def test_get_config_by_hash(self):
    self.mock(storage, 'get_config_by_hash_async', mock.Mock())
    storage.get_config_by_hash_async.return_value = future('some content')

    req = {'content_hash': 'deadbeef'}
    resp = self.call_api('get_config_by_hash', req).json_body

    self.assertEqual(resp, {
      'content': base64.b64encode('some content'),
    })

    storage.get_config_by_hash_async.return_value = future(None)
    with self.call_should_fail(httplib.NOT_FOUND):
      self.call_api('get_config_by_hash', req)

  ##############################################################################
  # get_projects

  def test_get_projects(self):
    projects.get_projects.return_value = [
      service_config_pb2.Project(id='chromium'),
      service_config_pb2.Project(id='v8'),
      service_config_pb2.Project(id='inconsistent'),
      service_config_pb2.Project(id='secret'),
    ]
    projects.get_metadata.side_effect = [
      project_config_pb2.ProjectCfg(
          name='Chromium, the best browser', access='all'),
      project_config_pb2.ProjectCfg(access='all'),
      project_config_pb2.ProjectCfg(access='all'),
      project_config_pb2.ProjectCfg(access='administrators'),
    ]
    projects.get_repos.return_value = [
      (projects.RepositoryType.GITILES, 'http://localhost/chromium'),
      (projects.RepositoryType.GITILES, 'http://localhost/v8'),
      (None, None),
      (projects.RepositoryType.GITILES, 'http://localhost/secret'),
    ]

    resp = self.call_api('get_projects', {}).json_body

    self.assertEqual(resp, {
      'projects': [
        {
          'id': 'chromium',
          'name': 'Chromium, the best browser',
          'repo_type': 'GITILES',
          'repo_url': 'http://localhost/chromium',
        },
        {
          'id': 'v8',
          'repo_type': 'GITILES',
          'repo_url': 'http://localhost/v8',
        },
      ],
    })

  def test_get_projects_without_permissions(self):
    acl.has_project_access.return_value = False
    self.call_api('get_projects', {})

  ##############################################################################
  # get_refs

  def test_get_refs(self):
    self.mock_refs()

    req = {'project_id': 'chromium'}
    resp = self.call_api('get_refs', req).json_body

    self.assertEqual(resp, {
      'refs': [
        {'name': 'refs/heads/master'},
        {'name': 'refs/heads/release42'},
      ],
    })

  def test_get_refs_without_permissions(self):
    self.mock_refs()
    acl.has_project_access.side_effect = None
    acl.has_project_access.return_value = False

    req = {'project_id': 'chromium'}
    with self.call_should_fail(httplib.NOT_FOUND):
      self.call_api('get_refs', req)
    self.assertFalse(projects.get_refs.called)

  def test_get_refs_of_non_existent_project(self):
    self.mock(projects, 'get_refs', mock.Mock())
    projects.get_refs.return_value = None
    req = {'project_id': 'non-existent'}
    with self.call_should_fail(httplib.NOT_FOUND):
      self.call_api('get_refs', req)

  ##############################################################################
  # get_project_configs

  def test_get_config_multi(self):
    self.mock_refs()
    projects.get_projects.return_value.extend([
      service_config_pb2.Project(id='inconsistent'),
      service_config_pb2.Project(id='secret'),
    ])

    self.mock(storage, 'get_latest_multi_async', mock.Mock())
    storage.get_latest_multi_async.return_value = future([
      {
        'config_set': 'projects/chromium',
        'revision': 'deadbeef',
        'content_hash': 'abc0123',
        'content': 'config text',
      },
      {
        'config_set': 'projects/v8',
        'revision': 'beefdead',
        'content_hash': 'ccc123',
        # No content
      },
      {
        'config_set': 'projects/secret',
        'revision': 'badcoffee',
        'content_hash': 'abcabc',
        'content': 'abcsdjksl',
      },
    ])

    req = {'path': 'cq.cfg'}
    resp = self.call_api('get_project_configs', req).json_body

    self.assertEqual(resp, {
      'configs': [{
        'config_set': 'projects/chromium',
        'revision': 'deadbeef',
        'content_hash': 'abc0123',
        'content': base64.b64encode('config text'),
      }],
    })

  ##############################################################################
  # get_ref_configs

  def test_get_ref_configs_without_permission(self):
    acl.has_project_access.return_value = False

    req = {'path': 'cq.cfg'}
    resp = self.call_api('get_ref_configs', req).json_body
    self.assertEqual(resp, {})

  ##############################################################################
  # reimport

  def test_reimport_without_permissions(self):
    req = {'config_set': 'services/x'}
    with self.call_should_fail(403):
      self.call_api('reimport', req)

  def test_reimport(self):
    self.mock(auth, 'is_admin', mock.Mock(return_value=True))
    self.mock(gitiles_import, 'import_config_set', mock.Mock())
    req = {'config_set': 'services/x'}
    self.call_api('reimport', req)
    gitiles_import.import_config_set.assert_called_once_with('services/x')

  def test_reimport_not_found(self):
    self.mock(auth, 'is_admin', mock.Mock(return_value=True))
    self.mock(gitiles_import, 'import_config_set', mock.Mock())
    gitiles_import.import_config_set.side_effect = gitiles_import.NotFoundError
    req = {'config_set': 'services/x'}
    with self.call_should_fail(404):
      self.call_api('reimport', req)

  def test_reimport_bad_request(self):
    self.mock(auth, 'is_admin', mock.Mock(return_value=True))
    self.mock(gitiles_import, 'import_config_set', mock.Mock())
    gitiles_import.import_config_set.side_effect = gitiles_import.Error
    req = {'config_set': 'services/x'}
    with self.call_should_fail(500):
      self.call_api('reimport', req)


if __name__ == '__main__':
  test_env.main()
