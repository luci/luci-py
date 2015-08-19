#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from test_env import future
import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components.config.proto import service_config_pb2
from components.config.proto import project_config_pb2
from test_support import test_case
import mock

import projects
import storage


class ProjectsTestCase(test_case.TestCase):
  def setUp(self):
    super(ProjectsTestCase, self).setUp()
    self.mock(storage, 'get_latest_async', mock.Mock())
    storage.get_latest_async.return_value = ndb.Future()

  def test_get_projects(self):
    storage.get_latest_async.return_value.set_result('''
      projects {
        id: "chromium"
        config_location {
          storage_type: GITILES
          url: "http://localhost"
        }
      }
    ''')
    expected = service_config_pb2.ProjectsCfg(
        projects=[
          service_config_pb2.Project(
              id='chromium',
              config_location=service_config_pb2.ConfigSetLocation(
                storage_type=service_config_pb2.ConfigSetLocation.GITILES,
                url='http://localhost')
              ),
        ],
    )
    self.assertEqual(projects.get_projects(), expected.projects)

  def test_get_refs(self):
    storage.get_latest_async.return_value.set_result('''
      refs {
        name: "refs/heads/master"
      }
      refs {
        name: "refs/heads/release42"
        config_path: "other"
      }
    ''')
    expected = project_config_pb2.RefsCfg(
        refs=[
          project_config_pb2.RefsCfg.Ref(
              name='refs/heads/master'),
          project_config_pb2.RefsCfg.Ref(
              name='refs/heads/release42', config_path='other'),
        ],
    )
    self.assertEqual(projects.get_refs('chromium'), expected.refs)

  def test_get_refs_of_non_existent_project(self):
    storage.get_latest_async.return_value.set_result(None)
    self.assertEqual(projects.get_refs('chromium'), None)

  def test_repo_info(self):
    self.assertEqual(projects.get_repo('x'), (None, None))
    projects.update_import_info(
        'x', projects.RepositoryType.GITILES, 'http://localhost/x')
    # Second time for coverage.
    projects.update_import_info(
        'x', projects.RepositoryType.GITILES, 'http://localhost/x')
    self.assertEqual(
        projects.get_repo('x'),
        (projects.RepositoryType.GITILES, 'http://localhost/x'))

    # Change it
    projects.update_import_info(
        'x', projects.RepositoryType.GITILES, 'http://localhost/y')


if __name__ == '__main__':
  test_env.main()
