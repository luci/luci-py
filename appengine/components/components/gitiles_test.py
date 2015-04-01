#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import datetime
import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from test_support import test_case
import mock

from components import gerrit
from components import gitiles


HOSTNAME = 'chromium.googlesource.com'
PROJECT = 'project'
REVISION = '404d1697dca23824bc1130061a5bd2be4e073922'
PATH = '/dir'


class GitilesTestCase(test_case.TestCase):
  def setUp(self):
    super(GitilesTestCase, self).setUp()
    self.mock(gerrit, 'fetch_json', mock.Mock())
    self.mock(gerrit, 'fetch', mock.Mock())

  def test_parse_time(self):
    time_str = 'Fri Nov 07 17:09:03 2014'
    expected = datetime.datetime(2014, 11, 07, 17, 9, 3)
    actual = gitiles.parse_time(time_str)
    self.assertEqual(expected, actual)

  def test_parse_time_with_positive_timezone(self):
    time_str = 'Fri Nov 07 17:09:03 2014 +01:00'
    expected = datetime.datetime(2014, 11, 07, 16, 9, 3)
    actual = gitiles.parse_time(time_str)
    self.assertEqual(expected, actual)

  def test_parse_time_with_negative_timezone(self):
    time_str = 'Fri Nov 07 17:09:03 2014 -01:00'
    expected = datetime.datetime(2014, 11, 07, 18, 9, 3)
    actual = gitiles.parse_time(time_str)
    self.assertEqual(expected, actual)

  def test_get_commit(self):
    req_path = 'project/+/%s' % REVISION
    gerrit.fetch_json.return_value = {
      'commit': REVISION,
      'tree': '3cfb41e1c6c37e61c3eccfab2395752298a5743c',
      'parents': [
        '4087678c002d57e1148f21da5e00867df9a7d973',
      ],
      'author': {
        'name': 'John Doe',
        'email': 'john.doe@chromium.org',
        'time': 'Tue Apr 29 00:00:00 2014',
      },
      'committer': {
        'name': 'John Doe',
        'email': 'john.doe@chromium.org',
        'time': 'Tue Apr 29 00:00:00 2014',
      },
      'message': 'Subject\\n\\nBody',
      'tree_diff': [
        {
          'type': 'modify',
          'old_id': 'f23dec7da271f7e9d8c55a35f32f6971b7ce486d',
          'old_mode': 33188,
          'old_path': 'codereview.settings',
          'new_id': '0bdbda926c49aa3cc4b7248bc22cc261abff5f94',
          'new_mode': 33188,
          'new_path': 'codereview.settings',
        },
        {
          'type': 'add',
          'old_id': '0000000000000000000000000000000000000000',
          'old_mode': 0,
          'old_path': '/dev/null',
          'new_id': 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
          'new_mode': 33188,
          'new_path': 'x',
        }
      ],
    }

    commit = gitiles.get_commit(HOSTNAME, PROJECT, REVISION)
    gerrit.fetch_json.assert_called_once_with(HOSTNAME, req_path)
    self.assertIsNotNone(commit)
    self.assertEqual(commit.sha, REVISION)
    self.assertEqual(commit.committer.name, 'John Doe')
    self.assertEqual(commit.committer.email, 'john.doe@chromium.org')
    self.assertEqual(commit.author.name, 'John Doe')
    self.assertEqual(commit.author.email, 'john.doe@chromium.org')

  def test_get_tree(self):
    req_path = 'project/+/deadbeef/./dir'
    gerrit.fetch_json.return_value = {
        'id': 'c244aa92a18cd719c55205f99e04333840330012',
        'entries': [
          {
            'id': '0244aa92a18cd719c55205f99e04333840330012',
            'name': 'a',
            'type': 'blob',
            'mode': 33188,
          },
          {
            'id': '9c247a8aa968a3e2641addf1f4bd4acfc24e7915',
            'name': 'b',
            'type': 'blob',
            'mode': 33188,
          },
        ],
    }

    tree = gitiles.get_tree(HOSTNAME, 'project', 'deadbeef', '/dir')
    gerrit.fetch_json.assert_called_once_with(HOSTNAME, req_path)
    self.assertIsNotNone(tree)
    self.assertEqual(tree.id, 'c244aa92a18cd719c55205f99e04333840330012')
    self.assertEqual(
        tree.entries[0].id, '0244aa92a18cd719c55205f99e04333840330012')
    self.assertEqual(tree.entries[0].name, 'a')

  def test_get_log(self):
    req_path = 'project/+log/master/./'
    gerrit.fetch_json.return_value = {
      'log': [
        {
          'commit': REVISION,
          'tree': '3cfb41e1c6c37e61c3eccfab2395752298a5743c',
          'parents': [
            '4087678c002d57e1148f21da5e00867df9a7d973',
          ],
          'author': {
            'name': 'John Doe',
            'email': 'john.doe@chromium.org',
            'time': 'Tue Apr 29 00:00:00 2014',
          },
          'committer': {
            'name': 'John Doe',
            'email': 'john.doe@chromium.org',
            'time': 'Tue Apr 29 00:00:00 2014',
          },
          'message': 'Subject\\n\\nBody',
        },
        {
          'commit': '4087678c002d57e1148f21da5e00867df9a7d973',
          'tree': '3cfb41asdc37e61c3eccfab2395752298a5743c',
          'parents': [
            '1237678c002d57e1148f21da5e00867df9a7d973',
          ],
          'author': {
            'name': 'John Doe',
            'email': 'john.doe@chromium.org',
            'time': 'Tue Apr 29 00:00:00 2014',
          },
          'committer': {
            'name': 'John Doe',
            'email': 'john.doe@chromium.org',
            'time': 'Tue Apr 29 00:00:00 2014',
          },
          'message': 'Subject2\\n\\nBody2',
        },
      ],
    }

    log = gitiles.get_log(HOSTNAME, 'project', 'master', limit=2)
    gerrit.fetch_json.assert_called_once_with(
        HOSTNAME, req_path, query_params={'n': 2})

    john = gitiles.Contribution(
        name='John Doe', email='john.doe@chromium.org',
        time=datetime.datetime(2014, 4, 29))
    self.assertEqual(
        log,
        gitiles.Log(
            commits=[
              gitiles.Commit(
                  sha=REVISION,
                  tree='3cfb41e1c6c37e61c3eccfab2395752298a5743c',
                  parents=[
                    '4087678c002d57e1148f21da5e00867df9a7d973',
                  ],
                  message='Subject\\n\\nBody',
                  author=john,
                  committer=john,
              ),
              gitiles.Commit(
                  sha='4087678c002d57e1148f21da5e00867df9a7d973',
                  tree='3cfb41asdc37e61c3eccfab2395752298a5743c',
                  parents=[
                    '1237678c002d57e1148f21da5e00867df9a7d973',
                  ],
                  message='Subject2\\n\\nBody2',
                  author=john,
                  committer=john,
              ),
            ]
        )
    )

  def test_get_file_content(self):
    req_path = 'project/+/master/./a.txt'
    gerrit.fetch.return_value.content = base64.b64encode('content')

    content = gitiles.get_file_content(HOSTNAME, 'project', 'master', '/a.txt')
    gerrit.fetch.assert_called_once_with(
        HOSTNAME, req_path, accept_header='text/plain')
    self.assertEqual(content, 'content')

  def test_get_archive(self):
    req_path = 'project/+archive/master/./dir.tar.gz'
    gerrit.fetch.return_value.content = 'tar gz bytes'

    content = gitiles.get_archive(HOSTNAME, 'project', 'master', '/dir')
    gerrit.fetch.assert_called_once_with(HOSTNAME, req_path)
    self.assertEqual('tar gz bytes', content)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
