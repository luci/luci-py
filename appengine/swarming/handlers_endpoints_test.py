#!/usr/bin/env python
# coding=utf-8
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import datetime
import json
import logging
import os
import random
import sys
import unittest

import test_env_handlers
from test_support import test_case

from google.appengine.ext import ndb
from protorpc.remote import protojson
import webapp2
import webtest

from components import ereporter2
from components import utils

import handlers_backend
import handlers_bot
import handlers_endpoints
import swarming_rpcs

from server import acl
from server import bot_code
from server import bot_management
from server import config
from server import large
from server import task_pack
from server import task_request
from server import task_result


def message_to_dict(rpc_message):
  return json.loads(protojson.encode_message(rpc_message))


class BaseTest(test_env_handlers.AppTestBase, test_case.EndpointsTestCase):

  DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
  DATETIME_NO_MICRO = '%Y-%m-%dT%H:%M:%S'

  def setUp(self):
    test_case.EndpointsTestCase.setUp(self)
    super(BaseTest, self).setUp()
    # handlers_bot is necessary to create fake tasks.
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(handlers_bot.get_routes(), debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()
    self.mock(utils, 'enqueue_task', self._enqueue_task)

  @ndb.non_transactional
  def _enqueue_task(self, url, queue_name, **kwargs):
    if queue_name == 'task-dimensions':
      # Call directly into it, ignores any current transaction.
      handlers_backend.TaskDimensionsHandler.tidy_stale(kwargs['payload'])
      return True
    if queue_name == 'pubsub':
      return True
    self.fail(url)


class ServerApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingServerService

  def test_details(self):
    """Asserts that server_details returns the correct version."""
    self.mock(config.config, 'config_service_hostname', lambda: 'a.server')

    cfg = config.settings()
    cfg.isolate.default_server = 'https://isolateserver.appspot.com'
    cfg.isolate.default_namespace = 'default-gzip'
    self.mock(config, 'settings', lambda: cfg)

    response = self.call_api('details')
    expected = {
      u'bot_version': unicode(
          bot_code.get_bot_version('https://testbed.example.com')[0]),
      u'display_server_url_template': u'',
      u'luci_config': u'a.server',
      u'default_isolate_server': u'https://isolateserver.appspot.com',
      u'default_isolate_namespace': u'default-gzip',
      u'machine_provider_template':
          u'https://machine-provider.appspot.com/leases/%s',
      u'server_version': unicode(utils.get_app_version()),
    }
    self.assertEqual(expected, response.json)

  def test_public_permissions(self):
    """Asserts that permissions respond correctly to an unauthed user."""
    self.set_as_anonymous()
    response = self.call_api('permissions')
    expected = {
        u'cancel_task': False,
        u'cancel_tasks': False,
        u'delete_bot': False,
        u'get_bootstrap_token': False,
        u'get_configs': False,
        u'put_configs': False,
        u'terminate_bot': False,
    }
    self.assertEqual(expected, response.json)

  def test_user_permissions(self):
    """Asserts that permissions respond correctly to a basic user."""
    self.set_as_user()
    response = self.call_api('permissions')
    expected = {
        u'cancel_task': True,
        u'cancel_tasks': False,
        u'delete_bot': False,
        u'get_bootstrap_token': False,
        u'get_configs': True,
        u'put_configs': False,
        u'terminate_bot': False,
    }
    self.assertEqual(expected, response.json)

  def test_privileged_user_permissions(self):
    """Asserts that permissions respond correctly to a privileged user."""
    self.set_as_privileged_user()
    response = self.call_api('permissions')
    expected = {
        u'cancel_task': True,
        u'cancel_tasks': False,
        u'delete_bot': False,
        u'get_bootstrap_token': False,
        u'get_configs': True,
        u'put_configs': False,
        u'terminate_bot': True,
    }
    self.assertEqual(expected, response.json)

  def test_admin_permissions(self):
    """Asserts that permissions respond correctly to an admin."""
    self.set_as_admin()
    response = self.call_api('permissions')
    expected = {
        u'cancel_task': True,
        u'cancel_tasks': True,
        u'delete_bot': True,
        u'get_bootstrap_token': True,
        u'get_configs': True,
        u'put_configs': True,
        u'terminate_bot': True,
    }
    self.assertEqual(expected, response.json)

  def _test_file(self, name, header):
    # Tests either get_bootstrap/put_bootstrap or get_bot_config/put_bot_config.
    self.set_as_admin()

    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    path = os.path.join(self.APP_DIR, 'swarming_bot', 'config', name + '.py')
    with open(path, 'rb') as f:
      content = f.read().decode('utf-8')

    expected = {
      u'content': header + content,
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    expected = {
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:admin@example.com',
    }
    response = self.call_api('put_' + name, {'content': u'print(\'hi ☀!\')'})
    self.assertEqual(expected, response.json)

    expected = {
      u'content': header + u'print(\'hi \u2600!\')',
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:admin@example.com',
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    self.mock_now(now, 60)
    expected = {
      u'version': u'1',
      u'when': u'2010-01-02T03:05:05',
      u'who': u'user:admin@example.com',
    }
    response = self.call_api('put_' + name, {'content': u'print(\'hi ♕!\')'})
    self.assertEqual(expected, response.json)

    expected = {
      u'content': header + u'print(\'hi ♕!\')',
      u'version': u'1',
      u'when': u'2010-01-02T03:05:05',
      u'who': u'user:admin@example.com',
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    # Retrieve a previous version.
    expected = {
      u'content': header + u'print(\'hi ☀!\')',
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:admin@example.com',
    }
    response = self.call_api('get_' + name, {'version': '0'})
    self.assertEqual(expected, response.json)

    # Define a script on the luci-config server.
    def get_self_config_mock(path, revision=None, store_last_good=False):
      self.assertEqual('scripts/%s.py' % name, path)
      if revision:
        self.assertEqual(False, store_last_good)
        return revision, 'old code'
      self.assertEqual(None, revision)
      self.assertEqual(True, store_last_good)
      return 'abc', 'foo bar'
    def config_service_hostname_mock():
      return 'localhost:1'
    self.mock(bot_code.config, 'get_self_config', get_self_config_mock)
    self.mock(
        bot_code.config, 'config_service_hostname',
        config_service_hostname_mock)

    expected = {
      u'content': header + u'foo bar',
      u'version': u'abc',
      u'who': u'localhost:1',
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    # Can't retrieve previous versions with the API (due to the way luci-config
    # works).
    self.call_api('get_' + name, {'version': 'old'}, status=400)

  def test_bootstrap(self):
    self._test_file(
        'bootstrap',
        '#!/usr/bin/env python\nhost_url = \'\'\nbootstrap_token = \'\'\n')

  def test_bot_config(self):
    self._test_file('bot_config', '')


class TasksApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingTasksService

  def setUp(self):
    super(TasksApiTest, self).setUp()
    utils.clear_cache(config.settings)

  def test_new_ok_raw(self):
    """Asserts that new generates appropriate metadata."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            cipd_input=swarming_rpcs.CipdInput(
                client_package=swarming_rpcs.CipdPackage(
                    package_name='infra/tools/cipd/${platform}',
                    version='git_revision:deadbeef'),
                packages=[
                  swarming_rpcs.CipdPackage(
                      package_name='rm', path='.', version='latest'),
                ],
                server='https://chrome-infra-packages.appspot.com'),
            command=['rm', '-rf', '/'],
            dimensions=[
              swarming_rpcs.StringPair(key='pool', value='default'),
            ],
            env=[
              swarming_rpcs.StringPair(key='PATH', value='/'),
            ],
            execution_timeout_secs=30,
            io_timeout_secs=30,
            outputs=['foo','path/to/dir']),
        tags=['foo:bar'],
        user='joe@localhost',
        pubsub_topic='projects/abc/topics/def',
        pubsub_auth_token='secret that must not be shown',
        pubsub_userdata='userdata',
        service_account_token='bot')
    expected = {
      u'request': {
        u'authenticated': u'user:user@example.com',
        u'created_ts': str_now,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'cipd_input': {
            u'client_package': {
              u'package_name': u'infra/tools/cipd/${platform}',
              u'version': u'git_revision:deadbeef',
            },
            u'packages': [{
              u'package_name': u'rm',
              u'path': u'.',
              u'version': u'latest',
            }],
            u'server': u'https://chrome-infra-packages.appspot.com',
          },
          u'command': [u'rm', u'-rf', u'/'],
          u'dimensions': [
            {u'key': u'pool', u'value': u'default'},
          ],
          u'env': [
            {u'key': u'PATH', u'value': u'/'},
          ],
          u'execution_timeout_secs': u'30',
          u'grace_period_secs': u'30',
          u'idempotent': False,
          u'io_timeout_secs': u'30',
          u'outputs': [u'foo', u'path/to/dir'],
        },
        u'pubsub_topic': u'projects/abc/topics/def',
        u'pubsub_userdata': u'userdata',
        u'service_account': u'bot',
        u'tags': [
          u'foo:bar',
          u'pool:default',
          u'priority:200',
          u'service_account:bot',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

  def test_new_ok_deduped(self):
    """Asserts that new returns task result for deduped."""
    # Run a task to completion.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.client_create_task_raw(
        name='task', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    self.mock(random, 'getrandbits', lambda _: 0x66)
    now_30 = self.mock_now(now, 30)
    str_now_30 = unicode(now_30.strftime(self.DATETIME_NO_MICRO))
    self.set_as_user()

    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            command=['python', 'run_test.py'],
            cipd_input=swarming_rpcs.CipdInput(
                client_package=swarming_rpcs.CipdPackage(
                    package_name='infra/tools/cipd/${platform}',
                    version='git_revision:deadbeef'),
                packages=[
                  swarming_rpcs.CipdPackage(
                      package_name='rm',
                      path='bin',
                      version='git_revision:deadbeef'),
                ],
                server='https://chrome-infra-packages.appspot.com'),
            dimensions=[
              swarming_rpcs.StringPair(key='os', value='Amiga'),
              swarming_rpcs.StringPair(key='pool', value='default'),
            ],
            execution_timeout_secs=3600,
            idempotent=True,
            io_timeout_secs=1200,
            outputs=['foo', 'path/to/foobar']),
        tags=['foo:bar'],
        user='joe@localhost')
    expected = {
      u'request': {
        u'authenticated': u'user:user@example.com',
        u'created_ts': str_now_30,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'cipd_input': {
            u'client_package': {
              u'package_name': u'infra/tools/cipd/${platform}',
              u'version': u'git_revision:deadbeef',
            },
            u'packages': [{
              u'package_name': u'rm',
              u'path': u'bin',
              u'version': u'git_revision:deadbeef',
            }],
            u'server': u'https://chrome-infra-packages.appspot.com',
          },
          u'command': [u'python', u'run_test.py'],
          u'dimensions': [
            {u'key': u'os', u'value': u'Amiga'},
            {u'key': u'pool', u'value': u'default'},
          ],
          u'execution_timeout_secs': u'3600',
          u'grace_period_secs': u'30',
          u'idempotent': True,
          u'io_timeout_secs': u'1200',
          u'outputs': [u'foo', u'path/to/foobar'],
        },
        u'service_account': u'none',
        u'tags': [
          u'foo:bar',
          u'os:Amiga',
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cf59b8006610',
      u'task_result': {
        u'bot_dimensions': [
          {u'key': u'id', u'value': [u'bot1']},
          {u'key': u'os', u'value': [u'Amiga']},
          {u'key': u'pool', u'value': [u'default']},
        ],
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'completed_ts': str_now,
        u'cost_saved_usd': 0.1,
        u'created_ts': str_now_30,
        u'deduped_from': u'5cee488008811',
        u'duration': 0.1,
        u'exit_code': u'0',
        u'failure': False,
        u'internal_failure': False,
        u'modified_ts': str_now_30,
        u'name': u'job1',
        u'run_id': u'5cee488008811',
        u'server_versions': [u'v1a'],
        u'started_ts': str_now,
        u'state': u'COMPLETED',
        u'tags': [
          u'foo:bar',
          u'os:Amiga',
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'task_id': u'5cf59b8006610',
        u'try_number': u'0',
        u'user': u'joe@localhost',
      },
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

    request = swarming_rpcs.TasksRequest(state=swarming_rpcs.TaskState.DEDUPED)
    expected = {
      u'items': [
        {
          u'bot_dimensions': [
            {u'key': u'id', u'value': [u'bot1']},
            {u'key': u'os', u'value': [u'Amiga']},
            {u'key': u'pool', u'value': [u'default']},
          ],
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': str_now,
          u'cost_saved_usd': 0.1,
          u'created_ts': str_now_30,
          u'deduped_from': u'5cee488008811',
          u'duration': 0.1,
          u'exit_code': u'0',
          u'failure': False,
          u'internal_failure': False,
          u'modified_ts': str_now_30,
          u'name': u'job1',
          u'run_id': u'5cee488008811',
          u'server_versions': [u'v1a'],
          u'started_ts': str_now,
          u'state': u'COMPLETED',
          u'tags': [
            u'foo:bar',
            u'os:Amiga',
            u'pool:default',
            u'priority:200',
            u'service_account:none',
            u'user:joe@localhost',
          ],
          u'task_id': u'5cf59b8006610',
          u'try_number': u'0',
          u'user': u'joe@localhost',
        },
      ],
      u'now': str_now_30,
    }

    self.set_as_privileged_user()
    self.assertEqual(
        expected,
        self.call_api('list', body=message_to_dict(request)).json)
    # Assert the entity presence.
    self.assertEqual(2, task_request.TaskRequest.query().count())
    self.assertEqual(2, task_result.TaskResultSummary.query().count())
    self.assertEqual(1, task_result.TaskRunResult.query().count())

    # Deduped task have no performance data associated.
    request = swarming_rpcs.TasksRequest(
        state=swarming_rpcs.TaskState.DEDUPED,
        include_performance_stats=True)
    actual = self.call_api('list', body=message_to_dict(request)).json
    self.assertEqual(expected, actual)

    # Use the occasion to test 'count' and 'requests'.
    start = utils.datetime_to_timestamp(now) / 1000000. - 1
    end = utils.datetime_to_timestamp(now_30) / 1000000. + 1
    request = swarming_rpcs.TasksCountRequest(
        start=start, end=end, state=swarming_rpcs.TaskState.DEDUPED)
    self.assertEqual(
        {u'now': str_now_30, u'count': u'1'},
        self.call_api('count', body=message_to_dict(request)).json)
    request = swarming_rpcs.TasksRequest(start=start, end=end)
    expected = {
      u'items': [
        {
          u'authenticated': u'user:user@example.com',
          u'created_ts': str_now_30,
          u'expiration_secs': u'30',
          u'name': u'job1',
          u'priority': u'200',
          u'properties': {
            u'cipd_input': {
              u'client_package': {
                u'package_name': u'infra/tools/cipd/${platform}',
                u'version': u'git_revision:deadbeef',
              },
              u'packages': [{
                u'package_name': u'rm',
                u'path': u'bin',
                u'version': u'git_revision:deadbeef',
              }],
              u'server': u'https://chrome-infra-packages.appspot.com',
            },
            u'command': [u'python', u'run_test.py'],
            u'dimensions': [
              {u'key': u'os', u'value': u'Amiga'},
              {u'key': u'pool', u'value': u'default'},
            ],
            u'execution_timeout_secs': u'3600',
            u'grace_period_secs': u'30',
            u'idempotent': True,
            u'io_timeout_secs': u'1200',
            u'outputs': [u'foo', u'path/to/foobar'],
          },
          u'service_account': u'none',
          u'tags': [
            u'foo:bar',
            u'os:Amiga',
            u'pool:default',
            u'priority:200',
            u'service_account:none',
            u'user:joe@localhost',
          ],
          u'user': u'joe@localhost',
        },
        {
          u'authenticated': u'user:user@example.com',
          u'created_ts': str_now,
          u'expiration_secs': u'86400',
          u'name': u'task',
          u'priority': u'100',
          u'properties': {
            u'cipd_input': {
              u'client_package': {
                u'package_name': u'infra/tools/cipd/${platform}',
                u'version': u'git_revision:deadbeef',
              },
              u'packages': [{
                u'package_name': u'rm',
                u'path': u'bin',
                u'version': u'git_revision:deadbeef',
              }],
              u'server': u'https://chrome-infra-packages.appspot.com',
            },
            u'command': [u'python', u'run_test.py'],
            u'dimensions': [
              {u'key': u'os', u'value': u'Amiga'},
              {u'key': u'pool', u'value': u'default'},
            ],
            u'execution_timeout_secs': u'3600',
            u'grace_period_secs': u'30',
            u'idempotent': True,
            u'io_timeout_secs': u'1200',
            u'outputs': [u'foo', u'path/to/foobar'],
          },
          u'service_account': u'none',
          u'tags': [
            u'commit:post',
            u'os:Amiga',
            u'os:Win',
            u'pool:default',
            u'priority:100',
            u'project:yay',
            u'service_account:none',
            u'user:joe@localhost',
          ],
          u'user': u'joe@localhost',
        },
      ],
      u'now': str_now_30,
    }
    self.assertEqual(
        expected,
        self.call_api('requests', body=message_to_dict(request)).json)

  def test_new_ok_isolated(self):
    """Asserts that new generates appropriate metadata."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            dimensions=[
              swarming_rpcs.StringPair(key='pool', value='default'),
              swarming_rpcs.StringPair(key='foo', value='bar'),
            ],
            env=[
              swarming_rpcs.StringPair(key='PATH', value='/'),
            ],
            execution_timeout_secs=30,
            inputs_ref=swarming_rpcs.FilesRef(
                isolated='1'*40,
                isolatedserver='http://localhost:1',
                namespace='default-gzip'),
            io_timeout_secs=30),
        tags=['foo:bar'],
        user='joe@localhost')
    expected = {
      u'request': {
        u'authenticated': u'user:user@example.com',
        u'created_ts': str_now,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'dimensions': [
            {u'key': u'foo', u'value': u'bar'},
            {u'key': u'pool', u'value': u'default'},
          ],
          u'env': [
            {u'key': u'PATH', u'value': u'/'},
          ],
          u'execution_timeout_secs': u'30',
          u'grace_period_secs': u'30',
          u'idempotent': False,
          u'inputs_ref': {
            'isolated': '1'*40,
            'isolatedserver': 'http://localhost:1',
            'namespace': 'default-gzip',
          },
          u'io_timeout_secs': u'30',
        },
        u'service_account': u'none',
        u'tags': [
          u'foo:bar',
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

  def test_new_ok_isolated_with_defaults(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))

    cfg = config.settings()
    cfg.isolate.default_server = 'https://isolateserver.appspot.com'
    cfg.isolate.default_namespace = 'default-gzip'
    self.mock(config, 'settings', lambda: cfg)

    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            dimensions=[
              swarming_rpcs.StringPair(key='pool', value='default'),
              swarming_rpcs.StringPair(key='foo', value='bar'),
            ],
            env=[
              swarming_rpcs.StringPair(key='PATH', value='/'),
            ],
            execution_timeout_secs=30,
            inputs_ref=swarming_rpcs.FilesRef(isolated='1'*40),
            io_timeout_secs=30),
        tags=['foo:bar'],
        user='joe@localhost')
    expected = {
      u'request': {
        u'authenticated': u'user:user@example.com',
        u'created_ts': str_now,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'dimensions': [
            {u'key': u'foo', u'value': u'bar'},
            {u'key': u'pool', u'value': u'default'},
          ],
          u'env': [
            {u'key': u'PATH', u'value': u'/'},
          ],
          u'execution_timeout_secs': u'30',
          u'grace_period_secs': u'30',
          u'idempotent': False,
          u'inputs_ref': {
            'isolated': '1'*40,
            'isolatedserver': 'https://isolateserver.appspot.com',
            'namespace': 'default-gzip',
          },
          u'io_timeout_secs': u'30',
        },
        u'service_account': u'none',
        u'tags': [
          u'foo:bar',
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

  def test_new_cipd_package_with_defaults(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))

    # Define settings on the server.
    cfg = config.settings()
    cfg.cipd.default_client_package.package_name = (
        'infra/tools/cipd/${platform}')
    cfg.cipd.default_client_package.version = 'git_revision:deadbeef'
    cfg.cipd.default_server = 'https://chrome-infra-packages.appspot.com'
    self.mock(config, 'settings', lambda: cfg)

    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            cipd_input=swarming_rpcs.CipdInput(
                packages=[
                  swarming_rpcs.CipdPackage(
                      package_name='rm',
                      path='.',
                      version='latest'),
                ],
            ),
            command=['rm', '-rf', '/'],
            dimensions=[
              swarming_rpcs.StringPair(key='pool', value='default'),
            ],
            env=[
              swarming_rpcs.StringPair(key='PATH', value='/'),
            ],
            execution_timeout_secs=30,
            io_timeout_secs=30),
        tags=['foo:bar'],
        user='joe@localhost',
        pubsub_topic='projects/abc/topics/def',
        pubsub_auth_token='secret that must not be shown',
        pubsub_userdata='userdata')
    expected = {
      u'request': {
        u'authenticated': u'user:user@example.com',
        u'created_ts': str_now,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'cipd_input': {
            u'client_package': {
              u'package_name': u'infra/tools/cipd/${platform}',
              u'version': u'git_revision:deadbeef',
            },
            u'packages': [{
              u'package_name': u'rm',
              u'path': u'.',
              u'version': u'latest',
            }],
            u'server': u'https://chrome-infra-packages.appspot.com',
          },
          u'command': [u'rm', u'-rf', u'/'],
          u'dimensions': [
            {u'key': u'pool', u'value': u'default'},
          ],
          u'env': [
            {u'key': u'PATH', u'value': u'/'},
          ],
          u'execution_timeout_secs': u'30',
          u'grace_period_secs': u'30',
          u'idempotent': False,
          u'io_timeout_secs': u'30',
        },
        u'pubsub_topic': u'projects/abc/topics/def',
        u'pubsub_userdata': u'userdata',
        u'service_account': u'none',
        u'tags': [
          u'foo:bar',
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

  def test_mass_cancel(self):
    notifies = []
    def enqueue_task_mock(**kwargs):
      notifies.append(kwargs)
      return True
    self.mock(utils, 'enqueue_task', enqueue_task_mock)

    # Create two tasks.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    first, second, _, _, _, now_120 = self._gen_three_pending_tasks()
    now_120_str = unicode(now_120.strftime(self.DATETIME_NO_MICRO))

    expected = {
      u'matched': u'2',
      u'now': now_120_str,
    }
    self.set_as_admin()

    def enqueue_task(*args, **kwargs):
      self.assertEqual('%s,%s' % (second, first),
                       kwargs.get('payload', ''))
      # check URL
      self.assertEqual('/internal/taskqueue/cancel-tasks', args[0])
      # check task queue
      self.assertEqual('cancel-tasks', args[1])
      return True
    self.mock(utils, 'enqueue_task', enqueue_task)

    response = self.call_api('cancel', body={u'tags': [u'os:Win']})
    self.assertEqual(expected, response.json)



  def test_list_ok(self):
    """Asserts that list requests all TaskResultSummaries."""
    first, second, str_now_120, start, end = self._gen_two_tasks()
    first_no_perf = first.copy()
    first_no_perf.pop('performance_stats')
    # Basic request.
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, include_performance_stats=True)
    expected = {u'now': str_now_120, u'items': [second, first]}
    actual = self.call_api('list', body=message_to_dict(request)).json
    for k in ('isolated_download', 'isolated_upload'):
      for j in ('items_cold', 'items_hot'):
        actual['items'][1]['performance_stats'][k][j] = large.unpack(
            base64.b64decode(actual['items'][1]['performance_stats'][k][j]))
    self.assertEqual(expected, actual)

    # Sort by CREATED_TS.
    request = swarming_rpcs.TasksRequest(
        sort=swarming_rpcs.TaskSort.CREATED_TS)
    actual = self.call_api('list', body=message_to_dict(request)).json
    self.assertEqual(
        {u'now': str_now_120, u'items': [second, first_no_perf]}, actual)

    # Sort by MODIFIED_TS.
    request = swarming_rpcs.TasksRequest(
        sort=swarming_rpcs.TaskSort.MODIFIED_TS)
    actual = self.call_api('list', body=message_to_dict(request)).json
    self.assertEqual(
        {u'now': str_now_120, u'items': [first_no_perf, second]}, actual)

    # With two tags.
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, tags=['project:yay', 'commit:pre'])
    self.assertEqual(
        {u'now': str_now_120, u'items': [second]},
        self.call_api('list', body=message_to_dict(request)).json)

    # A spurious tag.
    request = swarming_rpcs.TasksRequest(end=end, start=start, tags=['foo:bar'])
    self.assertEqual(
        {u'now': str_now_120},
        self.call_api('list', body=message_to_dict(request)).json)

    # Both state and tag.
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, tags=['commit:pre'],
        state=swarming_rpcs.TaskState.COMPLETED_SUCCESS)
    self.assertEqual(
        {u'now': str_now_120, u'items': [second]},
        self.call_api('list', body=message_to_dict(request)).json)

    # Both sort and tag.
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, tags=['commit:pre'],
        sort=swarming_rpcs.TaskSort.MODIFIED_TS,
        state=swarming_rpcs.TaskState.COMPLETED_SUCCESS)
    self.call_api('list', body=message_to_dict(request), status=400)

  def test_count_indexes(self):
    # Asserts that no combination crashes.
    _, _, str_now_120, start, end = self._gen_two_tasks()
    for state in swarming_rpcs.TaskState:
      for tags in ([], ['a:1'], ['a:1', 'b:2']):
        request = swarming_rpcs.TasksCountRequest(
            start=start, end=end, state=state, tags=tags)
        result = self.call_api('count', body=message_to_dict(request)).json
        # Don't check for correctness here, just assert that it doesn't throw
        # due to missing index.
        result.pop(u'count')
        expected = {u'now': str_now_120}
        self.assertEqual(expected, result)

  def test_list_indexes(self):
    # Asserts that no combination crashes unexpectedly.
    TaskState = swarming_rpcs.TaskState
    TaskSort = swarming_rpcs.TaskSort
    # List of all unsupported combinations. These can be added either with a new
    # index or by massaging the way entities are stored.
    blacklisted = [
        # (<Using start, end or tags>, TaskState, TaskSort)
        (None, TaskState.BOT_DIED, TaskSort.ABANDONED_TS),
        (None, TaskState.BOT_DIED, TaskSort.COMPLETED_TS),
        (None, TaskState.BOT_DIED, TaskSort.MODIFIED_TS),
        (None, TaskState.CANCELED, TaskSort.ABANDONED_TS),
        (None, TaskState.CANCELED, TaskSort.COMPLETED_TS),
        (None, TaskState.CANCELED, TaskSort.MODIFIED_TS),
        (None, TaskState.COMPLETED, TaskSort.ABANDONED_TS),
        (None, TaskState.COMPLETED, TaskSort.COMPLETED_TS),
        (None, TaskState.COMPLETED, TaskSort.MODIFIED_TS),
        (None, TaskState.COMPLETED_FAILURE, TaskSort.ABANDONED_TS),
        (None, TaskState.COMPLETED_FAILURE, TaskSort.COMPLETED_TS),
        (None, TaskState.COMPLETED_FAILURE, TaskSort.MODIFIED_TS),
        (None, TaskState.COMPLETED_SUCCESS, TaskSort.ABANDONED_TS),
        (None, TaskState.COMPLETED_SUCCESS, TaskSort.COMPLETED_TS),
        (None, TaskState.COMPLETED_SUCCESS, TaskSort.MODIFIED_TS),
        (None, TaskState.DEDUPED, TaskSort.ABANDONED_TS),
        (None, TaskState.DEDUPED, TaskSort.COMPLETED_TS),
        (None, TaskState.DEDUPED, TaskSort.MODIFIED_TS),
        (None, TaskState.EXPIRED, TaskSort.ABANDONED_TS),
        (None, TaskState.EXPIRED, TaskSort.COMPLETED_TS),
        (None, TaskState.EXPIRED, TaskSort.MODIFIED_TS),
        (None, TaskState.PENDING, TaskSort.ABANDONED_TS),
        (None, TaskState.PENDING, TaskSort.COMPLETED_TS),
        (None, TaskState.PENDING, TaskSort.MODIFIED_TS),
        (None, TaskState.PENDING_RUNNING, TaskSort.ABANDONED_TS),
        (None, TaskState.PENDING_RUNNING, TaskSort.COMPLETED_TS),
        (None, TaskState.PENDING_RUNNING, TaskSort.MODIFIED_TS),
        (None, TaskState.RUNNING, TaskSort.ABANDONED_TS),
        (None, TaskState.RUNNING, TaskSort.COMPLETED_TS),
        (None, TaskState.RUNNING, TaskSort.MODIFIED_TS),
        (None, TaskState.TIMED_OUT, TaskSort.ABANDONED_TS),
        (None, TaskState.TIMED_OUT, TaskSort.COMPLETED_TS),
        (None, TaskState.TIMED_OUT, TaskSort.MODIFIED_TS),
        (True, TaskState.ALL, TaskSort.ABANDONED_TS),
        (True, TaskState.ALL, TaskSort.COMPLETED_TS),
        (True, TaskState.ALL, TaskSort.MODIFIED_TS),
    ]
    _, _, str_now_120, start, end = self._gen_two_tasks()
    for state in TaskState:
      for tags in ([], ['a:1'], ['a:1', 'b:2']):
        for start in (None, start):
          for end in (None, end):
            for sort in TaskSort:
              request = swarming_rpcs.TasksRequest(
                  start=start, end=end, state=state, tags=tags, sort=sort)
              using_filter = bool(start or end or tags)
              if ((using_filter, state, sort) in blacklisted or
                  (None, state, sort) in blacklisted):
                try:
                  self.call_api(
                      'list', body=message_to_dict(request), status=400)
                except:  # pylint: disable=bare-except
                  self.fail(
                      'Is actually supported: (%s, %s, %s)' %
                      (using_filter, state, sort))
              else:
                try:
                  result = self.call_api(
                      'list', body=message_to_dict(request)).json
                except:  # pylint: disable=bare-except
                  self.fail(
                      'Is unsupported: (%s, %s, %s)' %
                      (using_filter, state, sort))
                # Don't check for correctness here, just assert that it doesn't
                # throw due to missing index or invalid query.
                result.pop(u'items', None)
                expected = {u'now': str_now_120}
                self.assertEqual(expected, result)

  def test_tags_ok(self):
    """Asserts that TasksTags is returned with the right data."""
    self.set_as_privileged_user()
    now = datetime.datetime(2009, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    task_result.TagAggregation(
        key=task_result.TagAggregation.KEY,
        tags=[
            task_result.TagValues(
                tag='foo', values=['alpha', 'beta']),
            task_result.TagValues(
                tag='bar', values=['gamma', 'delta', 'epsilon']),
        ],
        ts=now).put()
    expected = {
      u'tasks_tags': [
        {
          u'key': 'foo',
          u'value': [u'alpha', u'beta'],
        },
        {
          u'key': 'bar',
          u'value': [u'gamma', u'delta', u'epsilon'],
        },
      ],
      u'ts': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    self.assertEqual(expected, self.call_api('tags', body={}).json)

  def _gen_two_tasks(self):
    # first request
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.mock_now(now)
    self.mock(random, 'getrandbits', lambda _: 0x66)
    _, first_id = self.client_create_task_raw(
        name='first', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    # second request
    self.set_as_user()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now_60 = self.mock_now(now, 60)
    str_now_60 = unicode(now_60.strftime(self.DATETIME_NO_MICRO))
    self.client_create_task_raw(
        name='second', user='jack@localhost',
        tags=['project:yay', 'commit:pre', 'os:Win'],
        properties=dict(idempotent=True))

    # Hack the datastore so MODIFIED_TS returns in backward order compared to
    # CREATED_TS.
    now_120 = self.mock_now(now, 120)
    str_now_120 = unicode(now_120.strftime(self.DATETIME_NO_MICRO))
    entity = task_pack.unpack_result_summary_key(first_id).get()
    entity.modified_ts = now_120
    entity.put()
    properties_hash = entity.properties_hash.encode('hex')

    second = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'cost_saved_usd': 0.1,
      u'created_ts': str_now_60,
      u'completed_ts': str_now,
      u'deduped_from': u'5cee488006611',
      u'duration': 0.1,
      u'exit_code': u'0',
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now_60,
      u'name': u'second',
      u'run_id': u'5cee488006611',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'tags': [
        u'commit:pre',
        u'os:Amiga',
        u'os:Win',
        u'pool:default',
        u'priority:100',
        u'project:yay',
        u'service_account:none',
        u'user:jack@localhost',
      ],
      u'task_id': u'5cfcee8008810',
      u'try_number': u'0',
      u'user': u'jack@localhost',
    }
    first = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.1],
      u'created_ts': str_now,
      u'completed_ts': str_now,
      u'duration': 0.1,
      u'exit_code': u'0',
      u'failure': False,
      u'internal_failure': False,
      u'performance_stats': {
        u'bot_overhead': 0.1,
        u'isolated_download': {
          u'duration': 1.0,
          u'initial_number_items': u'10',
          u'initial_size': u'100000',
          u'items_cold': [20],
          u'items_hot': [30],
        },
        u'isolated_upload': {
          u'duration': 2.0,
          u'items_cold': [40],
          u'items_hot': [50],
        },
      },
      u'modified_ts': str_now_120,
      u'name': u'first',
      u'properties_hash': unicode(properties_hash),
      u'run_id': u'5cee488006611',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'tags': [
        u'commit:post',
        u'os:Amiga',
        u'os:Win',
        u'pool:default',
        u'priority:100',
        u'project:yay',
        u'service_account:none',
        u'user:joe@localhost',
      ],
      u'task_id': u'5cee488006610',
      u'try_number': u'1',
      u'user': u'joe@localhost'
    }

    start = (
        utils.datetime_to_timestamp(now - datetime.timedelta(days=1)) /
        1000000.)
    end = (
        utils.datetime_to_timestamp(now + datetime.timedelta(days=1)) /
        1000000.)
    self.set_as_privileged_user()
    return first, second, str_now_120, start, end

  def _gen_three_pending_tasks(self):
    # Creates three pending tasks, spaced 1 minute apart
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    self.mock(random, 'getrandbits', lambda _: 0x66)
    _, first_id = self.client_create_task_raw(
        name='first', tags=['project:yay', 'commit:abcd', 'os:Win'],
        pubsub_topic='projects/abc/topics/def',
        pubsub_userdata='1234',
        properties=dict(idempotent=True))

    now_60 = self.mock_now(now, 60)
    self.mock(random, 'getrandbits', lambda _: 0x88)
    _, second_id = self.client_create_task_raw(
        name='second', user='jack@localhost',
        pubsub_topic='projects/abc/topics/def',
        pubsub_userdata='5678',
        tags=['project:yay', 'commit:efgh', 'os:Win'],
        properties=dict(idempotent=True))

    now_120 = self.mock_now(now, 120)
    _, third_id = self.client_create_task_raw(
        name='third', user='jack@localhost',
        pubsub_topic='projects/abc/topics/def',
        pubsub_userdata='9000',
        tags=['project:yay', 'commit:ijkhl', 'os:Linux'],
        properties=dict(idempotent=True))

    return first_id, second_id, third_id, now, now_60, now_120


class TaskApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingTaskService

  def setUp(self):
    super(TaskApiTest, self).setUp()
    self.tasks_api = test_case.Endpoints(
        handlers_endpoints.SwarmingTasksService)

  def test_cancel_ok(self):
    """Asserts that task cancellation goes smoothly."""
    # catch PubSub notification
    # Create and cancel a task as a non-privileged user.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    _, task_id = self.client_create_task_raw(
        pubsub_topic='projects/abc/topics/def',
        pubsub_userdata='blah')
    expected = {u'ok': True, u'was_running': False}
    response = self.call_api('cancel', body={'task_id': task_id})
    self.assertEqual(expected, response.json)

    # determine that the task's state updates correctly
    expected = {
      u'abandoned_ts': str_now,
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'state': u'CANCELED',
      u'tags': [
        u'os:Amiga',
        u'pool:default',
        u'priority:100',
        u'service_account:none',
        u'user:joe@localhost',
      ],
      u'task_id': task_id,
      u'user': u'joe@localhost',
    }
    response = self.call_api('result', body={'task_id': task_id})
    self.assertEqual(expected, response.json)

    # notification has been sent.
    expected = [
      {
        'payload': '{"auth_token":null,"task_id":"5cee488008810",'
                   '"topic":"projects/abc/topics/def","userdata":"blah"}',
        'queue_name': 'pubsub',
        'transactional': True,
        'url': '/internal/taskqueue/pubsub/5cee488008810',
      },
    ]

  def test_cancel_forbidden(self):
    """Asserts that non-privileged non-owner can't cancel tasks."""
    # catch PubSub notification
    notifies = []
    def enqueue_task_mock(**kwargs):
      notifies.append(kwargs)
      return True
    self.mock(utils, 'enqueue_task', enqueue_task_mock)

    # Create a task as an admin.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    self.set_as_admin()
    _, task_id = self.client_create_task_raw(
        pubsub_topic='projects/abc/topics/def',
        pubsub_userdata='blah')

    # Attempt to cancel as non-privileged user -> HTTP 403.
    self.set_as_user()
    self.call_api('cancel', body={'task_id': task_id}, status=403)

  def test_task_canceled(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    _, task_id = self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    self.set_as_bot()
    params = self.do_handshake()
    data = self.post_json('/swarming/api/v1/bot/poll', params)
    run_id = data['manifest']['task_id']
    def _params(**kwargs):
      out = {
        'cost_usd': 0.1,
        'duration': None,
        'exit_code': None,
        'id': 'bot1',
        'output': None,
        'output_chunk_start': 0,
        'task_id': run_id,
      }
      out.update(**kwargs)
      return out

    def _expected(**kwargs):
      out = {
        u'bot_dimensions': [
          {u'key': u'id', u'value': [u'bot1']},
          {u'key': u'os', u'value': [u'Amiga']},
          {u'key': u'pool', u'value': [u'default']},
        ],
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'costs_usd': [0.1],
        u'created_ts': str_now,
        u'failure': False,
        u'internal_failure': False,
        u'modified_ts': str_now,
        u'name': u'hi',
        u'run_id': u'5cee488008811',
        u'server_versions': [u'v1a'],
        u'started_ts': str_now,
        u'state': u'RUNNING',
        u'tags': [
          u'os:Amiga',
          u'pool:default',
          u'priority:100',
          u'service_account:none',
          u'user:joe@localhost',
        ],
        u'task_id': task_id,
        u'try_number': u'1',
        u'user': u'joe@localhost',
      }
      out.update((unicode(k), v) for k, v in kwargs.iteritems())
      return out

    def _cycle(params, expected, must_stop):
      response = self.post_json('/swarming/api/v1/bot/task_update', params)
      self.assertEqual({u'must_stop': must_stop, u'ok': True}, response)
      self.assertEqual(expected, self.client_get_results(task_id))

    params = _params(output=base64.b64encode('Oh '))
    expected = _expected()
    _cycle(params, expected, False)

    # Canceling a running task is currently not supported.
    self.set_as_user()
    expected = {u'ok': False, u'was_running': True}
    response = self.call_api('cancel', body={'task_id': task_id})
    self.assertEqual(expected, response.json)

    self.set_as_bot()
    params = _params(output=base64.b64encode('hi'), output_chunk_start=3)
    expected = _expected()
    _cycle(params, expected, False)

  def test_result_unknown(self):
    """Asserts that result raises 404 for unknown task IDs."""
    self.call_api('result', body={'task_id': '12310'}, status=404)

  def test_result_ok(self):
    """Asserts that result produces a result entity."""
    self.mock(random, 'getrandbits', lambda _: 0x88)

    # pending task
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    _, task_id = self.client_create_task_raw()
    response = self.call_api('result', body={'task_id': task_id})
    expected = {
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'state': u'PENDING',
      u'tags': [
        u'os:Amiga',
        u'pool:default',
        u'priority:100',
        u'service_account:none',
        u'user:joe@localhost',
      ],
      u'task_id': u'5cee488008810',
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)

    # no bot started: running task
    run_id = task_id[:-1] + '1'
    self.call_api('result', body={'task_id': run_id}, status=404)

    # run as bot
    self.set_as_bot()
    self.bot_poll('bot1')

    self.set_as_user()
    response = self.call_api('result', body={'task_id': run_id})
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.0],
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'run_id': u'5cee488008811',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'RUNNING',
      u'task_id': u'5cee488008811',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response.json)

  def test_result_completed_task(self):
    """Tests that completed tasks are correctly reported."""
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    str_now = unicode(now.strftime(self.DATETIME_FORMAT))
    self.mock_now(now)
    self.client_create_task_raw()
    self.set_as_bot()
    task_id = self.bot_run_task()
    # First ask without perf metadata.
    self.set_as_user()
    response = self.call_api('result', body={'task_id': task_id})
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.1],
      u'created_ts': str_now,
      u'completed_ts': str_now,
      u'duration': 0.1,
      u'exit_code': u'0',
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'run_id': task_id[:-1] + '1',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'task_id': task_id,
      u'try_number': u'1',
    }
    self.assertEqual(expected, response.json)
    expected[u'performance_stats'] = {
      u'bot_overhead': 0.1,
      u'isolated_download': {
        u'duration': 1.0,
        u'initial_number_items': u'10',
        u'initial_size': u'100000',
        u'items_cold': [20],
        u'items_hot': [30],
      },
      u'isolated_upload': {
        u'duration': 2.0,
        u'items_cold': [40],
        u'items_hot': [50],
      },
    }
    response = self.call_api(
        'result',
        body={'task_id': task_id, 'include_performance_stats': True})
    actual = response.json
    for k in ('isolated_download', 'isolated_upload'):
      for j in ('items_cold', 'items_hot'):
        actual['performance_stats'][k][j] = large.unpack(
            base64.b64decode(actual['performance_stats'][k][j]))
    self.assertEqual(expected, actual)

  def test_stdout_ok(self):
    """Asserts that stdout reports a task's output."""
    self.client_create_task_raw()

    # task_id determined by bot run
    self.set_as_bot()
    task_id = self.bot_run_task()

    self.set_as_privileged_user()
    run_id = task_id[:-1] + '1'
    expected = {u'output': u'rÉsult string'}
    for i in (task_id, run_id):
      response = self.call_api('stdout', body={'task_id': i})
      self.assertEqual(expected, response.json)

  def test_stdout_empty(self):
    """Asserts that incipient tasks produce no output."""
    _, task_id = self.client_create_task_raw()
    response = self.call_api('stdout', body={'task_id': task_id})
    self.assertEqual({}, response.json)

    run_id = task_id[:-1] + '1'
    self.call_api('stdout', body={'task_id': run_id}, status=404)

  def test_result_run_not_found(self):
    """Asserts that getting results from incipient tasks raises 404."""
    _, task_id = self.client_create_task_raw()
    run_id = task_id[:-1] + '1'
    self.call_api('stdout', body={'task_id': run_id}, status=404)

  def test_task_deduped(self):
    """Asserts that task deduplication works as expected."""
    _, task_id_1 = self.client_create_task_raw(properties=dict(idempotent=True))

    self.set_as_bot()
    task_id_bot = self.bot_run_task()
    self.assertEqual(task_id_1, task_id_bot[:-1] + '0')
    self.assertEqual('1', task_id_bot[-1:])

    # second task; this one's results should be returned immediately
    self.set_as_user()
    _, task_id_2 = self.client_create_task_raw(
        name='second', user='jack@localhost', properties=dict(idempotent=True))

    self.set_as_bot()
    resp = self.bot_poll()
    self.assertEqual('sleep', resp['cmd'])

    self.set_as_user()

    # results shouldn't change, even if the second task wasn't executed
    response = self.call_api('stdout', body={'task_id': task_id_2})
    self.assertEqual({'output': u'rÉsult string'}, response.json)

  def test_request_unknown(self):
    """Asserts that 404 is raised for unknown tasks."""
    self.call_api('request', body={'task_id': '12310'}, status=404)

  def test_request_ok(self):
    """Asserts that request produces a task request."""
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    _, task_id = self.client_create_task_raw()
    response = self.call_api('request', body={'task_id': task_id})
    expected = {
      u'authenticated': u'user:user@example.com',
      u'created_ts': unicode(now.strftime(self.DATETIME_FORMAT)),
      u'expiration_secs': unicode(24 * 60 * 60),
      u'name': u'hi',
      u'priority': u'100',
      u'properties': {
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'version': u'git_revision:deadbeef',
          },
          u'packages': [{
            u'package_name': u'rm',
            u'path': u'bin',
            u'version': u'git_revision:deadbeef',
          }],
          u'server': u'https://chrome-infra-packages.appspot.com',
        },
        u'command': [u'python', u'run_test.py'],
        u'dimensions': [
          {u'key': u'os', u'value': u'Amiga'},
          {u'key': u'pool', u'value': u'default'},
        ],
        u'execution_timeout_secs': u'3600',
        u'grace_period_secs': u'30',
        u'idempotent': False,
        u'io_timeout_secs': u'1200',
        u'outputs': [u'foo', u'path/to/foobar'],
      },
      u'service_account': u'none',
      u'tags': [
        u'os:Amiga',
        u'pool:default',
        u'priority:100',
        u'service_account:none',
        u'user:joe@localhost',
      ],
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)


class BotsApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingBotsService

  def test_list_ok(self):
    """Asserts that BotInfo is returned for the appropriate set of bots."""
    self.set_as_privileged_user()
    then = datetime.datetime(2009, 1, 2, 3, 4, 5, 6)
    then_str = unicode(then.strftime(self.DATETIME_FORMAT))
    self.mock_now(then)
    # Add three bot events, corresponding to one dead bot, one quarantined bot,
    # and one good bot
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id3',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id3']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None,
        machine_type='mt')
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(self.DATETIME_FORMAT))
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, task_id=None, task_name=None)
    bot1 = {
      u'authenticated_as': u'bot:whitelisted-ip',
      u'bot_id': u'id1',
      u'deleted': False,
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id1']},
      ],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'is_dead': False,
      u'last_seen_ts': now_str,
      u'quarantined': False,
      u'state': u'{"ram":65}',
      u'version': u'123456789',
    }
    bot2 = {
      u'authenticated_as': u'bot:whitelisted-ip',
      u'bot_id': u'id2',
      u'deleted': False,
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id2']},
      ],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'is_dead': False,
      u'last_seen_ts': now_str,
      u'quarantined': True,
      u'state': u'{"ram":65}',
      u'version': u'123456789',
    }
    bot3 = {
      u'authenticated_as': u'bot:whitelisted-ip',
      u'bot_id': u'id3',
      u'deleted': False,
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id3']},
      ],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': then_str,
      u'is_dead': True,
      u'last_seen_ts': then_str,
      u'machine_type': u'mt',
      u'quarantined': False,
      u'state': u'{"ram":65}',
      u'version': u'123456789',
    }
    expected = {
      u'items': [bot1, bot2, bot3],
      u'death_timeout': unicode(config.settings().bot_death_timeout_secs),
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    # All bots should be returned with no params
    request = swarming_rpcs.BotsRequest()
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # All bots should be returned if we don't care about quarantined
    request = swarming_rpcs.BotsRequest(
        quarantined=swarming_rpcs.ThreeStateBool.NONE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # All bots should be returned if we don't care about is_dead
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.NONE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # Only bot1 corresponds to these two dimensions
    expected[u'items'] = [bot1]
    request = swarming_rpcs.BotsRequest(dimensions=['foo:bar', 'id:id1'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # Only bot1 corresponds to being not dead and not quarantined and
    # this dimension
    request = swarming_rpcs.BotsRequest(
      dimensions=['foo:bar'],
      quarantined=swarming_rpcs.ThreeStateBool.FALSE,
      is_dead=swarming_rpcs.ThreeStateBool.FALSE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # exclude bot2 only, which is quarantined
    expected[u'items'] = [bot1, bot3]
    request = swarming_rpcs.BotsRequest(
        quarantined=swarming_rpcs.ThreeStateBool.FALSE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # exclude bot3 only, which is dead
    expected[u'items'] = [bot1, bot2]
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.FALSE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # only bot2 is quarantined
    expected[u'items'] = [bot2]
    request = swarming_rpcs.BotsRequest(
        quarantined=swarming_rpcs.ThreeStateBool.TRUE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # quarantined:true can be paired with other dimensions and still work
    request = swarming_rpcs.BotsRequest(
        quarantined=swarming_rpcs.ThreeStateBool.TRUE, dimensions=['foo:bar'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # only bot3 is dead
    expected[u'items'] = [bot3]
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.TRUE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # is_dead:true can be paired with other dimensions and still work
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.TRUE, dimensions=['foo:bar'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # only 1 bot is "ready for work"
    expected[u'items'] = [bot1]
    request = swarming_rpcs.BotsRequest(
        is_busy=swarming_rpcs.ThreeStateBool.FALSE,
        is_dead=swarming_rpcs.ThreeStateBool.FALSE,
        quarantined=swarming_rpcs.ThreeStateBool.FALSE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # only bot3 is a machine provider bot
    expected[u'items'] = [bot3]
    request = swarming_rpcs.BotsRequest(is_mp=swarming_rpcs.ThreeStateBool.TRUE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    expected[u'items'] = [bot1, bot2]
    request = swarming_rpcs.BotsRequest(
        is_mp=swarming_rpcs.ThreeStateBool.FALSE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # not:existing is a dimension that doesn't exist, nothing returned.
    request = swarming_rpcs.BotsRequest(dimensions=['not:existing'])
    response = self.call_api('list', body=message_to_dict(request))
    del expected[u'items']
    self.assertEqual(expected, response.json)
    # quarantined:true can be paired with other non-existing dimensions and
    # still work
    request = swarming_rpcs.BotsRequest(
        quarantined=swarming_rpcs.ThreeStateBool.TRUE, dimensions=['not:exist'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # is_dead:true can be paired with other non-existing dimensions and
    # still work
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.TRUE, dimensions=['not:exist'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # is_mp:true can be paired with other non-existing dimensions and still work
    request = swarming_rpcs.BotsRequest(
        is_mp=swarming_rpcs.ThreeStateBool.TRUE, dimensions=['not:exist'])
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # No bot is both dead and quarantined
    request = swarming_rpcs.BotsRequest(
        is_dead=swarming_rpcs.ThreeStateBool.TRUE,
        quarantined=swarming_rpcs.ThreeStateBool.TRUE)
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)
    # A bad request returns 400
    request = swarming_rpcs.BotsRequest(dimensions=['bad'])
    self.call_api('list', body=message_to_dict(request), status=400)

  def test_count_ok(self):
    """Asserts that BotsCount is returned for the appropriate set of bots."""
    self.set_as_privileged_user()
    self.mock_now(datetime.datetime(2009, 1, 2, 3, 4, 5, 6))
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id3',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id3']}, state={'ram': 65},
        version='123456789', quarantined=True, task_id=None, task_name=None)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id='987', task_name=None)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, task_id=None, task_name=None)
    expected = {
      u'count': u'3',
      u'quarantined': u'2',
      u'dead': u'1',
      u'busy': u'1',
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    request = swarming_rpcs.BotsRequest()
    response = self.call_api('count', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

    expected = {
      u'count': u'1',
      u'quarantined': u'0',
      u'dead': u'0',
      u'busy': u'1',
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    request = swarming_rpcs.BotsCountRequest(dimensions=['foo:bar', 'id:id1'])
    response = self.call_api('count', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

    expected[u'quarantined'] = u'1'
    expected[u'busy'] = u'0'
    request = swarming_rpcs.BotsCountRequest(dimensions=['foo:bar', 'id:id2'])
    response = self.call_api('count', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

    expected[u'dead'] = u'1'
    request = swarming_rpcs.BotsCountRequest(dimensions=['foo:bar', 'id:id3'])
    response = self.call_api('count', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

    request = swarming_rpcs.BotsCountRequest(dimensions=['not:existing'])
    response = self.call_api('count', body=message_to_dict(request))
    expected = {
      u'count': u'0',
      u'quarantined': u'0',
      u'dead': u'0',
      u'busy': u'0',
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    self.assertEqual(expected, response.json)

    request = swarming_rpcs.BotsCountRequest(dimensions=['bad'])
    self.call_api('count', body=message_to_dict(request), status=400)

  def test_dimensions_ok(self):
    """Asserts that BotsDimensions is returned with the right data."""
    self.set_as_privileged_user()
    now = datetime.datetime(2009, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)

    bot_management.DimensionAggregation(
        key=bot_management.DimensionAggregation.KEY,
        dimensions=[
            bot_management.DimensionValues(
              dimension='foo', values=['alpha', 'beta']),
            bot_management.DimensionValues(
              dimension='bar', values=['gamma', 'delta', 'epsilon']),
        ],
        ts=now).put()

    expected = {
      u'bots_dimensions': [
        {
          u'key': 'foo',
          u'value': [u'alpha', u'beta'],
        },
        {
          u'key': 'bar',
          u'value': [u'gamma', u'delta', u'epsilon'],
        },
      ],
      u'ts': unicode(now.strftime(self.DATETIME_FORMAT)),
    }

    self.assertEqual(expected, self.call_api('dimensions', body={}).json)


class BotApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingBotService

  def test_get_ok(self):
    """Asserts that get shows the tasks a specific bot has executed."""
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    now_str = unicode(now.strftime(self.DATETIME_FORMAT))
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)

    expected = {
      u'authenticated_as': u'bot:whitelisted-ip',
      u'bot_id': u'id1',
      u'deleted': False,
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id1']},
      ],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'is_dead': False,
      u'last_seen_ts': now_str,
      u'quarantined': False,
      u'state': u'{"ram":65}',
      u'version': u'123456789',
    }
    response = self.call_api('get', body={'bot_id': 'id1'})
    self.assertEqual(expected, response.json)

  def test_get_no_bot(self):
    """Asserts that get raises 404 when no bot is found."""
    self.set_as_admin()
    self.call_api('get', body={'bot_id': 'not_a_bot'}, status=404)

  def test_delete_ok(self):
    """Assert that delete finds and deletes a bot."""
    self.set_as_admin()
    self.mock(acl, 'is_admin', lambda *_args, **_kwargs: True)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    state = {
      'dict': {'random': 'values'},
      'float': 0.,
      'list': ['of', 'things'],
      'str': u'uni',
    }
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1',
        external_ip='8.8.4.4', authenticated_as='bot:whitelisted-ip',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state=state,
        version='123456789', quarantined=False, task_id=None, task_name=None)

    # delete the bot
    response = self.call_api('delete', body={'bot_id': 'id1'})
    self.assertEqual({u'deleted': True}, response.json)

    # is it gone?
    self.call_api('delete', body={'bot_id': 'id1'}, status=404)

  def test_tasks_ok(self):
    """Asserts that tasks produces bot information."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)

    self.set_as_bot()
    self.client_create_task_raw()
    res = self.bot_poll()
    self.bot_complete_task(task_id=res['manifest']['task_id'])

    now_1 = self.mock_now(now, 1)
    now_1_str = unicode(now_1.strftime(self.DATETIME_FORMAT))
    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task_raw(name='philbert')
    res = self.bot_poll()
    self.bot_complete_task(exit_code=1, task_id=res['manifest']['task_id'])

    start = (
        utils.datetime_to_timestamp(now + datetime.timedelta(seconds=0.5)) /
        1000000.)
    end = (
        utils.datetime_to_timestamp(now_1 + datetime.timedelta(seconds=0.5)) /
        1000000.)

    self.set_as_privileged_user()
    request = swarming_rpcs.BotTasksRequest(
        end=end, start=start, include_performance_stats=True)
    body = message_to_dict(request)
    body['bot_id'] = 'bot1'
    response = self.call_api('tasks', body=body)
    expected = {
      u'items': [
        {
          u'bot_dimensions': [
            {u'key': u'id', u'value': [u'bot1']},
            {u'key': u'os', u'value': [u'Amiga']},
            {u'key': u'pool', u'value': [u'default']},
          ],
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': now_1_str,
          u'costs_usd': [0.1],
          u'created_ts': now_1_str,
          u'duration': 0.1,
          u'exit_code': u'1',
          u'failure': True,
          u'internal_failure': False,
          u'modified_ts': now_1_str,
          u'name': u'philbert',
          u'performance_stats': {
            u'bot_overhead': 0.1,
            u'isolated_download': {
              u'duration': 1.0,
              u'initial_number_items': u'10',
              u'initial_size': u'100000',
              u'items_cold': [20],
              u'items_hot': [30],
            },
            u'isolated_upload': {
              u'duration': 2.0,
              u'items_cold': [40],
              u'items_hot': [50],
            },
          },
          u'run_id': u'5cee870005511',
          u'server_versions': [u'v1a'],
          u'started_ts': now_1_str,
          u'state': u'COMPLETED',
          u'task_id': u'5cee870005511',
          u'try_number': u'1',
        },
      ],
      u'now': unicode(now_1.strftime(self.DATETIME_FORMAT)),
    }
    actual = response.json
    for k in ('isolated_download', 'isolated_upload'):
      for j in ('items_cold', 'items_hot'):
        actual['items'][0]['performance_stats'][k][j] = large.unpack(
            base64.b64decode(actual['items'][0]['performance_stats'][k][j]))
    self.assertEqual(expected, actual)

  def test_events(self):
    # Run one task, push an event manually.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))

    self.set_as_bot()
    self.client_create_task_raw()
    params = self.do_handshake()
    res = self.bot_poll()
    now_60 = self.mock_now(now, 60)
    str_now_60 = unicode(now_60.strftime(self.DATETIME_NO_MICRO))
    self.bot_complete_task(task_id=res['manifest']['task_id'])

    params['event'] = 'bot_rebooting'
    params['message'] = 'for the best'
    response = self.post_json('/swarming/api/v1/bot/event', params)
    self.assertEqual({}, response)

    start = utils.datetime_to_timestamp(now) / 1000000.
    end = utils.datetime_to_timestamp(now_60) / 1000000.
    self.set_as_privileged_user()
    body = message_to_dict(
        swarming_rpcs.BotEventsRequest(start=start, end=end+1))
    body['bot_id'] = 'bot1'
    response = self.call_api('events', body=body)
    dimensions = [
      {u'key': u'id', u'value': [u'bot1']},
      {u'key': u'os', u'value': [u'Amiga']},
      {u'key': u'pool', u'value': [u'default']},
    ]
    state_dict = {
      'bot_group_cfg_version': 'default',
      'running_time': 1234.,
      'sleep_streak': 0,
      'started_ts': 1410990411.111,
    }
    state = unicode(
        json.dumps(state_dict, sort_keys=True, separators=(',', ':')))
    state_dict.pop('bot_group_cfg_version')
    state_no_cfg_ver = unicode(
        json.dumps(state_dict, sort_keys=True, separators=(',', ':')))
    expected = {
      u'items': [
        {
          u'authenticated_as': u'bot:whitelisted-ip',
          u'dimensions': dimensions,
          u'event_type': u'bot_rebooting',
          u'external_ip': unicode(self.source_ip),
          u'message': u'for the best',
          u'quarantined': False,
          u'state': state,
          u'ts': str_now_60,
          u'version': unicode(self.bot_version),
        },
        {
          u'authenticated_as': u'bot:whitelisted-ip',
          u'dimensions': dimensions,
          u'event_type': u'task_completed',
          u'external_ip': unicode(self.source_ip),
          u'quarantined': False,
          u'state': state,
          u'task_id': u'5cee488008811',
          u'ts': str_now_60,
          u'version': unicode(self.bot_version),
        },
        {
          u'authenticated_as': u'bot:whitelisted-ip',
          u'dimensions': dimensions,
          u'event_type': u'request_task',
          u'external_ip': unicode(self.source_ip),
          u'quarantined': False,
          u'state': state,
          u'task_id': u'5cee488008811',
          u'ts': str_now,
          u'version': unicode(self.bot_version),
        },
        {
          u'authenticated_as': u'bot:whitelisted-ip',
          u'dimensions': dimensions,
          u'event_type': u'bot_connected',
          u'external_ip': unicode(self.source_ip),
          u'quarantined': False,
          u'state': state_no_cfg_ver,
          u'ts': str_now,
          u'version': u'123',
        },
        {
          u'authenticated_as': u'bot:whitelisted-ip',
          u'dimensions': dimensions,
          u'event_type': u'bot_connected',
          u'external_ip': unicode(self.source_ip),
          u'quarantined': False,
          u'state': state_no_cfg_ver,
          u'ts': str_now,
          u'version': u'123',
        },
      ],
        u'now': str_now_60,
    }
    self.assertEqual(expected, response.json)

    # Now test with a subset.
    body = message_to_dict(swarming_rpcs.BotEventsRequest(start=end, end=end+1))
    body['bot_id'] = 'bot1'
    response = self.call_api('events', body=body)
    expected['items'] = expected['items'][:-3]
    self.assertEqual(expected, response.json)

  def test_terminate_admin(self):
    self.set_as_bot()
    self.bot_poll()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))

    self.set_as_admin()
    response = self.call_api('terminate', body={'bot_id': 'bot1'})
    self.assertEqual({u'task_id': u'5cee488008810'}, response.json)

  def test_terminate_privileged_user(self):
    self.set_as_bot()
    self.bot_poll()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))

    self.set_as_privileged_user()
    response = self.call_api('terminate', body={'bot_id': 'bot1'})
    self.assertEqual({u'task_id': u'5cee488008810'}, response.json)

  def test_terminate_user(self):
    self.set_as_bot()
    self.bot_poll()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))

    self.set_as_user()
    self.call_api('terminate', body={'bot_id': 'bot1'}, status=403)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.CRITICAL)
  unittest.main()
