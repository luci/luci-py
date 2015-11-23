#!/usr/bin/env python
# coding=utf-8
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import random
import sys
import unittest

import test_env_handlers
from test_support import test_case

from protorpc.remote import protojson
import webapp2
import webtest

from components import auth
from components import ereporter2
from components import utils

import handlers_bot
import handlers_endpoints
import swarming_rpcs

from server import acl
from server import bot_management
from server import config
from server import task_pack


def message_to_dict(rpc_message):
  return json.loads(protojson.encode_message(rpc_message))


class BaseTest(test_env_handlers.AppTestBase, test_case.EndpointsTestCase):

  DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
  DATETIME_NO_MICRO = '%Y-%m-%dT%H:%M:%S'

  def setUp(self):
    test_case.EndpointsTestCase.setUp(self)
    super(BaseTest, self).setUp()
    self.mock(auth, 'is_group_member', lambda *_args, **_kwargs: True)
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


class ServerApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingServerService

  def test_details(self):
    """Asserts that server_details returns the correct version."""
    response = self.call_api('details')
    self.assertEqual({'server_version': utils.get_app_version()}, response.json)

  def _test_file(self, name):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    path = os.path.join(self.APP_DIR, 'swarming_bot', 'config', name + '.py')
    with open(path, 'rb') as f:
      content = f.read().decode('utf-8')

    expected = {
      u'content': content,
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    expected = {
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:user@example.com',
    }
    response = self.call_api('put_' + name, {'content': u'hi ☀!'})
    self.assertEqual(expected, response.json)

    expected = {
      u'content': u'hi \u2600!',
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:user@example.com',
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    self.mock_now(now, 60)
    expected = {
      u'version': u'1',
      u'when': u'2010-01-02T03:05:05',
      u'who': u'user:user@example.com',
    }
    response = self.call_api('put_' + name, {'content': u'hi ♕!'})
    self.assertEqual(expected, response.json)

    expected = {
      u'content': u'hi ♕!',
      u'version': u'1',
      u'when': u'2010-01-02T03:05:05',
      u'who': u'user:user@example.com',
    }
    self.assertEqual(expected, self.call_api('get_' + name).json)

    expected = {
      u'content': u'hi ☀!',
      u'version': u'0',
      u'when': u'2010-01-02T03:04:05',
      u'who': u'user:user@example.com',
    }
    response = self.call_api('get_' + name, {'version': '0'})
    self.assertEqual(expected, response.json)

  def test_bootstrap(self):
    self._test_file('bootstrap')

  def test_bot_config(self):
    self._test_file('bot_config')


class TasksApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingTasksService

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
            command=['rm', '-rf', '/'],
            dimensions=[
              swarming_rpcs.StringPair(key='a', value='b'),
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
          u'command': [u'rm', u'-rf', u'/'],
          u'dimensions': [
            {u'key': u'a', u'value': u'b'},
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
        u'tags': [
          u'a:b',
          u'foo:bar',
          u'priority:200',
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
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5))
    self.client_create_task_raw(
        name='task', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    self.mock(random, 'getrandbits', lambda _: 0x66)
    now = datetime.datetime(2010, 1, 2, 5, 5, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.set_as_user()

    request = swarming_rpcs.NewTaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperties(
            command=['python', 'run_test.py'],
            dimensions=[
              swarming_rpcs.StringPair(key='os', value='Amiga'),
            ],
            execution_timeout_secs=3600,
            io_timeout_secs=1200,
            idempotent=True),
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
          u'command': [u'python', u'run_test.py'],
          u'dimensions': [
            {u'key': u'os', u'value': u'Amiga'},
          ],
          u'execution_timeout_secs': u'3600',
          u'grace_period_secs': u'30',
          u'idempotent': True,
          u'io_timeout_secs': u'1200',
        },
        u'tags': [
          u'foo:bar',
          u'os:Amiga',
          u'priority:200',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'63dabe8006610',
      u'task_result': {
        u'bot_dimensions': [
          {u'key': u'id', u'value': [u'bot1']},
          {u'key': u'os', u'value': [u'Amiga']},
        ],
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'completed_ts': u'2010-01-02T03:04:05',
        u'cost_saved_usd': 0.1,
        u'created_ts': u'2010-01-02T05:05:05',
        u'deduped_from': u'5cee488008811',
        u'duration': 0.1,
        u'exit_code': u'0',
        u'failure': False,
        u'internal_failure': False,
        u'modified_ts': u'2010-01-02T05:05:05',
        u'name': u'job1',
        u'server_versions': [u'v1a'],
        u'started_ts': u'2010-01-02T03:04:05',
        u'state': u'COMPLETED',
        u'tags': [
          u'foo:bar',
          u'os:Amiga',
          u'priority:200',
          u'user:joe@localhost',
        ],
        u'task_id': u'63dabe8006610',
        u'try_number': u'0',
        u'user': u'joe@localhost',
      },
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

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
              swarming_rpcs.StringPair(key='foo', value='bar'),
              swarming_rpcs.StringPair(key='a', value='b'),
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
            {u'key': u'a', u'value': u'b'},
            {u'key': u'foo', u'value': u'bar'},
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
        u'tags': [
          u'a:b',
          u'foo:bar',
          u'priority:200',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', body=message_to_dict(request))
    self.assertEqual(expected, response.json)

  def test_list_ok(self):
    """Asserts that list requests all TaskResultSummaries."""

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

    second = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
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
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'tags': [
        u'commit:pre',
        u'os:Amiga',
        u'os:Win',
        u'priority:10',
        u'project:yay',
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
      u'modified_ts': str_now_120,
      u'name': u'first',
      u'properties_hash': u'8771754ee465a689f19c87f2d21ea0d9b8dd4f64',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'tags': [
        u'commit:post',
        u'os:Amiga',
        u'os:Win',
        u'priority:10',
        u'project:yay',
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

    # Basic request.
    request = swarming_rpcs.TasksRequest(end=end, start=start)
    self.assertEqual(
        {u'now': str_now_120, u'items': [second, first]},
        self.call_api('list', body=message_to_dict(request)).json)

    # Sort by CREATED_TS.
    request = swarming_rpcs.TasksRequest(
        sort=swarming_rpcs.TaskSort.CREATED_TS)
    self.assertEqual(
        {u'now': str_now_120, u'items': [second, first]},
        self.call_api('list', body=message_to_dict(request)).json)

    # Sort by MODIFIED_TS.
    request = swarming_rpcs.TasksRequest(
        sort=swarming_rpcs.TaskSort.MODIFIED_TS)
    self.assertEqual(
        {u'now': str_now_120, u'items': [first, second]},
        self.call_api('list', body=message_to_dict(request)).json)

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
    with self.call_should_fail('400'):
      self.call_api('list', body=message_to_dict(request))


class TaskApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingTaskService

  def setUp(self):
    super(TaskApiTest, self).setUp()
    self.tasks_api = test_case.Endpoints(
        handlers_endpoints.SwarmingTasksService)

  def test_cancel_ok(self):
    """Asserts that task cancellation goes smoothly."""
    # catch PubSub notification
    notifies = []
    def enqueue_task_mock(**kwargs):
      notifies.append(kwargs)
      return True
    self.mock(utils, 'enqueue_task', enqueue_task_mock)

    # create and cancel a task
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.set_as_admin()
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
      u'tags': [u'os:Amiga', u'priority:10', u'user:joe@localhost'],
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
    self.assertEqual(expected, notifies)

  def test_result_unknown(self):
    """Asserts that result raises 404 for unknown task IDs."""
    with self.call_should_fail('404'):
      _ = self.call_api('result', body={'task_id': '12300'})

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
      u'tags': [u'os:Amiga', u'priority:10', u'user:joe@localhost'],
      u'task_id': u'5cee488008810',
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)

    # no bot started: running task
    run_id = task_id[:-1] + '1'
    with self.call_should_fail('404'):
      _ = self.call_api('result', body={'task_id': run_id})

    # run as bot
    self.set_as_bot()
    self.bot_poll('bot1')
    self.set_as_user()
    response = self.call_api('result', body={'task_id': run_id})
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.0],
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
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
    response = self.call_api('result', body={'task_id': task_id})
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
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
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'task_id': task_id,
      u'try_number': u'1',
    }
    self.assertEqual(expected, response.json)

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
    with self.call_should_fail('404'):
      _ = self.call_api('stdout', body={'task_id': run_id})

  def test_result_run_not_found(self):
    """Asserts that getting results from incipient tasks raises 404."""
    _, task_id = self.client_create_task_raw()
    run_id = task_id[:-1] + '1'
    with self.call_should_fail('404'):
      _ = self.call_api('stdout', body={'task_id': run_id})

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
    with self.call_should_fail('404'):
      _ = self.call_api('request', body={'task_id': '12300'})

  def test_request_ok(self):
    """Asserts that request produces a task request."""
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    _, task_id = self.client_create_task_raw()
    self.set_as_bot()
    response = self.call_api('request', body={'task_id': task_id})
    expected = {
      u'authenticated': u'user:user@example.com',
      u'created_ts': unicode(now.strftime(self.DATETIME_FORMAT)),
      u'expiration_secs': unicode(24 * 60 * 60),
      u'name': u'hi',
      u'priority': u'10',
      u'properties': {
        u'command': [u'python', u'run_test.py'],
        u'dimensions': [{u'key': u'os', u'value': u'Amiga'},],
        u'execution_timeout_secs': u'3600',
        u'grace_period_secs': u'30',
        u'idempotent': False,
        u'io_timeout_secs': u'1200',
      },
      u'tags': [u'os:Amiga', u'priority:10', u'user:joe@localhost'],
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)


class BotsApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingBotsService

  def test_list_ok(self):
    """Asserts that BotInfo is returned for the appropriate set of bots."""
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(self.DATETIME_FORMAT))
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)
    expected = {
      u'items': [
        {
          u'bot_id': u'id1',
          u'dimensions': [
            {u'key': u'foo', u'value': [u'bar']},
            {u'key': u'id', u'value': [u'id1']},
          ],
          u'external_ip': u'8.8.4.4',
          u'first_seen_ts': now_str,
          u'is_dead': False,
          u'last_seen_ts': now_str,
          u'quarantined': False,
          u'version': u'123456789',
        },
      ],
      u'death_timeout': unicode(config.settings().bot_death_timeout_secs),
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    request = swarming_rpcs.BotsRequest()
    response = self.call_api('list', body=message_to_dict(request))
    self.assertEqual(expected, response.json)


class BotApiTest(BaseTest):
  api_service_cls = handlers_endpoints.SwarmingBotService

  def test_get_ok(self):
    """Asserts that get shows the tasks a specific bot has executed."""
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    now_str = unicode(now.strftime(self.DATETIME_FORMAT))
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)

    expected = {
      u'bot_id': u'id1',
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id1']},
      ],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'is_dead': False,
      u'last_seen_ts': now_str,
      u'quarantined': False,
      u'version': u'123456789',
    }
    response = self.call_api('get', body={'bot_id': 'id1'})
    self.assertEqual(expected, response.json)

  def test_get_no_bot(self):
    """Asserts that get raises 404 when no bot is found."""
    with self.call_should_fail('404'):
      self.call_api('get', body={'bot_id': 'not_a_bot'})

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
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state=state,
        version='123456789', quarantined=False, task_id=None, task_name=None)

    # delete the bot
    response = self.call_api('delete', body={'bot_id': 'id1'})
    self.assertEqual({u'deleted': True}, response.json)

    # is it gone?
    with self.call_should_fail('404'):
      self.call_api('delete', body={'bot_id': 'id1'})

  def test_tasks_ok(self):
    """Asserts that tasks produces bot information."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)

    self.set_as_bot()
    self.client_create_task_raw()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(token, task_id=res['manifest']['task_id'])

    now_1 = self.mock_now(now, 1)
    now_1_str = unicode(now_1.strftime(self.DATETIME_FORMAT))
    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task_raw(name='philbert')
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(
        token, exit_code=1, task_id=res['manifest']['task_id'])

    start = (
        utils.datetime_to_timestamp(now + datetime.timedelta(seconds=0.5)) /
        1000000.)
    end = (
        utils.datetime_to_timestamp(now_1 + datetime.timedelta(seconds=0.5)) /
        1000000.)
    request = swarming_rpcs.BotTasksRequest(end=end, start=start)

    self.set_as_privileged_user()
    body = message_to_dict(request)
    body['bot_id'] = 'bot1'
    response = self.call_api('tasks', body=body)
    expected = {
      u'items': [
        {
          u'bot_dimensions': [
            {u'key': u'id', u'value': [u'bot1']},
            {u'key': u'os', u'value': [u'Amiga']},
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
          u'server_versions': [u'v1a'],
          u'started_ts': now_1_str,
          u'state': u'COMPLETED',
          u'task_id': u'5cee870005511',
          u'try_number': u'1',
        },
      ],
      u'now': unicode(now_1.strftime(self.DATETIME_FORMAT)),
    }
    json_version = response.json
    self.assertEqual(expected, json_version)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.CRITICAL)
  unittest.main()
