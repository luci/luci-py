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

import handlers_api
import handlers_bot
import handlers_endpoints
import swarming_rpcs

from server import acl
from server import bot_management
from server import config


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
    # TODO(maruel): Get rid of handlers_api.get_routes() here. The API should be
    # self-sufficient.
    routes = handlers_bot.get_routes() + handlers_api.get_routes()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(routes, debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()


class TaskApiTest(BaseTest):

  api_service_cls = handlers_endpoints.SwarmingTaskService

  def test_server_details_ok(self):
    """Asserts that server_details returns the correct version."""
    response = self.call_api('server_details', {}, 200)
    self.assertEqual({'server_version': utils.get_app_version()}, response.json)

  def test_new_ok(self):
    """Asserts that new generates appropriate metadata."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    request = swarming_rpcs.TaskRequest(
        expiration_secs=30,
        name='job1',
        priority=200,
        properties=swarming_rpcs.TaskProperty(
            command=['rm', '-rf', '/'],
            data=[swarming_rpcs.StringPair(key='foo', value='bar')],
            dimensions=[],
            env=[],
            execution_timeout_secs=30,
            io_timeout_secs=30),
        tag=['foo:bar'],
        user='joe@localhost')
    expected = {
      u'request': {
        u'authenticated': u'anonymous=anonymous',
        u'created_ts': str_now,
        u'expiration_secs': u'30',
        u'name': u'job1',
        u'priority': u'200',
        u'properties': {
          u'command': [u'rm', u'-rf', u'/'],
          u'data': [{u'key': u'foo', u'value': u'bar'}],
          u'execution_timeout_secs': u'30',
          u'grace_period_secs': u'30',
          u'idempotent': False,
          u'io_timeout_secs': u'30',
        },
        u'tag': [
          u'foo:bar',
          u'priority:200',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    response = self.call_api('new', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

  def test_cancel_ok(self):
    """Asserts that task cancellation goes smoothly."""
    # create and cancel a task
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.set_as_admin()
    _, task_id = self.client_create_task()
    request = swarming_rpcs.TaskId(task_id=task_id)
    expected = {u'ok': True, u'was_running': False}
    response = self.call_api('cancel', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

    # determine that the task's state updates correctly
    request = swarming_rpcs.TaskId(task_id=task_id)
    expected = {
      u'abandoned_ts': str_now,
      u'created_ts': str_now,
      u'failure': False,
      u'id': task_id,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'state': u'CANCELED',
      u'user': u'joe@localhost',
    }
    response = self.call_api('result', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

  def test_result_unknown(self):
    """Asserts that result raises 404 for unknown task IDs."""
    request = swarming_rpcs.TaskId(task_id='12300')
    with self.call_should_fail('404'):
      _ = self.call_api('result', message_to_dict(request), 200)

  def test_result_ok(self):
    """Asserts that result produces a result entity."""
    self.mock(random, 'getrandbits', lambda _: 0x88)

    # pending task
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    _, task_id = self.client_create_task()
    request = swarming_rpcs.TaskId(task_id=task_id)
    response = self.call_api('result', message_to_dict(request), 200)
    expected = {
      u'created_ts': str_now,
      u'failure': False,
      u'id': u'5cee488008810',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'state': u'PENDING',
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)

    # no bot started: running task
    run_id = task_id[:-1] + '1'
    request = swarming_rpcs.TaskId(task_id=run_id)
    with self.call_should_fail('404'):
      _ = self.call_api('result', message_to_dict(request), 200)

    # run as bot
    self.set_as_bot()
    self.bot_poll('bot1')
    self.set_as_user()
    response = self.call_api('result', message_to_dict(request), 200)
    expected = {
      u'bot_id': u'bot1',
      u'failure': False,
      u'id': u'5cee488008811',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'started_ts': str_now,
      u'state': u'RUNNING',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response.json)

  def test_result_completed_task(self):
    """Tests that completed tasks are correctly reported."""
    self.client_create_task()
    self.set_as_bot()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.mock_now(now)
    task_id = self.bot_run_task()
    request = swarming_rpcs.TaskId(task_id=task_id)
    response = self.call_api('result', message_to_dict(request), 200)
    expected = {
      u'bot_id': u'bot1',
      u'completed_ts': str_now,
      u'duration': 0.1,
      u'exit_code': u'0',
      u'failure': False,
      u'id': task_id,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response.json)

  def test_result_output_ok(self):
    """Asserts that result_output reports a task's output."""
    self.client_create_task()

    # task_id determined by bot run
    self.set_as_bot()
    task_id = self.bot_run_task()

    self.set_as_privileged_user()
    run_id = task_id[:-1] + '1'
    requests = [swarming_rpcs.TaskId(task_id=ident)
                for ident in [task_id, run_id]]
    expected = {u'output': u'rÉsult string'}
    for request in requests:
      response = self.call_api(
          'result_output', message_to_dict(request), 200)
      self.assertEqual(expected, response.json)

  def test_result_output_empty(self):
    """Asserts that incipient tasks produce no output."""
    _, task_id = self.client_create_task()
    request = swarming_rpcs.TaskId(task_id=task_id)
    response = self.call_api(
        'result_output', message_to_dict(request), 200)
    self.assertEqual({}, response.json)

    run_id = task_id[:-1] + '1'
    with self.call_should_fail('404'):
      _ = self.call_api('result_output', {'task_id': run_id}, 200)

  def test_result_run_not_found(self):
    """Asserts that getting results from incipient tasks raises 404."""
    _, task_id = self.client_create_task()
    run_id = task_id[:-1] + '1'
    request = swarming_rpcs.TaskId(task_id=run_id)
    with self.call_should_fail('404'):
      _ = self.call_api(
          'result_output', message_to_dict(request), 200)

  def test_task_deduped(self):
    """Asserts that task deduplication works as expected."""
    _, task_id_1 = self.client_create_task(properties=dict(idempotent=True))

    self.set_as_bot()
    task_id_bot = self.bot_run_task()
    self.assertEqual(task_id_1, task_id_bot[:-1] + '0')
    self.assertEqual('1', task_id_bot[-1:])

    # second task; this one's results should be returned immediately
    self.set_as_user()
    _, task_id_2 = self.client_create_task(
        name='second', user='jack@localhost', properties=dict(idempotent=True))

    self.set_as_bot()
    resp = self.bot_poll()
    self.assertEqual('sleep', resp['cmd'])

    self.set_as_user()

    # results shouldn't change, even if the second task wasn't executed
    response = self.call_api(
        'result_output', {'task_id': task_id_2}, 200)
    self.assertEqual({'output': u'rÉsult string'}, response.json)

  def test_request_unknown(self):
    """Asserts that 404 is raised for unknown tasks."""
    request = swarming_rpcs.TaskId(task_id='12300')
    with self.call_should_fail('404'):
      _ = self.call_api('request', message_to_dict(request), 200)

  def test_request_ok(self):
    """Asserts that request produces a task request."""
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    _, task_id = self.client_create_task()
    self.set_as_bot()
    request = swarming_rpcs.TaskId(task_id=task_id)
    response = self.call_api('request', message_to_dict(request), 200)
    expected = {
      u'authenticated': u'user=user@example.com',
      u'created_ts': unicode(now.strftime(self.DATETIME_NO_MICRO)),
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
      u'tag': [u'os:Amiga', u'priority:10', u'user:joe@localhost'],
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response.json)

  def test_list_ok(self):
    """Asserts that list requests all TaskResultSummaries."""

    # first request
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    str_now = unicode(now.strftime(self.DATETIME_NO_MICRO))
    self.mock_now(now)
    self.mock(random, 'getrandbits', lambda _: 0x66)
    self.client_create_task(
        name='first', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    # second request
    self.set_as_user()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now_60 = self.mock_now(now, 60)
    str_now_60 = unicode(now_60.strftime(self.DATETIME_NO_MICRO))
    self.client_create_task(
        name='second', user='jack@localhost',
        tags=['project:yay', 'commit:pre', 'os:Win'],
        properties=dict(idempotent=True))

    start = now - datetime.timedelta(days=1)
    end = now + datetime.timedelta(days=1)
    self.set_as_privileged_user()
    request = swarming_rpcs.TasksRequest(end=end, start=start)
    response = self.call_api('list', message_to_dict(request), 200)

    # we expect [second, first]
    task_results = [
      {
        u'bot_id': u'bot1',
        u'cost_saved_usd': 0.1,
        u'created_ts': str_now_60,
        u'completed_ts': str_now,
        u'deduped_from': u'5cee488006611',
        u'duration': 0.1,
        u'exit_code': u'0',
        u'failure': False,
        u'id': u'5cfcee8008810',
        u'internal_failure': False,
        u'modified_ts': str_now_60,
        u'name': u'second',
        u'started_ts': str_now,
        u'state': u'COMPLETED',
        u'try_number': u'0',
        u'user': u'jack@localhost'},
      {
        u'bot_id': u'bot1',
        u'costs_usd': [0.1],
        u'created_ts': str_now,
        u'completed_ts': str_now,
        u'duration': 0.1,
        u'exit_code': u'0',
        u'failure': False,
        u'id': u'5cee488006610',
        u'internal_failure': False,
        u'modified_ts': str_now,
        u'name': u'first',
        u'started_ts': str_now,
        u'state': u'COMPLETED',
        u'try_number': u'1',
        u'user': u'joe@localhost'}]
    second, _ = task_results
    expected = {
      u'items': task_results}

    self.assertEqual(expected, response.json)

    # try with a tag: should still only contain second
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, tag=['project:yay', 'commit:pre'])
    response = self.call_api('list', message_to_dict(request), 200)
    self.assertEqual({u'items': [second]}, response.json)

    # try with a spurious tag: items should be empty
    request = swarming_rpcs.TasksRequest(end=end, start=start, tag=['foo:bar'])
    expected.pop('items')
    response = self.call_api('list', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

    # try with multiple sort/filter options
    request = swarming_rpcs.TasksRequest(
        end=end, start=start, tag=['foo:bar'],
        state=swarming_rpcs.TaskState.PENDING)
    with self.call_should_fail('400'):
      self.call_api('list', message_to_dict(request), 200)


class BotApiTest(BaseTest):

  api_service_cls = handlers_endpoints.SwarmingBotService

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
          u'dimensions': [
            {u'key': u'foo', u'value': [u'bar']},
            {u'key': u'id', u'value': [u'id1']}],
          u'external_ip': u'8.8.4.4',
          u'first_seen_ts': now_str,
          u'id': u'id1',
          u'last_seen_ts': now_str,
          u'quarantined': False,
          u'version': u'123456789',
        },
      ],
      u'death_timeout': unicode(config.settings().bot_death_timeout_secs),
      u'now': unicode(now.strftime(self.DATETIME_FORMAT)),
    }
    request = swarming_rpcs.BotsRequest()
    response = self.call_api('list', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

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
      u'dimensions': [
        {u'key': u'foo', u'value': [u'bar']},
        {u'key': u'id', u'value': [u'id1']}],
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'id': u'id1',
      u'last_seen_ts': now_str,
      u'quarantined': False,
      u'version': u'123456789',
    }
    request = swarming_rpcs.BotId(bot_id='id1')
    response = self.call_api('get', message_to_dict(request), 200)
    self.assertEqual(expected, response.json)

  def test_get_no_bot(self):
    """Asserts that get raises 404 when no bot is found."""
    with self.call_should_fail('404'):
      self.call_api('get', {'bot_id': 'not_a_bot'}, 200)

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
    response = self.call_api('delete', {'bot_id': 'id1'}, 200)
    self.assertEqual({u'deleted': True}, response.json)

    # is it gone?
    with self.call_should_fail('404'):
      self.call_api('delete', {'bot_id': 'id1'}, 200)

  def test_tasks_ok(self):
    """Asserts that tasks produces bot information."""
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)

    self.set_as_bot()
    self.client_create_task()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(token, task_id=res['manifest']['task_id'])

    now_1 = self.mock_now(now, 1)
    now_1_str = unicode(now_1.strftime(self.DATETIME_NO_MICRO))
    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task(name='philbert')
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(
        token, exit_code=1, task_id=res['manifest']['task_id'])

    start = now + datetime.timedelta(seconds=0.5)
    end = now_1 + datetime.timedelta(seconds=0.5)
    request = swarming_rpcs.BotTasksRequest(bot_id='bot1', end=end, start=start)

    self.set_as_privileged_user()
    response = self.call_api('tasks', message_to_dict(request), 200)
    expected = {
      u'items': [{
        u'bot_id': u'bot1',
        u'completed_ts': now_1_str,
        u'cost_usd': 0.1,
        u'duration': 0.1,
        u'exit_code': u'1',
        u'failure': True,
        u'id': u'5cee870005511',
        u'internal_failure': False,
        u'modified_ts': now_1_str,
        u'started_ts': now_1_str,
        u'state': u'COMPLETED',
        u'try_number': u'1'}],
      u'now': unicode(now_1.strftime(self.DATETIME_FORMAT)),
    }
    json_version = response.json
    self.assertEqual(expected, json_version)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
