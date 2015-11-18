#!/usr/bin/env python
# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import datetime
import logging
import os
import random
import sys
import unittest

# Setups environment.
import test_env_handlers

import webapp2
import webtest

import handlers_api
import handlers_bot
from components import ereporter2
from components import utils
from server import config
from server import bot_code
from server import bot_management
from server import task_result


class ClientApiTest(test_env_handlers.AppTestBase):
  def setUp(self):
    super(ClientApiTest, self).setUp()
    # By default requests in tests are coming from bot with fake IP.
    routes = handlers_bot.get_routes() + handlers_api.get_routes()
    app = webapp2.WSGIApplication(routes, debug=True)
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()

  def get_client_token(self):
    """Gets the XSRF token for client after handshake."""
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers,
        params=params).json
    return response['xsrf_token'].encode('ascii')

  def test_list(self):
    self.set_as_anonymous()
    response = self.app.get('/swarming/api/v1/client/list').json
    expected = {
      u'bot/<bot_id:[^/]+>': u'Bot\'s meta data',
      u'bot/<bot_id:[^/]+>/tasks': u'Tasks executed on a specific bot',
      u'bots': u'Bots known to the server',
      u'list': u'All query handlers',
      u'server': u'Server details',
      u'task/<task_id:[0-9a-f]+>': u'Task\'s result meta data',
      u'task/<task_id:[0-9a-f]+>/output/<command_index:[0-9]+>':
          u'Task\'s output for a single command',
      u'task/<task_id:[0-9a-f]+>/output/all':
          u'All output from all commands in a task',
      u'task/<task_id:[0-9a-f]+>/request': u'Task\'s request details',
      u'tasks': handlers_api.process_doc(handlers_api.ClientApiTasksHandler),
      u'tasks/count': handlers_api.process_doc(
          handlers_api.ClientApiTasksCountHandler),
    }
    self.assertEqual(expected, response)

  def test_handshake(self):
    # Bare minimum:
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers, params=params).json
    self.assertEqual(
        [u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(u'v1a', response['server_version'])

  def test_handshake_extra(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('client', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      # Works with unknown items but logs an error. This permits catching typos.
      'foo': 1,
    }
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers, params=params).json
    self.assertEqual(
        [u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(u'v1a', response['server_version'])
    expected = [
      'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_request_invalid(self):
    record = []
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: record.append((args, kwargs)))
    headers = {'X-XSRF-Token-Request': '1'}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake', headers=headers, params={}).json
    params = {
      'foo': 'bar',
      'properties': {},
      'scheduling_expiration_secs': 30,
      'tags': [],
    }
    headers = {'X-XSRF-Token': str(response['xsrf_token'])}
    response = self.app.post_json(
        '/swarming/api/v1/client/request',
        headers=headers, params=params, status=400).json
    expected = {
      u'error':
        u'Unexpected request keys missing: '
          u'[\'name\', \'priority\', \'user\'] superfluous: [u\'foo\']; '
          u'did you make a typo?',
    }
    self.assertEqual(expected, response)

  def test_request_invalid_lower_level(self):
    headers = {'X-XSRF-Token-Request': '1'}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake', headers=headers, params={}).json
    params = {
      'name': 'job1',
      'priority': 200,
      'properties': {
        'commands': [],
        'data': [],
        'dimensions': {},
        'env': {},
        'execution_timeout_secs': 10,
        'io_timeout_secs': 10,
      },
      'scheduling_expiration_secs': 30,
      'tags': ['foo:bar'],
      'user': 'joe@localhost',
    }
    headers = {'X-XSRF-Token': str(response['xsrf_token'])}
    response = self.app.post_json(
        '/swarming/api/v1/client/request',
        headers=headers, params=params, status=400).json
    self.assertEqual({u'error': u'use one of command or inputs_ref'}, response)

  def test_request(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    headers = {'X-XSRF-Token-Request': '1'}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake', headers=headers, params={}).json
    params = {
      'name': 'job1',
      'priority': 200,
      'properties': {
        'commands': [['rm', '-rf', '/']],
        'data': [],
        'dimensions': {},
        'env': {},
        'execution_timeout_secs': 30,
        'io_timeout_secs': 30,
      },
      'scheduling_expiration_secs': 30,
      'tags': ['foo:bar'],
      'user': 'joe@localhost',
    }
    headers = {'X-XSRF-Token': str(response['xsrf_token'])}
    response = self.app.post_json(
        '/swarming/api/v1/client/request',
        headers=headers, params=params).json
    expected = {
      u'request': {
        u'authenticated': [u'user', u'user@example.com'],
        u'created_ts': str_now,
        u'expiration_ts': unicode(
            (now + datetime.timedelta(seconds=30)).strftime(
                utils.DATETIME_FORMAT)),
        u'name': u'job1',
        u'parent_task_id': None,
        u'priority': 200,
        u'properties': {
          u'commands': [[u'rm', u'-rf', u'/']],
          u'data': [],
          u'dimensions': {},
          u'env': {},
          u'execution_timeout_secs': 30,
          u'extra_args': [],
          u'grace_period_secs': 30,
          u'idempotent': False,
          u'inputs_ref': None,
          u'io_timeout_secs': 30,
        },
        u'properties_hash': None,
        u'tags': [
          u'foo:bar',
          u'priority:200',
          u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
      },
      u'task_id': u'5cee488008810',
    }
    self.assertEqual(expected, response)

  def test_cancel(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.set_as_admin()
    token = self.get_client_token()
    _, task_id = self.client_create_task_raw()
    params = {
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/client/cancel', params, token)
    expected = {
      u'ok': True,
      u'was_running': False,
    }
    self.assertEqual(expected, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/' + task_id).json
    expected = {
      u'abandoned_ts': str_now,
      u'bot_dimensions': None,
      u'bot_id': None,
      u'bot_version': None,
      u'children_task_ids': [],
      u'completed_ts': None,
      u'costs_usd': [],
      u'cost_saved_usd': None,
      u'created_ts': str_now,
      u'deduped_from': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': task_id,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'outputs_ref': None,
      u'properties_hash': None,
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.CANCELED,
      u'tags': [u'os:Amiga', u'priority:10', u'user:joe@localhost'],
      u'try_number': None,
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response)

  def test_get_task_metadata_unknown(self):
    response = self.app.get(
        '/swarming/api/v1/client/task/12300', status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_get_task_metadata(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    _, task_id = self.client_create_task_raw()
    response = self.app.get(
        '/swarming/api/v1/client/task/' + task_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_dimensions': None,
      u'bot_id': None,
      u'bot_version': None,
      u'children_task_ids': [],
      u'completed_ts': None,
      u'costs_usd': [],
      u'cost_saved_usd': None,
      u'created_ts': str_now,
      u'deduped_from': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008810',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'outputs_ref': None,
      u'properties_hash': None,
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.PENDING,
      u'tags': [u'os:Amiga', u'priority:100', u'user:joe@localhost'],
      u'try_number': None,
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response)
    self.assertEqual('0', task_id[-1])

    # No bot started yet.
    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/' + run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

    self.set_as_bot()
    self.bot_poll('bot1')

    self.set_as_user()
    response = self.app.get(
        '/swarming/api/v1/client/task/' + run_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'children_task_ids': [],
      u'completed_ts': None,
      u'cost_usd': 0.,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008811',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'outputs_ref': None,
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': task_result.State.RUNNING,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_get_task_metadata_denied(self):
    # Asserts that a non-public task can not be seen by an anonymous user.
    _, task_id = self.client_create_task_raw()

    self.set_as_anonymous()
    self.app.get('/swarming/api/v1/client/task/' + task_id, status=403)
    self.assertEqual('0', task_id[-1])

  def test_get_task_output(self):
    self.client_create_task_raw()

    self.set_as_bot()
    task_id = self.bot_run_task()

    self.set_as_privileged_user()
    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % task_id).json
    self.assertEqual({'output': u'rÉsult string'}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % run_id).json
    self.assertEqual({'output': u'rÉsult string'}, response)

    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/1' % task_id).json
    self.assertEqual({'output': None}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/1' % run_id).json
    self.assertEqual({'output': None}, response)

  def test_get_task_output_empty(self):
    _, task_id = self.client_create_task_raw()
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % task_id).json
    self.assertEqual({'output': None}, response)

    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_task_deduped(self):
    _, task_id_1 = self.client_create_task_raw(properties=dict(idempotent=True))

    self.set_as_bot()
    task_id_bot = self.bot_run_task()
    self.assertEqual(task_id_1, task_id_bot[:-1] + '0')
    self.assertEqual('1', task_id_bot[-1:])

    # Create a second task. Results will be returned immediately without the bot
    # running anything.
    self.set_as_user()
    _, task_id_2 = self.client_create_task_raw(
         name='second', user='jack@localhost', properties=dict(idempotent=True))

    self.set_as_bot()
    resp = self.bot_poll()
    self.assertEqual('sleep', resp['cmd'])

    self.set_as_user()
    # Look at the results. It's the same as the previous run, even if task_id_2
    # was never executed.
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id_2).json
    self.assertEqual({'outputs': [u'rÉsult string']}, response)

  def test_get_task_output_all(self):
    self.client_create_task_raw()

    self.set_as_bot()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    task_id = res['manifest']['task_id']
    params = {
      'cost_usd': 0.1,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': base64.b64encode('result string'),
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

    self.set_as_privileged_user()
    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id).json
    self.assertEqual({'outputs': [u'result string']}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % run_id).json
    self.assertEqual({'outputs': [u'result string']}, response)

  def test_get_task_output_all_empty(self):
    _, task_id = self.client_create_task_raw()
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id).json
    self.assertEqual({'outputs': []}, response)

    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_get_task_request(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    _, task_id = self.client_create_task_raw()
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/request' % task_id).json
    expected = {
      u'authenticated': [u'user', u'user@example.com'],
      u'created_ts': unicode(now.strftime(utils.DATETIME_FORMAT)),
      u'expiration_ts': unicode(
          (now + datetime.timedelta(days=1)).strftime(utils.DATETIME_FORMAT)),
      u'name': u'hi',
      u'parent_task_id': None,
      u'priority': 100,
      u'properties': {
        u'commands': [[u'python', u'run_test.py']],
        u'data': [],
        u'dimensions': {u'os': u'Amiga'},
        u'env': {},
        u'execution_timeout_secs': 3600,
        u'extra_args': [],
        u'grace_period_secs': 30,
        u'idempotent': False,
        u'inputs_ref': None,
        u'io_timeout_secs': 1200,
      },
      u'properties_hash': None,
      u'tags': [u'os:Amiga', u'priority:100', u'user:joe@localhost'],
      u'user': u'joe@localhost',
    }
    self.assertEqual(expected, response)

  def test_tasks(self):
    # Create two tasks, one deduped.
    self.mock(random, 'getrandbits', lambda _: 0x66)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.mock_now(now)
    self.client_create_task_raw(
        name='first', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    self.set_as_user()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now_60 = self.mock_now(now, 60)
    now_60_str = unicode(now_60.strftime(utils.DATETIME_FORMAT))
    self.client_create_task_raw(
        name='second', user='jack@localhost',
        tags=['project:yay', 'commit:pre', 'os:Win'],
        properties=dict(idempotent=True))

    self.set_as_privileged_user()
    expected_first = {
      u'abandoned_ts': None,
      u'bot_dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'children_task_ids': [],
      u'completed_ts': now_str,
      u'costs_usd': [0.1],
      u'cost_saved_usd': None,
      u'created_ts': now_str,
      u'deduped_from': None,
      u'durations': [0.1],
      u'exit_codes': [0],
      u'failure': False,
      u'id': u'5cee488006610',
      u'internal_failure': False,
      u'modified_ts': now_str,
      u'name': u'first',
      u'outputs_ref': None,
      u'properties_hash': u'8771754ee465a689f19c87f2d21ea0d9b8dd4f64',
      u'server_versions': [u'v1a'],
      u'started_ts': now_str,
      u'state': task_result.State.COMPLETED,
      u'tags': [
        u'commit:post',
        u'os:Amiga',
        u'os:Win',
        u'priority:100',
        u'project:yay',
        u'user:joe@localhost',
      ],
      u'try_number': 1,
      u'user': u'joe@localhost',
    }
    expected_second = {
      u'abandoned_ts': None,
      u'bot_dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'children_task_ids': [],
      u'completed_ts': now_str,
      u'costs_usd': [],
      u'cost_saved_usd': 0.1,
      u'created_ts': now_60_str,
      u'deduped_from': u'5cee488006611',
      u'durations': [0.1],
      u'exit_codes': [0],
      u'failure': False,
      u'id': u'5cfcee8008810',
      u'internal_failure': False,
      u'modified_ts': now_60_str,
      u'name': u'second',
      u'outputs_ref': None,
      u'properties_hash': None,
      u'server_versions': [u'v1a'],
      u'started_ts': now_str,
      u'state': task_result.State.COMPLETED,
      u'tags': [
        u'commit:pre',
        u'os:Amiga',
        u'os:Win',
        u'priority:100',
        u'project:yay',
        u'user:jack@localhost',
      ],
      u'try_number': 0,
      u'user': u'jack@localhost',
    }

    expected = {
      u'cursor': None,
      u'items': [expected_second, expected_first],
      u'limit': 100,
      u'sort': u'created_ts',
      u'state': u'all',
    }
    resource = '/swarming/api/v1/client/tasks'
    self.assertEqual(expected, self.app.get(resource).json)

    # It has a cursor even if there's only one element because of Search API.
    expected = {
      u'items': [expected_second],
      u'limit': 100,
      u'sort': u'created_ts',
      u'state': u'all',
    }
    actual = self.app.get(resource + '?name=second').json
    self.assertTrue(actual.pop('cursor'))
    self.assertEqual(expected, actual)

    expected = {
      u'cursor': None,
      u'items': [],
      u'limit': 100,
      u'sort': u'created_ts',
      u'state': u'all',
    }
    self.assertEqual(expected, self.app.get(resource + '?&tag=foo:bar').json)

    expected = {
      u'cursor': None,
      u'items': [expected_second],
      u'limit': 100,
      u'sort': u'created_ts',
      u'state': u'all',
    }
    actual = self.app.get(resource + '?tag=project:yay&tag=commit:pre').json
    self.assertEqual(expected, actual)

    # Test output from deduped task.
    for task in expected['items']:
      response = self.app.get(
          '/swarming/api/v1/client/task/%s/output/all' % task['id']).json
      self.assertEqual({u'outputs': [u'r\xc9sult string']}, response)

  def test_tasks_fail(self):
    self.app.get('/swarming/api/v1/client/tasks?tags=a:b', status=403)
    self.set_as_privileged_user()
    # It's 'tag', not 'tags'.
    self.app.get('/swarming/api/v1/client/tasks?tags=a:b', status=400)

  def test_count(self):
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)

    # Task in completed state.
    self.set_as_user()
    self.mock_now(now)
    self.client_create_task_raw(
        name='first', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=False))
    self.set_as_bot()
    self.bot_run_task()

    # Task in pending state.
    self.set_as_user()
    self.mock_now(now, 60)
    self.client_create_task_raw(
        name='second', user='jack@localhost',
        tags=['project:yay', 'commit:pre', 'os:Win'],
        properties=dict(idempotent=False))

    self.set_as_privileged_user()

    # Default 24h cutoff interval.
    result = self.app.get('/swarming/api/v1/client/tasks/count').json
    self.assertEqual({'count': 2}, result)

    # Test cutoff.
    result = self.app.get(
        '/swarming/api/v1/client/tasks/count?interval=30').json
    self.assertEqual({'count': 1}, result)

    # Test filter by state.
    result = self.app.get(
        '/swarming/api/v1/client/tasks/count?state=pending').json
    self.assertEqual({'count': 1}, result)

    # Test filter by tag.
    result = self.app.get(
        '/swarming/api/v1/client/tasks/count?'
        'tag=project:yay&tag=commit:pre').json
    self.assertEqual({'count': 1}, result)

  def test_api_bots(self):
    self.set_as_privileged_user()
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5, 6))
    now_str = lambda: unicode(utils.utcnow().strftime(utils.DATETIME_FORMAT))
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)
    bot1_dict = {
      u'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str(),
      u'id': u'id1',
      u'is_dead': False,
      u'last_seen_ts': now_str(),
      u'quarantined': False,
      u'state': {u'ram': 65},
      u'task_id': None,
      u'task_name': None,
      u'version': u'123456789',
    }

    actual = self.app.get('/swarming/api/v1/client/bots', status=200).json
    expected = {
      u'items': [bot1_dict],
      u'cursor': None,
      u'death_timeout': config.settings().bot_death_timeout_secs,
      u'limit': 1000,
      u'now': now_str(),
    }
    self.assertEqual(expected, actual)

    # Test with limit.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['limit'] = 1
    self.assertEqual(expected, actual)

    # Advance time to make bot1 dead to test filtering for dead bots.
    self.mock_now(datetime.datetime(2011, 1, 2, 3, 4, 5, 6))
    bot1_dict['is_dead'] = True
    expected['now'] = now_str()

    # Use quarantined bot to check filtering by 'quarantined' flag.
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=True, task_id=None, task_name=None)
    bot2_dict = {
      u'dimensions': {u'foo': [u'bar'], u'id': [u'id2']},
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str(),
      u'id': u'id2',
      u'is_dead': False,
      u'last_seen_ts': now_str(),
      u'quarantined': True,
      u'state': {u'ram': 65},
      u'task_id': None,
      u'task_name': None,
      u'version': u'123456789',
    }

    # Test limit + cursor: start the query.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['cursor'] = actual['cursor']
    expected['items'] = [bot1_dict]
    self.assertTrue(actual['cursor'])
    self.assertEqual(expected, actual)

    # Test limit + cursor: continue the query.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1&cursor=%s' % actual['cursor'],
        status=200).json
    expected['cursor'] = None
    expected['items'] = [bot2_dict]
    self.assertEqual(expected, actual)

    # Filtering by 'quarantined'.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?filter=quarantined',
        status=200).json
    expected['limit'] = 1000
    expected['cursor'] = None
    expected['items'] = [bot2_dict]
    self.assertEqual(expected, actual)

    # Filtering by 'is_dead'.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?filter=is_dead',
        status=200).json
    expected['limit'] = 1000
    expected['cursor'] = None
    expected['items'] = [bot1_dict]
    self.assertEqual(expected, actual)

  def test_api_bot(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)

    actual = self.app.get('/swarming/api/v1/client/bot/id1', status=200).json
    expected = {
      u'dimensions': {u'foo': [u'bar'], u'id': [u'id1']},
      u'external_ip': u'8.8.4.4',
      u'first_seen_ts': now_str,
      u'id': u'id1',
      u'is_dead': False,
      u'last_seen_ts': now_str,
      u'quarantined': False,
      u'state': {u'ram': 65},
      u'task_id': None,
      u'task_name': None,
      u'version': u'123456789',
    }
    self.assertEqual(expected, actual)

  def test_api_bot_delete(self):
    self.set_as_admin()
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

    token = self.get_client_token()
    actual = self.app.delete(
        '/swarming/api/v1/client/bot/id1',
        status=200,
        headers={'X-XSRF-Token': str(token)}).json
    expected = {
      u'deleted': True,
    }
    self.assertEqual(expected, actual)

    actual = self.app.get('/swarming/api/v1/client/bot/id1', status=404).json
    expected = {
      u'error': u'Bot not found',
    }
    self.assertEqual(expected, actual)

  def test_api_bot_tasks_empty(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    actual = self.app.get('/swarming/api/v1/client/bot/id1/tasks').json
    expected = {
      u'cursor': None,
      u'limit': 100,
      u'now': now.strftime(utils.DATETIME_FORMAT),
      u'items': [],
    }
    self.assertEqual(expected, actual)

  def test_api_bot_tasks(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.mock_now(now)

    self.set_as_bot()
    self.client_create_task_raw()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(token, task_id=res['manifest']['task_id'])

    now_1 = self.mock_now(now, 1)
    now_1_str = unicode(now_1.strftime(utils.DATETIME_FORMAT))
    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task_raw(name='ho')
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(
        token, exit_code=1, task_id=res['manifest']['task_id'])

    self.set_as_privileged_user()
    actual = self.app.get('/swarming/api/v1/client/bot/bot1/tasks?limit=1').json
    expected = {
      u'limit': 1,
      u'now': now_1_str,
      u'items': [
        {
          u'abandoned_ts': None,
          u'bot_dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'children_task_ids': [],
          u'completed_ts': now_1_str,
          u'cost_usd': 0.1,
          u'durations': [0.1],
          u'exit_codes': [1],
          u'failure': True,
          u'id': u'5cee870005511',
          u'internal_failure': False,
          u'modified_ts': now_1_str,
          u'outputs_ref': None,
          u'server_versions': [u'v1a'],
          u'started_ts': now_1_str,
          u'state': task_result.State.COMPLETED,
          u'try_number': 1,
        },
      ],
    }
    cursor = actual.pop('cursor')
    self.assertEqual(expected, actual)

    actual = self.app.get(
        '/swarming/api/v1/client/bot/bot1/tasks?limit=1&cursor=' + cursor).json
    expected = {
      u'cursor': None,
      u'limit': 1,
      u'now': now_1_str,
      u'items': [
        {
          u'abandoned_ts': None,
          u'bot_dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'children_task_ids': [],
          u'completed_ts': now_str,
          u'cost_usd': 0.1,
          u'durations': [0.1],
          u'exit_codes': [0],
          u'failure': False,
          u'id': u'5cee488008811',
          u'internal_failure': False,
          u'modified_ts': now_str,
          u'outputs_ref': None,
          u'server_versions': [u'v1a'],
          u'started_ts': now_str,
          u'state': task_result.State.COMPLETED,
          u'try_number': 1,
        },
      ],
    }
    self.assertEqual(expected, actual)

  def test_api_bot_missing(self):
    self.set_as_privileged_user()
    self.app.get('/swarming/api/v1/client/bot/unknown', status=404)

  def test_api_server(self):
    self.set_as_privileged_user()
    actual = self.app.get('/swarming/api/v1/client/server').json
    expected = {
      'bot_version': bot_code.get_bot_version('http://localhost'),
    }
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
