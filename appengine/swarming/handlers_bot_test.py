#!/usr/bin/env python
# coding: utf-8
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import datetime
import logging
import os
import random
import StringIO
import sys
import unittest
import zipfile

# Setups environment.
import test_env_handlers

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

import webapp2
import webtest

import handlers_bot
from components import ereporter2
from components import utils
from server import bot_archive
from server import bot_management
from server import task_result


DATETIME_FORMAT = u'%Y-%m-%dT%H:%M:%S'


class BotApiTest(test_env_handlers.AppTestBase):
  def setUp(self):
    super(BotApiTest, self).setUp()
    # By default requests in tests are coming from bot with fake IP.
    routes = handlers_bot.get_routes()
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
    # Bot API test cases run by default as bot.
    self.set_as_bot()

  def test_handshake(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'dimensions': {'id': ['id1']},
      'state': {u'running_time': 0, u'sleep_streak': 0},
      'version': '1',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'expiration_sec', u'server_version', u'xsrf_token'],
        sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'v1a', response['server_version'])
    self.assertEqual([], errors)

  def test_handshake_minimum(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params={}).json
    self.assertEqual(
        [u'bot_version', u'expiration_sec', u'server_version', u'xsrf_token'],
        sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'v1a', response['server_version'])
    expected = [
      'Quarantined Bot\nhttps://None/restricted/bot/None\n'
        'Unexpected keys missing: [u\'dimensions\', u\'state\', u\'version\']; '
        'did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_handshake_extra(self):
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      # Works with unknown items but logs an error. This permits catching typos.
      'foo': 1,
      'dimensions': {
        'id': ['bot1'],
      },
      'state': {
        'bar': 2,
        'ip': '127.0.0.1',
      },
      'version': '123',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'expiration_sec',u'server_version', u'xsrf_token'],
        sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'v1a', response['server_version'])
    expected = [
      u'Quarantined Bot\nhttps://None/restricted/bot/bot1\n'
        u'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_poll_bad_bot(self):
    # If bot is not sending required keys but report right version, enforce
    # sleeping.
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    token, params = self.get_bot_token()
    params.pop('dimensions')
    params.pop('state')
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'sleep',
      u'quarantined': True,
    }
    self.assertTrue(response.pop(u'duration'))
    self.assertEqual(expected, response)
    expected = [
      'Quarantined Bot\nhttps://None/restricted/bot/None\n'
        'Unexpected keys missing: [u\'dimensions\', u\'state\']; '
        'did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_poll_bad_version(self):
    token, params = self.get_bot_token()
    old_version = params['version']
    params['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_sleep(self):
    # A bot polls, gets nothing.
    token, params = self.get_bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # Sleep again
    params['state']['sleep_streak'] += 1
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

  def test_poll_update(self):
    token, params = self.get_bot_token()
    old_version = params['version']
    params['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_restart(self):
    def mock_should_restart_bot(bot_id, state):
      self.assertEqual('bot1', bot_id)
      expected_state = {
        'running_time': 1234.0,
        'sleep_streak': 0,
        'started_ts': 1410990411.111,
      }
      self.assertEqual(expected_state, state)
      return True, 'Mocked restart message'
    self.mock(bot_management, 'should_restart_bot', mock_should_restart_bot)

    token, params = self.get_bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'restart',
      u'message': 'Mocked restart message',
    }
    self.assertEqual(expected, response)

  def test_poll_task_raw(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    token, params = self.get_bot_token()
    # Enqueue a task.
    _, task_id = self.client_create_task_raw()
    self.assertEqual('0', task_id[-1])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'command': [u'python', u'run_test.py'],
        u'data': [],
        u'dimensions': {u'os': u'Amiga'},
        u'env': {},
        u'extra_args': [],
        u'grace_period': 30,
        u'hard_timeout': 3600,
        u'host': u'http://localhost:8080',
        u'inputs_ref': None,
        u'io_timeout': 1200,
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)
    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.],
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
    self.assertEqual(expected, response)

  def test_poll_task_isolated(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    token, params = self.get_bot_token()
    # Enqueue a task.
    _, task_id = self.client_create_task_isolated()
    self.assertEqual('0', task_id[-1])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'command': None,
        u'data': [],
        u'dimensions': {u'os': u'Amiga'},
        u'env': {},
        u'extra_args': [],
        u'hard_timeout': 3600,
        u'grace_period': 30,
        u'host': u'http://localhost:8080',
        u'inputs_ref': {
          u'isolated': u'0123456789012345678901234567890123456789',
          u'isolatedserver': u'http://localhost:1',
          u'namespace': u'default-gzip',
        },
        u'io_timeout': 1200,
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)
    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.],
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
    self.assertEqual(expected, response)

  def test_poll_not_enough_time(self):
    # Make sure there's a task that we don't get.
    _, task_id = self.client_create_task_raw()
    self.assertEqual('0', task_id[-1])
    token, params = self.get_bot_token()
    params['state']['lease_expiration_ts'] = 0
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertTrue(response.pop('duration'))
    self.assertEqual(expected, response)

  def test_poll_enough_time(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    _, task_id = self.client_create_task_isolated()
    self.assertEqual('0', task_id[-1])
    token, params = self.get_bot_token()
    params['state']['lease_expiration_ts'] = (
        int(utils.time_time()) + 3600 + 1200 + 3 * 30 + 10 + 1)
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'command': None,
        u'data': [],
        u'dimensions': {u'os': u'Amiga'},
        u'env': {},
        u'extra_args': [],
        u'hard_timeout': 3600,
        u'grace_period': 30,
        u'host': u'http://localhost:8080',
        u'inputs_ref': {
          u'isolated': u'0123456789012345678901234567890123456789',
          u'isolatedserver': u'http://localhost:1',
          u'namespace': u'default-gzip',
        },
        u'io_timeout': 1200,
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)

  def test_bot_ereporter2_error(self):
    # ereporter2's //client/utils/on_error.py traps unhandled exceptions
    # automatically.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    errors = []
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: errors.append((args, kwargs)))
    token, params = self.get_bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # The bot fails somehow.
    error_params = {
      'v': 1,
      'r': {
        'args': ['a', 'b'],
        'category': 'junk',
        'cwd': '/root',
        'duration': 0.1,
        'endpoint': '/root',
        'env': {'a': 'b'},
        'exception_type': 'FooError',
        'hostname': 'localhost',
        'message': 'Something happened',
        'method': 'GET',
        'os': 'Amiga',
        'params': {'a': 123},
        'python_version': '2.7',
        'request_id': '123',
        'source': params['dimensions']['id'][0],
        'source_ip': '127.0.0.1',
        'stack': 'stack trace...',
        'user': 'Joe',
        'version': '12',
      },
    }
    ereporter2_app = webtest.TestApp(
        webapp2.WSGIApplication(ereporter2.get_frontend_routes(), debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })
    response = ereporter2_app.post_json(
        '/ereporter2/api/v1/on_error', error_params)
    expected = {
      u'id': 1,
      u'url': u'http://localhost/restricted/ereporter2/errors/1',
    }
    self.assertEqual(expected, response.json)

    # A bot error currently does not result in permanent quarantine. It will
    # eventually.
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)
    self.assertEqual([], errors)

  def test_bot_event_error(self):
    # Native bot error reporting.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    errors = []
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: errors.append((args, kwargs)))
    token, params = self.get_bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # The bot fails somehow.
    token, params2 = self.get_bot_token()
    params2['event'] = 'bot_error'
    params2['message'] = 'for the worst'
    response = self.post_with_token(
        '/swarming/api/v1/bot/event', params2, token)
    self.assertEqual({}, response)

    # A bot error currently does not result in permanent quarantine. It will
    # eventually.
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)
    expected = [
      {
        'message':
            u'Bot: https://None/restricted/bot/bot1\nBot error:\nfor the worst',
        'source': 'bot',
      },
    ]
    self.assertEqual(expected, [e[1] for e in errors])

  def test_bot_event(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    token, params = self.get_bot_token()
    params['event'] = 'bot_rebooting'
    params['message'] = 'for the best'
    response = self.post_with_token('/swarming/api/v1/bot/event', params, token)
    self.assertEqual({}, response)

    # TODO(maruel): Replace with client api to query last BotEvent.
    actual = [
        e.to_dict() for e in bot_management.get_events_query('bot1', True)]
    expected = [
      {
        'dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
        'event_type': u'bot_rebooting',
        'external_ip': u'192.168.2.2',
        'message': u'for the best',
        'quarantined': False,
        'state': {
          u'running_time': 1234.0,
          u'sleep_streak': 0,
          u'started_ts': 1410990411.111,
        },
        'task_id': u'',
        'ts': now,
        'version': self.bot_version,
      },
      {
        'dimensions': {u'id': [u'bot1'], u'os': [u'Amiga']},
        'event_type': u'bot_connected',
        'external_ip': u'192.168.2.2',
        'message': None,
        'quarantined': False,
        'state': {
          u'running_time': 1234.0,
          u'sleep_streak': 0,
          u'started_ts': 1410990411.111,
        },
        'task_id': u'',
        'ts': now,
        'version': u'123',
      },
    ]
    self.assertEqual(expected, actual)

  def test_task_complete(self):
    # Runs a task with 2 commands up to completion.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    token, params = self.get_bot_token()
    self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    def _params(**kwargs):
      out = {
        'cost_usd': 0.1,
        'duration': None,
        'exit_code': None,
        'id': 'bot1',
        'output': None,
        'output_chunk_start': 0,
        'task_id': task_id,
      }
      out.update(**kwargs)
      return out

    def _expected(**kwargs):
      out = {
        u'bot_dimensions': [
          {u'key': u'id', u'value': [u'bot1']},
          {u'key': u'os', u'value': [u'Amiga']},
        ],
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'costs_usd': [0.1],
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
      out.update(**kwargs)
      return out

    def _cycle(params, expected):
      response = self.post_with_token(
          '/swarming/api/v1/bot/task_update', params, token)
      self.assertEqual({u'ok': True}, response)
      response = self.client_get_results(task_id)
      self.assertEqual(expected, response)

    # 1. Initial task update with no data.
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']
    params = _params()
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)
    response = self.client_get_results(task_id)
    self.assertEqual(_expected(), response)

    # 2. Task update with some output.
    params = _params(output=base64.b64encode('Oh '))
    expected = _expected()
    _cycle(params, expected)

    # 3. Task update with some more output.
    params = _params(output=base64.b64encode('hi'), output_chunk_start=3)
    expected = _expected()
    _cycle(params, expected)

    # 4. Task update with completion of the command.
    params = _params(
        duration=0.1, exit_code=23, output=base64.b64encode('Ahahah'))
    expected = _expected(
        completed_ts=str_now,
        duration=0.1,
        exit_code=u'23',
        failure=True,
        state=u'COMPLETED')
    _cycle(params, expected)

  def test_task_update_db_failure(self):
    # The error is caught in task_scheduler.bot_update_task().
    self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    token, params = self.get_bot_token()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    def r(*_):
      raise datastore_errors.Timeout('Sorry!')
    self.mock(ndb, 'put_multi', r)
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
        '/swarming/api/v1/bot/task_update', params, token, status=500)
    self.assertEqual({u'error': u'Failed to update, please retry'}, response)

  def test_task_update_failure(self):
    self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    token, params = self.get_bot_token()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    class NewError(Exception):
      pass

    def r(*_):
      raise NewError('Sorry!')
    self.mock(ndb, 'put_multi', r)
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
        '/swarming/api/v1/bot/task_update', params, token, status=500)
    self.assertEqual({u'error': u'Sorry!'}, response)

  def test_task_failure(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    token, params = self.get_bot_token()
    self.client_create_task_raw()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    params = {
      'cost_usd': 0.1,
      'duration': 0.1,
      'exit_code': 1,
      'id': 'bot1',
      'output': base64.b64encode('result string'),
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': str_now,
      u'costs_usd': [0.1],
      u'created_ts': str_now,
      u'duration': 0.1,
      u'exit_code': u'1',
      u'failure': True,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'task_id': u'5cee488008811',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response)

  def test_task_internal_failure(self):
    # E.g. task_runner blew up.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    errors = []
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: errors.append((args, kwargs)))
    token, params = self.get_bot_token()
    self.client_create_task_raw()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    # Let's say it failed to start task_runner because the new bot code is
    # broken. The end result is still BOT_DIED. The big change is that it
    # doesn't need to wait for a cron job to set this status.
    params = {
      'id': params['dimensions']['id'][0],
      'message': 'Oh',
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_error', params, token)

    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': str_now,
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.],
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': True,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'BOT_DIED',
      u'task_id': u'5cee488008811',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response)
    self.assertEqual(1, len(errors))
    expected = [
      {
        'message':
          u'Bot: https://None/restricted/bot/bot1\n'
          'Task failed: https://None/user/task/5cee488008811\nOh',
        'source': 'bot',
      },
    ]
    self.assertEqual(expected, [e[1] for e in errors])

  def test_bot_code(self):
    code = self.app.get('/bot_code')
    expected = {'config/bot_config.py', 'config/config.json'}.union(
        bot_archive.FILES)
    with zipfile.ZipFile(StringIO.StringIO(code.body), 'r') as z:
      self.assertEqual(expected, set(z.namelist()))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
