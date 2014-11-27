#!/usr/bin/env python
# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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

import handlers_api
from components import ereporter2
from components import utils
from server import bot_archive
from server import bot_code
from server import bot_management
from server import task_result


class AppTestBase(test_env_handlers.AppTestBase):
  def setUp(self):
    super(AppTestBase, self).setUp()
    # By default requests in tests are coming from bot with fake IP.
    app = webapp2.WSGIApplication(handlers_api.get_routes(), debug=True)
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': self.fake_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })


class BotApiTest(AppTestBase):
  def setUp(self):
    super(BotApiTest, self).setUp()
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
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
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
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
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
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
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

  def test_poll_task(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    token, params = self.get_bot_token()
    # Enqueue a task.
    _, task_id = self.client_create_task()
    self.assertEqual('0', task_id[-1])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'commands': [[u'python', u'run_test.py']],
        u'data': [],
        u'env': {},
        u'hard_timeout': 3600,
        u'host': u'http://localhost:8080',
        u'io_timeout': 1200,
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)
    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008811',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'server_versions': [u'default-version'],
      u'started_ts': str_now,
      u'state': task_result.State.RUNNING,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_bot_error(self):
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
      'id': params['dimensions']['id'][0],
      'message': 'Something happened',
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/error', error_params, token)
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
    self.assertEqual(1, len(errors))

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
    actual = [e.to_dict() for e in bot_management.get_events_query('bot1')]
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
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self.get_bot_token()
    self.client_create_task(
        properties=dict(commands=[['python', 'runtest.py'], ['cleanup.py']]))

    def _params(**kwargs):
      out = {
        'command_index': 0,
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
        u'abandoned_ts': None,
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'completed_ts': None,
        u'durations': [],
        u'exit_codes': [],
        u'failure': False,
        u'id': u'5cee488008811',
        u'internal_failure': False,
        u'modified_ts': str_now,
        u'server_versions': [u'default-version'],
        u'started_ts': str_now,
        u'state': task_result.State.RUNNING,
        u'try_number': 1,
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
    params = _params(output='Oh ')
    expected = _expected()
    _cycle(params, expected)

    # 3. Task update with some more output.
    params = _params(output='hi', output_chunk_start=3)
    expected = _expected()
    _cycle(params, expected)

    # 4. Task update with completion of first command.
    params = _params(duration=0.2, exit_code=0)
    expected = _expected(exit_codes=[0])
    expected = _expected(durations=[0.2], exit_codes=[0])
    _cycle(params, expected)

    # 5. Task update with completion of second command along with full output.
    params = _params(
        command_index=1, duration=0.1, exit_code=23, output='Ahahah')
    expected = _expected(
        completed_ts=str_now,
        durations=[0.2, 0.1],
        exit_codes=[0, 23],
        failure=True,
        state=task_result.State.COMPLETED)
    _cycle(params, expected)

  def test_task_update_db_failure(self):
    # The error is caught in task_scheduler.bot_update_task().
    self.client_create_task(
        properties=dict(commands=[['python', 'runtest.py'], ['cleanup.py']]))

    token, params = self.get_bot_token()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    def r(*_):
      raise datastore_errors.Timeout('Sorry!')
    self.mock(ndb, 'put_multi', r)
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': 'result string',
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token, status=500)
    expected = {
      u'error':
          u'The server has either erred or is incapable of performing the '
          u'requested operation.',
    }
    self.assertEqual(expected, response)

  def test_task_update_failure(self):
    # The error is caught in handlers_api.BotTaskUpdateHandler.post().
    self.client_create_task(
        properties=dict(commands=[['python', 'runtest.py'], ['cleanup.py']]))

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
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': 'result string',
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
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self.get_bot_token()
    self.client_create_task()
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 1,
      'id': 'bot1',
      'output': 'result string',
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': str_now,
      u'durations': [0.1],
      u'exit_codes': [1],
      u'failure': True,
      u'id': u'5cee488008811',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'server_versions': [u'default-version'],
      u'started_ts': str_now,
      u'state': task_result.State.COMPLETED,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_task_internal_failure(self):
    # E.g. task_runner blew up.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    errors = []
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: errors.append((args, kwargs)))
    token, params = self.get_bot_token()
    self.client_create_task()
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
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008811',
      u'internal_failure': True,
      u'modified_ts': str_now,
      u'server_versions': [u'default-version'],
      u'started_ts': str_now,
      u'state': task_result.State.BOT_DIED,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)
    self.assertEqual(1, len(errors))

  def test_get_slave_code(self):
    code = self.app.get('/get_slave_code')
    expected = set(('bot_config.py', 'config.json')).union(bot_archive.FILES)
    with zipfile.ZipFile(StringIO.StringIO(code.body), 'r') as z:
      self.assertEqual(expected, set(z.namelist()))


class ClientApiTest(AppTestBase):
  def setUp(self):
    super(ClientApiTest, self).setUp()
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()

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
    self.assertEqual(u'default-version', response['server_version'])

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
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_request_invalid(self):
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
          u'Unexpected request keys; did you make a typo?\n'
            u'Missing: name, priority, user\nSuperfluous: foo\n',
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
    self.assertEqual({u'error': u'commands is required'}, response)

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
        u'created_ts': str_now,
        u'expiration_ts': unicode(
            (now + datetime.timedelta(seconds=30)).strftime(
                utils.DATETIME_FORMAT)),
        u'name': u'job1',
        u'priority': 200,
        u'properties': {
          u'commands': [[u'rm', u'-rf', u'/']],
          u'data': [],
          u'dimensions': {},
          u'env': {},
          u'execution_timeout_secs': 30,
          u'idempotent': False,
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
    _, task_id = self.client_create_task()
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
      u'bot_id': None,
      u'bot_version': None,
      u'completed_ts': None,
      u'created_ts': str_now,
      u'deduped_from': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': task_id,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'properties_hash': None,
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.CANCELED,
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
    _, task_id = self.client_create_task()
    response = self.app.get(
        '/swarming/api/v1/client/task/' + task_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_id': None,
      u'bot_version': None,
      u'completed_ts': None,
      u'created_ts': str_now,
      u'deduped_from': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008810',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'properties_hash': None,
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.PENDING,
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
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': None,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'5cee488008811',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'server_versions': [u'default-version'],
      u'started_ts': str_now,
      u'state': task_result.State.RUNNING,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_get_task_metadata_denied(self):
    # Asserts that a non-public task can not be seen by an anonymous user.
    _, task_id = self.client_create_task()

    self.set_as_anonymous()
    self.app.get('/swarming/api/v1/client/task/' + task_id, status=403)
    self.assertEqual('0', task_id[-1])

  def test_get_task_output(self):
    self.client_create_task()

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
    self.assertEqual({'output': u'bar'}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/1' % run_id).json
    self.assertEqual({'output': u'bar'}, response)

    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/2' % task_id).json
    self.assertEqual({'output': None}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/2' % run_id).json
    self.assertEqual({'output': None}, response)

  def test_get_task_output_empty(self):
    _, task_id = self.client_create_task()
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % task_id).json
    self.assertEqual({'output': None}, response)

    run_id = task_id[:-1] + '1'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_task_deduped(self):
    _, task_id_1 = self.client_create_task(properties=dict(idempotent=True))

    self.set_as_bot()
    task_id_bot = self.bot_run_task()
    self.assertEqual(task_id_1, task_id_bot[:-1] + '0')
    self.assertEqual('1', task_id_bot[-1:])

    # Create a second task. Results will be returned immediately without the bot
    # running anything.
    self.set_as_user()
    _, task_id_2 = self.client_create_task(
         name='second', user='jack@localhost', properties=dict(idempotent=True))

    self.set_as_bot()
    resp = self.bot_poll()
    self.assertEqual('sleep', resp['cmd'])

    self.set_as_user()
    # Look at the results. It's the same as the previous run, even if task_id_2
    # was never executed.
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id_2).json
    self.assertEqual({'outputs': [u'rÉsult string', u'bar']}, response)

  def test_get_task_output_all(self):
    self.client_create_task()

    self.set_as_bot()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    task_id = res['manifest']['task_id']
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': 'result string',
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)
    params = {
      'command_index': 1,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': 'bar',
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
    self.assertEqual({'outputs': [u'result string', u'bar']}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % run_id).json
    self.assertEqual({'outputs': [u'result string', u'bar']}, response)

  def test_get_task_output_all_empty(self):
    _, task_id = self.client_create_task()
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
    _, task_id = self.client_create_task()
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/request' % task_id).json
    expected = {
      u'created_ts': unicode(now.strftime(utils.DATETIME_FORMAT)),
      u'expiration_ts': unicode(
          (now + datetime.timedelta(days=1)).strftime(utils.DATETIME_FORMAT)),
      u'name': u'hi',
      u'priority': 100,
      u'properties': {
        u'commands': [[u'python', u'run_test.py']],
        u'data': [],
        u'dimensions': {u'os': u'Amiga'},
        u'env': {},
        u'execution_timeout_secs': 3600,
        u'idempotent': False,
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
    self.client_create_task(
        name='first', tags=['project:yay', 'commit:post', 'os:Win'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    self.set_as_user()
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now_60 = self.mock_now(now, 60)
    now_60_str = unicode(now_60.strftime(utils.DATETIME_FORMAT))
    self.client_create_task(
        name='second', user='jack@localhost',
        tags=['project:yay', 'commit:pre', 'os:Win'],
        properties=dict(idempotent=True))

    self.set_as_privileged_user()
    expected_first = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': now_str,
      u'created_ts': now_str,
      u'deduped_from': None,
      u'durations': [0.1, 0.2],
      u'exit_codes': [0, 0],
      u'failure': False,
      u'id': u'5cee488006610',
      u'internal_failure': False,
      u'modified_ts': now_str,
      u'name': u'first',
      u'properties_hash': u'd181190fea9de5dfa28ebcd155548e3f6db6ab93',
      u'server_versions': [u'default-version'],
      u'started_ts': now_str,
      u'state': task_result.State.COMPLETED,
      u'try_number': 1,
      u'user': u'joe@localhost',
    }
    expected_second = {
      u'abandoned_ts': None,
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': now_str,
      u'created_ts': now_60_str,
      u'deduped_from': u'5cee488006611',
      u'durations': [0.1, 0.2],
      u'exit_codes': [0, 0],
      u'failure': False,
      u'id': u'5cfcee8008810',
      u'internal_failure': False,
      u'modified_ts': now_60_str,
      u'name': u'second',
      u'properties_hash': u'd181190fea9de5dfa28ebcd155548e3f6db6ab93',
      u'server_versions': [u'default-version'],
      u'started_ts': now_str,
      u'state': task_result.State.COMPLETED,
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

  def test_tasks_fail(self):
    self.app.get('/swarming/api/v1/client/tasks?tags=a:b', status=403)
    self.set_as_privileged_user()
    # It's 'tag', not 'tags'.
    self.app.get('/swarming/api/v1/client/tasks?tags=a:b', status=400)

  def test_api_bots(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    now_str = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.mock_now(now)
    bot_management.bot_event(
        event_type='bot_connected', bot_id='id1', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id1']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)

    actual = self.app.get('/swarming/api/v1/client/bots', status=200).json
    expected = {
      u'items': [
        {
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
        },
      ],
      u'cursor': None,
      u'death_timeout': bot_management.BOT_DEATH_TIMEOUT.total_seconds(),
      u'limit': 1000,
      u'now': unicode(now.strftime(utils.DATETIME_FORMAT)),
    }
    self.assertEqual(expected, actual)

    # Test with limit.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['limit'] = 1
    self.assertEqual(expected, actual)

    bot_management.bot_event(
        event_type='bot_connected', bot_id='id2', external_ip='8.8.4.4',
        dimensions={'foo': ['bar'], 'id': ['id2']}, state={'ram': 65},
        version='123456789', quarantined=False, task_id=None, task_name=None)

    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1', status=200).json
    expected['cursor'] = actual['cursor']
    self.assertTrue(actual['cursor'])
    self.assertEqual(expected, actual)

    # Test with cursor.
    actual = self.app.get(
        '/swarming/api/v1/client/bots?limit=1&cursor=%s' % actual['cursor'],
        status=200).json
    expected['cursor'] = None
    expected['items'][0]['dimensions']['id'] = [u'id2']
    expected['items'][0]['id'] = u'id2'
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
    self.client_create_task()
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    self.bot_complete_task(token, task_id=res['manifest']['task_id'])

    now_1 = self.mock_now(now, 1)
    now_1_str = unicode(now_1.strftime(utils.DATETIME_FORMAT))
    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task(name='ho')
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
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': now_1_str,
          u'durations': [0.1],
          u'exit_codes': [1],
          u'failure': True,
          u'id': u'5cee870005511',
          u'internal_failure': False,
          u'modified_ts': now_1_str,
          u'server_versions': [u'default-version'],
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
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': now_str,
          u'durations': [0.1],
          u'exit_codes': [0],
          u'failure': False,
          u'id': u'5cee488008811',
          u'internal_failure': False,
          u'modified_ts': now_str,
          u'server_versions': [u'default-version'],
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
