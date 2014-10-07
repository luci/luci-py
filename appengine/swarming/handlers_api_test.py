#!/usr/bin/env python
# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import random
import StringIO
import sys
import unittest
import zipfile

APP_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env
test_env.setup_test_env()

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb

import webapp2
import webtest

import handlers_api
from common import test_request_message
from components import auth
from components import ereporter2
from components import stats_framework
from components import utils
from server import acl
from server import bot_archive
from server import bot_management
from server import stats
from server import task_common
from server import task_result
from server import task_scheduler
from server import task_to_run
from server import user_manager
from support import test_case


# remote_addr of a fake bot that makes requests in tests.
FAKE_IP = '192.168.0.0'


def clear_ip_whitelist():
  """Removes all IPs from the whitelist."""
  entries = user_manager.MachineWhitelist.query().fetch(keys_only=True)
  ndb.delete_multi(entries)


def GetRequestMessage(requestor=None):
  """Return a properly formatted request message text.

  Returns:
    A properly formatted request message text.
  """
  tests = [
    test_request_message.TestObject(test_name='t1', action=['ignore-me.exe']),
  ]
  configurations = [
    test_request_message.TestConfiguration(
        config_name='c1_0',
        dimensions=dict(os='win-xp', cpu='Unknown', browser='Unknown'),
        num_instances=1,
        priority=10)
  ]
  request = test_request_message.TestCase(
      restart_on_failure=False,
      requestor=requestor,
      test_case_name='tc',
      configurations=configurations,
      data=[('http://b.ina.ry/files2.zip', 'files2.zip')],
      env_vars=None,
      tests=tests)
  return test_request_message.Stringize(request, json_readable=True)


def CreateRunner(config_name=None, machine_id=None, ran_successfully=None,
                 exit_codes=None, created=None, started=None,
                 ended=None, requestor=None, results=None):
  """Creates entities to represent a new task request and a bot running it.

  The entities are TaskRequest, TaskToRun, TaskResultSummary, TaskRunResult,
  ResultChunk and Results.

  The entity may be in a pending, running or completed state.

  Args:
    config_name: The config name for the runner.
    machine_id: The machine id for the runner. Also, if this is given the
      runner is marked as having started.
    ran_succesfully: True if the runner ran successfully.
    exit_codes: The exit codes for the runner. Also, if this is given the
      runner is marked as having finished.
    created: timedelta from now to mark this request was created in the future.
    started: timedelta from .created to when the task execution started.
    ended: timedelta from .started to when the bot completed the task.
    requestor: string representing the user name that requested the task.
    results: string that represents stdout the bot sent back.

  Returns:
    The pending runner created.
  """
  config_name = config_name or ('c1_0')
  request_message = GetRequestMessage(requestor=requestor)
  data = handlers_api.convert_test_case(request_message)
  request, result_summary = task_scheduler.make_request(data)
  if created:
    # For some reason, pylint is being obnoxious here and generates a W0201
    # warning that cannot be disabled. See http://www.logilab.org/19607
    setattr(request, 'created_ts', datetime.datetime.utcnow() + created)
    request.put()

  # The task is reaped by a bot, so it is in a running state. Pending state is
  # not supported here.
  task_key = task_to_run.request_to_task_to_run_key(request)
  task = task_to_run.is_task_reapable(task_key, None)
  task.put()

  machine_id = machine_id or 'localhost'
  run_result = task_result.new_run_result(request, 1, machine_id, 'abc')

  if ran_successfully or exit_codes:
    exit_codes = map(int, exit_codes.split(',')) if exit_codes else [0]
    # The entity needs to be saved before it can be updated, since
    # bot_update_task() accepts the key of the entity.
    ndb.put_multi(task_result.prepare_put_run_result(run_result))
    for index, code in enumerate(exit_codes):
      with task_scheduler.bot_update_task(
          run_result.key, machine_id, index, results, 0, code, 0.1) as entities:
        ndb.put_multi(entities)
    # Refresh it from the DB.

  if started:
    run_result.started_ts = run_result.created + started
  if ended:
    run_result.completed_ts = run_result.started_ts + ended

  # Mark the job as at least running.
  ndb.put_multi(task_result.prepare_put_run_result(run_result))

  return result_summary, run_result


class AppTestBase(test_case.TestCase):
  APP_DIR = APP_DIR

  def setUp(self):
    super(AppTestBase, self).setUp()
    self.bot_version = None
    self.testbed.init_user_stub()
    self.testbed.init_search_stub()

    # By default requests in tests are coming from bot with fake IP.
    app = webapp2.WSGIApplication(handlers_api.get_routes(), debug=True)
    self.app = webtest.TestApp(
        app,
        extra_environ={
          'REMOTE_ADDR': FAKE_IP,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # WSGI app that implements auth REST API.
    self.auth_app = webtest.TestApp(
        auth.create_wsgi_application(debug=True),
        extra_environ={
          'REMOTE_ADDR': FAKE_IP,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # Note that auth.ADMIN_GROUP != acl.ADMINS_GROUP.
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'super-admin@example.com'), '')
    auth.bootstrap_group(
        acl.ADMINS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'admin@example.com'), '')
    auth.bootstrap_group(
        acl.PRIVILEGED_USERS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'priv@example.com'), '')
    auth.bootstrap_group(
        acl.USERS_GROUP,
        auth.Identity(auth.IDENTITY_USER, 'user@example.com'), '')
    auth.bootstrap_group(
        acl.BOTS_GROUP,
        auth.Identity(auth.IDENTITY_BOT, FAKE_IP), '')

    self.mock(stats_framework, 'add_entry', self._parse_line)

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {})
    self.assertEqual(True, actual, line)

  def set_as_anonymous(self):
    clear_ip_whitelist()
    self.testbed.setup_env(USER_EMAIL='', overwrite=True)

  def set_as_super_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='super-admin@example.com', overwrite=True)

  def set_as_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='admin@example.com', overwrite=True)

  def set_as_privileged_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='priv@example.com', overwrite=True)

  def set_as_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='user@example.com', overwrite=True)

  def set_as_bot(self):
    self.set_as_anonymous()
    user_manager.AddWhitelist(FAKE_IP)

  def assertResponse(self, response, status, body):
    self.assertEqual(status, response.status, response.status)
    self.assertEqual(body, response.body, repr(response.body))

  def getXsrfToken(self):
    return self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json['xsrf_token']

  def _client_token(self):
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers,
        params=params).json
    token = response['xsrf_token'].encode('ascii')
    return token

  def _bot_token(self, bot='bot1'):
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'attributes': {
        'dimensions': {
          'id': bot,
          'os': ['Amiga'],
        },
        'id': bot,
        'ip': '127.0.0.1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake',
        headers=headers,
        params=params).json
    token = response['xsrf_token'].encode('ascii')
    self.bot_version = response['bot_version']
    params['attributes']['version'] = self.bot_version
    params['state'] = {
      'running_time': 1234.0,
      'sleep_streak': 0,
      'started_ts': 1410990411.111,
    }
    return token, params

  def post_with_token(self, url, params, token):
    """Does an HTTP POST with a JSON API and a XSRF token."""
    return self.app.post_json(
        url, params=params, headers={'X-XSRF-Token': token}).json

  def bot_poll(self, bot='bot1'):
    """Simulates a bot that polls for task."""
    token, params = self._bot_token(bot)
    return self.post_with_token('/swarming/api/v1/bot/poll', params, token)

  def client_create_task(self, name, extra_command=None):
    """Simulate a client that creates a task."""
    # TODO(maruel): Switch to new API.
    request = {
      'configurations': [
        {
          'config_name': 'X',
          'dimensions': {'os': 'Amiga'},
          'priority': 10,
        },
      ],
      'test_case_name': name,
      'tests': [
        {'action': ['python', 'run_test.py'], 'test_name': 'Y'},
      ],
    }
    if extra_command:
      # Add a second command
      request['tests'].append({'action': extra_command, 'test_name': 'Z'})
    raw = self.app.post('/test', {'request': json.dumps(request)}).json
    task_id = raw['test_keys'][0]['test_key']
    return raw, task_id


class BotApiTest(AppTestBase):
  def setUp(self):
    super(BotApiTest, self).setUp()
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Bot API test cases run by default as bot.
    self.set_as_bot()

  def client_get_results(self, task_id):
    return self.app.get(
        '/swarming/api/v1/client/task/%s' % task_id).json

  def test_handshake(self):
    # Bare minimum:
    errors = []
    def add_error(request, source, message):
      self.assertTrue(request)
      self.assertEqual('bot', source)
      errors.append(message)
    self.mock(ereporter2, 'log_request', add_error)
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'attributes': {
        'id': 'bot1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected attributes missing: [u\'dimensions\', u\'ip\']; did you make '
          'a typo?',
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
      'attributes': {
        'bar': 2,
        'id': 'bot1',
        'ip': '127.0.0.1',
        'version': '123',
      },
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', headers=headers, params=params).json
    self.assertEqual(
        [u'bot_version', u'server_version', u'xsrf_token'], sorted(response))
    self.assertTrue(response['xsrf_token'])
    self.assertEqual(40, len(response['bot_version']))
    self.assertEqual(u'default-version', response['server_version'])
    expected = [
      'Unexpected keys superfluous: [u\'foo\']; did you make a typo?',
      'Unexpected attributes missing: [u\'dimensions\'] superfluous: [u\'bar\']'
          '; did you make a typo?',
    ]
    self.assertEqual(expected, errors)

  def test_poll_bad_bot(self):
    # If bot is not sending required keys, assume it is old and update it.
    token, params = self._bot_token()
    old_version = params['attributes']['version']
    params.pop('attributes')
    params.pop('state')
    # log_request is expected to be called in that case, multiple times.
    error_calls = []
    self.mock(
        ereporter2,
        'log_request',
        lambda *args, **kwargs: error_calls.append((args, kwargs)))
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)
    self.assertTrue(error_calls)

  def test_poll_bad_version(self):
    token, params = self._bot_token()
    old_version = params['attributes']['version']
    params['attributes']['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_sleep(self):
    # A bot polls, gets nothing.
    token, params = self._bot_token()
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
    token, params = self._bot_token()
    old_version = params['attributes']['version']
    params['attributes']['version'] = 'badversion'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_restart(self):
    def mock_should_restart_bot(bot_id, _attributes, state):
      self.assertEqual('bot1', bot_id)
      expected_state = {
        'running_time': 1234.0,
        'sleep_streak': 0,
        'started_ts': 1410990411.111,
      }
      self.assertEqual(expected_state, state)
      return True, 'Mocked restart message'
    self.mock(bot_management, 'should_restart_bot', mock_should_restart_bot)

    token, params = self._bot_token()
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
    token, params = self._bot_token()
    # Enqueue a task.
    _, task_id = self.client_create_task('hi')
    self.assertEqual('00', task_id[-2:])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-2] + '01'
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'commands': [[u'python', u'run_test.py']],
        u'data': [],
        u'env': {},
        u'hard_timeout': 3600,
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
      u'id': u'125ecfd5c888801',
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
    token, params = self._bot_token()
    response = self.post_with_token('/swarming/api/v1/bot/poll', params, token)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # The bot fails somehow.
    error_params = {
      'id': params['attributes']['id'],
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

  def test_update(self):
    # Runs a task with 2 commands up to completion.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self._bot_token()
    self.client_create_task('hi', extra_command=['cleanup.py'])

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
        u'id': u'125ecfd5c888801',
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

  def test_task_failure(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    token, params = self._bot_token()
    self.client_create_task('hi')
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
      u'id': u'125ecfd5c888801',
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
    token, params = self._bot_token()
    self.client_create_task('hi')
    response = self.post_with_token(
        '/swarming/api/v1/bot/poll', params, token)
    task_id = response['manifest']['task_id']

    # Let's say it failed to start task_runner because the new bot code is
    # broken. The end result is still BOT_DIED. The big change is that it
    # doesn't need to wait for a cron job to set this status.
    params = {
      'id': params['attributes']['id'],
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
      u'id': u'125ecfd5c888801',
      u'internal_failure': True,
      u'modified_ts': str_now,
      u'server_versions': [u'default-version'],
      u'started_ts': str_now,
      u'state': task_result.State.BOT_DIED,
      u'try_number': 1,
    }
    self.assertEqual(expected, response)

  def test_get_slave_code(self):
    code = self.app.get('/get_slave_code')
    expected = set(('config.json', 'start_slave.py')).union(bot_archive.FILES)
    with zipfile.ZipFile(StringIO.StringIO(code.body), 'r') as z:
      self.assertEqual(expected, set(z.namelist()))


class NewClientApiTest(AppTestBase):
  def setUp(self):
    super(NewClientApiTest, self).setUp()
    self.mock(
        ereporter2, 'log_request',
        lambda *args, **kwargs: self.fail('%s, %s' % (args, kwargs)))
    # Client API test cases run by default as user.
    self.set_as_user()

  def _complete_task(self, token, task_id, bot_id='bot1'):
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 1,
      'id': bot_id,
      'output': u'rÉsult string',
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

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
      'tags': ['foo', 'bar'],
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
      'tags': ['foo', 'bar'],
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
          u'io_timeout_secs': 30,
        },
        u'properties_hash': u'b066ff178d6d987c0ef85d0f765c929f27026719',
        u'tags': [u'foo', u'bar'],
        u'user': u'joe@localhost',
      },
      u'task_id': u'125ecfd5c888800',
    }
    self.assertEqual(expected, response)

  def test_cancel(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(utils.DATETIME_FORMAT))
    self.set_as_admin()
    # Note: this is still the old API.
    token = self._client_token()
    _, task_id = self.client_create_task('hi')
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
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': task_id,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.CANCELED,
      u'try_number': None,
      u'user': u'unknown',
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
    # Note: this is still the old API.
    _, task_id = self.client_create_task('hi')
    response = self.app.get(
        '/swarming/api/v1/client/task/' + task_id).json
    expected = {
      u'abandoned_ts': None,
      u'bot_id': None,
      u'bot_version': None,
      u'completed_ts': None,
      u'created_ts': str_now,
      u'durations': [],
      u'exit_codes': [],
      u'failure': False,
      u'id': u'125ecfd5c888800',
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'server_versions': [],
      u'started_ts': None,
      u'state': task_result.State.PENDING,
      u'try_number': None,
      u'user': u'unknown',
    }
    self.assertEqual(expected, response)
    self.assertEqual('00', task_id[-2:])

    # No bot started yet.
    run_id = task_id[:-2] + '01'
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
      u'id': u'125ecfd5c888801',
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
    # Note: this is still the old API.
    _, task_id = self.client_create_task('hi')

    self.set_as_anonymous()
    self.app.get('/swarming/api/v1/client/task/' + task_id, status=403)
    self.assertEqual('00', task_id[-2:])

  def test_get_task_output(self):
    # Note: this is still the old API.
    self.client_create_task('hi')

    self.set_as_bot()
    token, _ = self._bot_token()
    res = self.bot_poll()
    task_id = res['manifest']['task_id']
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': u'résult string',
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
    run_id = task_id[:-2] + '01'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % task_id).json
    self.assertEqual({'output': u'résult string'}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % run_id).json
    self.assertEqual({'output': u'résult string'}, response)

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
    _, task_id = self.client_create_task('hi')
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % task_id).json
    self.assertEqual({'output': None}, response)

    run_id = task_id[:-2] + '01'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/0' % run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_get_task_output_all(self):
    # Note: this is still the old API.
    self.client_create_task('hi')

    self.set_as_bot()
    token, _ = self._bot_token()
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
    run_id = task_id[:-2] + '01'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id).json
    self.assertEqual({'outputs': [u'result string', u'bar']}, response)
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % run_id).json
    self.assertEqual({'outputs': [u'result string', u'bar']}, response)

  def test_get_task_output_all_empty(self):
    _, task_id = self.client_create_task('hi')
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % task_id).json
    self.assertEqual({'outputs': []}, response)

    run_id = task_id[:-2] + '01'
    response = self.app.get(
        '/swarming/api/v1/client/task/%s/output/all' % run_id, status=404).json
    self.assertEqual({u'error': u'Task not found'}, response)

  def test_get_task_request(self):
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    _, task_id = self.client_create_task('hi')
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
        u'io_timeout_secs': 1200,
      },
      u'properties_hash': u'f0cc8a33cfaf3172e1c92400c7666ceae094e68f',
      u'tags': [],
      u'user': u'unknown',
    }
    self.assertEqual(expected, response)

  def test_api_bots(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)
    bot.put()

    actual = self.app.get('/swarming/api/v1/client/bots', status=200).json
    expected = {
      u'items': [
        {
          u'created_ts': u'2000-01-02 03:04:05',
          u'dimensions': {u'foo': u'bar'},
          u'external_ip': u'8.8.4.4',
          u'hostname': u'localhost',
          u'id': u'id1',
          u'internal_ip': u'127.0.0.1',
          u'is_dead': False,
          u'last_seen_ts': u'2000-01-02 03:04:05',
          u'quarantined': False,
          u'task': None,
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

    bot = bot_management.tag_bot_seen(
        'id2', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)
    bot.put()

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
    expected['items'][0]['id'] = u'id2'
    self.assertEqual(expected, actual)

  def test_api_bot(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)
    bot = bot_management.tag_bot_seen(
        'id1', 'localhost', '127.0.0.1', '8.8.4.4', {'foo': 'bar'}, '123456789',
        False)
    bot.put()

    actual = self.app.get('/swarming/api/v1/client/bot/id1', status=200).json
    expected = {
      u'created_ts': u'2000-01-02 03:04:05',
      u'dimensions': {u'foo': u'bar'},
      u'external_ip': u'8.8.4.4',
      u'hostname': u'localhost',
      u'id': u'id1',
      u'internal_ip': u'127.0.0.1',
      u'is_dead': False,
      u'last_seen_ts': u'2000-01-02 03:04:05',
      u'quarantined': False,
      u'task': None,
      u'version': u'123456789',
    }
    self.assertEqual(expected, actual)

  def test_api_bot_tasks_empty(self):
    self.set_as_privileged_user()
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
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
    now = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
    self.mock_now(now)

    self.set_as_bot()
    self.client_create_task('hi')
    token, _ = self._bot_token()
    res = self.bot_poll()
    self._complete_task(token, res['manifest']['task_id'])

    self.mock(random, 'getrandbits', lambda _: 0x55)
    self.client_create_task('ho')
    token, _ = self._bot_token()
    res = self.bot_poll()
    self._complete_task(token, res['manifest']['task_id'])

    self.set_as_privileged_user()
    actual = self.app.get('/swarming/api/v1/client/bot/bot1/tasks?limit=1').json
    expected = {
      u'limit': 1,
      u'now': unicode(now.strftime(utils.DATETIME_FORMAT)),
      u'items': [
        {
          u'abandoned_ts': None,
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': u'2000-01-02 03:04:05',
          u'durations': [0.1],
          u'exit_codes': [1],
          u'failure': True,
          u'id': u'dc709e90885501',
          u'internal_failure': False,
          u'modified_ts': u'2000-01-02 03:04:05',
          u'server_versions': [u'default-version'],
          u'started_ts': u'2000-01-02 03:04:05',
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
      u'now': unicode(now.strftime(utils.DATETIME_FORMAT)),
      u'items': [
        {
          u'abandoned_ts': None,
          u'bot_id': u'bot1',
          u'bot_version': self.bot_version,
          u'completed_ts': u'2000-01-02 03:04:05',
          u'durations': [0.1],
          u'exit_codes': [1],
          u'failure': True,
          u'id': u'dc709e90888801',
          u'internal_failure': False,
          u'modified_ts': u'2000-01-02 03:04:05',
          u'server_versions': [u'default-version'],
          u'started_ts': u'2000-01-02 03:04:05',
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
      'bot_version': bot_management.get_slave_version('http://localhost'),
    }
    self.assertEqual(expected, actual)


class OldClientApiTest(AppTestBase):
  def setUp(self):
    super(OldClientApiTest, self).setUp()
    # Client API test cases run by default as user.
    self.set_as_user()

  def _check_task(self, task, priority):
    # The value is using both timestamp and random value, so it is not
    # deterministic by definition.
    task_id = task['test_keys'][0].pop('test_key')
    self.assertTrue(int(task_id, 16))
    self.assertEqual('00', task_id[-2:])
    expected = {
      u'priority': priority,
      u'test_case_name': u'hi',
      u'test_keys': [
        {
          u'config_name': u'hi',
          u'instance_index': 0,
          u'num_instances': 1,
        },
      ],
    }
    self.assertEqual(expected, task)

  def test_add_task_admin(self):
    # Admins can trigger high priority tasks.
    self.set_as_admin()
    task, _ = self.client_create_task('hi')
    self._check_task(task, 10)

  def test_add_task_bot(self):
    # The bot has access to use high priority. By default on dev server
    # localhost is whitelisted as a bot.
    self.set_as_bot()
    task, _ = self.client_create_task('hi')
    self._check_task(task, 10)

  def test_add_task_and_list_user(self):
    task, _ = self.client_create_task('hi')
    # Since the priority 10 was too high for a user (not an admin, neither a
    # bot), it was reset to 100.
    self._check_task(task, 100)

  def testMatchingTestCasesHandler(self):
    # Ensure that matching works even when the datastore is not being
    # consistent.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=0)
    self.testbed.init_datastore_v3_stub(
        require_indexes=True, consistency_policy=policy, root_path=self.APP_DIR)

    # Mock out all the authentication, since it doesn't work with the server
    # being only eventually consistent (but it is ok for the machines to take
    # a while to appear).
    self.mock(user_manager, 'IsWhitelistedMachine', lambda *_arg: True)

    # Test when no matching tests.
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': 'tc'},
        expect_errors=True)
    self.assertResponse(response, '404 Not Found', '[]')

    # Test with a single matching runner.
    result_summary, _run_result = CreateRunner('tc')
    response = self.app.get(
        '/get_matching_test_cases',
        {'name': 'tc'})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)

    # Test with a multiple matching runners.
    result_summary_2, _run_result = CreateRunner(
        machine_id='foo', exit_codes='0', results='Rock on')

    response = self.app.get(
        '/get_matching_test_cases',
        {'name': 'tc'})
    self.assertEqual('200 OK', response.status)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary.key),
        response.body)
    self.assertIn(
        task_common.pack_result_summary_key(result_summary_2.key),
        response.body)

  def testGetResultHandler(self):
    self.set_as_privileged_user()

    # Test when no matching key
    response = self.app.get('/get_result', {'r': 'fake_key'}, status=400)

    # Create test and runner.
    result_summary, run_result = CreateRunner(
        machine_id='bot1',
        exit_codes='0',
        results='\xe9 Invalid utf-8 string')

    # Valid key.
    packed = task_common.pack_result_summary_key(result_summary.key)
    response = self.app.get('/get_result', {'r': packed})
    self.assertEqual('200 OK', response.status)
    try:
      results = json.loads(response.body)
    except (ValueError, TypeError), e:
      self.fail(e)
    # TODO(maruel): Stop using the DB directly in HTTP handlers test.
    self.assertEqual(
        ','.join(map(str, run_result.exit_codes)),
        ''.join(results['exit_codes']))
    self.assertEqual(result_summary.bot_id, results['machine_id'])
    expected_result_string = '\n'.join(
        o.decode('utf-8', 'replace')
        for o in result_summary.get_outputs())
    self.assertEqual(expected_result_string, results['output'])
    self.assertEqual(u'\ufffd Invalid utf-8 string', results['output'])

  def test_convert_test_case(self):
    self.set_as_bot()
    data = {
      'configurations': [
        {
          'config_name': 'ignored',
          'deadline_to_run': 63,
          'dimensions': {
            'OS': 'Windows-3.1.1',
            'hostname': 'localhost',
          },
          # Do not block sharded requests, simply ignore the sharding request
          # and run it as a single shard.
          'num_instances': 23,
          'priority': 50,
        },
      ],
      'data': [
        ('http://localhost/foo', 'foo.zip'),
        ('http://localhost/bar', 'bar.zip'),
      ],
      'env_vars': {
        'foo': 'bar',
        'joe': '2',
      },
      'requestor': 'Jesus',
      'test_case_name': 'Request name',
      'tests': [
        {
          'action': ['command1', 'arg1'],
          'hard_time_out': 66.1,
          'io_time_out': 68.1,
          'test_name': 'very ignored',
        },
        {
          'action': ['command2', 'arg2'],
          'test_name': 'very ignored but must be different',
          'hard_time_out': 60000000,
          'io_time_out': 60000000,
        },
      ],
    }
    actual = handlers_api.convert_test_case(json.dumps(data))
    expected = {
      'name': u'Request name',
      'user': u'Jesus',
      'properties': {
        'commands': [
          [u'command1', u'arg1'],
          [u'command2', u'arg2'],
        ],
        'data': [
          [u'http://localhost/foo', u'foo.zip'],
          [u'http://localhost/bar', u'bar.zip'],
        ],
        'dimensions': {u'OS': u'Windows-3.1.1', u'hostname': u'localhost'},
        'env': {u'foo': u'bar', u'joe': u'2'},
        'execution_timeout_secs': 66,
        'io_timeout_secs': 68,
      },
      'priority': 50,
      'scheduling_expiration_secs': 63,
      'tags': [],
    }
    self.assertEqual(expected, actual)

  def testResultHandlerNotDone(self):
    result_summary, _run_result = CreateRunner(machine_id='bot1')

    packed = task_common.pack_result_summary_key(result_summary.key)
    resp = self.app.get('/get_result?r=%s' % packed, status=200)
    expected = {
      u'config_instance_index': 0,
      u'exit_codes': None,
      u'machine_id': u'bot1',
      u'machine_tag': u'bot1',
      u'num_config_instances': 1,
      u'output': None,
    }
    self.assertEqual(expected, resp.json)

  def testResultHandler(self):
    # TODO(maruel): Stop using the DB directly.
    self.set_as_bot()
    self.client_create_task('hi')
    token, _ = self._bot_token()
    res = self.bot_poll()
    params = {
      'command_index': 0,
      'duration': 0.1,
      'exit_code': 1,
      'id': 'bot1',
      'output': 'result string',
      'output_chunk_start': 0,
      'task_id': res['manifest']['task_id'],
    }
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

    self.set_as_privileged_user()
    resp = self.app.get(
        '/get_result?r=%s' % res['manifest']['task_id'], status=200)
    expected = {
      u'config_instance_index': 0,
      u'exit_codes': u'1',
      u'machine_id': u'bot1',
      u'machine_tag': u'bot1',
      u'num_config_instances': 1,
      u'output': u'result string',
    }
    self.assertEqual(expected, resp.json)

  def testTestRequest(self):
    # Ensure that a test request fails without a request.
    response = self.app.post('/test', expect_errors=True)
    self.assertResponse(response, '400 Bad Request',
                        '400 Bad Request\n\nThe server could not comply with '
                        'the request since it is either malformed or otherwise '
                        'incorrect.\n\n No request parameter found.  ')

    # Ensure invalid requests are rejected.
    request = {
        "configurations": [{
            "config_name": "win",
            "dimensions": {"os": "win"},
        }],
        "test_case_name": "",
        "tests":[{
            "action": ["python", "run_test.py"],
            "test_name": "Run Test",
        }],
    }
    response = self.app.post('/test', {'request': json.dumps(request)},
                             expect_errors=True)
    self.assertEquals('400 Bad Request', response.status)

    # Ensure that valid requests are accepted.
    request['test_case_name'] = 'test_case'
    response = self.app.post('/test', {'request': json.dumps(request)})
    self.assertEquals('200 OK', response.status)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
