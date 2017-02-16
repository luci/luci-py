#!/usr/bin/env python
# coding: utf-8
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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
from server import bot_auth
from server import bot_code
from server import bot_groups_config
from server import bot_management
from server import stats


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
    params = {
      'dimensions': {
        'id': ['id1'],
        'pool': ['default'],
      },
      'state': {u'running_time': 0, u'sleep_streak': 0},
      'version': '1',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', params=params).json
    self.assertEqual(
        [
          u'bot_group_cfg',
          u'bot_group_cfg_version',
          u'bot_version',
          u'server_version',
        ],
        sorted(response))
    self.assertEqual({u'dimensions': {}}, response['bot_group_cfg'])
    self.assertEqual('default', response['bot_group_cfg_version'])
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
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', params={}).json
    self.assertEqual(
        [
          u'bot_group_cfg',
          u'bot_group_cfg_version',
          u'bot_version',
          u'server_version',
        ],
        sorted(response))
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
    params = {
      # Works with unknown items but logs an error. This permits catching typos.
      'foo': 1,
      'dimensions': {
        'id': ['bot1'],
        'pool': ['default'],
      },
      'state': {
        'bar': 2,
        'ip': '127.0.0.1',
      },
      'version': '123',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', params=params).json
    self.assertEqual(
        [
          u'bot_group_cfg',
          u'bot_group_cfg_version',
          u'bot_version',
          u'server_version',
        ],
        sorted(response))
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
    params = self.do_handshake()
    params.pop('dimensions')
    params.pop('state')
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    params = self.do_handshake()
    old_version = params['version']
    params['version'] = 'badversion'
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_sleep(self):
    # A bot polls, gets nothing.
    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # Sleep again
    params['state']['sleep_streak'] += 1
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

  def test_poll_update(self):
    params = self.do_handshake()
    old_version = params['version']
    params['version'] = 'badversion'
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'update',
      u'version': old_version,
    }
    self.assertEqual(expected, response)

  def test_poll_bot_group_config_change(self):
    params = self.do_handshake()
    params['state']['bot_group_cfg_version'] = 'badversion'
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'bot_restart',
      u'message': u'Restarting to pick up new bots.cfg config',
    }
    self.assertEqual(expected, response)

  def test_poll_restart(self):
    def mock_should_restart_bot(bot_id, state):
      self.assertEqual('bot1', bot_id)
      expected_state = {
        'bot_group_cfg_version': 'default',
        'running_time': 1234.0,
        'sleep_streak': 0,
        'started_ts': 1410990411.111,
      }
      self.assertEqual(expected_state, state)
      return True, 'Mocked restart message'
    self.mock(bot_management, 'should_restart_bot', mock_should_restart_bot)

    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'host_reboot',
      u'message': u'Mocked restart message',
    }
    self.assertEqual(expected, response)

  def test_poll_task_raw(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    params = self.do_handshake()
    # Enqueue a task.
    _, task_id = self.client_create_task_raw()
    self.assertEqual('0', task_id[-1])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'caches': [],
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'path': None,
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
        u'dimensions': {
          u'os': u'Amiga',
          u'pool': u'default',
        },
        u'env': {},
        u'extra_args': [],
        u'grace_period': 30,
        u'hard_timeout': 3600,
        u'host': u'http://localhost:8080',
        u'isolated': None,
        u'secret_bytes': None,
        u'io_timeout': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
        u'service_account': u'none',
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)
    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.],
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
    self.assertEqual(expected, response)

  def test_poll_task_with_bot_service_account(self):
    params = self.do_handshake()

    _, task_id = self.client_create_task_raw(service_account_token='bot')
    self.assertEqual('0', task_id[-1])
    task_id = task_id[:-1] + '1'

    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'caches': [],
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'path': None,
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
        u'dimensions': {
          u'os': u'Amiga',
          u'pool': u'default',
        },
        u'env': {},
        u'extra_args': [],
        u'grace_period': 30,
        u'hard_timeout': 3600,
        u'host': u'http://localhost:8080',
        u'isolated': None,
        u'secret_bytes': None,
        u'io_timeout': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
        u'service_account': u'bot',
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)

  def test_poll_task_with_caches(self):
    params = self.do_handshake()

    _, task_id = self.client_create_task_raw({
      'caches': [{
        'name': 'git_infra',
        'path': 'git_cache',
      }],
    })
    self.assertEqual('0', task_id[-1])
    task_id = task_id[:-1] + '1'

    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'caches': [{
          u'name': u'git_infra',
          u'path': u'git_cache',
        }],
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'path': None,
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
        u'dimensions': {
          u'os': u'Amiga',
          u'pool': u'default',
        },
        u'env': {},
        u'extra_args': [],
        u'grace_period': 30,
        u'hard_timeout': 3600,
        u'host': u'http://localhost:8080',
        u'isolated': None,
        u'io_timeout': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
        u'secret_bytes': None,
        u'service_account': u'none',
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)

  def test_poll_conflicting_dimensions(self):
    params = self.do_handshake()
    self.assertEqual(params['dimensions']['pool'], ['default'])

    cfg = bot_groups_config.BotGroupConfig(
        version='default',
        require_luci_machine_token=False,
        require_service_account=None,
        ip_whitelist=None,
        owners=None,
        dimensions={u'pool': [u'server-side']},
        bot_config_script=None,
        bot_config_script_content=None)
    self.mock(bot_auth, 'validate_bot_id_and_fetch_config',
              lambda *args, **kwargs: cfg)

    actions = []
    self.mock(stats, 'add_entry', lambda **kwargs: actions.append(kwargs))

    # Bot sends 'default' pool, but server config defined it as 'server-side'.
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # 'server-side' was actually used.
    self.assertEqual([{
        'action': 'bot_active',
        'bot_id': u'bot1',
        'dimensions': {
            u'id': [u'bot1'],
            u'os': [u'Amiga'],
            u'pool': [u'server-side'],
        },
    }], actions)

  def test_poll_extra_bot_config(self):
    cfg = bot_groups_config.BotGroupConfig(
        version='default',
        require_luci_machine_token=False,
        require_service_account=None,
        ip_whitelist=None,
        owners=None,
        dimensions={},
        bot_config_script='foo.py',
        bot_config_script_content='print "Hi";import sys; sys.exit(1)')
    self.mock(bot_auth, 'validate_bot_id_and_fetch_config',
              lambda *args, **kwargs: cfg)
    params = self.do_handshake()
    self.assertEqual(
        u'print "Hi";import sys; sys.exit(1)', params['bot_config'])

  def test_complete_task_isolated(self):
    # Successfully poll a task.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    # A bot polls, gets a task, updates it, completes it.
    params = self.do_handshake()
    # Enqueue a task.
    _, task_id = self.client_create_task_isolated()
    self.assertEqual('0', task_id[-1])
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'caches': [],
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'path': None,
            u'version': u'git_revision:deadbeef',
          },
          u'packages': [{
            u'package_name': u'rm',
            u'path': u'bin',
            u'version': u'git_revision:deadbeef',
          }],
          u'server': u'https://chrome-infra-packages.appspot.com',
        },
        u'command': None,
        u'dimensions': {
          u'os': u'Amiga',
          u'pool': u'default',
        },
        u'env': {},
        u'extra_args': [],
        u'hard_timeout': 3600,
        u'grace_period': 30,
        u'host': u'http://localhost:8080',
        u'isolated': {
          u'input': u'0123456789012345678901234567890123456789',
          u'server': u'http://localhost:1',
          u'namespace': u'default-gzip',
        },
        u'secret_bytes': None,
        u'io_timeout': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
        u'service_account': u'none',
        u'task_id': task_id,
      },
    }
    self.assertEqual(expected, response)

    # Complete the isolated task.
    params = {
      'cost_usd': 0.1,
      'duration': 3.,
      'bot_overhead': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'isolated_stats': {
        'download': {
          'duration': 0.1,
          'initial_number_items': 10,
          'initial_size': 1000,
          'items_cold': '',
          'items_hot': '',
        },
        'upload': {
          'duration': 0.1,
          'items_cold': '',
          'items_hot': '',
        },
      },
      'output': base64.b64encode('Ahahah'),
      'output_chunk_start': 0,
      'outputs_ref': {
        u'isolated': u'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        u'isolatedserver': u'http://localhost:1',
        u'namespace': u'default-gzip',
      },
      'cipd_pins': {
        u'client_package': {
          u'package_name': u'infra/tools/cipd/windows-amd64',
          u'version': u'deadbeef'*5,
        },
        u'packages': [{
          u'package_name': u'rm',
          u'path': u'bin',
          u'version': u'badc0fee'*5,
        }]
      },
      'task_id': task_id,
    }
    response = self.post_json('/swarming/api/v1/bot/task_update', params)
    self.assertEqual({u'must_stop': False, u'ok': True}, response)

    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'completed_ts': str_now,
      u'costs_usd': [0.1],
      u'created_ts': str_now,
      u'duration': 3.,
      u'exit_code': u'0',
      u'failure': False,
      u'internal_failure': False,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'outputs_ref': {
        u'isolated': u'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        u'isolatedserver': u'http://localhost:1',
        u'namespace': u'default-gzip',
      },
      u'cipd_pins': {
        u'client_package': {
          u'package_name': u'infra/tools/cipd/windows-amd64',
          u'version': u'deadbeef'*5,
        },
        u'packages': [{
          u'package_name': u'rm',
          u'path': u'bin',
          u'version': u'badc0fee'*5,
        }]
      },
      u'run_id': u'5cee488008811',
      u'server_versions': [u'v1a'],
      u'started_ts': str_now,
      u'state': u'COMPLETED',
      u'task_id': u'5cee488008811',
      u'try_number': u'1',
    }
    self.assertEqual(expected, response)

  def test_poll_not_enough_time(self):
    # Make sure there's a task that we don't get.
    _, task_id = self.client_create_task_raw()
    self.assertEqual('0', task_id[-1])
    params = self.do_handshake()
    params['state']['lease_expiration_ts'] = 0
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    params = self.do_handshake()
    params['state']['lease_expiration_ts'] = (
        int(utils.time_time()) + 3600 + 1200 + 3 * 30 + 10 + 1)
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    # Convert TaskResultSummary reference to TaskRunResult.
    task_id = task_id[:-1] + '1'
    expected = {
      u'cmd': u'run',
      u'manifest': {
        u'bot_id': u'bot1',
        u'caches': [],
        u'cipd_input': {
          u'client_package': {
            u'package_name': u'infra/tools/cipd/${platform}',
            u'path': None,
            u'version': u'git_revision:deadbeef',
          },
          u'packages': [{
            u'package_name': u'rm',
            u'path': u'bin',
            u'version': u'git_revision:deadbeef',
          }],
          u'server': u'https://chrome-infra-packages.appspot.com',
        },
        u'command': None,
        u'dimensions': {
          u'os': u'Amiga',
          u'pool': u'default',
        },
        u'env': {},
        u'extra_args': [],
        u'hard_timeout': 3600,
        u'grace_period': 30,
        u'host': u'http://localhost:8080',
        u'isolated': {
          u'input': u'0123456789012345678901234567890123456789',
          u'server': u'http://localhost:1',
          u'namespace': u'default-gzip',
        },
        u'secret_bytes': None,
        u'io_timeout': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
        u'service_account': u'none',
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
    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)

    # The bot fails somehow.
    params2 = self.do_handshake()
    params2['event'] = 'bot_error'
    params2['message'] = 'for the worst'
    response = self.post_json('/swarming/api/v1/bot/event', params2)
    self.assertEqual({}, response)

    # A bot error currently does not result in permanent quarantine. It will
    # eventually.
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    self.assertTrue(response.pop(u'duration'))
    expected = {
      u'cmd': u'sleep',
      u'quarantined': False,
    }
    self.assertEqual(expected, response)
    expected = [
      {
        'message':
          u'for the worst\n\nhttps://None/restricted/bot/bot1',
        'source': 'bot',
      },
    ]
    self.assertEqual(expected, [e[1] for e in errors])

  def test_bot_event(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    params = self.do_handshake()
    for e in handlers_bot.BotEventHandler.ALLOWED_EVENTS:
      if e == 'bot_error':
        # This one is tested specifically since it also logs an error message.
        continue
      params['event'] = e
      params['message'] = 'for the best'
      response = self.post_json('/swarming/api/v1/bot/event', params)
      self.assertEqual({}, response)

    # TODO(maruel): Replace with client api to query last BotEvent.
    actual = [
        e.to_dict() for e in bot_management.get_events_query('bot1', True)]
    expected = [
      {
        'authenticated_as': u'bot:whitelisted-ip',
        'dimensions': {
          u'id': [u'bot1'],
          u'os': [u'Amiga'],
          u'pool': [u'default'],
        },
        'event_type': unicode(e),
        'external_ip': u'192.168.2.2',
        'lease_id': None,
        'lease_expiration_ts': None,
        'machine_type': None,
        'message': u'for the best',
        'quarantined': False,
        'state': {
          u'bot_group_cfg_version': u'default',
          u'running_time': 1234.0,
          u'sleep_streak': 0,
          u'started_ts': 1410990411.111,
        },
        'task_id': u'',
        'ts': now,
        'version': self.bot_version,
      } for e in reversed(handlers_bot.BotEventHandler.ALLOWED_EVENTS)
      if e != 'bot_error'
    ]
    expected.append(
      {
        'authenticated_as': u'bot:whitelisted-ip',
        'dimensions': {
          u'id': [u'bot1'],
          u'os': [u'Amiga'],
          u'pool': [u'default'],
        },
        'event_type': u'bot_connected',
        'external_ip': u'192.168.2.2',
        'lease_id': None,
        'lease_expiration_ts': None,
        'machine_type': None,
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
      })

    self.assertEqual(expected, actual)

  def test_task_complete(self):
    # Runs a task up to completion.
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    params = self.do_handshake()
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
        u'task_id': u'5cee488008811',
        u'try_number': u'1',
      }
      out.update((unicode(k), v) for k, v in kwargs.iteritems())
      return out

    def _cycle(params, expected, must_stop):
      response = self.post_json('/swarming/api/v1/bot/task_update', params)
      self.assertEqual({u'must_stop': must_stop, u'ok': True}, response)
      self.assertEqual(expected, self.client_get_results(task_id))

    # 1. Initial task update with no data.
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    task_id = response['manifest']['task_id']
    params = _params()
    response = self.post_json('/swarming/api/v1/bot/task_update', params)
    self.assertEqual({u'must_stop': False, u'ok': True}, response)
    response = self.client_get_results(task_id)
    self.assertEqual(_expected(), response)

    # 2. Task update with some output.
    params = _params(output=base64.b64encode('Oh '))
    expected = _expected()
    _cycle(params, expected, False)

    # 3. Task update with some more output.
    params = _params(output=base64.b64encode('hi'), output_chunk_start=3)
    expected = _expected()
    _cycle(params, expected, False)

    # 4. Task update with completion of the command.
    params = _params(
        duration=0.1, exit_code=23, output=base64.b64encode('Ahahah'))
    expected = _expected(
        completed_ts=str_now,
        duration=0.1,
        exit_code=u'23',
        failure=True,
        state=u'COMPLETED')
    _cycle(params, expected, False)

  def test_task_update_db_failure(self):
    # The error is caught in task_scheduler.bot_update_task().
    self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    response = self.post_json(
        '/swarming/api/v1/bot/task_update', params, status=500)
    self.assertEqual({u'error': u'Failed to update, please retry'}, response)

  def test_task_update_failure(self):
    self.client_create_task_raw(
        properties=dict(command=['python', 'runtest.py']))

    params = self.do_handshake()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    task_id = response['manifest']['task_id']

    class NewError(Exception):
      pass

    def r(*_):
      raise NewError('Sorry!')
    self.mock(ndb, 'put_multi', r)
    params = {
      'bot_overhead': 0.,
      'cost_usd': 0.1,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'isolated_stats': {
        'download': {},
      },
      'output': base64.b64encode('result string'),
      'output_chunk_start': 0,
      'task_id': task_id,
    }
    response = self.post_json(
        '/swarming/api/v1/bot/task_update', params, status=500)
    self.assertEqual({u'error': u'Sorry!'}, response)

  def test_task_failure(self):
    self.mock(random, 'getrandbits', lambda _: 0x88)
    now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(now)
    str_now = unicode(now.strftime(DATETIME_FORMAT))
    params = self.do_handshake()
    self.client_create_task_raw()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
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
    self.post_json('/swarming/api/v1/bot/task_update', params)
    response = self.client_get_results(task_id)
    expected = {
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
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
      u'run_id': u'5cee488008811',
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
    params = self.do_handshake()
    self.client_create_task_raw()
    response = self.post_json('/swarming/api/v1/bot/poll', params)
    task_id = response['manifest']['task_id']

    # Let's say it failed to start task_runner because the new bot code is
    # broken. The end result is still BOT_DIED. The big change is that it
    # doesn't need to wait for a cron job to set this status.
    params = {
      'id': params['dimensions']['id'][0],
      'message': 'Oh',
      'task_id': task_id,
    }
    self.post_json('/swarming/api/v1/bot/task_error', params)

    response = self.client_get_results(task_id)
    expected = {
      u'abandoned_ts': str_now,
      u'bot_dimensions': [
        {u'key': u'id', u'value': [u'bot1']},
        {u'key': u'os', u'value': [u'Amiga']},
        {u'key': u'pool', u'value': [u'default']},
      ],
      u'bot_id': u'bot1',
      u'bot_version': self.bot_version,
      u'costs_usd': [0.],
      u'created_ts': str_now,
      u'failure': False,
      u'internal_failure': True,
      u'modified_ts': str_now,
      u'name': u'hi',
      u'run_id': u'5cee488008811',
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

  def test_bot_code_as_bot(self):
    code = self.app.get('/bot_code')
    expected = {'config/bot_config.py', 'config/config.json'}.union(
        bot_archive.FILES)
    with zipfile.ZipFile(StringIO.StringIO(code.body), 'r') as z:
      self.assertEqual(expected, set(z.namelist()))

  def test_bot_code_without_token(self):
    self.set_as_anonymous()
    self.app.get('/bot_code', status=403)

  def test_bot_code_with_token(self):
    self.set_as_anonymous()
    tok = bot_code.generate_bootstrap_token()
    self.app.get('/bot_code?tok=%s' % tok, status=200)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL,
      format='%(levelname)-7s %(filename)s:%(lineno)3d %(message)s')
  unittest.main()
