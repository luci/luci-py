# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Base class for handlers_*_test.py"""

import base64
import os

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

import webtest

from components import auth
from components import auth_testing
from components import stats_framework
from test_support import test_case

from server import acl
from server import stats


class AppTestBase(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def setUp(self):
    super(AppTestBase, self).setUp()
    self.bot_version = None
    self.source_ip = '192.168.2.2'
    self.testbed.init_user_stub()
    self.testbed.init_search_stub()

    # By default requests in tests are coming from bot with fake IP.
    # WSGI app that implements auth REST API.
    self.auth_app = webtest.TestApp(
        auth.create_wsgi_application(debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    # Note that auth.ADMIN_GROUP != acl.ADMINS_GROUP.
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        [auth.Identity(auth.IDENTITY_USER, 'super-admin@example.com')])
    auth.bootstrap_group(
        acl.ADMINS_GROUP,
        [auth.Identity(auth.IDENTITY_USER, 'admin@example.com')])
    auth.bootstrap_group(
        acl.PRIVILEGED_USERS_GROUP,
        [auth.Identity(auth.IDENTITY_USER, 'priv@example.com')])
    auth.bootstrap_group(
        acl.USERS_GROUP,
        [auth.Identity(auth.IDENTITY_USER, 'user@example.com')])
    auth.bootstrap_group(
        acl.BOTS_GROUP,
        [auth.Identity(auth.IDENTITY_BOT, self.source_ip)])

    self.mock(stats_framework, 'add_entry', self._parse_line)

  def _parse_line(self, line):
    # pylint: disable=W0212
    actual = stats._parse_line(line, stats._Snapshot(), {}, {}, {})
    self.assertEqual(True, actual, line)

  def set_as_anonymous(self):
    """Removes all IPs from the whitelist."""
    self.testbed.setup_env(USER_EMAIL='', overwrite=True)
    auth.ip_whitelist_key(auth.BOTS_IP_WHITELIST).delete()
    auth_testing.reset_local_state()

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
    auth.bootstrap_ip_whitelist(auth.BOTS_IP_WHITELIST, [self.source_ip])

  # Web or generic

  def get_xsrf_token(self):
    """Gets the generic XSRF token for web clients."""
    resp = self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={'X-XSRF-Token-Request': '1'}).json
    return resp['xsrf_token'].encode('ascii')

  def post_with_token(self, url, params, token, **kwargs):
    """Does an HTTP POST with a JSON API and a XSRF token."""
    return self.app.post_json(
        url, params=params, headers={'X-XSRF-Token': token}, **kwargs).json

  # Bot

  def get_bot_token(self, bot='bot1'):
    """Gets the XSRF token for bot after handshake."""
    headers = {'X-XSRF-Token-Request': '1'}
    params = {
      'dimensions': {
        'id': [bot],
        'os': ['Amiga'],
      },
      'state': {
        'running_time': 1234.0,
        'sleep_streak': 0,
        'started_ts': 1410990411.111,
      },
      'version': '123',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake',
        headers=headers,
        params=params).json
    token = response['xsrf_token'].encode('ascii')
    self.bot_version = response['bot_version']
    params['version'] = self.bot_version
    return token, params

  def bot_poll(self, bot='bot1'):
    """Simulates a bot that polls for task."""
    token, params = self.get_bot_token(bot)
    return self.post_with_token('/swarming/api/v1/bot/poll', params, token)

  def bot_complete_task(self, token, **kwargs):
    params = {
      'cost_usd': 0.1,
      'duration': 0.1,
      'exit_code': 0,
      'id': 'bot1',
      'output': base64.b64encode(u'rÉsult string'.encode('utf-8')),
      'output_chunk_start': 0,
      'task_id': None,
    }
    params.update(kwargs)
    response = self.post_with_token(
        '/swarming/api/v1/bot/task_update', params, token)
    self.assertEqual({u'ok': True}, response)

  def bot_run_task(self):
    token, _ = self.get_bot_token()
    res = self.bot_poll()
    task_id = res['manifest']['task_id']
    self.bot_complete_task(token, task_id=task_id)
    return task_id

  # Client

  def get_client_token(self):
    """Gets the XSRF token for client after handshake."""
    headers = {'X-XSRF-Token-Request': '1'}
    params = {}
    response = self.app.post_json(
        '/swarming/api/v1/client/handshake',
        headers=headers,
        params=params).json
    return response['xsrf_token'].encode('ascii')

  def client_create_task_raw(self, properties=None, **kwargs):
    """Creates a TaskRequest via the client API."""
    token = self.get_client_token()
    params = {
      'name': 'hi',
      'priority': 10,
      'properties': {
        'commands': [['python', 'run_test.py']],
        'data': [],
        'dimensions': {'os': 'Amiga'},
        'env': {},
        'execution_timeout_secs': 3600,
        'io_timeout_secs': 1200,
      },
      'scheduling_expiration_secs': 24*60*60,
      'tags': [],
      'user': 'joe@localhost',
    }
    params.update(kwargs)
    params['properties'].update(properties or {})
    response = self.post_with_token(
        '/swarming/api/v1/client/request', params, token)
    return response, response['task_id']

  def client_get_results(self, task_id):
    return self.app.get(
        '/swarming/api/v1/client/task/%s' % task_id).json
