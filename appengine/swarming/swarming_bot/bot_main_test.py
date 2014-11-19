#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

# Import them first before manipulating sys.path to ensure they can load fine.
import bot_main
import logging_utils
import os_utilities
import xsrf_client
from utils import net
from utils import zip_package

THIS_FILE = os.path.abspath(__file__)
BASE_DIR = os.path.dirname(THIS_FILE)
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

CLIENT_TESTS = os.path.join(ROOT_DIR, '..', '..', 'client', 'tests')
sys.path.insert(0, CLIENT_TESTS)

import bot
# Creates a server mock for functions in net.py.
import net_utils


# Access to a protected member XX of a client class - pylint: disable=W0212


class TestBotMain(net_utils.TestCase):
  def setUp(self):
    super(TestBotMain, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='bot_main')
    self.old_cwd = os.getcwd()
    os.chdir(self.root_dir)
    self.server = xsrf_client.XsrfRemote('https://localhost:1/')
    self.attributes = {
      'dimensions': {'foo', 'bar'},
      'id': 'localhost',
      'version': '123',
    }
    self.mock(zip_package, 'generate_version', lambda: '123')
    self.bot = bot.Bot(self.server, self.attributes, 'version1', self.root_dir)
    self.mock(self.bot, 'post_error', self.fail)
    self.mock(self.bot, 'restart', self.fail)

  def tearDown(self):
    os.chdir(self.old_cwd)
    shutil.rmtree(self.root_dir)
    super(TestBotMain, self).tearDown()

  def test_get_dimensions(self):
    self.assertEqual(
        ['cores', 'cpu', 'disk', 'gpu', 'hostname', 'id', 'os', 'ram'],
        sorted(bot_main.get_dimensions()))

  def test_generate_version(self):
    self.assertEqual('123', bot_main.generate_version())

  def test_get_state(self):
    self.mock(time, 'time', lambda: 126.0)
    expected = os_utilities.get_state()
    expected['sleep_streak'] = 12
    self.assertEqual(expected, bot_main.get_state(12))

  def test_post_error_task(self):
    self.mock(time, 'time', lambda: 126.0)
    self.mock(logging, 'error', lambda *_: None)
    self.mock(bot_main, 'get_remote', lambda: self.server)
    expected_attribs = bot_main.get_attributes()
    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {
              'data': expected_attribs,
              'headers': {'X-XSRF-Token-Request': '1'},
            },
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/task_error/23',
            {
              'data': {
                'id': expected_attribs['dimensions']['id'][0],
                'message': 'error',
                'task_id': 23,
              },
              'headers': {'X-XSRF-Token': 'token'},
            },
            {},
          ),
        ])
    botobj = bot_main.get_bot()
    bot_main.post_error_task(botobj, 'error', 23)

  def test_run_bot(self):
    # Test the run_bot() loop.
    self.mock(time, 'time', lambda: 126.0)
    class Foo(Exception):
      pass

    expected_attribs = bot_main.get_attributes()

    def poll_server(botobj):
      sleep_streak = botobj.state['sleep_streak']
      self.assertEqual(botobj.remote, self.server)
      if sleep_streak == 5:
        raise Exception('Jumping out of the loop')
      return False
    self.mock(bot_main, 'poll_server', poll_server)

    def post_error(botobj, e):
      self.assertEqual(self.server, botobj._remote)
      self.assertEqual('Jumping out of the loop', e)
      # Necessary to get out of the loop.
      raise Foo()
    self.mock(bot.Bot, 'post_error', post_error)

    self.mock(bot_main, 'get_remote', lambda: self.server)

    self.expected_requests(
        [
          ('https://localhost:1/server_ping', {}, None, None),
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {
              'data': expected_attribs,
              'headers': {'X-XSRF-Token-Request': '1'},
            },
            {'xsrf_token': 'token'},
          ),
        ])

    with self.assertRaises(Foo):
      bot_main.run_bot(None)

  def test_poll_server_sleep(self):
    slept = []
    self.mock(time, 'sleep', slept.append)
    self.mock(bot_main, 'run_manifest', self.fail)
    self.mock(bot_main, 'update_bot', self.fail)

    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/poll',
            {
              'data': self.attributes,
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'sleep',
              'duration': 1.24,
            },
          ),
        ])
    self.assertFalse(bot_main.poll_server(self.bot))
    self.assertEqual([1.24], slept)

  def test_poll_server_run(self):
    manifest = []
    self.mock(time, 'sleep', self.fail)
    self.mock(bot_main, 'run_manifest', lambda *args: manifest.append(args))
    self.mock(bot_main, 'update_bot', self.fail)

    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/poll',
            {
              'data': self.bot._attributes,
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'run',
              'manifest': {'foo': 'bar'},
            },
          ),
        ])
    self.assertTrue(bot_main.poll_server(self.bot))
    expected = [(self.bot, {'foo': 'bar'})]
    self.assertEqual(expected, manifest)

  def test_poll_server_update(self):
    update = []
    self.mock(time, 'sleep', self.fail)
    self.mock(bot_main, 'run_manifest', self.fail)
    self.mock(bot_main, 'update_bot', lambda *args: update.append(args))

    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/poll',
            {
              'data': self.attributes,
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'update',
              'version': '123',
            },
          ),
        ])
    self.assertTrue(bot_main.poll_server(self.bot))
    self.assertEqual([(self.bot, '123')], update)

  def test_poll_server_restart(self):
    restart = []
    self.mock(time, 'sleep', self.fail)
    self.mock(bot_main, 'run_manifest', self.fail)
    self.mock(bot_main, 'update_bot', self.fail)
    self.mock(self.bot, 'restart', lambda *args: restart.append(args))

    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/poll',
            {
              'data': self.attributes,
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'restart',
              'message': 'Please die now',
            },
          ),
        ])
    self.assertTrue(bot_main.poll_server(self.bot))
    self.assertEqual([('Please die now',)], restart)

  def _mock_popen(self, returncode, url='https://localhost:1'):
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      def __init__(self2, cmd, cwd, stdout, stderr):
        self2.returncode = None
        expected = [
          sys.executable, THIS_FILE, 'local_test_runner',
          '-S', url, '-f', os.path.join(self.root_dir, 'work', 'test_run.json'),
        ]
        self.assertEqual(expected, cmd)
        self.assertEqual(bot_main.ROOT_DIR, cwd)
        self.assertEqual(subprocess.PIPE, stdout)
        self.assertEqual(subprocess.STDOUT, stderr)

      def communicate(self2):
        self2.returncode = returncode
        return 'foo', None
    self.mock(subprocess, 'Popen', Popen)

  def test_run_manifest(self):
    self.mock(bot_main, 'post_error_task', lambda *args: self.fail(args))
    def on_after_task(botobj, failure, internal_failure):
      self.assertEqual(self.attributes['dimensions'], botobj.dimensions)
      self.assertEqual(False, failure)
      self.assertEqual(False, internal_failure)
    self.mock(bot_main, 'on_after_task', on_after_task)
    self._mock_popen(0, url='https://localhost:3')

    params = {
      'host': 'https://localhost:3',
      'task_id': 24,
    }
    bot_main.run_manifest(self.bot, params)

  def test_run_manifest_task_failure(self):
    self.mock(bot_main, 'post_error_task', self.fail)
    def on_after_task(_bot, failure, internal_failure):
      self.assertEqual(True, failure)
      self.assertEqual(False, internal_failure)
    self.mock(bot_main, 'on_after_task', on_after_task)
    self._mock_popen(bot_main.TASK_FAILED)

    bot_main.run_manifest(self.bot, {'task_id': 24})

  def test_run_manifest_internal_failure(self):
    posted = []
    self.mock(bot_main, 'post_error_task', lambda *args: posted.append(args))
    def on_after_task(_bot, failure, internal_failure):
      self.assertEqual(False, failure)
      self.assertEqual(True, internal_failure)
    self.mock(bot_main, 'on_after_task', on_after_task)
    self._mock_popen(1)

    bot_main.run_manifest(self.bot, {'task_id': 24})
    expected = [(self.bot, 'Execution failed, internal error:\nfoo', 24)]
    self.assertEqual(expected, posted)

  def test_run_manifest_exception(self):
    posted = []
    self.mock(bot_main, 'post_error_task', lambda *args: posted.append(args))
    def on_after_task(_bot, failure, internal_failure):
      self.assertEqual(False, failure)
      self.assertEqual(True, internal_failure)
    self.mock(bot_main, 'on_after_task', on_after_task)
    def raiseOSError(*_a, **_k):
      raise OSError('Dang')
    self.mock(subprocess, 'Popen', raiseOSError)

    bot_main.run_manifest(self.bot, {'task_id': 24})
    expected = [(self.bot, 'Internal exception occured: Dang', 24)]
    self.assertEqual(expected, posted)

  def test_update_bot_linux(self):
    self.mock(sys, 'platform', 'linux2')

    # In a real case 'update_bot' never exits and doesn't call 'post_error'.
    # Under the test however forever-blocking calls finish, and post_error is
    # called.
    self.mock(self.bot, 'post_error', lambda *_: None)
    self.mock(bot_main, 'THIS_FILE', 'swarming_bot.1.zip')
    self.mock(net, 'url_retrieve', lambda *_: True)

    calls = []
    cmd = [sys.executable, 'swarming_bot.2.zip', 'start_slave', '--survive']
    self.mock(subprocess, 'call', self.fail)
    self.mock(os, 'execv', lambda *args: calls.append(args))

    bot_main.update_bot(self.bot, '123')
    self.assertEqual([(sys.executable, cmd)], calls)

  def test_update_bot_win(self):
    self.mock(sys, 'platform', 'win32')

    # In a real case 'update_bot' never exists and doesn't call 'post_error'.
    # Under the test forever blocking calls
    self.mock(self.bot, 'post_error', lambda *_: None)
    self.mock(bot_main, 'THIS_FILE', 'swarming_bot.1.zip')
    self.mock(net, 'url_retrieve', lambda *_: True)

    calls = []
    cmd = [sys.executable, 'swarming_bot.2.zip', 'start_slave', '--survive']
    self.mock(subprocess, 'call', lambda *args: calls.append(args))
    self.mock(os, 'execv', self.fail)

    with self.assertRaises(SystemExit):
      bot_main.update_bot(self.bot, '123')
    self.assertEqual([(cmd,)], calls)

  def test_get_config(self):
    expected = {
      u'server': u'http://localhost:8080',
      u'server_version': u'version1',
    }
    self.assertEqual(expected, bot_main.get_config())

  def test_main(self):
    def check(x):
      self.assertEqual(logging.WARNING, x)
    self.mock(logging_utils, 'set_console_level', check)

    def run_bot(error):
      self.assertEqual(None, error)
      return 0
    self.mock(bot_main, 'run_bot', run_bot)

    self.assertEqual(0, bot_main.main([]))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
