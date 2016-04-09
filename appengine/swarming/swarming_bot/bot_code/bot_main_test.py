#!/usr/bin/env python
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import zipfile

import test_env_bot_code
test_env_bot_code.setup_test_env()

# Creates a server mock for functions in net.py.
import net_utils

import bot_main
import xsrf_client
from api import bot
from api import os_utilities
from depot_tools import fix_encoding
from utils import file_path
from utils import logging_utils
from utils import net
from utils import subprocess42
from utils import zip_package


# Access to a protected member XX of a client class - pylint: disable=W0212


class TestBotMain(net_utils.TestCase):
  maxDiff = 2000

  def setUp(self):
    super(TestBotMain, self).setUp()
    os.environ.pop('SWARMING_LOAD_TEST', None)
    self.root_dir = tempfile.mkdtemp(prefix='bot_main')
    self.old_cwd = os.getcwd()
    os.chdir(self.root_dir)
    # __main__ does it for us.
    os.mkdir('logs')
    self.server = xsrf_client.XsrfRemote('https://localhost:1/')
    self.attributes = {
      'dimensions': {
        'foo': ['bar'],
        'id': ['localhost'],
        'pool': ['default'],
      },
      'state': {
        'cost_usd_hour': 3600.,
      },
      'version': '123',
    }
    self.mock(zip_package, 'generate_version', lambda: '123')
    self.bot = bot.Bot(
        self.server, self.attributes, 'https://localhost:1/', 'version1',
        self.root_dir, self.fail)
    self.mock(self.bot, 'post_error', self.fail)
    self.mock(self.bot, 'restart', self.fail)
    self.mock(subprocess42, 'call', self.fail)
    self.mock(time, 'time', lambda: 100.)
    config_path = os.path.join(
        test_env_bot_code.BOT_DIR, 'config', 'config.json')
    with open(config_path, 'rb') as f:
      config = json.load(f)
    self.mock(bot_main, 'get_config', lambda: config)
    self.mock(
        bot_main, 'THIS_FILE',
        os.path.join(test_env_bot_code.BOT_DIR, 'swarming_bot.zip'))

  def tearDown(self):
    os.environ.pop('SWARMING_BOT_ID', None)
    os.chdir(self.old_cwd)
    file_path.rmtree(self.root_dir)
    super(TestBotMain, self).tearDown()

  def test_get_dimensions(self):
    dimensions = set(bot_main.get_dimensions(None))
    dimensions.discard('hidpi')
    dimensions.discard('zone')  # Only set on GCE bots.
    expected = {'cores', 'cpu', 'gpu', 'id', 'machine_type', 'os', 'pool'}
    self.assertEqual(expected, dimensions)

  def test_get_dimensions_load_test(self):
    os.environ['SWARMING_LOAD_TEST'] = '1'
    self.assertEqual(['id', 'load_test'], sorted(bot_main.get_dimensions(None)))

  def test_generate_version(self):
    self.assertEqual('123', bot_main.generate_version())

  def test_get_state(self):
    self.mock(time, 'time', lambda: 126.0)
    expected = os_utilities.get_state()
    expected['sleep_streak'] = 12
    # During the execution of this test case, the free disk space could have
    # changed.
    for disk in expected['disks'].itervalues():
      self.assertGreater(disk.pop('free_mb'), 1.)
    actual = bot_main.get_state(None, 12)
    for disk in actual['disks'].itervalues():
      self.assertGreater(disk.pop('free_mb'), 1.)
    self.assertGreater(actual.pop('nb_files_in_temp'), 0)
    self.assertGreater(expected.pop('nb_files_in_temp'), 0)
    self.assertGreater(actual.pop('uptime'), 0)
    self.assertGreater(expected.pop('uptime'), 0)
    self.assertEqual(sorted(expected.pop('temp', {})),
                     sorted(actual.pop('temp', {})))
    self.assertEqual(expected, actual)

  def test_setup_bot(self):
    self.mock(bot_main, 'get_remote', lambda: self.server)
    setup_bots = []
    def setup_bot(_bot):
      setup_bots.append(1)
      return False
    from config import bot_config
    self.mock(bot_config, 'setup_bot', setup_bot)
    restarts = []
    post_event = []
    self.mock(
        os_utilities, 'restart', lambda *a, **kw: restarts.append((a, kw)))
    self.mock(
        bot.Bot, 'post_event', lambda *a, **kw: post_event.append((a, kw)))
    self.expected_requests([])
    bot_main.setup_bot(False)
    expected = [
      (('Starting new swarming bot: %s' % bot_main.THIS_FILE,),
        {'timeout': 900}),
    ]
    self.assertEqual(expected, restarts)
    # It is called twice, one as part of setup_bot(False), another as part of
    # on_shutdown_hook().
    self.assertEqual([1, 1], setup_bots)
    expected = [
      'Starting new swarming bot: %s' % bot_main.THIS_FILE,
      'Bot is stuck restarting for: Starting new swarming bot: %s' %
        bot_main.THIS_FILE,
    ]
    self.assertEqual(expected, [i[0][2] for i in post_event])

  def test_post_error_task(self):
    self.mock(time, 'time', lambda: 126.0)
    self.mock(logging, 'error', lambda *_, **_kw: None)
    self.mock(bot_main, 'get_remote', lambda: self.server)
    # get_state() return value changes over time. Hardcode its value for the
    # duration of this test.
    self.mock(os_utilities, 'get_state', lambda : {'foo': 'bar'})
    expected_attribs = bot_main.get_attributes(None)
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
    # Test the run_bot() loop. Does not use self.bot.
    self.mock(time, 'time', lambda: 126.0)
    class Foo(Exception):
      pass

    def poll_server(botobj, _):
      sleep_streak = botobj.state['sleep_streak']
      self.assertEqual(botobj.remote, self.server)
      if sleep_streak == 5:
        raise Exception('Jumping out of the loop')
      return False
    self.mock(bot_main, 'poll_server', poll_server)

    def post_error(botobj, e):
      self.assertEqual(self.server, botobj._remote)
      lines = e.splitlines()
      self.assertEqual('Jumping out of the loop', lines[0])
      self.assertEqual('Traceback (most recent call last):', lines[1])
      raise Foo('Necessary to get out of the loop')
    self.mock(bot.Bot, 'post_error', post_error)

    self.mock(bot_main, 'get_remote', lambda: self.server)

    # Method should have "self" as first argument - pylint: disable=E0213
    # pylint: disable=unused-argument
    class Popen(object):
      def __init__(
          self2, cmd, detached, cwd, stdout, stderr, stdin, close_fds):
        self2.returncode = None
        expected = [sys.executable, bot_main.THIS_FILE, 'run_isolated']
        self.assertEqual(expected, cmd[:len(expected)])
        self.assertEqual(True, detached)
        self.assertEqual(subprocess42.PIPE, stdout)
        self.assertEqual(subprocess42.STDOUT, stderr)
        self.assertEqual(subprocess42.PIPE, stdin)
        self.assertEqual(sys.platform != 'win32', close_fds)

      def communicate(self2, i):
        self.assertEqual(None, i)
        self2.returncode = 0
        return '', None
    self.mock(subprocess42, 'Popen', Popen)

    self.expected_requests(
        [
          (
            'https://localhost:1/swarming/api/v1/bot/server_ping',
            {}, 'foo', None,
          ),
        ])

    with self.assertRaises(Foo):
      bot_main.run_bot(None)
    self.assertEqual(
        os_utilities.get_hostname_short(), os.environ['SWARMING_BOT_ID'])

  def test_poll_server_sleep(self):
    slept = []
    bit = threading.Event()
    self.mock(bit, 'wait', slept.append)
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
    self.assertFalse(bot_main.poll_server(self.bot, bit))
    self.assertEqual([1.24], slept)

  def test_poll_server_run(self):
    manifest = []
    bit = threading.Event()
    self.mock(bit, 'wait', self.fail)
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
    self.assertTrue(bot_main.poll_server(self.bot, bit))
    expected = [(self.bot, {'foo': 'bar'}, time.time())]
    self.assertEqual(expected, manifest)

  def test_poll_server_update(self):
    update = []
    bit = threading.Event()
    self.mock(bit, 'wait', self.fail)
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
    self.assertTrue(bot_main.poll_server(self.bot, bit))
    self.assertEqual([(self.bot, '123')], update)

  def test_poll_server_restart(self):
    restart = []
    bit = threading.Event()
    self.mock(bit, 'wait', self.fail)
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
    self.assertTrue(bot_main.poll_server(self.bot, bit))
    self.assertEqual([('Please die now',)], restart)

  def test_poll_server_restart_load_test(self):
    os.environ['SWARMING_LOAD_TEST'] = '1'
    bit = threading.Event()
    self.mock(bit, 'wait', self.fail)
    self.mock(bot_main, 'run_manifest', self.fail)
    self.mock(bot_main, 'update_bot', self.fail)
    self.mock(self.bot, 'restart', self.fail)

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
    self.assertTrue(bot_main.poll_server(self.bot, bit))

  def _mock_popen(self, returncode=0, exit_code=0, url='https://localhost:1'):
    result = {
      'exit_code': exit_code,
      'must_signal_internal_failure': None,
      'version': 3,
    }
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      def __init__(
          self2, cmd, detached, cwd, env, stdout, stderr, stdin, close_fds):
        self2.returncode = None
        self2._out_file = os.path.join(
            self.root_dir, 'work', 'task_runner_out.json')
        expected = [
          sys.executable, bot_main.THIS_FILE, 'task_runner',
          '--swarming-server', url,
          '--in-file',
          os.path.join(self.root_dir, 'work', 'task_runner_in.json'),
          '--out-file', self2._out_file,
          '--cost-usd-hour', '3600.0', '--start', '100.0',
          '--min-free-space',
          str(int(
            (os_utilities.get_min_free_space(bot_main.THIS_FILE) + 250.) *
            1024 * 1024)),
        ]
        self.assertEqual(expected, cmd)
        self.assertEqual(True, detached)
        self.assertEqual(self.bot.base_dir, cwd)
        self.assertEqual('24', env['SWARMING_TASK_ID'])
        self.assertTrue(stdout)
        self.assertEqual(subprocess42.STDOUT, stderr)
        self.assertEqual(subprocess42.PIPE, stdin)
        self.assertEqual(sys.platform != 'win32', close_fds)

      def wait(self2, timeout=None): # pylint: disable=unused-argument
        self2.returncode = returncode
        with open(self2._out_file, 'wb') as f:
          json.dump(result, f)
        return 0

    self.mock(subprocess42, 'Popen', Popen)
    return result

  def test_run_manifest(self):
    self.mock(bot_main, 'post_error_task', lambda *args: self.fail(args))
    def call_hook(botobj, name, *args):
      if name == 'on_after_task':
        failure, internal_failure, dimensions, summary = args
        self.assertEqual(self.attributes['dimensions'], botobj.dimensions)
        self.assertEqual(False, failure)
        self.assertEqual(False, internal_failure)
        self.assertEqual({'os': 'Amiga', 'pool': 'default'}, dimensions)
        self.assertEqual(result, summary)
    self.mock(bot_main, 'call_hook', call_hook)
    result = self._mock_popen(url='https://localhost:3')

    manifest = {
      'command': ['echo', 'hi'],
      'dimensions': {'os': 'Amiga', 'pool': 'default'},
      'grace_period': 30,
      'hard_timeout': 60,
      'host': 'https://localhost:3',
      'task_id': '24',
    }
    self.assertEqual(self.root_dir, self.bot.base_dir)
    bot_main.run_manifest(self.bot, manifest, time.time())

  def test_run_manifest_task_failure(self):
    self.mock(bot_main, 'post_error_task', lambda *args: self.fail(args))
    def call_hook(_botobj, name, *args):
      if name == 'on_after_task':
        failure, internal_failure, dimensions, summary = args
        self.assertEqual(True, failure)
        self.assertEqual(False, internal_failure)
        self.assertEqual({'pool': 'default'}, dimensions)
        self.assertEqual(result, summary)
    self.mock(bot_main, 'call_hook', call_hook)
    result = self._mock_popen(exit_code=1)

    manifest = {
      'command': ['echo', 'hi'],
      'dimensions': {'pool': 'default'},
      'grace_period': 30,
      'hard_timeout': 60,
      'io_timeout': 60,
      'task_id': '24',
    }
    bot_main.run_manifest(self.bot, manifest, time.time())

  def test_run_manifest_internal_failure(self):
    posted = []
    self.mock(bot_main, 'post_error_task', lambda *args: posted.append(args))
    def call_hook(_botobj, name, *args):
      if name == 'on_after_task':
        failure, internal_failure, dimensions, summary = args
        self.assertEqual(False, failure)
        self.assertEqual(True, internal_failure)
        self.assertEqual({'pool': 'default'}, dimensions)
        self.assertEqual(result, summary)
    self.mock(bot_main, 'call_hook', call_hook)
    result = self._mock_popen(returncode=1)

    manifest = {
      'command': ['echo', 'hi'],
      'dimensions': {'pool': 'default'},
      'grace_period': 30,
      'hard_timeout': 60,
      'io_timeout': 60,
      'task_id': '24',
    }
    bot_main.run_manifest(self.bot, manifest, time.time())
    expected = [(self.bot, 'Execution failed: internal error (1).', '24')]
    self.assertEqual(expected, posted)

  def test_run_manifest_exception(self):
    posted = []
    def post_error_task(botobj, msg, task_id):
      posted.append((botobj, msg.splitlines()[0], task_id))
    self.mock(bot_main, 'post_error_task', post_error_task)
    def call_hook(_botobj, name, *args):
      if name == 'on_after_task':
        failure, internal_failure, dimensions, summary = args
        self.assertEqual(False, failure)
        self.assertEqual(True, internal_failure)
        self.assertEqual({'pool': 'default'}, dimensions)
        self.assertEqual({}, summary)
    self.mock(bot_main, 'call_hook', call_hook)
    def raiseOSError(*_a, **_k):
      raise OSError('Dang')
    self.mock(subprocess42, 'Popen', raiseOSError)

    manifest = {
      'command': ['echo', 'hi'],
      'dimensions': {'pool': 'default'},
      'grace_period': 30,
      'hard_timeout': 60,
      'task_id': '24',
    }
    bot_main.run_manifest(self.bot, manifest, time.time())
    expected = [(self.bot, 'Internal exception occured: Dang', '24')]
    self.assertEqual(expected, posted)

  def test_update_bot(self):
    # In a real case 'update_bot' never exits and doesn't call 'post_error'.
    # Under the test however forever-blocking calls finish, and post_error is
    # called.
    self.mock(self.bot, 'post_error', lambda *_: None)
    # Mock the file to download in the temporary directory.
    self.mock(
        bot_main, 'THIS_FILE',
        os.path.join(self.root_dir, 'swarming_bot.1.zip'))
    new_zip = os.path.join(self.root_dir, 'swarming_bot.2.zip')
    # This is necessary otherwise zipfile will crash.
    self.mock(time, 'time', lambda: 1400000000)
    def url_retrieve(f, url):
      self.assertEqual(
          'https://localhost:1/swarming/api/v1/bot/bot_code/123', url)
      self.assertEqual(new_zip, f)
      # Create a valid zip that runs properly.
      with zipfile.ZipFile(f, 'w') as z:
        z.writestr('__main__.py', 'print("hi")')
      return True
    self.mock(net, 'url_retrieve', url_retrieve)

    calls = []
    def exec_python(args):
      calls.append(args)
      return 23
    self.mock(bot_main.common, 'exec_python', exec_python)

    with self.assertRaises(SystemExit) as e:
      bot_main.update_bot(self.bot, '123')
    self.assertEqual(23, e.exception.code)

    self.assertEqual([[new_zip, 'start_slave', '--survive']], calls)

  def test_main(self):
    def check(x):
      self.assertEqual(logging.WARNING, x)
    self.mock(logging_utils, 'set_console_level', check)

    def run_bot(error):
      self.assertEqual(None, error)
      return 0
    self.mock(bot_main, 'run_bot', run_bot)

    class Singleton(object):
      # pylint: disable=no-self-argument
      def acquire(self2):
        return True
      def release(self2):
        self.fail()
    self.mock(bot_main, 'SINGLETON', Singleton())

    self.assertEqual(0, bot_main.main([]))


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
