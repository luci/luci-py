#!/usr/bin/env python
# coding=utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import StringIO
import base64
import json
import logging
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# Small hack to make it work on Windows even without symlink support.
if os.path.isfile(os.path.join(THIS_DIR, 'utils')):
  sys.path.insert(0, os.path.join(THIS_DIR, '..', '..', '..', 'client'))


# Import everything that does not require sys.path hack first.
import logging_utils
import task_runner

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

import xsrf_client
from utils import subprocess42


CLIENT_TESTS = os.path.join(ROOT_DIR, '..', '..', 'client', 'tests')
sys.path.insert(0, CLIENT_TESTS)

# Creates a server mock for functions in net.py.
import net_utils


def compress_to_zip(files):
  out = StringIO.StringIO()
  with zipfile.ZipFile(out, 'w') as zip_file:
    for item, content in files.iteritems():
      zip_file.writestr(item, content)
  return out.getvalue()


class TestLocalTaskRunnerBase(net_utils.TestCase):
  def setUp(self):
    super(TestLocalTaskRunnerBase, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')
    self.work_dir = os.path.join(self.root_dir, 'work')
    os.chdir(self.root_dir)
    os.mkdir(self.work_dir)

  def tearDown(self):
    os.chdir(BASE_DIR)
    shutil.rmtree(self.root_dir)
    super(TestLocalTaskRunnerBase, self).tearDown()

  @staticmethod
  def get_task_details(
      script, hard_timeout=10., io_timeout=10., grace_period=30.):
    return task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [[sys.executable, '-u', '-c', script]],
          'data': [],
          'env': {},
          'grace_period': grace_period,
          'hard_timeout': hard_timeout,
          'io_timeout': io_timeout,
          'task_id': 23,
        })

  def requests(self, cost_usd=0., **kwargs):
    requests = [
      (
        'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
        {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
        {'xsrf_token': 'token'},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_first(cost_usd),
        {},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_final(**kwargs),
        {},
      ),
    ]
    self.expected_requests(requests)

  def get_check_first(self, cost_usd):
    def check_first(kwargs):
      self.assertLessEqual(cost_usd, kwargs['data'].pop('cost_usd'))
      self.assertEqual(
        {
          'data': {
            'command_index': 0,
            'id': 'localhost',
            'task_id': 23,
          },
          'headers': {'X-XSRF-Token': 'token'},
        },
        kwargs)
    return check_first


class TestLocalTaskRunner(TestLocalTaskRunnerBase):
  def setUp(self):
    super(TestLocalTaskRunner, self).setUp()
    self.mock(time, 'time', lambda: 1000000000.)

  def get_check_final(self, exit_code=0, output='hi\n'):
    def check_final(kwargs):
      # It makes the diffing easier.
      kwargs['data']['output'] = base64.b64decode(kwargs['data']['output'])
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'cost_usd': 10.,
              'duration': 0.,
              'exit_code': exit_code,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': False,
              'output': output,
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'headers': {'X-XSRF-Token': 'token'},
          },
          kwargs)
    return check_final

  def _run(self, task_details):
    start = time.time()
    self.mock(time, 'time', lambda: start + 10)
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    return task_runner.run_command(
        server, 0, task_details, '.', 3600., start)

  def test_download_data(self):
    requests = [
      (
        'https://localhost:1/a',
        {},
        compress_to_zip({'file1': 'content1', 'file2': 'content2'}),
        None,
      ),
      (
        'https://localhost:1/b',
        {},
        compress_to_zip({'file3': 'content3'}),
        None,
      ),
    ]
    self.expected_requests(requests)
    items = [(i[0], 'foo.zip') for i in requests]
    task_runner.download_data(self.root_dir, items)
    self.assertEqual(
        ['file1', 'file2', 'file3', 'work'], sorted(os.listdir(self.root_dir)))

  def test_load_and_run(self):
    requests = [
      (
        'https://localhost:1/f',
        {},
        compress_to_zip({'file3': 'content3'}),
        None,
      ),
    ]
    self.expected_requests(requests)
    server = xsrf_client.XsrfRemote('https://localhost:1/')

    runs = []
    def run_command(
        swarming_server, index, task_details, work_dir, cost_usd_hour, start):
      self.assertEqual(server, swarming_server)
      # Necessary for OSX.
      self.assertEqual(os.path.realpath(self.work_dir), work_dir)
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      runs.append(index)
      return 0
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'commands': [['a'], ['b', 'c']],
        'data': [('https://localhost:1/f', 'foo.zip')],
        'env': {'d': 'e'},
        'grace_period': 30.,
        'hard_timeout': 10,
        'io_timeout': 11,
        'task_id': 23,
      }
      json.dump(data, f)

    self.assertEqual(
        True, task_runner.load_and_run(manifest, server, 3600., time.time()))
    self.assertEqual([0, 1], runs)

  def test_load_and_run_fail(self):
    requests = [
      (
        'https://localhost:1/f',
        {},
        compress_to_zip({'file3': 'content3'}),
        None,
      ),
    ]
    self.expected_requests(requests)
    server = xsrf_client.XsrfRemote('https://localhost:1/')

    runs = []
    def run_command(
        swarming_server, index, task_details, work_dir, cost_usd_hour, start):
      self.assertEqual(server, swarming_server)
      # Necessary for OSX.
      self.assertEqual(os.path.realpath(self.work_dir), work_dir)
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      runs.append(index)
      # Fails the first, pass the second.
      return 1 if len(runs) == 1 else 0
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'commands': [['a'], ['b', 'c']],
        'data': [('https://localhost:1/f', 'foo.zip')],
        'env': {'d': 'e'},
        'grace_period': 30.,
        'hard_timeout': 10,
        'io_timeout': 11,
        'task_id': 23,
      }
      json.dump(data, f)

    self.assertEqual(
        False, task_runner.load_and_run(manifest, server, 3600., time.time()))
    self.assertEqual([0, 1], runs)

  def test_run_command(self):
    # This runs the command for real.
    self.requests(cost_usd=1, exit_code=0)
    task_details = self.get_task_details('print(\'hi\')')
    self.assertEqual(0, self._run(task_details))

  def test_run_command_fail(self):
    # This runs the command for real.
    self.requests(cost_usd=10., exit_code=1)
    task_details = self.get_task_details(
        'import sys; print(\'hi\'); sys.exit(1)')
    self.assertEqual(1, self._run(task_details))

  def test_run_command_os_error(self):
    # This runs the command for real.
    # OS specific error, fix expectation for other OSes.
    output = (
      'Command "executable_that_shouldnt_be_on_your_system '
      'thus_raising_OSError" failed to start.\n'
      'Error: [Error 2] The system cannot find the file specified'
      ) if sys.platform == 'win32' else (
      'Command "executable_that_shouldnt_be_on_your_system '
      'thus_raising_OSError" failed to start.\n'
      'Error: [Errno 2] No such file or directory')
    self.requests(cost_usd=10., exit_code=1, output=output)
    task_details = task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [
            [
              'executable_that_shouldnt_be_on_your_system',
              'thus_raising_OSError',
            ],
          ],
          'data': [],
          'env': {},
          'grace_period': 30.,
          'hard_timeout': 6,
          'io_timeout': 6,
          'task_id': 23,
        })
    self.assertEqual(1, self._run(task_details))

  def test_run_command_large(self):
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      """Mocks the process so we can control how data is returned."""
      def __init__(self2, cmd, cwd, env, stdout, stderr, stdin, detached):
        self.assertEqual(task_details.commands[0], cmd)
        self.assertEqual('./', cwd)
        self.assertEqual(os.environ, env)
        self.assertEqual(subprocess.PIPE, stdout)
        self.assertEqual(subprocess.STDOUT, stderr)
        self.assertEqual(subprocess.PIPE, stdin)
        self.assertEqual(True, detached)
        self2._out = [
          'hi!\n',
          'hi!\n',
          'hi!\n' * 100000,
          'hi!\n',
        ]

      def yield_any(self2, maxsize, soft_timeout):
        self.assertLess(0, maxsize)
        self.assertLess(0, soft_timeout)
        for i in self2._out:
          yield 'stdout', i

      @staticmethod
      def wait():
        return 0

      @staticmethod
      def kill():
        self.fail()

    self.mock(subprocess42, 'Popen', Popen)

    def check_final(kwargs):
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'cost_usd': 0.,
              # Due to time.time() mock.
              'duration': 0.,
              'exit_code': 0,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': False,
              'output': base64.b64encode('hi!\n'),
              'output_chunk_start': 100002*4,
              'task_id': 23,
            },
            'headers': {'X-XSRF-Token': 'token'},
          },
          kwargs)

    requests = [
      (
        'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
        {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
        {'xsrf_token': 'token'},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        {
          'data': {
            'command_index': 0,
            'cost_usd': 0.,
            'id': 'localhost',
            'task_id': 23,
          },
          'headers': {'X-XSRF-Token': 'token'},
        },
        {},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        {
          'data': {
            'command_index': 0,
            'cost_usd': 0.,
            'id': 'localhost',
            'output': base64.b64encode('hi!\n' * 100002),
            'output_chunk_start': 0,
            'task_id': 23,
          },
          'headers': {'X-XSRF-Token': 'token'},
        },
        {},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        check_final,
        {},
      ),
    ]
    self.expected_requests(requests)
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    task_details = task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [['large', 'executable']],
          'data': [],
          'env': {},
          'grace_period': 30.,
          'hard_timeout': 60,
          'io_timeout': 60,
          'task_id': 23,
        })
    r = task_runner.run_command(
        server, 0, task_details, './', 3600., time.time())
    self.assertEqual(0, r)

  def test_main(self):
    def load_and_run(manifest, swarming_server, cost_usd_hour, start):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server.url)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      return True

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost', '--file', 'foo',
      '--cost-usd-hour', '3600', '--start', str(time.time()),
    ]
    self.assertEqual(0, task_runner.main(cmd))

  def test_main_reboot(self):
    def load_and_run(manifest, swarming_server, cost_usd_hour, start):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server.url)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      return False

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost', '--file', 'foo',
      '--cost-usd-hour', '3600', '--start', str(time.time()),
    ]
    self.assertEqual(task_runner.TASK_FAILED, task_runner.main(cmd))


class TestLocalTaskRunnerNoTimeMock(TestLocalTaskRunnerBase):
  # Do not mock time.time() for these tests otherwise it becomes a tricky
  # implementation detail check.
  # These test cases run the command for real.

  # TODO(maruel): Calculate this value automatically through iteration?
  SHORT_TIME_OUT = 0.3

  # Here's a simple script that handles signals properly. Sadly SIGBREAK is not
  # defined on posix.
  SCRIPT_SIGNAL = (
    'import signal, sys, time;\n'
    'l = [];\n'
    'def handler(signum, _):\n'
    '  l.append(signum);\n'
    '  print(\'got signal %%d\' %% signum);\n'
    '  sys.stdout.flush();\n'
    'signal.signal(%s, handler);\n'
    'print(\'hi\');\n'
    'sys.stdout.flush();\n'
    'while not l:\n'
    '  try:\n'
    '    time.sleep(0.01);\n'
    '  except IOError:\n'
    '    pass;\n'
    'print(\'bye\')') % (
        'signal.SIGBREAK' if sys.platform == 'win32' else 'signal.SIGTERM')

  SCRIPT_SIGNAL_HANG = (
    'import signal, sys, time;\n'
    'l = [];\n'
    'def handler(signum, _):\n'
    '  l.append(signum);\n'
    '  print(\'got signal %%d\' %% signum);\n'
    '  sys.stdout.flush();\n'
    'signal.signal(%s, handler);\n'
    'print(\'hi\');\n'
    'sys.stdout.flush();\n'
    'while not l:\n'
    '  try:\n'
    '    time.sleep(0.01);\n'
    '  except IOError:\n'
    '    pass;\n'
    'print(\'bye\');\n'
    'time.sleep(100)') % (
        'signal.SIGBREAK' if sys.platform == 'win32' else 'signal.SIGTERM')

  SCRIPT_HANG = 'import time; print(\'hi\'); time.sleep(100)'

  def get_check_final(
      self, hard_timeout=False, io_timeout=False, exit_code=None,
      output='hi\n'):
    def check_final(kwargs):
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('cost_usd'))
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('duration'))
      # It makes the diffing easier.
      kwargs['data']['output'] = base64.b64decode(kwargs['data']['output'])
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': exit_code,
              'hard_timeout': hard_timeout,
              'id': 'localhost',
              'io_timeout': io_timeout,
              'output': output,
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'headers': {'X-XSRF-Token': 'token'},
          },
          kwargs)
    return check_final

  def _run(self, task_details):
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    return task_runner.run_command(
        server, 0, task_details, '.', 3600., time.time())

  def test_hard(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(hard_timeout=True, exit_code=sig)
    details = self.get_task_details(
        self.SCRIPT_HANG, hard_timeout=self.SHORT_TIME_OUT)
    self.assertEqual(sig, self._run(details))

  def test_io(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(io_timeout=True, exit_code=sig)
    details = self.get_task_details(
        self.SCRIPT_HANG, io_timeout=self.SHORT_TIME_OUT)
    self.assertEqual(sig, self._run(details))

  def test_hard_signal(self):
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        hard_timeout=True, exit_code=0, output='hi\ngot signal %d\nbye\n' % sig)
    details = self.get_task_details(
        self.SCRIPT_SIGNAL, hard_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    self.assertEqual(0, self._run(details))

  def test_io_signal(self):
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        io_timeout=True, exit_code=0, output='hi\ngot signal %d\nbye\n' % sig)
    details = self.get_task_details(
        self.SCRIPT_SIGNAL, io_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    self.assertEqual(0, self._run(details))

  def test_hard_no_grace(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(hard_timeout=True, exit_code=sig)
    details = self.get_task_details(
        self.SCRIPT_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    self.assertEqual(sig, self._run(details))

  def test_io_no_grace(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(io_timeout=True, exit_code=sig)
    details = self.get_task_details(
        self.SCRIPT_HANG, io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    self.assertEqual(sig, self._run(details))

  def test_hard_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        hard_timeout=True, exit_code=exit_code,
        output='hi\ngot signal %d\nbye\n' % sig)
    details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    self.assertEqual(exit_code, self._run(details))

  def test_io_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        io_timeout=True, exit_code=exit_code,
        output='hi\ngot signal %d\nbye\n' % sig)
    details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    self.assertEqual(exit_code, self._run(details))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging_utils.prepare_logging(None)
  logging_utils.set_console_level(
      logging.DEBUG if '-v' in sys.argv else logging.CRITICAL+1)
  unittest.main()
