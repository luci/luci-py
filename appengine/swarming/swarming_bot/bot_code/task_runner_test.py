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

import test_env_bot_code
test_env_bot_code.setup_test_env()

# Creates a server mock for functions in net.py.
import net_utils

from utils import file_path
from utils import logging_utils
from utils import subprocess42
from utils import tools
import task_runner
import xsrf_client


def compress_to_zip(files):
  # TODO(maruel): Remove once 'data' support is removed.
  out = StringIO.StringIO()
  with zipfile.ZipFile(out, 'w') as zip_file:
    for item, content in files.iteritems():
      zip_file.writestr(item, content)
  return out.getvalue()


class TestTaskRunnerBase(net_utils.TestCase):
  def setUp(self):
    super(TestTaskRunnerBase, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')
    self.work_dir = os.path.join(self.root_dir, 'work')
    os.chdir(self.root_dir)
    os.mkdir(self.work_dir)

  def tearDown(self):
    os.chdir(test_env_bot_code.BOT_DIR)
    file_path.rmtree(self.root_dir)
    super(TestTaskRunnerBase, self).tearDown()

  @staticmethod
  def get_manifest(
      script=None, hard_timeout=10., io_timeout=10., grace_period=30.,
      data=None, inputs_ref=None, extra_args=None):
    out = {
      'bot_id': 'localhost',
      'command':
          [sys.executable, '-u', '-c', script] if not inputs_ref else None,
      'data': data or [],
      'env': {},
      'extra_args': extra_args or [],
      'grace_period': grace_period,
      'hard_timeout': hard_timeout,
      'inputs_ref': inputs_ref,
      'io_timeout': io_timeout,
      'task_id': 23,
    }
    return out

  @classmethod
  def get_task_details(cls, *args, **kwargs):
    return task_runner.TaskDetails(cls.get_manifest(*args, **kwargs))

  def gen_requests(self, cost_usd=0., **kwargs):
    return [
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

  def requests(self, **kwargs):
    """Generates the expected HTTP requests for a task run."""
    self.expected_requests(self.gen_requests(**kwargs))

  def get_check_first(self, cost_usd):
    def check_first(kwargs):
      self.assertLessEqual(cost_usd, kwargs['data'].pop('cost_usd'))
      self.assertEqual(
        {
          'data': {
            'id': 'localhost',
            'task_id': 23,
          },
          'headers': {'X-XSRF-Token': 'token'},
        },
        kwargs)
    return check_first


class TestTaskRunner(TestTaskRunnerBase):
  def setUp(self):
    super(TestTaskRunner, self).setUp()
    self.mock(time, 'time', lambda: 1000000000.)

  def get_check_final(self, exit_code=0, output='hi\n', outputs_ref=None):
    def check_final(kwargs):
      # It makes the diffing easier.
      if 'output' in kwargs['data']:
        kwargs['data']['output'] = base64.b64decode(kwargs['data']['output'])
      expected = {
        'data': {
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
      }
      if outputs_ref:
        expected['data']['outputs_ref'] = outputs_ref
      self.assertEqual(expected, kwargs)
    return check_final

  def _run_command(self, task_details):
    start = time.time()
    self.mock(time, 'time', lambda: start + 10)
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    return task_runner.run_command(server, task_details, '.', 3600., start)

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

  def test_load_and_run_raw(self):
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

    def run_command(
        swarming_server, task_details, work_dir, cost_usd_hour, start):
      self.assertEqual(server, swarming_server)
      # Necessary for OSX.
      self.assertEqual(os.path.realpath(self.work_dir), work_dir)
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      return {
        u'exit_code': 1,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'version': 2,
      }
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'command': ['a'],
        'data': [('https://localhost:1/f', 'foo.zip')],
        'env': {'d': 'e'},
        'extra_args': [],
        'grace_period': 30.,
        'hard_timeout': 10,
        'inputs_ref': None,
        'io_timeout': 11,
        'task_id': 23,
      }
      json.dump(data, f)

    out_file = os.path.join(self.root_dir, 'task_runner_out.json')
    task_runner.load_and_run(manifest, server, 3600., time.time(), out_file)
    expected = {
      u'exit_code': 1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    with open(out_file, 'rb') as f:
      self.assertEqual(expected, json.load(f))

  def test_load_and_run_isolated(self):
    self.expected_requests([])
    server = xsrf_client.XsrfRemote('https://localhost:1/')

    def run_command(
        swarming_server, task_details, work_dir, cost_usd_hour, start):
      self.assertEqual(server, swarming_server)
      # Necessary for OSX.
      self.assertEqual(os.path.realpath(self.work_dir), work_dir)
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      return {
        u'exit_code': 0,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'version': 2,
      }
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'command': None,
        'data': None,
        'env': {'d': 'e'},
        'extra_args': ['foo', 'bar'],
        'grace_period': 30.,
        'hard_timeout': 10,
        'io_timeout': 11,
        'inputs_ref': {
          'isolated': '123',
          'isolatedserver': 'http://localhost:1',
          'namespace': 'default-gzip',
        },
        'task_id': 23,
      }
      json.dump(data, f)

    out_file = os.path.join(self.root_dir, 'task_runner_out.json')
    task_runner.load_and_run(manifest, server, 3600., time.time(), out_file)
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    with open(out_file, 'rb') as f:
      self.assertEqual(expected, json.load(f))

  def test_run_command_raw(self):
    # This runs the command for real.
    self.requests(cost_usd=1, exit_code=0)
    task_details = self.get_task_details('print(\'hi\')')
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_isolated(self):
    # This runs the command for real.
    self.requests(
        cost_usd=1, exit_code=0,
        outputs_ref={
          u'isolated': u'123',
          u'isolatedserver': u'http://localhost:1',
          u'namespace': u'default-gzip',
        })
    task_details = self.get_task_details(inputs_ref={
      'isolated': '123',
      'isolatedserver': 'localhost:1',
      'namespace': 'default-gzip',
    }, extra_args=['foo', 'bar'])
    #out = os.path.join(self.root_dir, 'foo')
    #self.mock(task_runner, 'mkstemp', lambda: out)
    # Mock running run_isolated with a script.
    SCRIPT_ISOLATED = (
      'import json, sys;\n'
      'if len(sys.argv) != 2:\n'
      '  raise Exception(sys.argv);\n'
      'with open(sys.argv[1], \'wb\') as f:\n'
      '  json.dump({\n'
      '    \'isolated\': \'123\',\n'
      '    \'isolatedserver\': \'http://localhost:1\',\n'
      '    \'namespace\': \'default-gzip\',\n'
      '  }, f)\n'
      'sys.stdout.write(\'hi\\n\')')
    self.mock(
        task_runner, 'get_isolated_cmd',
        lambda _, x: [sys.executable, '-u', '-c', SCRIPT_ISOLATED, x])
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_fail(self):
    # This runs the command for real.
    self.requests(cost_usd=10., exit_code=1)
    task_details = self.get_task_details(
        'import sys; print(\'hi\'); sys.exit(1)')
    expected = {
      u'exit_code': 1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

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
          'command': [
            'executable_that_shouldnt_be_on_your_system',
            'thus_raising_OSError',
          ],
          'data': [],
          'env': {},
          'extra_args': [],
          'grace_period': 30.,
          'hard_timeout': 6,
          'inputs_ref': None,
          'io_timeout': 6,
          'task_id': 23,
        })
    expected = {
      u'exit_code': 255,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_large(self):
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      """Mocks the process so we can control how data is returned."""
      def __init__(self2, cmd, cwd, env, stdout, stderr, stdin, detached):
        self.assertEqual(task_details.command, cmd)
        self.assertEqual('.', cwd)
        expected_env = os.environ.copy()
        expected_env['foo'] = 'bar'
        self.assertEqual(expected_env, env)
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
              # That's because the cost includes the duration starting at start,
              # not when the process was started.
              'cost_usd': 10.,
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
            'cost_usd': 10.,
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
            'cost_usd': 10.,
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
    task_details = task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'command': ['large', 'executable'],
          'data': [],
          'env': {'foo': 'bar'},
          'extra_args': [],
          'grace_period': 30.,
          'hard_timeout': 60,
          'inputs_ref': None,
          'io_timeout': 60,
          'task_id': 23,
        })
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_main(self):
    def load_and_run(
        manifest, swarming_server, cost_usd_hour, start, json_file):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server.url)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual('task_summary.json', json_file)

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost',
      '--in-file', 'foo',
      '--out-file', 'task_summary.json',
      '--cost-usd-hour', '3600',
      '--start', str(time.time()),
    ]
    self.assertEqual(0, task_runner.main(cmd))

  def test_main_reboot(self):
    def load_and_run(
        manifest, swarming_server, cost_usd_hour, start, json_file):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server.url)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual('task_summary.json', json_file)

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost',
      '--in-file', 'foo',
      '--out-file', 'task_summary.json',
      '--cost-usd-hour', '3600',
      '--start', str(time.time()),
    ]
    self.assertEqual(0, task_runner.main(cmd))


class TestTaskRunnerNoTimeMock(TestTaskRunnerBase):
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
      if hard_timeout or io_timeout:
        self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('cost_usd'))
        self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('duration'))
      else:
        self.assertLess(0., kwargs['data'].pop('cost_usd'))
        self.assertLess(0., kwargs['data'].pop('duration'))
      # It makes the diffing easier.
      kwargs['data']['output'] = base64.b64decode(kwargs['data']['output'])
      self.assertEqual(
          {
            'data': {
              'exit_code': exit_code,
              'hard_timeout': hard_timeout,
              'id': u'localhost',
              'io_timeout': io_timeout,
              'output': output,
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'headers': {'X-XSRF-Token': 'token'},
          },
          kwargs)
    return check_final

  def _load_and_run(self, manifest):
    # Dot not mock time since this test class is testing timeouts.
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    in_file = os.path.join(self.work_dir, 'manifest.json')
    with open(in_file, 'wb') as f:
      json.dump(manifest, f)
    out_file = os.path.join(self.work_dir, 'task_summary.json')
    task_runner.load_and_run(in_file, server, 3600., time.time(), out_file)
    with open(out_file, 'rb') as f:
      return json.load(f)

  def _run_command(self, task_details):
    # Dot not mock time since this test class is testing timeouts.
    server = xsrf_client.XsrfRemote('https://localhost:1/')
    return task_runner.run_command(
        server, task_details, '.', 3600., time.time())

  def test_hard(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(hard_timeout=True, exit_code=sig)
    task_details = self.get_task_details(
        self.SCRIPT_HANG, hard_timeout=self.SHORT_TIME_OUT)
    expected = {
      u'exit_code': sig,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(io_timeout=True, exit_code=sig)
    task_details = self.get_task_details(
        self.SCRIPT_HANG, io_timeout=self.SHORT_TIME_OUT)
    expected = {
      u'exit_code': sig,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_hard_signal(self):
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        hard_timeout=True, exit_code=0, output='hi\ngot signal %d\nbye\n' % sig)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL, hard_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': 0,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io_signal(self):
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        io_timeout=True, exit_code=0, output='hi\ngot signal %d\nbye\n' % sig)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL, io_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_hard_no_grace(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(hard_timeout=True, exit_code=sig)
    task_details = self.get_task_details(
        self.SCRIPT_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    expected = {
      u'exit_code': sig,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io_no_grace(self):
    # Actually 0xc000013a
    sig = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM
    self.requests(io_timeout=True, exit_code=sig)
    task_details = self.get_task_details(
        self.SCRIPT_HANG, io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    expected = {
      u'exit_code': sig,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_hard_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        hard_timeout=True, exit_code=exit_code,
        output='hi\ngot signal %d\nbye\n' % sig)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': exit_code,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    sig = signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM
    self.requests(
        io_timeout=True, exit_code=exit_code,
        output='hi\ngot signal %d\nbye\n' % sig)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': exit_code,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'version': 2,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_grand_children(self):
    # Uses load_and_run()
    data = [
      ('http://localhost:1/foo.zip', 'ignored'),
    ]
    parent = (
      'import subprocess, sys\n'
      'sys.exit(subprocess.call([sys.executable, \'-u\', \'children.py\']))\n')
    children = (
      'import subprocess, sys\n'
      'sys.exit(subprocess.call('
          '[sys.executable, \'-u\', \'grand_children.py\']))\n')
    grand_children = 'print \'hi\''

    requests = self.gen_requests(exit_code=0, output='hi\n')
    requests.append(
      (
        'http://localhost:1/foo.zip',
        {},
        compress_to_zip(
            {'children.py': children, 'grand_children.py': grand_children}),
        None,
      ))
    self.expected_requests(requests)

    manifest = self.get_manifest(parent, data=data)
    expected = {
      u'version': 2,
      u'exit_code': 0,
      u'io_timeout': False,
      u'hard_timeout': False,
    }
    self.assertEqual(expected, self._load_and_run(manifest))

  def test_hard_signal_no_grace_grand_children(self):
    # Uses load_and_run()
    # Actually 0xc000013a
    exit_code = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM

    data = [
      ('http://localhost:1/foo.zip', 'ignored'),
    ]
    parent = (
      'import subprocess, sys\n'
      'p = subprocess.Popen([sys.executable, \'-u\', \'children.py\'])\n'
      'print(p.pid)\n'
      'p.wait()\n'
      'sys.exit(p.returncode)\n')
    children = (
      'import subprocess, sys\n'
      'p = subprocess.Popen([sys.executable, \'-u\', \'grand_children.py\'])\n'
      'print(p.pid)\n'
      'p.wait()\n'
      'sys.exit(p.returncode)\n')
    grand_children = self.SCRIPT_SIGNAL_HANG

    # We need to catch the pid of the grand children to be able to kill it, so
    # create our own check_final() instead of using self._gen_requests().
    to_kill = []
    def check_final(kwargs):
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('cost_usd'))
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('duration'))
      # It makes the diffing easier.
      output = base64.b64decode(kwargs['data'].pop('output'))
      # The command print the pid of this child and grand-child processes, each
      # on its line.
      to_kill.extend(int(i) for i in output.splitlines()[:2])
      self.assertEqual(
          {
            'data': {
              'exit_code': exit_code,
              'hard_timeout': True,
              'id': u'localhost',
              'io_timeout': False,
              'output_chunk_start': 0,
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
        self.get_check_first(0.),
        {},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        check_final,
        {},
      ),
      (
        'http://localhost:1/foo.zip',
        {},
        compress_to_zip(
            {'children.py': children, 'grand_children.py': grand_children}),
        None,
      ),
    ]
    self.expected_requests(requests)

    try:
      manifest = self.get_manifest(
          parent, hard_timeout=self.SHORT_TIME_OUT,
          grace_period=self.SHORT_TIME_OUT, data=data)
      expected = {
        u'exit_code': exit_code,
        u'hard_timeout': True,
        u'io_timeout': False,
        u'version': 2,
      }
      self.assertEqual(expected, self._load_and_run(manifest))
    finally:
      for k in to_kill:
        try:
          if sys.platform == 'win32':
            os.kill(k, signal.SIGTERM)
          else:
            os.kill(k, signal.SIGKILL)
        except OSError:
          pass


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging_utils.prepare_logging(None)
  logging_utils.set_console_level(
      logging.DEBUG if '-v' in sys.argv else logging.CRITICAL+1)
  unittest.main()
