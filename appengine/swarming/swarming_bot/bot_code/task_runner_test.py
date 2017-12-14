#!/usr/bin/env python
# coding=utf-8
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import json
import logging
import os
import re
import signal
import sys
import tempfile
import time
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

# Creates a server mock for functions in net.py.
import net_utils

from depot_tools import fix_encoding
from utils import file_path
from utils import large
from utils import logging_utils
from utils import subprocess42
from libs import luci_context
import bot_auth
import fake_swarming
import named_cache
import remote_client
import task_runner

CLIENT_DIR = os.path.normpath(
    os.path.join(test_env_bot_code.BOT_DIR, '..', '..', '..', 'client'))

sys.path.insert(0, os.path.join(CLIENT_DIR, 'tests'))
import isolateserver_mock


def get_manifest(script=None, isolated=None, **kwargs):
  isolated_input = isolated and isolated.get('input')
  out = {
    'bot_id': 'localhost',
    'command':
        [sys.executable, '-u', '-c', script] if not isolated_input else None,
    'env': {},
    'env_prefixes': {},
    'extra_args': [],
    'grace_period': 30.,
    'hard_timeout': 10.,
    'io_timeout': 10.,
    'isolated': isolated,
    'task_id': 23,
  }
  out.update(kwargs)
  return out


class FakeAuthSystem(object):
  local_auth_context = None

  def __init__(self, auth_params_file):
    self._running = False
    assert auth_params_file == '/path/to/auth-params-file'

  def set_remote_client(self, _remote_client):
    pass

  def start(self):
    assert not self._running
    self._running = True
    return self.local_auth_context

  def stop(self):
    self._running = False

  def get_bot_headers(self):
    assert self._running
    return {'Fake': 'Header'}, int(time.time() + 300)


class TestTaskRunnerBase(net_utils.TestCase):
  def setUp(self):
    super(TestTaskRunnerBase, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')
    logging.info('Temp: %s', self.root_dir)
    self.work_dir = os.path.join(self.root_dir, 'w')
    os.chdir(self.root_dir)
    os.mkdir(self.work_dir)
    # Create the logs directory so run_isolated.py can put its log there.
    self._logs = os.path.join(self.root_dir, 'logs')
    os.mkdir(self._logs)
    def _get_run_isolated():
      return [sys.executable, os.path.join(CLIENT_DIR, 'run_isolated.py')]
    self.mock(task_runner, 'get_run_isolated', _get_run_isolated)

    # In case this test itself is running on Swarming, clear the bot
    # environment.
    os.environ.pop('LUCI_CONTEXT', None)
    os.environ.pop('SWARMING_AUTH_PARAMS', None)
    os.environ.pop('SWARMING_BOT_ID', None)
    os.environ.pop('SWARMING_TASK_ID', None)
    os.environ.pop('SWARMING_SERVER', None)
    os.environ.pop('ISOLATE_SERVER', None)

  def tearDown(self):
    os.chdir(test_env_bot_code.BOT_DIR)
    try:
      logging.debug(self._logs)
      for i in os.listdir(self._logs):
        with open(os.path.join(self._logs, i), 'rb') as f:
          logging.debug('%s:\n%s', i, ''.join('  ' + line for line in f))
      file_path.rmtree(self.root_dir)
    except OSError:
      print >> sys.stderr, 'Failed to delete %s' % self.root_dir
    finally:
      super(TestTaskRunnerBase, self).tearDown()

  @classmethod
  def get_task_details(cls, *args, **kwargs):
    return task_runner.TaskDetails(get_manifest(*args, **kwargs))

  def gen_requests(self, cost_usd=0., auth_headers=None, **kwargs):
    return [
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_first(cost_usd, auth_headers=auth_headers),
        {'must_stop': False, 'ok': True},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_final(auth_headers=auth_headers, **kwargs),
        {'must_stop': False, 'ok': True},
      ),
    ]

  def requests(self, **kwargs):
    """Generates the expected HTTP requests for a task run."""
    self.expected_requests(self.gen_requests(**kwargs))

  def get_check_first(self, cost_usd, auth_headers=None):
    def check_first(kwargs):
      self.assertLessEqual(cost_usd, kwargs['data'].pop('cost_usd'))
      self.assertEqual(
        {
          'data': {
            'id': 'localhost',
            'task_id': 23,
          },
          'follow_redirects': False,
          'timeout': 180,
          'headers': auth_headers or {},
        },
        kwargs)
    return check_first


class TestTaskRunner(TestTaskRunnerBase):
  def setUp(self):
    super(TestTaskRunner, self).setUp()
    self.mock(time, 'time', lambda: 1000000000.)

  def get_check_final(
      self, exit_code=0, output_re=r'^hi\n$', outputs_ref=None,
      auth_headers=None):
    def check_final(kwargs):
      # Ignore these values.
      kwargs['data'].pop('bot_overhead', None)
      kwargs['data'].pop('duration', None)

      output = ''
      if 'output' in kwargs['data']:
        output = base64.b64decode(kwargs['data'].pop('output'))
      self.assertTrue(
          re.match(output_re, output),
          '%r does not match %s' % (output, output_re))

      expected = {
        'data': {
          'cost_usd': 10.,
          'exit_code': exit_code,
          'hard_timeout': False,
          'id': 'localhost',
          'io_timeout': False,
          'output_chunk_start': 0,
          'task_id': 23,
        },
        'follow_redirects': False,
        'timeout': 180,
        'headers': auth_headers or {},
      }
      if outputs_ref:
        expected['data']['outputs_ref'] = outputs_ref
      self.assertEqual(expected, kwargs, kwargs)
    return check_final

  def _run_command(self, task_details, headers_cb=None):
    start = time.time()
    self.mock(time, 'time', lambda: start + 10)
    remote = remote_client.createRemoteClient('https://localhost:1', headers_cb,
                                              False)
    with luci_context.stage(local_auth=None) as ctx_file:
      return task_runner.run_command(
          remote, task_details, self.work_dir, 3600.,
          start, ['--min-free-space', '1'], '/path/to/file', ctx_file)

  def test_load_and_run_raw(self):
    local_auth_ctx = {
      'accounts': [{'id': 'a'}, {'id': 'b'}],
      'default_account_id': 'a',
      'rpc_port': 123,
      'secret': 'abcdef',
    }
    FakeAuthSystem.local_auth_context = local_auth_ctx
    self.mock(bot_auth, 'AuthSystem', FakeAuthSystem)

    def run_command(
        remote, task_details, work_dir,
        cost_usd_hour, start, run_isolated_flags, bot_file, ctx_file):
      self.assertTrue(remote.uses_auth) # mainly to avoid "unused arg" warning
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      # Necessary for OSX.
      self.assertEqual(
          os.path.realpath(self.work_dir), os.path.realpath(work_dir))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual(['--min-free-space', '1'], run_isolated_flags)
      self.assertEqual('/path/to/bot-file', bot_file)
      with open(ctx_file, 'r') as f:
        self.assertDictEqual(local_auth_ctx, json.load(f)['local_auth'])
      return {
        u'exit_code': 1,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure': None,
        u'version': task_runner.OUT_VERSION,
      }
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'command': ['a'],
        'env': {'d': 'e'},
        'env_prefixes': {},
        'extra_args': [],
        'grace_period': 30.,
        'hard_timeout': 10,
        'io_timeout': 11,
        'isolated': None,
        'task_id': 23,
      }
      json.dump(data, f)

    out_file = os.path.join(self.root_dir, 'w', 'task_runner_out.json')
    task_runner.load_and_run(
        manifest, 'localhost:1', False, 3600., time.time(), out_file,
        ['--min-free-space', '1'], '/path/to/bot-file',
        '/path/to/auth-params-file')
    expected = {
      u'exit_code': 1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    with open(out_file, 'rb') as f:
      self.assertEqual(expected, json.load(f))

  def test_load_and_run_isolated(self):
    self.expected_requests([])

    FakeAuthSystem.local_auth_context = None
    self.mock(bot_auth, 'AuthSystem', FakeAuthSystem)

    def run_command(
        remote, task_details, work_dir,
        cost_usd_hour, start, run_isolated_flags, bot_file, ctx_file):
      self.assertTrue(remote.uses_auth) # mainly to avoid unused arg warning
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      # Necessary for OSX.
      self.assertEqual(
          os.path.realpath(self.work_dir), os.path.realpath(work_dir))
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual(['--min-free-space', '1'], run_isolated_flags)
      self.assertEqual('/path/to/bot-file', bot_file)
      with open(ctx_file, 'r') as f:
        self.assertIsNone(json.load(f).get('local_auth'))
      return {
        u'exit_code': 0,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure': None,
        u'version': task_runner.OUT_VERSION,
      }
    self.mock(task_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'command': None,
        'env': {'d': 'e'},
        'env_prefixes': {},
        'extra_args': ['foo', 'bar'],
        'grace_period': 30.,
        'hard_timeout': 10,
        'io_timeout': 11,
        'isolated': {
          'input': '123',
          'server': 'http://localhost:1',
          'namespace': 'default-gzip',
        },
        'task_id': 23,
      }
      json.dump(data, f)

    out_file = os.path.join(self.root_dir, 'w', 'task_runner_out.json')
    task_runner.load_and_run(
        manifest, 'localhost:1', False, 3600., time.time(), out_file,
        ['--min-free-space', '1'], '/path/to/bot-file',
        '/path/to/auth-params-file')
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_env_prefix(self):
    # This runs the command for real.
    self.requests(cost_usd=1, exit_code=0,
                  output_re='.*%slocal%ssmurf\n$' % (os.sep, os.sep))
    task_details = self.get_task_details(
      'import os\nprint os.getenv("PATH").split(os.pathsep)[0]',
      env_prefixes={
        'PATH': ['./local/smurf', './other/thing'],
      }
    )
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_env_prefix(self):
    # This runs the command for real.
    self.requests(
        cost_usd=1, exit_code=0,
        output_re=(
          r'^'
          r'(?P<cwd>[^\n]*)\n'
          r'(?P=cwd)%slocal%ssmurf\n'
          r'(?P=cwd)%sother%sthing\n'
          r'$'
        ) % (os.sep, os.sep, os.sep, os.sep))
    task_details = self.get_task_details(
        '\n'.join([
          'import os',
          'print os.path.realpath(os.getcwd())',
          'path = os.getenv("PATH").split(os.pathsep)',
          'print os.path.realpath(path[0])',
          'print os.path.realpath(path[1])',
        ]),
        env_prefixes={
          'PATH': ['./local/smurf', './other/thing'],
        })
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_raw_with_auth(self):
    # This runs the command for real.
    self.requests(cost_usd=1, exit_code=0, auth_headers={'A': 'a'})
    task_details = self.get_task_details('print(\'hi\')')
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(
        expected,
        self._run_command(task_details, headers_cb=lambda: ({'A': 'a'}, 0)))

  def test_run_command_isolated(self):
    # This runs the command for real.
    self.requests(
        cost_usd=1, exit_code=0,
        outputs_ref={
          u'isolated': u'123',
          u'isolatedserver': u'http://localhost:1',
          u'namespace': u'default-gzip',
        })
    task_details = self.get_task_details(isolated={
      'input': '123',
      'server': 'localhost:1',
      'namespace': 'default-gzip',
    }, extra_args=['foo', 'bar'])
    # Mock running run_isolated with a script.
    SCRIPT_ISOLATED = (
      'import json, sys;\n'
      'args = []\n'
      'if len(sys.argv) != 3 or sys.argv[1] != \'-a\':\n'
      '  raise Exception(sys.argv)\n'
      'with open(sys.argv[2], \'r\') as argsfile:\n'
      '  args = json.loads(argsfile.read())\n'
      'if len(args) != 1:\n'
      '  raise Exception(args);\n'
      'with open(args[0], \'wb\') as f:\n'
      '  json.dump({\n'
      '    \'exit_code\': 0,\n'
      '    \'had_hard_timeout\': False,\n'
      '    \'internal_failure\': None,\n'
      '    \'outputs_ref\': {\n'
      '      \'isolated\': \'123\',\n'
      '      \'isolatedserver\': \'http://localhost:1\',\n'
      '       \'namespace\': \'default-gzip\',\n'
      '    },\n'
      '  }, f)\n'
      'sys.stdout.write(\'hi\\n\')')
    self.mock(
        task_runner, 'get_run_isolated',
        lambda :
          [sys.executable, '-u', '-c', SCRIPT_ISOLATED])
    self.mock(
        task_runner, 'get_isolated_args',
        lambda _work_dir, _details, isolated_result,
          bot_file, run_isolated_flags:
          [isolated_result])
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_os_error(self):
    self.requests(
        cost_usd=10.,
        exit_code=1,
        output_re=(
            # This is a beginning of run_isolate.py's output if binary is not
            # found.
            r'^<The executable does not exist or a dependent library is '
            r'missing>'))
    task_details = task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'command': [
            'executable_that_shouldnt_be_on_your_system',
            'thus_raising_OSError',
          ],
          'env': {},
          'env_prefixes': {},
          'extra_args': [],
          'grace_period': 30.,
          'hard_timeout': 6,
          'io_timeout': 6,
          'isolated': None,
          'task_id': 23,
        })
    expected = {
      u'exit_code': 1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_large(self):
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      """Mocks the process so we can control how data is returned."""
      def __init__(self2, _cmd, cwd, env, stdout, stderr, stdin, detached):
        self.assertEqual(self.work_dir, cwd)
        expected_env = os.environ.copy()
        # In particular, foo=bar is not set here, it will be passed to
        # run_isolated as an argument.
        expected_env['LUCI_CONTEXT'] = env['LUCI_CONTEXT']  # tmp file
        self.assertEqual(expected_env, env)
        self.assertEqual(subprocess42.PIPE, stdout)
        self.assertEqual(subprocess42.STDOUT, stderr)
        self.assertEqual(subprocess42.PIPE, stdin)
        self.assertEqual(True, detached)
        self2._out = [
          'hi!\n',
          'hi!\n',
          'hi!\n' * 100000,
          'hi!\n',
        ]

      def yield_any(self2, maxsize, timeout):
        self.assertLess(0, maxsize)
        self.assertLess(0, timeout)
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
            'follow_redirects': False,
            'timeout': 180,
            'headers': {},
          },
          kwargs)

    requests = [
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        {
          'data': {
            'cost_usd': 10.,
            'id': 'localhost',
            'task_id': 23,
          },
          'follow_redirects': False,
          'timeout': 180,
          'headers': {},
        },
        {'must_stop': False, 'ok': True},
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
          'follow_redirects': False,
          'timeout': 180,
          'headers': {},
        },
        {'must_stop': False, 'ok': True},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        check_final,
        {'must_stop': False, 'ok': True},
      ),
    ]
    self.expected_requests(requests)
    task_details = task_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'command': ['large', 'executable'],
          'env': {'foo': 'bar'},
          'env_prefixes': {},
          'extra_args': [],
          'grace_period': 30.,
          'hard_timeout': 60,
          'io_timeout': 60,
          'isolated': None,
          'task_id': 23,
        })
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_canceled(self):
    # This runs the command for real.
    requests = [
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_first(1),
        {u'must_stop': True, u'ok': True},
      ),
    ]
    self.expected_requests(requests)
    task_details = self.get_task_details('print(\'hi\')')
    expected = {
      u'exit_code': -1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': 3,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_run_command_caches(self):
    # Put a file into a named cache.
    cache_manager = named_cache.CacheManager(os.path.join(self.root_dir, u'c'))
    install_dir = os.path.join(self.root_dir, u'install')

    with cache_manager.open():
      cache_manager.install(install_dir, 'foo')
      with open(os.path.join(install_dir, 'bar'), 'wb') as f:
        f.write('thecache')
      cache_manager.uninstall(install_dir, 'foo')

    # This runs the command for real.
    self.requests(cost_usd=1, exit_code=0)
    script = (
      'import os\n'
      'print "hi"\n'
      'with open("cache_foo/bar", "rb") as f:\n'
      '  cached = f.read()\n'
      'with open("../../result", "wb") as f:\n'
      '  f.write(cached)\n'
      'with open("cache_foo/bar", "wb") as f:\n'
      '  f.write("updated_cache")\n'
    )
    task_details = self.get_task_details(
        script,
        caches=[{'name': 'foo', 'path': 'cache_foo'}])
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

    with open(os.path.join(self.root_dir, 'result'), 'rb') as f:
      self.assertEqual('thecache', f.read())

    with cache_manager.open():
      cache_manager.install(install_dir, 'foo')
      with open(os.path.join(install_dir, 'bar'), 'rb') as f:
        self.assertEqual('updated_cache', f.read())
      cache_manager.uninstall(install_dir, 'foo')

  def test_start_task_runner_fail_on_startup(self):
    def _get_run_isolated():
      return ['invalid_commad_that_shouldnt_exist']
    self.mock(task_runner, 'get_run_isolated', _get_run_isolated)
    with self.assertRaises(task_runner._FailureOnStart) as e:
      task_runner._start_task_runner([], self.work_dir, None)
    # TODO(maruel): Fix on Windows.
    self.assertEqual(2, e.exception.exit_code)

  def test_main(self):
    def load_and_run(
        manifest, swarming_server, is_grpc, cost_usd_hour, start,
        json_file, run_isolated_flags, bot_file, auth_params_file):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server)
      self.assertFalse(is_grpc)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual('task_summary.json', json_file)
      self.assertEqual(['--min-free-space', '1'], run_isolated_flags)
      self.assertEqual('/path/to/bot-file', bot_file)
      self.assertEqual('/path/to/auth-params-file', auth_params_file)

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost',
      '--in-file', 'foo',
      '--out-file', 'task_summary.json',
      '--cost-usd-hour', '3600',
      '--start', str(time.time()),
      '--bot-file', '/path/to/bot-file',
      '--auth-params-file', '/path/to/auth-params-file',
      '--',
      '--min-free-space', '1',
    ]
    self.assertEqual(0, task_runner.main(cmd))

  def test_main_grpc(self):
    def load_and_run(
        manifest, swarming_server, is_grpc, cost_usd_hour, start,
        json_file, run_isolated_flags, bot_file, auth_params_file):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server)
      self.assertTrue(is_grpc)
      self.assertEqual(3600., cost_usd_hour)
      self.assertEqual(time.time(), start)
      self.assertEqual('task_summary.json', json_file)
      self.assertEqual(['--min-free-space', '1'], run_isolated_flags)
      self.assertEqual('/path/to/bot-file', bot_file)
      self.assertEqual('/path/to/auth-params-file', auth_params_file)

    self.mock(task_runner, 'load_and_run', load_and_run)
    cmd = [
      '--swarming-server', 'http://localhost',
      '--in-file', 'foo',
      '--out-file', 'task_summary.json',
      '--cost-usd-hour', '3600',
      '--start', str(time.time()),
      '--bot-file', '/path/to/bot-file',
      '--auth-params-file', '/path/to/auth-params-file',
      '--is-grpc',
      '--',
      '--min-free-space', '1',
    ]
    self.assertEqual(0, task_runner.main(cmd))


class TestTaskRunnerNoTimeMock(TestTaskRunnerBase):
  # Do not mock time.time() for these tests otherwise it becomes a tricky
  # implementation detail check.
  # These test cases run the command for real.

  # TODO(maruel): Calculate this value automatically through iteration? This is
  # really bad and prone to flakiness.
  SHORT_TIME_OUT = 1.

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
      output_re='^hi\n$', auth_headers=None):
    def check_final(kwargs):
      kwargs['data'].pop('bot_overhead', None)
      if hard_timeout or io_timeout:
        self.assertLess(
            self.SHORT_TIME_OUT, kwargs['data'].pop('cost_usd', None))
        self.assertLess(
            self.SHORT_TIME_OUT, kwargs['data'].pop('duration', None))
      else:
        self.assertLess(0., kwargs['data'].pop('cost_usd', None))
        self.assertLess(0., kwargs['data'].pop('duration', None))

      output = ''
      if 'output' in kwargs['data']:
        output = base64.b64decode(kwargs['data'].pop('output'))
      self.assertTrue(re.match(output_re, output), (kwargs, output))

      self.assertEqual(
          {
            'data': {
              'exit_code': exit_code,
              'hard_timeout': hard_timeout,
              'id': 'localhost',
              'io_timeout': io_timeout,
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'follow_redirects': False,
            'timeout': 180,
            'headers': auth_headers or {},
          },
          kwargs)
    return check_final

  def _load_and_run(self, manifest):
    # Dot not mock time since this test class is testing timeouts.
    server = 'https://localhost:1'
    in_file = os.path.join(self.work_dir, 'task_runner_in.json')
    with open(in_file, 'wb') as f:
      json.dump(manifest, f)
    out_file = os.path.join(self.work_dir, 'task_runner_out.json')
    task_runner.load_and_run(
        in_file, server, False, 3600., time.time(), out_file,
        ['--min-free-space', '1'], None, None)
    with open(out_file, 'rb') as f:
      return json.load(f)

  def _run_command(self, task_details):
    # Dot not mock time since this test class is testing timeouts.
    remote = remote_client.createRemoteClient('https://localhost:1', None,
                                              False)
    with luci_context.stage(local_auth=None) as ctx_file:
      return task_runner.run_command(
          remote, task_details, self.work_dir, 3600., time.time(),
          ['--min-free-space', '1'], '/path/to/file', ctx_file)

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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    actual = self._run_command(task_details)
    actual.pop('bot_overhead', None)
    self.assertEqual(expected, actual)

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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_hard_signal(self):
    self.requests(
        hard_timeout=True,
        exit_code=0,
        output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL, hard_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': 0,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io_signal(self):
    self.requests(
        io_timeout=True, exit_code=0,
        output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL, io_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': 0,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
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
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_hard_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    self.requests(
        hard_timeout=True, exit_code=exit_code,
        output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': exit_code,
      u'hard_timeout': True,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_io_signal_no_grace(self):
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    self.requests(
        io_timeout=True, exit_code=exit_code,
        output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    task_details = self.get_task_details(
        self.SCRIPT_SIGNAL_HANG, io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
      u'exit_code': exit_code,
      u'hard_timeout': False,
      u'io_timeout': True,
      u'must_signal_internal_failure': None,
      u'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))

  def test_isolated_grand_children(self):
    """Runs a normal test involving 3 level deep subprocesses."""
    # Uses load_and_run()
    files = {
      'parent.py': (
        'import subprocess, sys\n'
        'sys.exit(subprocess.call([sys.executable,\'-u\',\'children.py\']))\n'),
      'children.py': (
        'import subprocess, sys\n'
        'sys.exit(subprocess.call('
            '[sys.executable, \'-u\', \'grand_children.py\']))\n'),
      'grand_children.py': 'print \'hi\'',
    }

    def check_final(kwargs):
      # Warning: this modifies input arguments.
      # Makes the diffing easier.
      kwargs['data']['output'] = base64.b64decode(kwargs['data']['output'])
      self.assertLess(0, kwargs['data'].pop('cost_usd'))
      self.assertLess(
          0, kwargs['data'].pop('bot_overhead', None), kwargs['data'])
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertLess(
          0., kwargs['data']['isolated_stats']['download'].pop('duration'))
      # duration==0 can happen on Windows when the clock is in the default
      # resolution, 15.6ms.
      self.assertLessEqual(
          0., kwargs['data']['isolated_stats']['upload'].pop('duration'))
      for k in ('download', 'upload'):
        for j in ('items_cold', 'items_hot'):
          kwargs['data']['isolated_stats'][k][j] = large.unpack(
              base64.b64decode(kwargs['data']['isolated_stats'][k][j]))
      self.assertEqual(
          {
            'data': {
              'exit_code': 0,
              'hard_timeout': False,
              'id': u'localhost',
              'io_timeout': False,
              'isolated_stats': {
                u'download': {
                  u'initial_number_items': 0,
                  u'initial_size': 0,
                  u'items_cold': [10, 86, 94, 276],
                  u'items_hot': [],
                },
                u'upload': {
                  u'items_cold': [],
                  u'items_hot': [],
                },
              },
              'output': 'hi\n',
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'follow_redirects': False,
            'timeout': 180,
            'headers': {},
          },
          kwargs)
    requests = [
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_first(0.),
        {'must_stop': False, 'ok': True},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        check_final,
        {'must_stop': False, 'ok': True},
      ),
    ]
    self.expected_requests(requests)

    server = isolateserver_mock.MockIsolateServer()
    try:
      isolated = json.dumps({
        'command': ['python', 'parent.py'],
        'files': {
          name: {
            'h': server.add_content_compressed('default-gzip', content),
            's': len(content),
          } for name, content in files.iteritems()
        },
      })
      isolated_digest = server.add_content_compressed('default-gzip', isolated)
      manifest = get_manifest(
          isolated={
            'input': isolated_digest,
            'namespace': 'default-gzip',
            'server': server.url,
          })
      expected = {
        u'exit_code': 0,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure': None,
        u'version': task_runner.OUT_VERSION,
      }
      self.assertEqual(expected, self._load_and_run(manifest))
    finally:
      server.close()

  def test_isolated_io_signal_no_grace_grand_children(self):
    """Handles grand-children process hanging and signal management.

    In this case, the I/O timeout is implemented by task_runner. An hard timeout
    would be implemented by run_isolated (depending on overhead).
    """
    # Uses load_and_run()
    # https://msdn.microsoft.com/library/cc704588.aspx
    # STATUS_CONTROL_C_EXIT=0xC000013A. Python sees it as -1073741510.
    exit_code = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM

    files = {
      'parent.py': (
        'import subprocess, sys\n'
        'print(\'parent\')\n'
        'p = subprocess.Popen([sys.executable, \'-u\', \'children.py\'])\n'
        'print(p.pid)\n'
        'p.wait()\n'
        'sys.exit(p.returncode)\n'),
      'children.py': (
        'import subprocess, sys\n'
        'print(\'children\')\n'
        'p = subprocess.Popen([sys.executable,\'-u\',\'grand_children.py\'])\n'
        'print(p.pid)\n'
        'p.wait()\n'
        'sys.exit(p.returncode)\n'),
      'grand_children.py': self.SCRIPT_SIGNAL_HANG,
    }
    # We need to catch the pid of the grand children to be able to kill it, so
    # create our own check_final() instead of using self._gen_requests().
    to_kill = []
    def check_final(kwargs):
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('cost_usd'))
      self.assertLess(self.SHORT_TIME_OUT, kwargs['data'].pop('duration'))
      self.assertLess(0., kwargs['data'].pop('bot_overhead'))
      self.assertLess(
          0., kwargs['data']['isolated_stats']['download'].pop('duration'))
      self.assertLess(
          0., kwargs['data']['isolated_stats']['upload'].pop('duration'))
      # Makes the diffing easier.
      for k in ('download', 'upload'):
        for j in ('items_cold', 'items_hot'):
          kwargs['data']['isolated_stats'][k][j] = large.unpack(
              base64.b64decode(kwargs['data']['isolated_stats'][k][j]))
      # The command print the pid of this child and grand-child processes, each
      # on its line.
      output = base64.b64decode(kwargs['data'].pop('output', ''))
      for line in output.splitlines():
        try:
          to_kill.append(int(line))
        except ValueError:
          pass
      self.assertEqual(
          {
            'data': {
              'exit_code': exit_code,
              'hard_timeout': False,
              'id': u'localhost',
              'io_timeout': True,
              'isolated_stats': {
                u'download': {
                  u'initial_number_items': 0,
                  u'initial_size': 0,
                  u'items_cold': [144, 150, 285, 307],
                  u'items_hot': [],
                },
                u'upload': {
                  u'items_cold': [],
                  u'items_hot': [],
                },
              },
              'output_chunk_start': 0,
              'task_id': 23,
            },
            'follow_redirects': False,
            'timeout': 180,
            'headers': {},
          },
          kwargs)
    requests = [
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        self.get_check_first(0.),
        {'must_stop': False, 'ok': True},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        check_final,
        {'must_stop': False, 'ok': True},
      ),
    ]
    self.expected_requests(requests)

    server = isolateserver_mock.MockIsolateServer()
    try:
      # TODO(maruel): -u is needed if you don't want python buffering to
      # interfere.
      isolated = json.dumps({
        'command': ['python', '-u', 'parent.py'],
        'files': {
          name: {
            'h': server.add_content_compressed('default-gzip', content),
            's': len(content),
          } for name, content in files.iteritems()
        },
      })
      isolated_digest = server.add_content_compressed('default-gzip', isolated)
      try:
        manifest = get_manifest(
            isolated={
              'input': isolated_digest,
              'namespace': 'default-gzip',
              'server': server.url,
            },
            # TODO(maruel): A bit cheezy, we'd want the I/O timeout to be just
            # enough to have the time for the PID to be printed but not more.
            io_timeout=1,
            grace_period=self.SHORT_TIME_OUT)
        expected = {
          u'exit_code': exit_code,
          u'hard_timeout': False,
          u'io_timeout': True,
          u'must_signal_internal_failure': None,
          u'version': task_runner.OUT_VERSION,
        }
        self.assertEqual(expected, self._load_and_run(manifest))
        self.assertEqual(2, len(to_kill))
      finally:
        for k in to_kill:
          try:
            if sys.platform == 'win32':
              os.kill(k, signal.SIGTERM)
            else:
              os.kill(k, signal.SIGKILL)
          except OSError:
            pass
    finally:
      server.close()


class TaskRunnerSmoke(unittest.TestCase):
  # Runs a real process and a real Swarming fake server.
  def setUp(self):
    super(TaskRunnerSmoke, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')
    logging.info('Temp: %s', self.root_dir)
    self._server = fake_swarming.Server(self)

  def tearDown(self):
    try:
      self._server.shutdown()
    finally:
      try:
        file_path.rmtree(self.root_dir)
      except OSError:
        print >> sys.stderr, 'Failed to delete %s' % self.root_dir
      finally:
        super(TaskRunnerSmoke, self).tearDown()

  def test_signal(self):
    # Tests when task_runner gets a SIGTERM.

    # https://msdn.microsoft.com/library/cc704588.aspx
    # STATUS_ENTRYPOINT_NOT_FOUND=0xc0000139. Python sees it as -1073741510.
    exit_code = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM

    os.mkdir(os.path.join(self.root_dir, 'w'))
    signal_file = os.path.join(self.root_dir, 'w', 'signal')
    open(signal_file, 'wb').close()

    # As done by bot_main.py.
    manifest = get_manifest(
        script='import os,time;os.remove(%r);time.sleep(60)' % signal_file,
        hard_timeout=60., io_timeout=60.)
    task_in_file = os.path.join(self.root_dir, 'w', 'task_runner_in.json')
    task_result_file = os.path.join(self.root_dir, 'w', 'task_runner_out.json')
    with open(task_in_file, 'wb') as f:
      json.dump(manifest, f)

    bot = os.path.join(self.root_dir, 'swarming_bot.1.zip')
    code, _ = fake_swarming.gen_zip(self._server.url)
    with open(bot, 'wb') as f:
      f.write(code)
    cmd = [
      sys.executable, bot, 'task_runner',
      '--swarming-server', self._server.url,
      '--in-file', task_in_file,
      '--out-file', task_result_file,
      '--cost-usd-hour', '1',
      # Include the time taken to poll the task in the cost.
      '--start', str(time.time()),
      '--',
      '--cache', 'isolated_cache_party',
    ]
    logging.info('%s', cmd)
    proc = subprocess42.Popen(cmd, cwd=self.root_dir, detached=True)
    # Wait for the child process to be alive.
    while os.path.isfile(signal_file):
      time.sleep(0.01)
    # Send SIGTERM to task_runner itself. Ensure the right thing happen.
    # Note that on Windows, this is actually sending a SIGBREAK since there's no
    # such thing as SIGTERM.
    proc.send_signal(signal.SIGTERM)
    proc.wait()
    task_runner_log = os.path.join(self.root_dir, 'logs', 'task_runner.log')
    with open(task_runner_log, 'rb') as f:
      logging.info('task_runner.log:\n---\n%s---', f.read())
    self.assertEqual([], self._server.get_events())
    tasks = self._server.get_tasks()
    for task in tasks.itervalues():
      for event in task:
        event.pop('cost_usd')
        event.pop('duration', None)
        event.pop('bot_overhead', None)
    expected = {
      '23': [
        {
          u'id': u'localhost',
          u'task_id': 23,
        },
        {
          u'exit_code': exit_code,
          u'hard_timeout': False,
          u'id': u'localhost',
          u'io_timeout': False,
          u'task_id': 23,
        },
      ],
    }
    self.assertEqual(expected, tasks)
    expected = {
      'swarming_bot.1.zip',
      'e2bfe61c8f0dc89e72a854f4afb14f4b662ea6301fc5652ebe03f80fa2b06263-cacert.'
          'pem',
      'w',
      'isolated_cache_party',
      'logs',
      'c',
    }
    self.assertEqual(expected, set(os.listdir(self.root_dir)))
    expected = {
      u'exit_code': exit_code,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure':
          u'task_runner received signal %d' % task_runner.SIG_BREAK_OR_TERM,
      u'version': 3,
    }
    with open(task_result_file, 'rb') as f:
      self.assertEqual(expected, json.load(f))
    self.assertEqual(0, proc.returncode)


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging_utils.prepare_logging(None)
  logging_utils.set_console_level(
      logging.DEBUG if '-v' in sys.argv else logging.CRITICAL+1)
  # Fix litteral text expectation.
  os.environ['LANG'] = 'en_US.UTF-8'
  os.environ['LANGUAGE'] = 'en_US.UTF-8'
  unittest.main()
