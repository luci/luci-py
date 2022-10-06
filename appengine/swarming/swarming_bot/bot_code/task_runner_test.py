#!/usr/bin/env vpython3
# coding=utf-8
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

from __future__ import print_function

import base64
import datetime
import fnmatch
import json
import logging
import os
import random
import re
import signal
import string
import sys
import tempfile
import threading
import time
import unittest

import mock

import test_env_bot_code
test_env_bot_code.setup_test_env()

ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(test_env_bot_code.BOT_DIR)))
CLIENT_DIR = os.path.join(ROOT_DIR, 'client')
LUCI_GO_CLIENT_DIR = os.path.join(ROOT_DIR, 'luci-go')

# Needed for local_caching, and others on Windows when symlinks are not enabled.
sys.path.insert(0, CLIENT_DIR)

from bot_code import bot_auth
from bot_code import remote_client
from bot_code import task_runner
from depot_tools import auto_stub
from depot_tools import fix_encoding
from libs import luci_context
from utils import file_path
from utils import fs
from utils import large
from utils import logging_utils
from utils import subprocess42
from utils import tools
import cas_util
import local_caching
import swarmingserver_bot_fake

def to_native_eol(s):
  if sys.platform == 'win32':
    return s.replace('\n', '\r\n')
  return s

def gen_task_id():
  return ''.join([random.choice(string.digits) for _ in range(10)])


DISABLE_CIPD_FOR_TESTS = ['--cipd-enabled', 'false']

EXIT_CODE_TERM = -signal.SIGTERM
# Actually 0xc000013a, unsigned in python3 on windows
if sys.platform == 'win32':
  EXIT_CODE_TERM = 3221225786


def get_manifest(script=None, **kwargs):
  """Returns a task manifest similar to what the server sends back to the bot.

  Eventually this should be a proto message.
  """
  out = {
      'bot_authenticated_as': 'foo',
      'bot_id': 'localhost',
      'bot_dimensions': {
          'id': ['localhost'],
          'cpu': ['x86', 'x86-64'],
      },
      'caches': [],
      'cipd_input': {},
      'command': [sys.executable, '-u', '-c', script],
      'containment': {
          'containment_type': 'NONE',
      },
      'dimensions': {},
      'env': {},
      'env_prefixes': {},
      'grace_period': 30.,
      'hard_timeout': 10.,
      'host': 'bar',
      'io_timeout': 10.,
      'cas_input_root': None,
      'outputs': [],
      'realm': None,
      'relative_cwd': None,
      'resultdb': None,
      'secret_bytes': None,
      'service_accounts': None,
      'task_id': gen_task_id(),
  }
  out.update(kwargs)
  return out


def get_task_details(*args, **kwargs):
  return task_runner.TaskDetails(get_manifest(*args, **kwargs))


def run_command(server_url, work_dir, task_details, headers_cb):
  """Runs a command with an initialized client."""
  remote = remote_client.createRemoteClient(server_url, headers_cb, 'localhost',
                                            work_dir)
  remote.bot_id = task_details.bot_id
  with luci_context.stage(local_auth=None) as ctx_file:
    return task_runner.run_command(remote, task_details, work_dir, 3600.,
                                   time.time(), ['--min-free-space', '1'] +
                                   DISABLE_CIPD_FOR_TESTS, '/path/to/file',
                                   ctx_file)


def load_and_run(server_url, work_dir, manifest, auth_params_file):
  """Wraps task_runner.load_and_run() which runs a Swarming task."""
  in_file = os.path.join(work_dir, 'task_runner_in.json')
  with open(in_file, 'w') as f:
    json.dump(manifest, f)
  out_file = os.path.join(work_dir, 'task_runner_out.json')
  task_runner.load_and_run(in_file, server_url, 3600., time.time(), out_file,
                           ['--min-free-space', '1'] + DISABLE_CIPD_FOR_TESTS,
                           None, auth_params_file)
  with open(out_file, 'rb') as f:
    return json.load(f)


def wrap_output_bytes_in_re(output):
  """Wraps output in a regexp which matches '<any-string>output'"""
  return re.compile(b"(\\s|\\S)*%s" % output)


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


class TestTaskRunnerBase(auto_stub.TestCase):
  def setUp(self):
    super(TestTaskRunnerBase, self).setUp()
    tools.clear_cache_all()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')
    self.work_dir = os.path.join(self.root_dir, 'w')
    # Create the logs directory so run_isolated.py can put its log there.
    self.logs_dir = os.path.join(self.root_dir, 'logs')
    os.chdir(self.root_dir)
    os.mkdir(self.work_dir)
    os.mkdir(self.logs_dir)
    logging.info('Temp: %s', self.root_dir)

    # Mock this since swarming_bot.zip is not accessible.
    def _get_run_isolated():
      return [sys.executable, '-u', os.path.join(CLIENT_DIR, 'run_isolated.py')]
    self.mock(task_runner, 'get_run_isolated', _get_run_isolated)

    # In case this test itself is running on Swarming, clear the bot
    # environment.
    os.environ.pop('LUCI_CONTEXT', None)
    os.environ.pop('SWARMING_AUTH_PARAMS', None)
    os.environ.pop('SWARMING_BOT_ID', None)
    os.environ.pop('SWARMING_TASK_ID', None)
    os.environ.pop('SWARMING_SERVER', None)
    os.environ['LUCI_GO_CLIENT_DIR'] = LUCI_GO_CLIENT_DIR
    # Make HTTP headers consistent
    self.mock(remote_client, 'make_appengine_id', lambda *a: 42)
    self._server = None
    self._cas = None

  def tearDown(self):
    os.chdir(test_env_bot_code.BOT_DIR)
    try:
      try:
        if self._server:
          self._server.close()
        if self._cas:
          self._cas.stop()
      finally:
        logging.debug(self.logs_dir)
        for i in os.listdir(self.logs_dir):
          with open(os.path.join(self.logs_dir, i), 'r') as f:
            logging.debug('%s:\n%s', i, ''.join('  ' + line for line in f))
        file_path.rmtree(self.root_dir)
    except OSError:
      print('Failed to delete %s' % self.root_dir, file=sys.stderr)
      raise
    finally:
      super(TestTaskRunnerBase, self).tearDown()

  @property
  def server(self):
    """Lazily starts a Swarming fake bot API server."""
    if not self._server:
      self._server = swarmingserver_bot_fake.Server()
    return self._server

  @property
  def cas(self):
    """Lazily starts an local CAS server."""
    if not self._cas:
      self._cas = cas_util.LocalCAS(tempfile.mkdtemp(prefix='local_cas'))
      self._cas.start()
      os.environ['RUN_ISOLATED_CAS_ADDRESS'] = self._cas.address
    return self._cas

  def getTaskResults(self, task_id):
    """Returns a flattened task result."""
    tasks = self.server.get_tasks()
    self.assertEqual([task_id], sorted(tasks))
    actual = swarmingserver_bot_fake.flatten_task_updates(tasks[task_id])
    # Always decode the output;
    if 'output' in actual:
      actual['output'] = cas_util.filter_out_go_logs(
          base64.b64decode(actual['output']).decode()).encode()
    if 'isolated_stats' in actual:
      stats = actual['isolated_stats']
      for i in stats.keys():
        for j in ['items_cold', 'items_hot']:
          if stats[i] and j in stats[i]:
            stats[i][j] = large.unpack(base64.b64decode(stats[i][j] or ''))
    return actual

  def expectTask(self, task_id, **kwargs):
    """Asserts the task update sent by task_runner to the server.

    It doesn't disambiguate individual task_update, so if you care about the
    individual packets (like internal timeouts), check them separately.

    Returns:
      flattened task result as seen on the server, with output decoded.
    """
    actual = self.getTaskResults(task_id)
    out = actual.copy()
    expected = {
        'bot_overhead': 0.,
        'cost_usd': 0.,
        'duration': 0.,
        'exit_code': 0,
        'hard_timeout': False,
        'id': 'localhost',
        'io_timeout': False,
        'isolated_stats': {
            'download': {},
        },
        'output': wrap_output_bytes_in_re(to_native_eol('hi\n').encode()),
        'output_chunk_start': 0,
        'task_id': task_id,
    }
    for k, v in kwargs.items():
      if v is None:
        expected.pop(k)
      else:
        expected[k] = v

    # Use explicit <= verification for these.
    for k in ('bot_overhead', 'cost_usd', 'duration'):
      # Actual values must be equal or larger than the expected values.
      if k in actual:
        self.assertLessEqual(expected.pop(k), actual.pop(k))
    # Use regexp if requested.
    if hasattr(expected['output'], 'pattern'):
      v = actual.pop('output')
      r = expected.pop('output')
      self.assertTrue(
          r.match(v),
          "failed to match output. pattern: %s, actual: %s" % (r.pattern, v))
    for key, stats in expected.get('isolated_stats', {}).items():
      if 'isolated_stats' not in actual:
        # expected but not actual.
        break
      if not stats:
        continue
      if 'duration' in stats:
        v = actual['isolated_stats'][key].pop('duration')
        self.assertLessEqual(stats.pop('duration'), v)

    # drop duration stats.
    actual.pop('cache_trim_stats', None)
    actual.pop('named_caches_stats', None)
    actual.pop('cleanup_stats', None)
    # Rest is explicit comparison.
    self.assertEqual(expected, actual)
    return out

  def _run_command(self, task_details):
    return run_command(self.server.url, self.work_dir, task_details, None)


class TestTaskRunner(TestTaskRunnerBase):
  # Test cases that do not involve a timeout.
  # These test cases run the command for real.

  def _expect_files(self, expected):
    # Confirm work_dir against a dict of expected files.
    expected = expected[:]
    for root, dirs, filenames in fs.walk(self.root_dir):
      if 'logs' in dirs:
        dirs.remove('logs')
      for filename in filenames:
        p = os.path.relpath(os.path.join(root, filename), self.root_dir)
        for i, e in enumerate(expected):
          if fnmatch.fnmatch(p, e):
            expected.pop(i)
            break
        else:
          self.fail((p, expected))
    if expected:
      self.fail(expected)

  def test_run_command_raw(self):
    task_details = get_task_details('print(\'hi\')')
    expected = {
      'exit_code': 0,
      'hard_timeout': False,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id)

  def test_run_command_env_prefix_one(self):
    task_details = get_task_details(
        'import os\nprint(os.getenv("PATH").split(os.pathsep)[0])',
        env_prefixes={
            'PATH': ['./local/smurf', './other/thing'],
        })
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    sep = re.escape(os.sep)
    self.expectTask(task_details.task_id,
                    output=re.compile(
                        (to_native_eol('(\\s|\\S)*%slocal%ssmurf\n$') %
                         (sep, sep)).encode()))

  def test_run_command_env_prefix_multiple(self):
    task_details = get_task_details(
        '\n'.join([
            'import os',
            'print(os.path.realpath(os.getcwd()))',
            'path = os.getenv("PATH").split(os.pathsep)',
            'print(os.path.realpath(path[0]))',
            'print(os.path.realpath(path[1]))',
        ]),
        env_prefixes={
            'PATH': ['./local/smurf', './other/thing'],
        })
    expected = {
      'exit_code': 0,
      'hard_timeout': False,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    sep = re.escape(os.sep)
    output = re.compile(((r'(\s|\S)*'
                          r'(?P<cwd>[^\n]*)\n'
                          r'(?P=cwd)%slocal%ssmurf\n'
                          r'(?P=cwd)%sother%sthing\n'
                          r'$') % (sep, sep, sep, sep)).encode())
    if sys.platform == 'win32':
      output = re.compile(output.pattern.replace(br'\n', br'\r\n'))
    self.expectTask(task_details.task_id, output=output)

  def test_run_command_isolated(self):
    # Hook run_isolated out to see that everything still work.
    task_details = get_task_details()
    # Mock running run_isolated with a script.
    SCRIPT_ISOLATED = ('import json, sys;\n'
                       'args = []\n'
                       'if len(sys.argv) != 3 or sys.argv[1] != \'-a\':\n'
                       '  raise Exception(sys.argv)\n'
                       'with open(sys.argv[2], \'r\') as argsfile:\n'
                       '  args = json.loads(argsfile.read())\n'
                       'if len(args) != 1:\n'
                       '  raise Exception(args);\n'
                       'with open(args[0], \'w\') as f:\n'
                       '  json.dump({\n'
                       '    \'exit_code\': 0,\n'
                       '    \'had_hard_timeout\': False,\n'
                       '    \'internal_failure\': None,\n'
                       '    \'outputs_ref\': None,\n'
                       '    \'cas_output_root\': {\n'
                       '      \'cas_instance\': '
                       '        \'projects/test/instances/default_instance\',\n'
                       '      \'digest\': {\n'
                       '        \'hash\': \'123\',\n'
                       '        \'size_bytes\': 1,\n'
                       '      }, \n'
                       '    },\n'
                       '  }, f)\n'
                       'sys.stdout.write(\'hi\\n\')')
    self.mock(task_runner, 'get_run_isolated',
              lambda: [sys.executable, '-u', '-c', SCRIPT_ISOLATED])
    self.mock(
        task_runner, 'get_isolated_args',
        lambda work_dir, details, isolated_result, bot_file, run_isolated_flags:
            [isolated_result])
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id,
                    isolated_stats=None,
                    cas_output_root={
                        'cas_instance':
                        'projects/test/instances/default_instance',
                        'digest': {
                            'hash': '123',
                            'size_bytes': 1,
                        },
                    })

  def test_run_command_fail(self):
    task_details = get_task_details('import sys; print(\'hi\'); sys.exit(1)')
    expected = {
        'exit_code': 1,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id, exit_code=1)

  def test_run_command_bad_cas(self):
    task_details = get_task_details()
    # Mock running run_isolated with a script that outputs 'missing_cas' field
    SCRIPT_ISOLATED = (
        'import json, sys;\n'
        'args = []\n'
        'if len(sys.argv) != 3 or sys.argv[1] != \'-a\':\n'
        '  raise Exception(sys.argv)\n'
        'with open(sys.argv[2], \'r\') as argsfile:\n'
        '  args = json.loads(argsfile.read())\n'
        'if len(args) != 1:\n'
        '  raise Exception(args);\n'
        'with open(args[0], \'w\') as f:\n'
        '  json.dump({\n'
        '    \'exit_code\': 1,\n'
        '    \'had_hard_timeout\': False,\n'
        '    \'internal_failure\': \'Invalid CAS blahblahblah\',\n'
        '    \'missing_cas\': {\n'
        '       \'digest\': \'blahblahblah\',\n'
        '       \'instance\': \'some_instance\'\n'
        '    },\n'
        '  }, f)\n'
        'sys.stdout.write(\'hi\\n\')')
    self.mock(task_runner, 'get_run_isolated', lambda:
              [sys.executable, '-u', '-c', SCRIPT_ISOLATED])
    self.mock(
        task_runner, 'get_isolated_args', lambda work_dir, details,
        isolated_result, bot_file, run_isolated_flags: [isolated_result])

    self._run_command(task_details)
    # Verify the correct error was posted.
    errors = self.server.get_task_errors()
    expected = {
        task_details.task_id: [{
            'message': 'Invalid CAS blahblahblah',
            'id': 'localhost',
            'task_id': task_details.task_id,
            'client_error': {
                'missing_cas': {
                    'digest': 'blahblahblah',
                    'instance': 'some_instance'
                },
                'missing_cipd': [],
            }
        }],
    }
    self.assertEqual(expected, errors)

  def test_run_command_bad_cipd(self):
    task_details = get_task_details()
    # Mock running run_isolated with a script that outputs 'missing_cipd' field
    SCRIPT_ISOLATED = (
        'import json, sys;\n'
        'args = []\n'
        'if len(sys.argv) != 3 or sys.argv[1] != \'-a\':\n'
        '  raise Exception(sys.argv)\n'
        'with open(sys.argv[2], \'r\') as argsfile:\n'
        '  args = json.loads(argsfile.read())\n'
        'if len(args) != 1:\n'
        '  raise Exception(args);\n'
        'with open(args[0], \'w\') as f:\n'
        '  json.dump({\n'
        '    \'exit_code\': 1,\n'
        '    \'had_hard_timeout\': False,\n'
        '    \'internal_failure\': \'Invalid CIPD blahblahblah\',\n'
        '    \'missing_cipd\': [{\n'
        '       \'package_name\': \'blahblahblah\',\n'
        '       \'version\': \'abcd1234\',\n'
        '       \'path\': \'not/found/here\'\n'
        '     }],\n'
        '  }, f)\n'
        'sys.stdout.write(\'hi\\n\')')
    self.mock(task_runner, 'get_run_isolated', lambda:
              [sys.executable, '-u', '-c', SCRIPT_ISOLATED])
    self.mock(
        task_runner, 'get_isolated_args', lambda work_dir, details,
        isolated_result, bot_file, run_isolated_flags: [isolated_result])

    self._run_command(task_details)
    # Verify the correct error was posted.
    errors = self.server.get_task_errors()
    expected = {
        task_details.task_id: [{
            'message': 'Invalid CIPD blahblahblah',
            'id': 'localhost',
            'task_id': task_details.task_id,
            'client_error': {
                'missing_cipd': [{
                    'package_name': 'blahblahblah',
                    'version': 'abcd1234',
                    'path': 'not/found/here'
                }],
                'missing_cas': [],
            },
        }],
    }
    self.assertEqual(expected, errors)

  def test_run_command_os_error(self):
    task_details = get_task_details(
        command=[
          'executable_that_shouldnt_be_on_your_system',
          'thus_raising_OSError',
        ])
    expected = {
        'exit_code': 1,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    pattern = (
        # This is a beginning of run_isolated.py's output if binary is not
        # found.
        br'(\S|\s)*'
        br'<The executable does not exist, a dependent library is missing or '
        br'the command line is too long>\n'
        br'<Check for missing .so/.dll in the .isolate or GN file or length of '
        br'command line args>')
    if sys.platform == 'win32':
      pattern = pattern.replace(br'\n', br'\r\n')
    output = re.compile(pattern)
    out = self.expectTask(task_details.task_id, exit_code=1, output=output)
    self.assertGreater(10., out['cost_usd'])

  def test_isolated_grand_children(self):
    """Runs a normal test involving 3 level deep subprocesses.

    It is the equivalent of test_isolated_io_signal_grand_children() that fails,
    this is the succeeding version.
    """
    files = {
        'parent.py':
            (b'import subprocess, sys\n'
             b'res = subprocess.call([sys.executable,\'-u\',\'children.py\'])\n'
             b'sys.exit(res)\n'),
        'children.py': (b'import subprocess, sys\n'
                        b'sys.exit(subprocess.call('
                        b'[sys.executable, \'-u\', \'grand_children.py\']))\n'),
        'grand_children.py': b'print(\'hi\')',
    }

    digest = self.cas.archive_files(files)
    manifest = get_manifest(
        cas_input_root={
            'cas_instance': 'projects/test/instances/default_instance',
            'digest': {
                'hash': digest.split('/')[0],
                'size_bytes': digest.split('/')[1],
            },
        },
        command=['python3', 'parent.py'],
    )
    actual = load_and_run(self.server.url, self.work_dir, manifest, None)
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    contens = list(files.values())
    items_in = [len(c) for c in contens]
    self.assertEqual(expected, actual)
    self.expectTask(manifest['task_id'],
                    isolated_stats={
                        'download': {
                            'duration': 0,
                            'items_cold': sorted(items_in),
                            'items_hot': [],
                        },
                    })

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
            b'hi!\n',
            b'hi!\n',
            b'hi!\n' * 100000,
            b'hi!\n',
        ]
        if sys.platform == 'win32':
          self2._out = [out.replace(b'\n', b'\r\n') for out in self2._out]

      def yield_any(self2, maxsize, timeout):
        self.assertLess(0, maxsize())
        self.assertLess(0, timeout())
        for i in self2._out:
          yield 'stdout', i

      @staticmethod
      def wait():
        return 0

      @staticmethod
      def kill():
        return True

    self.mock(subprocess42, 'Popen', Popen)

    task_details = get_task_details()
    expected = {
      'exit_code': 0,
      'hard_timeout': False,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(
        task_details.task_id,
        bot_overhead=None,
        isolated_stats=None,
        output=to_native_eol('hi!\n' * 100003).encode())
    # Here, we want to carefully check the packets sent to ensure the internal
    # timer works as expected. There's 3 updates:
    # - initial task startup with no output
    # - buffer filled with the 3 first yield
    # - last yield
    updates = self.server.get_tasks()[task_details.task_id]
    self.assertEqual(3, len(updates))
    self.assertEqual(None, updates[0].get('output'))
    self.assertEqual(base64.b64encode(
      to_native_eol('hi!\n' * 100002).encode()), updates[1]['output'].encode())
    self.assertEqual(base64.b64encode(
      to_native_eol('hi!\n').encode()), updates[2]['output'].encode())

  def test_run_command_caches(self):
    # This test puts a file into a named cache, remove it, runs a test that
    # updates the named cache, remaps it and asserts the content was updated.
    #
    # Directories:
    #   <root_dir>/
    #   <root_dir>/c - <cache_dir> named cache root
    #   <root_dir>/dest - <dest_dir> used for manual cache update
    #   <root_dir>/w - <self.work_dir> used by the task.
    cache_dir = os.path.join(self.root_dir, 'c')
    dest_dir = os.path.join(self.root_dir, 'dest')
    policies = local_caching.CachePolicies(0, 0, 0, 0)

    # Inject file 'bar' in the named cache 'foo'.
    cache = local_caching.NamedCache(cache_dir, policies)
    cache.install(dest_dir, 'foo')
    with open(os.path.join(dest_dir, 'bar'), 'wb') as f:
      f.write(b'thecache')
    cache.uninstall(dest_dir, 'foo')
    self.assertFalse(os.path.exists(dest_dir))

    self._expect_files(['c/*/bar', 'c/state.json'])

    # Maps the cache 'foo' as 'cache_foo'. This runs inside self.work_dir.
    # This runs the command for real.
    script = ('import os\n'
              'print("hi")\n'
              'with open("cache_foo/bar", "r") as f:\n'
              '  cached = f.read()\n'
              'with open("../../result", "w") as f:\n'
              '  f.write(cached)\n'
              'with open("cache_foo/bar", "w") as f:\n'
              '  f.write("updated_cache")\n')
    task_details = get_task_details(
        script, caches=[{'name': 'foo', 'path': 'cache_foo', 'hint': '100'}])
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    self._expect_files([
        'c/*/bar',
        'c/state.json',
        'w/cas-cache/state.json',
        'result',
        'w/run_isolated_args.json',
    ])

    # Ensure the 'result' file written my the task contained foo/bar.
    with open(os.path.join(self.root_dir, 'result'), 'rb') as f:
      self.assertEqual(b'thecache', f.read())
    os.remove(os.path.join(self.root_dir, 'result'))

    cache = local_caching.NamedCache(cache_dir, policies)
    self.assertFalse(os.path.exists(dest_dir))
    self._expect_files([
        'w/cas-cache/state.json',
        'c/*/bar',
        'c/state.json',
        'w/run_isolated_args.json',
    ])
    cache.install(dest_dir, 'foo')
    self._expect_files([
        'dest/bar',
        'c/state.json',
        'w/run_isolated_args.json',
        'w/cas-cache/state.json',
    ])
    with open(os.path.join(dest_dir, 'bar'), 'rb') as f:
      self.assertEqual(b'updated_cache', f.read())
    cache.uninstall(dest_dir, 'foo')
    self.assertFalse(os.path.exists(dest_dir))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id)

  def test_start_task_runner_fail_on_startup(self):
    def _get_run_isolated():
      return ['invalid_commad_that_shouldnt_exist']
    self.mock(task_runner, 'get_run_isolated', _get_run_isolated)
    with self.assertRaises(task_runner._FailureOnStart) as e:
      task_runner._start_task_runner([], self.work_dir, None)
    self.assertEqual(2, e.exception.exit_code)

  def test_main(self):

    def _load_and_run(manifest, swarming_server, cost_usd_hour, start,
                      json_file, run_isolated_flags, bot_file,
                      auth_params_file):
      self.assertEqual('foo', manifest)
      self.assertEqual(self.server.url, swarming_server)
      self.assertEqual(3600., cost_usd_hour)
      self.assertGreaterEqual(time.time(), start)
      self.assertEqual('task_summary.json', json_file)
      self.assertEqual(['--min-free-space', '1'], run_isolated_flags)
      self.assertEqual('/path/to/bot-file', bot_file)
      self.assertEqual('/path/to/auth-params-file', auth_params_file)

    self.mock(task_runner, 'load_and_run', _load_and_run)
    cmd = [
        '--swarming-server',
        self.server.url,
        '--in-file',
        'foo',
        '--out-file',
        'task_summary.json',
        '--cost-usd-hour',
        '3600',
        '--start',
        str(time.time()),
        '--bot-file',
        '/path/to/bot-file',
        '--auth-params-file',
        '/path/to/auth-params-file',
        '--',
        '--min-free-space',
        '1',
    ]
    self.assertEqual(0, task_runner.main(cmd))


class TestTaskRunnerKilled(TestTaskRunnerBase):
  # These test cases run the command for real where the process is killed.

  # TODO(maruel): Calculate this value automatically through iteration? This is
  # really bad and prone to flakiness.
  SHORT_TIME_OUT = 3.

  # Here's a simple script that handles signals properly. Sadly SIGBREAK is not
  # defined on posix.
  SCRIPT_SIGNAL = ('import signal, sys, threading;\n'
                   'event = threading.Event();\n'
                   'def handler(signum, _):\n'
                   '  event.set();\n'
                   '  print(\'got signal %%d\' %% signum);\n'
                   '  sys.stdout.flush();\n'
                   'signal.signal(signal.%s, handler);\n'
                   'print(\'hi\');\n'
                   'sys.stdout.flush();\n'
                   'while not event.is_set():\n'
                   '  pass;\n'
                   'print(\'bye\');\n'
                   'sys.stdout.flush();') % ('SIGBREAK' if sys.platform ==
                                             'win32' else 'SIGTERM')

  SCRIPT_SIGNAL_HANG = ('import signal, sys, time, threading;\n'
                        'event = threading.Event();\n'
                        'def handler(signum, _):\n'
                        '  event.set();\n'
                        '  print(\'got signal %%d\' %% signum);\n'
                        '  sys.stdout.flush();\n'
                        'signal.signal(signal.%s, handler);\n'
                        'print(\'hi\');\n'
                        'sys.stdout.flush();\n'
                        'while not event.is_set():\n'
                        '  pass;\n'
                        'print(\'bye\');\n'
                        'sys.stdout.flush();\n'
                        'time.sleep(100);') % ('SIGBREAK' if sys.platform ==
                                               'win32' else 'SIGTERM')

  SCRIPT_HANG = 'import time; print(\'hi\'); time.sleep(100)'

  def test_killed_early(self):
    # The task is killed on first update, so it doesn't have the chance to do
    # anything.
    task_details = get_task_details('print(\'hi\')')
    # task_runner is told to kill the task right on the first task update.
    self.server.must_stop = True
    expected = {
        'exit_code': -1,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': 3,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    expected = {
        'id': 'localhost',
        'task_id': task_details.task_id,
        'canceled': True,
        'exit_code': -1
    }
    actual = self.getTaskResults(task_details.task_id)
    self.assertLessEqual(0, actual.pop('cost_usd'))
    self.assertEqual(expected, actual)

  def test_killed_later(self):
    # Case where a task started and a client asks the server to kill the task.
    # In this case the task results in state KILLED.

    # Make the task update a busy loop to reduce the duration of this test case.
    self.mock(task_runner._OutputBuffer, '_MIN_PACKET_INTERVAL', 0.2)
    self.mock(task_runner._OutputBuffer, '_MAX_PACKET_INTERVAL', 0.2)

    # We need to 'prime' the server before starting the thread.
    self.assertTrue(self.server.url)

    task_details = get_task_details(
        'import sys,time;sys.stdout.write(\'hi\\n\');time.sleep(100)')

    # Cheezy but good enough.
    def run():
      # Wait until there's output.
      while True:
        self.server.has_updated_task.wait()
        self.server.has_updated_task.clear()
        if 'output' in self.getTaskResults(task_details.task_id):
          self.server.must_stop = True
          break

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()

    expected = {
      'exit_code': EXIT_CODE_TERM,
      'hard_timeout': False,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': 3,
    }
    self.assertEqual(expected, self._run_command(task_details))

    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id, exit_code=EXIT_CODE_TERM)
    t.join()

  def test_hard(self):
    task_details = get_task_details(
        self.SCRIPT_HANG, hard_timeout=self.SHORT_TIME_OUT)
    expected = {
      'exit_code': EXIT_CODE_TERM,
      'hard_timeout': True,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(
        task_details.task_id, hard_timeout=True, exit_code=EXIT_CODE_TERM)

  def test_io(self):
    task_details = get_task_details(
        self.SCRIPT_HANG, io_timeout=self.SHORT_TIME_OUT)
    expected = {
        'exit_code': EXIT_CODE_TERM,
        'hard_timeout': False,
        'io_timeout': True,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(
      task_details.task_id, io_timeout=True, exit_code=EXIT_CODE_TERM)

  def test_hard_signal(self):
    task_details = get_task_details(
        self.SCRIPT_SIGNAL, hard_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
        'exit_code': 0,
        'hard_timeout': True,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    expected_output_re = wrap_output_bytes_in_re(
        (to_native_eol('hi\ngot signal %d\nbye\n') %
         task_runner.SIG_BREAK_OR_TERM).encode())
    self.expectTask(task_details.task_id,
                    hard_timeout=True,
                    output=expected_output_re)

  def test_io_signal(self):
    task_details = get_task_details(
        self.SCRIPT_SIGNAL, io_timeout=self.SHORT_TIME_OUT)
    # Returns 0 because the process cleaned up itself.
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': True,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    #    output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    expected_output_re = wrap_output_bytes_in_re(
        (to_native_eol('hi\ngot signal %d\nbye\n') %
         task_runner.SIG_BREAK_OR_TERM).encode())
    self.expectTask(task_details.task_id,
                    io_timeout=True,
                    output=expected_output_re)

  def test_hard_no_grace(self):
    task_details = get_task_details(
        self.SCRIPT_HANG,
        hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    expected = {
      'exit_code': EXIT_CODE_TERM,
      'hard_timeout': True,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(
        task_details.task_id, hard_timeout=True, exit_code=EXIT_CODE_TERM)

  @unittest.skipIf(
      sys.platform == 'win32',
      'As run_isolated is killed, the children process leaks')
  def test_io_no_grace(self):
    task_details = get_task_details(
        self.SCRIPT_HANG,
        io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    exit_code = -1 if sys.platform == 'win32' else -signal.SIGTERM
    expected = {
      'exit_code': exit_code,
      'hard_timeout': False,
      'io_timeout': True,
      'must_signal_internal_failure': None,
      'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    self.expectTask(task_details.task_id, io_timeout=True, exit_code=exit_code)

  def test_hard_signal_no_grace(self):
    task_details = get_task_details(
        self.SCRIPT_SIGNAL_HANG, hard_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    exit_code = 1 if sys.platform == 'win32' else -signal.SIGKILL
    expected = {
        'exit_code': exit_code,
        'hard_timeout': True,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    #  output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    expected_output_re = wrap_output_bytes_in_re(
        (to_native_eol('hi\ngot signal %d\nbye\n') %
         task_runner.SIG_BREAK_OR_TERM).encode())
    self.expectTask(task_details.task_id,
                    hard_timeout=True,
                    exit_code=exit_code,
                    output=expected_output_re)

  @unittest.skipIf(sys.platform == 'win32',
                   'As run_isolated is killed, the children process leaks')
  def test_io_signal_no_grace(self):
    task_details = get_task_details(
        self.SCRIPT_SIGNAL_HANG,
        io_timeout=self.SHORT_TIME_OUT,
        grace_period=self.SHORT_TIME_OUT)
    exit_code = -1 if sys.platform == 'win32' else -signal.SIGKILL
    expected = {
        'exit_code': exit_code,
        'hard_timeout': False,
        'io_timeout': True,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, self._run_command(task_details))
    # Now look at the updates sent by the bot as seen by the server.
    #  output_re='^hi\ngot signal %d\nbye\n$' % task_runner.SIG_BREAK_OR_TERM)
    output = (to_native_eol('hi\ngot signal %d\nbye\n') %
              task_runner.SIG_BREAK_OR_TERM).encode()
    output_re = b'(\\s|\\S)*%s' % output
    self.expectTask(task_details.task_id,
                    io_timeout=True,
                    exit_code=exit_code,
                    output=re.compile(output_re))

  def test_isolated_io_signal_grand_children(self):
    """Handles grand-children process hanging and signal management.

    In this case, the I/O timeout is implemented by task_runner. The hard
    timeout is implemented by run_isolated.
    """
    files = {
        'parent.py': (
            b'import subprocess, sys\n'
            b'print(\'parent\')\n'
            b'p = subprocess.Popen([sys.executable, \'-u\', \'children.py\'])\n'
            b'print(p.pid)\n'
            b'p.wait()\n'
            b'sys.exit(p.returncode)\n'),
        'children.py': (b'import subprocess, sys\n'
                        b'print(\'children\')\n'
                        b'p = subprocess.Popen('
                        b'[sys.executable,\'-u\',\'grand_children.py\'])\n'
                        b'print(p.pid)\n'
                        b'p.wait()\n'
                        b'sys.exit(p.returncode)\n'),
        'grand_children.py': self.SCRIPT_SIGNAL_HANG.encode(),
    }
    digest = self.cas.archive_files(files)
    manifest = get_manifest(
        cas_input_root={
            'cas_instance': 'projects/test/instances/default_instance',
            'digest': {
                'hash': digest.split('/')[0],
                'size_bytes': digest.split('/')[1],
            },
        },
        command=['python3', '-u', 'parent.py'],
        # TODO(maruel): A bit cheezy, we'd want the I/O timeout to be just
        # enough to have the time for the PID to be printed but not more.
        #
        # This could be achieved by mocking time, and using a text file as a
        # signal.
        io_timeout=self.SHORT_TIME_OUT,
        grace_period=60.)
    expected = {
        'exit_code': EXIT_CODE_TERM,
        'hard_timeout': False,
        'io_timeout': True,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    try:
      actual = load_and_run(self.server.url, self.work_dir, manifest, None)
    finally:
      # We need to catch the pid of the grand children to be able to kill it. We
      # do so by processing stdout. Do not use expectTask() output, since it can
      # throw.
      output = self.getTaskResults(manifest['task_id'])['output']
      for k in output.splitlines():
        if not k.isdigit():
          continue
        pid = int(k)
        try:
          if sys.platform == 'win32':
            # This effectively kills.
            os.kill(pid, signal.SIGTERM)
          else:
            os.kill(pid, signal.SIGKILL)
        except OSError:
          pass
    self.assertEqual(expected, actual)
    contens = list(files.values())
    items_in = [len(c) for c in contens]
    self.expectTask(
        manifest['task_id'],
        io_timeout=True,
        exit_code=EXIT_CODE_TERM,
        output=re.compile(
            to_native_eol(
                '(\\s|\\S)*parent\n\\d+\nchildren\n\\d+\nhi\n').encode()),
        isolated_stats={
            'download': {
                'duration': 0,
                'items_cold': sorted(items_in),
                'items_hot': [],
            },
        })

  def test_kill_and_wait(self):
    # Test the case where the script swallows the SIGTERM/SIGBREAK signal and
    # hangs.
    script = os.path.join(self.root_dir, 'ignore_sigterm.py')
    with open(script, 'wb') as f:
      # The warning signal is received as SIGTERM on posix and SIGBREAK on
      # Windows.
      sig = 'SIGBREAK' if sys.platform == 'win32' else 'SIGTERM'
      f.write((('import signal, sys, time\n'
                'def handler(_signum, _frame):\n'
                '  sys.stdout.write("got it\\n")\n'
                'signal.signal(signal.%s, handler)\n'
                'sys.stdout.write("ok\\n")\n'
                'while True:\n'
                '  try:\n'
                '    time.sleep(0.01)\n'
                '  except IOError:\n'
                '    pass\n') % sig).encode())
    cmd = [sys.executable, '-u', script]
    # detached=True is required on Windows for SIGBREAK to propagate properly.
    p = subprocess42.Popen(cmd, detached=True, stdout=subprocess42.PIPE)

    # Wait for it to write 'ok', so we know it's handling signals. It's
    # important because otherwise SIGTERM/SIGBREAK could be sent before the
    # signal handler is installed, and this is not what we're testing here.
    self.assertEqual(to_native_eol('ok\n').encode(), p.stdout.readline())

    # Send a SIGTERM/SIGBREAK, the process ignores it, send a SIGKILL.
    exit_code = task_runner.kill_and_wait(p, 1, 'testing purposes')
    expected = 1 if sys.platform == 'win32' else -signal.SIGKILL
    self.assertEqual(expected, exit_code)
    self.assertEqual(to_native_eol('got it\n').encode(), p.stdout.readline())

  def test_signal(self):
    # Tests when task_runner gets a SIGTERM.
    signal_file = os.path.join(self.work_dir, 'signal')
    open(signal_file, 'wb').close()

    # As done by bot_main.py.
    manifest = get_manifest(
        script='import os,time;os.remove(%r);time.sleep(60)' % signal_file,
        hard_timeout=60.,
        io_timeout=60.)
    task_in_file = os.path.join(self.work_dir, 'task_runner_in.json')
    task_result_file = os.path.join(self.work_dir, 'task_runner_out.json')
    with open(task_in_file, 'w') as f:
      json.dump(manifest, f)

    bot = os.path.join(self.root_dir, 'swarming_bot.1.zip')
    code, _ = swarmingserver_bot_fake.gen_zip(self.server.url)
    with open(bot, 'wb') as f:
      f.write(code)
    cmd = [
        sys.executable,
        bot,
        'task_runner',
        '--swarming-server',
        self.server.url,
        '--in-file',
        task_in_file,
        '--out-file',
        task_result_file,
        '--cost-usd-hour',
        '1',
        # Include the time taken to poll the task in the cost.
        '--start',
        str(time.time()),
        '--',
    ] + DISABLE_CIPD_FOR_TESTS
    logging.info('%s', cmd)
    proc = subprocess42.Popen(cmd, cwd=self.root_dir, detached=True)
    logging.info('Waiting for child process to be alive')

    now = time.time()
    while os.path.isfile(signal_file):
      time.sleep(0.01)
      # prevent infinite loop.
      self.assertLess(time.time(), now + 20)

    # Send SIGTERM to task_runner itself. Ensure the right thing happen.
    # Note that on Windows, this is actually sending a SIGBREAK since there's no
    # such thing as SIGTERM.
    logging.info('Sending SIGTERM')
    proc.send_signal(signal.SIGTERM)
    proc.wait()
    task_runner_log = os.path.join(self.logs_dir, 'task_runner.log')
    with open(task_runner_log, 'rb') as f:
      logging.info('task_runner.log:\n---\n%s---', f.read())
    self.assertEqual([], self.server.get_bot_events())
    expected = {
        'swarming_bot.1.zip',
        '065eef161fd0db0753c811e430c8e537f2ce152fa2f14b99405430d49b1e95b0'
        '-cacert.'
        'pem',
        'w',
        'logs',
        'c',
        'cas-cache',
    }
    self.assertEqual(expected, set(os.listdir(self.root_dir)))

    expected = {
        'hard_timeout': False,
        'id': 'localhost',
        'io_timeout': False,
        'task_id': manifest['task_id'],
    }
    actual = self.getTaskResults(manifest['task_id'])
    # We don't care about output in this test
    actual.pop('output')
    actual.pop('output_chunk_start')

    self.assertLessEqual(0, actual.pop('cost_usd'))
    self.assertEqual(expected, actual)

    # TODO(sethkoehler): Set exit_code to 'exit_code' variable rather than None
    # when we correctly pass exit_code on failure (see TODO in task_runner.py).
    expected = {
        'exit_code': None,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': '',
        'version': 3,
    }
    with open(task_result_file, 'rb') as f:
      self.assertEqual(expected, json.load(f))
    self.assertEqual(0, proc.returncode)

    # Also verify the correct error was posted.
    errors = self.server.get_task_errors()
    expected = {
        manifest['task_id']: [{
            'message':
            'task_runner received signal %d' % task_runner.SIG_BREAK_OR_TERM,
            'client_error': {
                'missing_cas': [],
                'missing_cipd': [],
            },
            'id':
            'localhost',
            'task_id':
            manifest['task_id'],
        }],
    }
    self.assertEqual(expected, errors)


class TaskRunnerNoServer(auto_stub.TestCase):
  """Test cases that do not talk to the server."""

  def setUp(self):
    super(TaskRunnerNoServer, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='task_runner')

  def tearDown(self):
    try:
      file_path.rmtree(self.root_dir)
    except OSError:
      print('Failed to delete %s' % self.root_dir, file=sys.stderr)
      raise
    finally:
      super(TaskRunnerNoServer, self).tearDown()

  def test_load_and_run_isolated(self):
    self.mock(bot_auth, 'AuthSystem', FakeAuthSystem)

    def _run_command(remote, task_details, work_dir, cost_usd_hour, start,
                     run_isolated_flags, bot_file, ctx_file):
      self.assertTrue(remote.uses_auth) # mainly to avoid unused arg warning
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      # Necessary for OSX.
      self.assertEqual(
          os.path.realpath(self.root_dir), os.path.realpath(work_dir))
      self.assertEqual(3600., cost_usd_hour)
      self.assertGreaterEqual(time.time(), start)
      self.assertEqual(['--min-free-space', '1'] + DISABLE_CIPD_FOR_TESTS,
                       run_isolated_flags)
      self.assertEqual(None, bot_file)
      with open(ctx_file, 'r') as f:
        self.assertIsNone(json.load(f).get('local_auth'))
      return {
          'exit_code': 0,
          'hard_timeout': False,
          'io_timeout': False,
          'must_signal_internal_failure': None,
          'version': task_runner.OUT_VERSION,
      }
    self.mock(task_runner, 'run_command', _run_command)

    manifest = get_manifest(command=['hello.exe'],
                            env={'d': 'e'},
                            cas_input_root={
                                'cas_instance':
                                'projects/test/instances/default_instance',
                                'digest': {
                                    'hash': '123',
                                    'size': 1,
                                },
                            })
    actual = load_and_run(
        'http://localhost:1', self.root_dir, manifest,
        '/path/to/auth-params-file')
    expected = {
        'exit_code': 0,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, actual)

  def test_load_and_run_raw(self):
    local_auth_ctx = {
        'accounts': [{
            'id': 'a'
        }, {
            'id': 'b'
        }],
        'default_account_id': 'a',
        'rpc_port': 123,
        'secret': 'abcdef',
    }
    realm_ctx = {'name': 'test:realm'}

    def _run_command(
        remote, task_details, work_dir,
        cost_usd_hour, start, run_isolated_flags, bot_file, ctx_file):
      self.assertTrue(remote.uses_auth) # mainly to avoid "unused arg" warning
      self.assertTrue(isinstance(task_details, task_runner.TaskDetails))
      # Necessary for OSX.
      self.assertEqual(
          os.path.realpath(self.root_dir), os.path.realpath(work_dir))
      self.assertEqual(3600., cost_usd_hour)
      self.assertGreaterEqual(time.time(), start)
      self.assertEqual(['--min-free-space', '1'] + DISABLE_CIPD_FOR_TESTS,
                       run_isolated_flags)
      self.assertEqual(None, bot_file)
      with open(ctx_file, 'r') as f:
        ctx = json.load(f)
        self.assertDictEqual(local_auth_ctx, ctx['local_auth'])
        self.assertDictEqual(realm_ctx, ctx['realm'])
      return {
          'exit_code': 1,
          'hard_timeout': False,
          'io_timeout': False,
          'must_signal_internal_failure': None,
          'version': task_runner.OUT_VERSION,
      }
    self.mock(task_runner, 'run_command', _run_command)
    manifest = get_manifest(command=['a'])
    FakeAuthSystem.local_auth_context = local_auth_ctx
    task_details = get_task_details(realm={'name': 'test:realm'})
    try:
      self.mock(bot_auth, 'AuthSystem', FakeAuthSystem)
      with mock.patch('%s.TaskDetails.load' % task_runner.__name__,
                      mock.Mock(return_value=task_details)):
        actual = load_and_run('http://localhost:1', self.root_dir, manifest,
                              '/path/to/auth-params-file')
    finally:
      FakeAuthSystem.local_auth_context = None
    expected = {
        'exit_code': 1,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': task_runner.OUT_VERSION,
    }
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging_utils.prepare_logging(None)
  logging_utils.set_console_level(logging.DEBUG if '-v' in sys
                                  .argv else logging.CRITICAL + 1)
  # Fix literal text expectation.
  os.environ['LANG'] = 'en_US.UTF-8'
  os.environ['LANGUAGE'] = 'en_US.UTF-8'
  unittest.main()
