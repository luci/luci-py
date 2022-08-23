#!/usr/bin/env vpython3
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

from __future__ import print_function
from __future__ import unicode_literals

import base64
import contextlib
import functools
import json
import logging
import os
import sys
import tempfile
import unittest

import mock

# Mutates sys.path.
import test_env

# third_party/
from depot_tools import auto_stub

import cipdserver_fake

import cas_util
import cipd
import errors
import local_caching
import run_isolated
from libs import luci_context
from utils import file_path
from utils import fs
from utils import large
from utils import logging_utils
from utils import on_error
from utils import subprocess42
from utils import tools


ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LUCI_GO_CLIENT_DIR = os.path.join(ROOT_DIR, 'luci-go')


def json_dumps(data):
  return json.dumps(data, sort_keys=True, separators=(',', ':'))


@contextlib.contextmanager
def init_named_caches_stub(_run_dir, _stats):
  yield


def trim_caches_stub(_stats):
  pass


class StorageFake:
  def __init__(self, files, server_ref):
    self._files = files.copy()
    self._server_ref = server_ref

  def __enter__(self, *_):
    return self

  def __exit__(self, *_):
    pass

  @property
  def server_ref(self):
    return self._server_ref

  def async_fetch(self, channel, _priority, digest, _size, sink):
    sink([self._files[digest]])
    channel.send_result(digest)

  def upload_items(self, items_to_upload, _verify_push):
    # Return all except the first one.
    return list(items_to_upload)[1:]


class RunIsolatedTestBase(auto_stub.TestCase):
  # These tests fail with the following error
  # 'AssertionError: Items in the first set but not the second'
  # Need to run in sequential_test_runner.py as an executable
  no_run = 1
  DISABLE_CIPD_FOR_TESTS = ['--cipd-enabled', False]

  @classmethod
  def setUpClass(cls):
    if not file_path.enable_symlink():
      raise Exception(
          'Failed to enable symlink; this test requires it. On Windows, maybe '
          'try running as Administrator')

  def setUp(self):
    super(RunIsolatedTestBase, self).setUp()
    os.environ.pop('LUCI_CONTEXT', None)
    os.environ['LUCI_GO_CLIENT_DIR'] = LUCI_GO_CLIENT_DIR
    self._previous_dir = os.getcwd()
    self.tempdir = tempfile.mkdtemp(prefix='run_isolated_test')
    logging.debug('Temp dir: %s', self.tempdir)
    cwd = os.path.join(self.tempdir, 'cwd')
    fs.mkdir(cwd)
    os.chdir(cwd)
    self.mock(run_isolated, 'make_temp_dir', self.fake_make_temp_dir)
    self.mock(run_isolated.auth, 'ensure_logged_in', lambda _: None)
    self.mock(
        logging_utils.OptionParserWithLogging, 'logger_root',
        logging.Logger('unittest'))

    self._cipd_server = None  # initialized lazily

  def tearDown(self):
    # Remove mocks.
    super(RunIsolatedTestBase, self).tearDown()
    fs.chdir(self._previous_dir)
    file_path.rmtree(self.tempdir)
    if self._cipd_server:
      self._cipd_server.close()

  @property
  def cipd_server(self):
    if not self._cipd_server:
      self._cipd_server = cipdserver_fake.FakeCipdServer()
    return self._cipd_server

  def fake_make_temp_dir(self, prefix, _root_dir):
    """Predictably returns directory for run_tha_test (one per test case)."""
    self.assertIn(
        prefix, (run_isolated.ISOLATED_OUT_DIR, run_isolated.ISOLATED_RUN_DIR,
                 run_isolated.ISOLATED_TMP_DIR,
                 run_isolated.ISOLATED_CLIENT_DIR, run_isolated._CAS_CLIENT_DIR,
                 'cipd_site_root', run_isolated._NSJAIL_DIR))
    temp_dir = os.path.join(self.tempdir, prefix)
    self.assertFalse(fs.isdir(temp_dir))
    fs.makedirs(temp_dir)
    return temp_dir

  def ir_dir(self, *args):
    """Shortcut for joining path with ISOLATED_RUN_DIR.

    Where to map all files in run_isolated.run_tha_test().
    """
    return os.path.join(self.tempdir, run_isolated.ISOLATED_RUN_DIR, *args)

  def assertExpectedTree(self, expected, root_dir=None):
    # Assume expected path are relative to root if not specified.
    root_dir = root_dir or os.path.join(self.tempdir, 'io')

    # Return True is the entries in out_dir are exactly the same as entries in
    # expected. Return False otherwise.
    count = 0
    for path in expected:
      content = expected[path]
      full_path = os.path.join(root_dir, path)
      self.assertTrue(fs.exists(full_path), "%s doesn't exist" % full_path)
      while fs.islink(full_path):
        full_path = fs.readlink(full_path)
      # If we expect a non-empty directory, check the entries in dir.
      # If we expect an empty dir, its existence (checked above) is sufficient.
      if not fs.isdir(full_path):
        with open(full_path, 'r') as f:
          self.assertEqual(f.read(), content)
      count += 1
    self.assertEqual(count, len(expected))


class RunIsolatedTest(RunIsolatedTestBase):
  # Mocked Popen so no subprocess is started.
  def setUp(self):
    super(RunIsolatedTest, self).setUp()
    # list of func(args, **kwargs) -> retcode
    # if the func returns None, then it's skipped. The first function to return
    # non-None is taken as the retcode for the mocked Popen call.
    self.popen_fakes = []
    self.popen_calls = []

    self.capture_popen_env = False
    self.capture_luci_ctx = False

    # pylint: disable=no-self-argument
    class Popen:
      def __init__(self2, args, **kwargs):
        if not self.capture_popen_env:
          kwargs.pop('env', None)
        if self.capture_luci_ctx:
          with open(os.environ['LUCI_CONTEXT']) as f:
            kwargs['luci_ctx'] = json.load(f)
        self2.returncode = None
        self2.args = args
        self2.kwargs = kwargs
        self.popen_calls.append((args, kwargs))

      def yield_any_line(self2, timeout=None):
        self.assertEqual(0.1, timeout)
        return ()

      def wait(self2, timeout=None):
        self.assertIn(timeout, (None, 30, 60))
        self2.returncode = 0
        for mock_fn in self.popen_fakes:
          ret = mock_fn(self2.args, **self2.kwargs)
          if ret is not None:
            self2.returncode = ret
            break
        return self2.returncode

      def kill(self):
        pass

    self.mock(subprocess42, 'Popen', Popen)

  def test_copy_recusrsively(self):
    src = os.path.join(self.tempdir, 'src')
    dst = os.path.join(self.tempdir, 'dst')
    with open(src, 'w'):
      pass

    run_isolated.copy_recursively(src, dst)
    self.assertTrue(os.path.isfile(dst))

  def test_copy_recusrsively_not_exist(self):
    src = os.path.join(self.tempdir, 'src')
    dst = os.path.join(self.tempdir, 'dst')
    run_isolated.copy_recursively(src, dst)
    self.assertFalse(os.path.exists(dst))

  def test_get_command_env(self):
    old_env = os.environ
    try:
      os.environ = os.environ.copy()
      os.environ.pop('B', None)
      self.assertNotIn('B', os.environ)
      os.environ['C'] = 'foo'
      os.environ['D'] = 'bar'
      os.environ['E'] = 'baz'
      env = run_isolated.get_command_env(
          '/a',
          None,
          '/b',
          {
              'A': 'a',
              'B': None,
              'C': None,
              'E': '${ISOLATED_OUTDIR}/eggs'
          },
          {'D': ['foo']},
          '/spam',
          None)
      self.assertNotIn('B', env)
      self.assertNotIn('C', env)
      if sys.platform == 'win32':
        self.assertEqual('\\b\\foo;bar', env['D'])
      else:
        self.assertEqual('/b/foo:bar', env['D'])
      self.assertEqual(os.sep + os.path.join('spam', 'eggs'), env['E'])
    finally:
      os.environ = old_env

  @mock.patch.dict(os.environ, {'SWARMING_TASK_ID': '4242'})
  def test_main(self):
    self.mock(tools, 'disable_buffering', lambda: None)

    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log', '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'), '--root-dir', self.tempdir,
        '--', 'foo.exe', 'cmd with space', '-task-id', '${SWARMING_TASK_ID}'
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)
    self.assertEqual([
        (
            [self.ir_dir('foo.exe'), 'cmd with space', '-task-id', '4242'],
            {
                'cwd': self.ir_dir(),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': subprocess42.Containment(),
            },
        ),
    ], self.popen_calls)

  def test_main_args(self):
    self.mock(tools, 'disable_buffering', lambda: None)

    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--root-dir',
        self.tempdir,
        '--',
        'foo.exe',
        'cmd w/ space',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)
    self.assertEqual([
        (
            [self.ir_dir('foo.exe'), 'cmd w/ space'],
            {
                'cwd': self.ir_dir(),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': subprocess42.Containment(),
            },
        ),
    ], self.popen_calls)

  def _run_tha_test(self,
                    command=None,
                    lower_priority=False,
                    relative_cwd=None):
    make_tree_call = []
    def add(i, _):
      make_tree_call.append(i)

    for i in ('make_tree_files_read_only', 'make_tree_deleteable'):
      self.mock(file_path, i, functools.partial(add, i))

    data = run_isolated.TaskData(
        command=command or [],
        relative_cwd=relative_cwd,
        cas_instance=None,
        cas_digest=None,
        outputs=None,
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=run_isolated.copy_local_packages,
        cas_cache_dir=None,
        cas_cache_policies=None,
        cas_kvs='',
        env={},
        env_prefix={},
        lower_priority=lower_priority,
        containment=None,
        trim_caches_fn=trim_caches_stub)
    ret = run_isolated.run_tha_test(data, None)
    self.assertEqual(0, ret)
    return make_tree_call

  def test_run_tha_test_naked(self):
    self._run_tha_test(command=['invalid', 'command'])
    self.assertEqual([
        (
            [self.ir_dir('invalid'), 'command'],
            {
                'cwd': self.ir_dir(),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': None,
            },
        ),
    ], self.popen_calls)

  def mock_popen_with_oserr(self):
    def r(self, args, **kwargs):
      old_init(self, args, **kwargs)
      raise OSError('Unknown')
    old_init = self.mock(subprocess42.Popen, '__init__', r)

  def test_main_naked(self):
    self.mock_popen_with_oserr()
    self.mock(on_error, 'report', lambda _: None)
    # The most naked .isolated file that can exist.
    self.mock(tools, 'disable_buffering', lambda: None)

    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log', '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'), '--root-dir', self.tempdir,
        '--', 'invalid', 'command'
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(1, ret)
    self.assertEqual(1, len(self.popen_calls))
    self.assertEqual([
        (
            [self.ir_dir('invalid'), 'command'],
            {
                'cwd': self.ir_dir(),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': subprocess42.Containment(),
            },
        ),
    ], self.popen_calls)

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_main_naked_without_isolated(self):
    self.mock_popen_with_oserr()
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--root-dir',
        self.tempdir,
        '--',
        '/bin/echo',
        'hello',
        'world',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(1, ret)
    self.assertEqual(
        [
          (
            ['/bin/echo', 'hello', 'world'],
            {
              'cwd': self.ir_dir(),
              'detached': True,
              'close_fds': True,
              'lower_priority': False,
              'containment': subprocess42.Containment(),
            },
          ),
        ],
        self.popen_calls)

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_main_naked_with_account_switch(self):
    self.capture_luci_ctx = True
    self.mock_popen_with_oserr()
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--switch-to-account',
        'task',
        '--',
        '/bin/echo',
        'hello',
        'world',
    ]
    root_ctx = {
      'accounts': [{'id': 'bot'}, {'id': 'task'}],
      'default_account_id' : 'bot',
      'secret': 'sekret',
      'rpc_port': 12345,
    }
    with luci_context.write(local_auth=root_ctx):
      run_isolated.main(cmd)
    # Switched default account to task.
    task_ctx = root_ctx.copy()
    task_ctx['default_account_id'] = 'task'
    self.assertEqual(task_ctx, self.popen_calls[0][1]['luci_ctx']['local_auth'])

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_main_naked_with_account_pop(self):
    self.capture_luci_ctx = True
    self.mock_popen_with_oserr()
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--switch-to-account',
        'task',
        '--',
        '/bin/echo',
        'hello',
        'world',
    ]
    root_ctx = {
      'accounts': [{'id': 'bot'}],  # only 'bot', there's no 'task'
      'default_account_id' : 'bot',
      'secret': 'sekret',
      'rpc_port': 12345,
    }
    with luci_context.write(local_auth=root_ctx):
      run_isolated.main(cmd)
    # Unset default account, since 'task' account is not defined.
    task_ctx = root_ctx.copy()
    task_ctx.pop('default_account_id')
    self.assertEqual(task_ctx, self.popen_calls[0][1]['luci_ctx']['local_auth'])

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_main_naked_leaking(self):
    workdir = tempfile.mkdtemp()
    try:
      cmd = self.DISABLE_CIPD_FOR_TESTS + [
          '--no-log',
          '--root-dir',
          workdir,
          '--leak-temp-dir',
          '--named-cache-root',
          os.path.join(self.tempdir, 'named_cache'),
          '--',
          '/bin/echo',
          'hello',
          'world',
      ]
      ret = run_isolated.main(cmd)
      self.assertEqual(0, ret)
    finally:
      fs.rmtree(workdir)

  def test_main_naked_with_packages(self):
    self.mock(cipd, 'get_platform', lambda: 'linux-amd64')

    def pins_generator():
      yield {
          '': [
              ('infra/data/x', 'badc0fee' * 5),
              ('infra/data/y', 'cafebabe' * 5),
          ],
          'bin': [('infra/tools/echo/linux-amd64', 'deadbeef' * 5),],
      }
      yield {
          '': [('infra/tools/luci/cas/linux-amd64',
                run_isolated._LUCI_GO_REVISION)],
      }

    pins_gen = pins_generator()

    suffix = '.exe' if sys.platform == 'win32' else ''
    def fake_ensure(args, **kwargs):
      if (args[0].endswith(os.path.join('bin', 'cipd' + suffix)) and
          args[1] == 'ensure'
          and '-json-output' in args):
        idx = args.index('-json-output')
        with open(args[idx+1], 'w') as json_out:
          json.dump(
              {
                  'result': {
                      subdir: [{
                          'package': pkg,
                          'instance_id': ver
                      } for pkg, ver in packages]
                      for subdir, packages in next(pins_gen).items()
                  }
              }, json_out)
        return 0
      if args[0].endswith(os.sep + 'echo' + suffix):
        return 0
      self.fail('unexpected: %s, %s' % (args, kwargs))
      return 1

    self.popen_fakes.append(fake_ensure)
    cipd_cache = os.path.join(self.tempdir, 'cipd_cache')
    cmd = [
        '--no-log',
        '--cipd-client-version',
        'git:wowza',
        '--cipd-package',
        'bin:infra/tools/echo/${platform}:latest',
        '--cipd-package',
        '.:infra/data/x:latest',
        '--cipd-package',
        '.:infra/data/y:canary',
        '--cipd-server',
        self.cipd_server.url,
        '--cipd-cache',
        cipd_cache,
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--',
        'bin/echo${EXECUTABLE_SUFFIX}',
        'hello',
        'world',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)

    self.assertEqual(3, len(self.popen_calls))

    # Test cipd-ensure command for installing packages.
    cipd_ensure_cmd, _ = self.popen_calls[0]
    self.assertEqual(cipd_ensure_cmd[:2], [
      os.path.join(cipd_cache, 'bin', 'cipd' + cipd.EXECUTABLE_SUFFIX),
      'ensure',
    ])
    cache_dir_index = cipd_ensure_cmd.index('-cache-dir')
    self.assertEqual(
        cipd_ensure_cmd[cache_dir_index+1],
        os.path.join(cipd_cache, 'cache'))

    # Test cipd client cache. `git:wowza` was a tag and so is cacheable.
    self.assertEqual(len(fs.listdir(os.path.join(cipd_cache, 'versions'))), 2)
    version_file = os.path.join(
        cipd_cache, 'versions',
        '5c2ee864d65c435dfa1eb06bf4c52f58854db39392340a91fc91e88d6000d737')
    self.assertTrue(fs.isfile(version_file))
    with open(version_file) as f:
      self.assertEqual(f.read(), 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')

    client_binary_file = os.path.join(
        cipd_cache, 'clients', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    self.assertTrue(fs.isfile(client_binary_file))

    # Test echo call.
    echo_cmd, _ = self.popen_calls[2]
    self.assertTrue(echo_cmd[0].endswith(
        os.path.sep + 'bin' + os.path.sep + 'echo' + cipd.EXECUTABLE_SUFFIX),
        echo_cmd[0])
    self.assertEqual(echo_cmd[1:], ['hello', 'world'])

  def test_main_naked_with_invalid_cipd_package(self):
    self.mock(cipd, 'get_platform', lambda: 'linux-amd64')
    suffix = '.exe' if sys.platform == 'win32' else ''

    def fake_ensure(args, **kwargs):
      if (args[0].endswith(os.path.join('bin', 'cipd' + suffix))
          and args[1] == 'ensure' and '-json-output' in args):
        idx = args.index('-json-output')
        with open(args[idx + 1], 'w') as json_out:
          json.dump(
              {
                  "error":
                  "failed to resolve does/not/exists/linux-amd64@latest",
                  "error_code": "invalid_version_error",
                  "error_details": {
                      "package": "does/not/exists/linux-amd64",
                      "version": "latest"
                  },
                  "result": None
              }, json_out)
        return 0
      if args[0].endswith(os.sep + 'echo' + suffix):
        return 0
      self.fail('unexpected: %s, %s' % (args, kwargs))
      return 1

    self.popen_fakes.append(fake_ensure)
    result_json_path = os.path.join(self.tempdir, 'result.json')
    cipd_cache = os.path.join(self.tempdir, 'cipd_cache')
    cmd = [
        '--json',
        result_json_path,
        '--no-log',
        '--cipd-package',
        'bin:does/not/exists/${platform}:latest',
        '--cipd-server',
        self.cipd_server.url,
        '--cipd-cache',
        cipd_cache,
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--',
        'bin/echo${EXECUTABLE_SUFFIX}',
        'hello',
        'world',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(1, ret)
    with open(result_json_path, 'r') as fp:
      result_json = json.load(fp)
    self.assertEqual(1, len(result_json['missing_cipd']))
    missing_cipd = result_json['missing_cipd'][0]
    self.assertEqual('does/not/exists/linux-amd64',
                     missing_cipd['package_name'])
    self.assertEqual('invalid_version_error', missing_cipd['status'])
    self.assertEqual('latest', missing_cipd['version'])
    self.assertIsNone(missing_cipd['path'])

  def test_main_naked_with_cipd_client_no_packages(self):
    self.mock(cipd, 'get_platform', lambda: 'linux-amd64')

    cipd_cache = os.path.join(self.tempdir, 'cipd_cache')
    cmd = [
        '--no-log',
        '--cipd-client-version',
        'git:wowza',
        '--cipd-server',
        self.cipd_server.url,
        '--cipd-cache',
        cipd_cache,
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--relative-cwd',
        'a',
        '--root-dir',
        self.tempdir,
        '--',
        'bin/echo${EXECUTABLE_SUFFIX}',
        'hello',
        'world',
    ]

    pins = {
        '': [('infra/tools/luci/isolated/linux-amd64',
              run_isolated._LUCI_GO_REVISION)],
    }

    suffix = '.exe' if sys.platform == 'win32' else ''

    def fake_ensure(args, **kwargs):
      if (args[0].endswith(os.path.join('bin', 'cipd' + suffix)) and
          args[1] == 'ensure' and '-json-output' in args):
        idx = args.index('-json-output')
        with open(args[idx + 1], 'w') as json_out:
          json.dump({
              'result': {
                  subdir: [{
                      'package': pkg,
                      'instance_id': ver
                  } for pkg, ver in packages
                          ] for subdir, packages in pins.items()
              }
          }, json_out)
        return 0
      if args[0].endswith(os.sep + 'echo' + suffix):
        return 0
      self.fail('unexpected: %s, %s' % (args, kwargs))
      return 1

    self.popen_fakes.append(fake_ensure)

    self.capture_popen_env = True
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)

    # The CIPD client was bootstrapped and hardlinked (or copied on Win).
    client_binary_file = os.path.join(
        cipd_cache, 'clients', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    self.assertTrue(fs.isfile(client_binary_file))
    client_binary_link = os.path.join(cipd_cache, 'bin',
                                      'cipd' + cipd.EXECUTABLE_SUFFIX)
    self.assertTrue(fs.isfile(client_binary_link))

    env = self.popen_calls[1][1].pop('env')
    exec_path = self.ir_dir('a', 'bin', 'echo')
    if sys.platform == 'win32':
      exec_path += '.exe'
    self.assertEqual(
        [
            (
                [exec_path, 'hello', 'world'],
                {
                    'cwd': self.ir_dir('a'),
                    'detached': True,
                    'close_fds': True,
                    'lower_priority': False,
                    'containment': subprocess42.Containment(),
                },
            ),
        ],
        # Ignore `cipd ensure` for isolated client here.
        self.popen_calls[1:])

    # Directory with cipd client is in front of PATH.
    path = env['PATH'].split(os.pathsep)
    self.assertEqual(os.path.join(cipd_cache, 'bin'), path[0])

    # CIPD_CACHE_DIR is set.
    self.assertEqual(os.path.join(cipd_cache, 'cache'), env['CIPD_CACHE_DIR'])

  def test_main_relative_cwd_no_cmd(self):
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--relative-cwd',
        'a',
    ]
    with self.assertRaises(SystemExit):
      run_isolated.main(cmd)

  def test_main_bad_relative_cwd(self):
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--relative-cwd',
        'a/../../b',
        '--',
        'bin/echo${EXECUTABLE_SUFFIX}',
        'hello',
        'world',
    ]
    with self.assertRaises(SystemExit):
      run_isolated.main(cmd)

  def test_main_naked_with_caches(self):
    # An empty named cache is not kept!
    # Interestingly, because we would need to put something in the named cache
    # for it to be kept, we need the tool to write to it. This is tested in the
    # smoke test.
    trimmed = []
    def trim_caches(caches, root, min_free_space, max_age_secs):
      trimmed.append(True)
      self.assertEqual(2, len(caches))
      self.assertTrue(root)
      # The name cache root is increased by the sum of the two hints and buffer.
      self.assertEqual(
          run_isolated._FREE_SPACE_BUFFER_FOR_CIPD_PACKAGES +
          2 * 1024 * 1024 * 1024 + 1100, min_free_space)
      self.assertEqual(1814400, max_age_secs)
    self.mock(local_caching, 'trim_caches', trim_caches)
    nc = os.path.join(self.tempdir, 'named_cache')
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--leak-temp-dir',
        '100',
        '--named-cache-root',
        nc,
        '--named-cache',
        'cache_foo',
        'foo',
        '100',
        '--named-cache',
        'cache_bar',
        'bar',
        '1000',
        '--',
        'bin/echo${EXECUTABLE_SUFFIX}',
        'hello',
        'world',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)

    for cache_name in ('cache_foo', 'cache_bar'):
      named_path = os.path.join(nc, 'named', cache_name)
      self.assertFalse(fs.exists(named_path))
    self.assertTrue(trimmed)

  def test_main_clean(self):
    cas_cache_dir = os.path.join(self.tempdir, 'cas_cache')
    named_cache_dir = os.path.join(self.tempdir, 'named_cache')
    kvs_dir = os.path.join(self.tempdir, 'kvs_dir')
    os.mkdir(kvs_dir)
    with open(os.path.join(kvs_dir, 'dummy'), 'w') as f:
      f.write('0' * 100)

    # override size threshold.
    self.mock(run_isolated, '_CAS_KVS_CACHE_THRESHOLD', 99)

    min_free_space = 1
    max_cache_size = 2

    cmd = [
        '--no-log',
        '--clean',
        # Shared options.
        '--min-free-space',
        str(min_free_space),
        '--max-cache-size',
        str(max_cache_size),
        # CAS cache option.
        '--cas-cache',
        cas_cache_dir,
        # Named cache option.
        '--named-cache-root',
        named_cache_dir,
        '--kvs-dir',
        kvs_dir,
    ]

    def trim_caches_mock(caches, _root_dir, min_free_space, max_age_secs):
      self.assertEqual(min_free_space, min_free_space)
      self.assertEqual(max_age_secs, run_isolated.MAX_AGE_SECS)

      # CAS cache.
      cas_cache = caches[0]
      self.assertEqual(cas_cache.state_file,
                       os.path.join(cas_cache_dir, cas_cache.STATE_FILE))
      self.assertEqual(cas_cache.policies.max_cache_size, max_cache_size)
      self.assertEqual(cas_cache.policies.min_free_space, min_free_space)
      self.assertIsNone(cas_cache.policies.max_items)

      # Named cache.
      named_cache = caches[1]
      self.assertEqual(named_cache.state_file,
                       os.path.join(named_cache_dir, named_cache.STATE_FILE))
      # max_cache_size, max_items, max_age_secs are hardcoded in
      # run_isolated.process_named_cache_options().
      self.assertEqual(named_cache._policies.max_cache_size, 1024**4)
      self.assertEqual(named_cache._policies.min_free_space, min_free_space)
      self.assertEqual(named_cache._policies.max_items, 50)
      self.assertEqual(named_cache._policies.max_age_secs,
                       run_isolated.MAX_AGE_SECS)

    self.mock(local_caching, 'trim_caches', trim_caches_mock)

    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)

    with self.assertRaises(OSError):
      # kvs dir should be removed.
      fs.stat(kvs_dir)

  def test_modified_cwd(self):
    self._run_tha_test(command=['../out/some.exe', 'arg'], relative_cwd='some')
    self.assertEqual([
        (
            [self.ir_dir('out', 'some.exe'), 'arg'],
            {
                'cwd': self.ir_dir('some'),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': None,
            },
        ),
    ], self.popen_calls)

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_python_cmd_lower_priority(self):
    self._run_tha_test(
        command=['../out/cmd.py', 'arg'],
        relative_cwd='some',
        lower_priority=True)
    # Injects sys.executable but on macOS, the path may be different than
    # sys.executable due to symlinks.
    self.assertEqual(1, len(self.popen_calls))
    cmd, args = self.popen_calls[0]
    self.assertEqual(
        {
          'cwd': self.ir_dir('some'),
          'detached': True,
          'close_fds': True,
          'lower_priority': True,
          'containment': None,
        },
        args)
    self.assertIn('python', cmd[0])
    self.assertEqual([os.path.join('..', 'out', 'cmd.py'), 'arg'], cmd[1:])

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_run_tha_test_non_isolated(self):
    _ = self._run_tha_test(command=['/bin/echo', 'hello', 'world'])
    self.assertEqual([
        (
            ['/bin/echo', 'hello', 'world'],
            {
                'cwd': self.ir_dir(),
                'detached': True,
                'close_fds': True,
                'lower_priority': False,
                'containment': None,
            },
        ),
    ], self.popen_calls)

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_main_containment(self):
    def fake_wait(args, **kwargs):  # pylint: disable=unused-argument
      # Success.
      return 0
    self.popen_fakes.append(fake_wait)
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--lower-priority',
        '--containment-type',
        'JOB_OBJECT',
        '--limit-processes',
        '42',
        '--limit-total-committed-memory',
        '1024',
        '--root-dir',
        self.tempdir,
        '--',
        '/bin/echo',
        'hello',
        'world',
    ]
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)
    self.assertEqual([
        (
            ['/bin/echo', 'hello', 'world'],
            {
                'cwd':
                    self.ir_dir(),
                'detached':
                    True,
                'close_fds':
                    True,
                'lower_priority':
                    True,
                'containment':
                    subprocess42.Containment(
                        containment_type=subprocess42.Containment.JOB_OBJECT,
                        limit_processes=42,
                        limit_total_committed_memory=1024,
                    ),
            },
        ),
    ], self.popen_calls)


class RunIsolatedTestRun(RunIsolatedTestBase):

  def setUp(self):
    super(RunIsolatedTestRun, self).setUp()
    # Starts a full CAS server mock and have run_tha_test() uploads results
    # back after the task completed.
    self._server = cas_util.LocalCAS(self.tempdir)
    self._server.start()

  def tearDown(self):
    self._server.stop()
    super(RunIsolatedTestRun, self).tearDown()

  # Runs the actual command requested.
  def test_output(self):
    digest = self._server.archive_files(
        {'cmd.py': b'import sys\n'
         b'open(sys.argv[1], "w").write("bar")\n'})

    os.environ['RUN_ISOLATED_CAS_ADDRESS'] = self._server.address
    data = run_isolated.TaskData(
        command=['python3', 'cmd.py', '${ISOLATED_OUTDIR}/foo'],
        relative_cwd=None,
        cas_instance=None,
        cas_digest=digest,
        outputs=None,
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=run_isolated.copy_local_packages,
        cas_cache_dir='',
        cas_cache_policies=local_caching.CachePolicies(0, 0, 0, 0),
        cas_kvs='',
        env={},
        env_prefix={},
        lower_priority=False,
        containment=None,
        trim_caches_fn=trim_caches_stub)

    result_json = os.path.join(self.tempdir, 'result.json')
    ret = run_isolated.run_tha_test(data, result_json)
    self.assertEqual(0, ret)
    with open(result_json) as f:
      result = json.load(f)

    output_digest = result['cas_output_root']['digest']
    dest = os.path.join(self.tempdir, 'output')
    self._server.download(
        output_digest['hash'] + '/' + str(output_digest['size_bytes']), dest)

    self.assertEqual(os.listdir(dest), ['foo'])
    with open(os.path.join(dest, 'foo'), 'rb') as f:
      self.assertEqual(f.read(), b'bar')


class RunIsolatedTestCase(RunIsolatedTestRun):
  def test_bad_cas_json_output(self):
    def dump_bad_json(cmd, _):
      json_path = cmd[cmd.index('-dump-json') + 1]
      with open(json_path, 'w') as fp:
        fp.write("{i[]nv[[]alid js{}on")

    digest = self._server.archive_files(
        {'cmd.py': b'import sys\n'
         b'open(sys.argv[1], "w").write("bar")\n'})

    self.mock(run_isolated, "_run_go_cmd_and_wait", dump_bad_json)

    data = run_isolated.TaskData(
        command=['python3', 'cmd.py', '${ISOLATED_OUTDIR}/foo'],
        relative_cwd=None,
        cas_instance="some_instance",
        cas_digest=digest,
        outputs=None,
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=run_isolated.copy_local_packages,
        cas_cache_dir='',
        cas_cache_policies=local_caching.CachePolicies(0, 0, 0, 0),
        cas_kvs=None,
        env={},
        env_prefix={},
        lower_priority=False,
        containment=None,
        trim_caches_fn=trim_caches_stub)

    result_json = os.path.join(self.tempdir, 'result.json')
    run_isolated.run_tha_test(data, result_json)
    with open(result_json) as f:
      result = json.load(f)
    # Don't care the exact error, so long as its not reported as a missing_cas
    self.assertTrue(result.get('internal_failure'))
    self.assertIsNone(result.get('missing_cas'))

  def test_bad_cas_digest(self):
    digest = "notvaliddigest"
    os.environ['RUN_ISOLATED_CAS_ADDRESS'] = self._server.address
    data = run_isolated.TaskData(
        command=['python3', 'cmd.py', '${ISOLATED_OUTDIR}/foo'],
        relative_cwd=None,
        cas_instance="some_instance",
        cas_digest=digest,
        outputs=None,
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=run_isolated.copy_local_packages,
        cas_cache_dir='',
        cas_cache_policies=local_caching.CachePolicies(0, 0, 0, 0),
        cas_kvs='',
        env={},
        env_prefix={},
        lower_priority=False,
        containment=None,
        trim_caches_fn=trim_caches_stub)

    result_json = os.path.join(self.tempdir, 'result.json')
    ret = run_isolated.run_tha_test(data, result_json)
    with open(result_json) as f:
      result = json.load(f)
    self.assertEqual(1, ret)
    self.assertEqual(digest, result['missing_cas']['digest'])
    self.assertEqual('some_instance', result['missing_cas']['instance'])

  def test_bad_cipd_package(self):
    def emulate_bad_cipd(_run_dir, cas_dir, _nsjail_dir):
      raise errors.NonRecoverableCipdException("missing_cipd", "foo_package",
                                               "not/found/foo", "deadbeef")

    data = run_isolated.TaskData(
        command=['python3', 'cmd.py', '${ISOLATED_OUTDIR}/foo'],
        relative_cwd=None,
        cas_instance=None,
        cas_digest=None,
        outputs=None,
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=emulate_bad_cipd,
        cas_cache_dir='',
        cas_cache_policies=local_caching.CachePolicies(0, 0, 0, 0),
        cas_kvs='',
        env={},
        env_prefix={},
        lower_priority=False,
        containment=None,
        trim_caches_fn=trim_caches_stub)

    result_json = os.path.join(self.tempdir, 'result.json')
    ret = run_isolated.run_tha_test(data, result_json)
    with open(result_json) as f:
      result = json.load(f)

    self.assertEqual(1, ret)
    self.assertEqual('foo_package', result['missing_cipd'][0]['package_name'])
    self.assertEqual('not/found/foo', result['missing_cipd'][0]['path'])
    self.assertEqual('deadbeef', result['missing_cipd'][0]['version'])


FILE, LINK, RELATIVE_LINK, DIR = range(4)


class RunIsolatedTestOutputs(RunIsolatedTestBase):
  # Unit test for link_outputs_to_outdir function.

  def create_src_tree(self, run_dir, src_dir):
    # Create files and directories specified by src_dir in run_dir.
    for path in src_dir:
      full_path = os.path.join(run_dir, path)
      (t, content) = src_dir[path]
      if t == FILE:
        with open(full_path, 'w') as f:
          f.write(content)
      elif t == RELATIVE_LINK:
        fs.symlink(content, full_path)
      elif t == LINK:
        root_dir = os.path.join(self.tempdir, 'ir')
        real_path = os.path.join(root_dir, content)
        fs.symlink(real_path, full_path)
      else:
        fs.mkdir(full_path)
        self.create_src_tree(os.path.join(run_dir, path), content)

  def link_outputs_test(self, src_dir, outputs):
    run_dir = os.path.join(self.tempdir, 'ir')
    out_dir = os.path.join(self.tempdir, 'io')
    fs.mkdir(run_dir)
    fs.mkdir(out_dir)
    self.create_src_tree(run_dir, src_dir)
    run_isolated.link_outputs_to_outdir(run_dir, out_dir, outputs)

  def test_file(self):
    src_dir = {
        'foo_file': (FILE, 'contents of foo'),
    }
    outputs = ['foo_file']
    expected = {
        'foo_file': 'contents of foo',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_file(self):
    src_dir = {
        'foo_file': (FILE, 'contents of foo'),
        'foo_link': (LINK, 'foo_file'),
    }
    outputs = ['foo_link']
    expected = {
        'foo_link': 'contents of foo',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_dir_containing_files(self):
    src_dir = {
        'subdir': (DIR, {
            'child_a': (FILE, 'contents of a'),
            'child_b': (FILE, 'contents of b'),
        })
    }
    outputs = [os.path.join('subdir', '')]
    expected = {
        os.path.join('subdir', 'child_a'): 'contents of a',
        os.path.join('subdir', 'child_b'): 'contents of b',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_relative_symlink(self):
    src_dir = {
        'foo_file': (FILE, 'contents of foo'),
        'subdir': (DIR, {
            'foo_link': (RELATIVE_LINK, '../foo_file'),
            'subsubdir': (DIR, {
                'bar_link': (RELATIVE_LINK, '../foo_link'),
            }),
        }),
    }
    outputs = [os.path.join('subdir', 'subsubdir', 'bar_link')]
    expected = {
        os.path.join('subdir', 'subsubdir', 'bar_link'): 'contents of foo',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_dir_containing_files(self):
    src_dir = {
        'subdir_link': (LINK, 'subdir'),
        'subdir': (DIR, {
            'child_a': (FILE, 'contents of a'),
        }),
    }
    outputs = ['subdir_link']
    expected = {
        os.path.join('subdir_link', 'child_a'): 'contents of a',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_symlink_to_dir_containing_files(self):
    src_dir = {
        'subdir_link': (LINK, 'subdir_link2'),
        'subdir_link2': (LINK, 'subdir'),
        'subdir': (DIR, {
            'child_a': (FILE, 'contents of a'),
            'child_b': (FILE, 'contents of b'),
        }),
    }
    outputs = ['subdir_link']
    expected = {
        os.path.join('subdir_link', 'child_a'): 'contents of a',
        os.path.join('subdir_link', 'child_b'): 'contents of b',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_empty_dir(self):
    src_dir = {
        'subdir': (DIR, {}),
    }
    outputs = [os.path.join('subdir', '')]
    expected = {
        os.path.join('subdir', ''): '',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_dir_ignore_trailing_slash(self):
    src_dir = {
        'subdir': (DIR, {}),
    }
    outputs = [os.path.join('subdir', '')]
    expected = {
        'subdir': '',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_dir_containing_empty_dir(self):
    src_dir = {
        'subdir': (DIR, {
            'subsubdir': (DIR, ''),
        }),
    }
    outputs = [os.path.join('subdir', '')]
    expected = {
        os.path.join('subdir', 'subsubdir', ''): '',
        os.path.join('subdir', 'subsubdir', ''): '',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_empty_dir(self):
    src_dir = {
        'subdir': (DIR, {}),
        'subdir_link': (LINK, 'subdir'),
    }
    outputs = ['subdir_link']
    expected = {
        os.path.join('subdir_link', ''): '',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_nonexistent_file(self):
    src_dir = {
        'bad_link': (LINK, 'nonexistent_file'),
    }
    outputs = ['bad_link']
    expected = {}
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_symlink_to_file(self):
    src_dir = {
        'first_link': (LINK, 'second_link'),
        'second_link': (LINK, 'foo_file'),
        'foo_file': (FILE, 'contents of foo'),
    }
    outputs = ['first_link']
    expected = {
        'first_link': 'contents of foo',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_symlink_to_nonexistent_file(self):
    src_dir = {
        'first_link': (LINK, 'second_link'),
        'second_link': (LINK, 'nonexistent_file'),
    }
    outputs = ['first_link']
    expected = {}
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_file_in_parent(self):
    src_dir = {
        'subdir': (DIR, {
            'subsubdir': (DIR, {
                'foo_link': (LINK, 'subdir/foo_file'),
            }),
            'foo_file': (FILE, 'contents of foo'),
        }),
    }
    outputs = [
      os.path.join('subdir', 'subsubdir', 'foo_link'),
      os.path.join('subdir', 'foo_file'),
    ]
    expected = {
        os.path.join('subdir', 'subsubdir', 'foo_link'): 'contents of foo',
        os.path.join('subdir', 'foo_file'): 'contents of foo',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_file_in_dir(self):
    src_dir = {
        'subdir_link': (LINK, 'subdir/child_a'),
        'subdir': (DIR, {
            'child_a': (FILE, 'contents of a'),
        }),
    }
    outputs = ['subdir_link']
    expected = {
        'subdir_link': 'contents of a',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)

  def test_symlink_to_symlink_to_file_in_dir(self):
    src_dir = {
        'first_link': (LINK, 'subdir_link'),
        'subdir_link': (LINK, 'subdir/child_a'),
        'subdir': (DIR, {
            'child_a': (FILE, 'contents of a'),
        }),
    }
    outputs = ['first_link']
    expected = {
        'first_link': 'contents of a',
    }
    self.link_outputs_test(src_dir, outputs)
    self.assertExpectedTree(expected)


class RunIsolatedTestOutputFiles(RunIsolatedTestBase):

  def setUp(self):
    super(RunIsolatedTestOutputFiles, self).setUp()

    # Starts a full CAS server mock and have run_tha_test() uploads results
    # back after the task completed.
    self._server = cas_util.LocalCAS(self.tempdir)
    self._server.start()
    os.environ['RUN_ISOLATED_CAS_ADDRESS'] = self._server.address

  def tearDown(self):
    self._server.stop()
    super(RunIsolatedTestOutputFiles, self).tearDown()

  # Like RunIsolatedTestRun, but ensures that specific output files
  # (as opposed to anything in $(ISOLATED_OUTDIR)) are returned.
  def test_output_cmd(self):
    # Output the following structure:
    #
    # foo1
    # foodir --> foo2_sl (symlink to "foo2_content" file)
    # bardir --> bar1
    #
    # Create the symlinks only on Linux.
    cas_digest = self._server.archive_files({
        'cmd.py':
        b'import os\n'
        b'import sys\n'
        b'open(sys.argv[1], "w").write("foo1")\n'
        b'bar1_path = os.path.join(sys.argv[3], "bar1")\n'
        b'open(bar1_path, "w").write("bar1")\n'
        b'if sys.platform == "linux":\n'
        b'  foo_realpath = os.path.abspath("foo2_content")\n'
        b'  open(foo_realpath, "w").write("foo2")\n'
        b'  os.symlink(foo_realpath, sys.argv[2])\n'
        b'else:\n'
        b'  open(sys.argv[2], "w").write("foo2")\n'
    })

    data = run_isolated.TaskData(
        command=['python3', 'cmd.py', 'foo1', 'foodir/foo2_sl', 'bardir/'],
        relative_cwd=None,
        cas_instance=None,
        cas_digest=cas_digest,
        outputs=[
            'foo1',
            # They must be in OS native path.
            os.path.join('foodir', 'foo2_sl'),
            os.path.join('bardir', ''),
        ],
        install_named_caches=init_named_caches_stub,
        leak_temp_dir=False,
        root_dir=self.tempdir,
        hard_timeout=60,
        grace_period=30,
        bot_file=None,
        switch_to_account=False,
        install_packages_fn=run_isolated.copy_local_packages,
        cas_cache_dir='',
        cas_cache_policies=local_caching.CachePolicies(0, 0, 0, 0),
        cas_kvs='',
        env={},
        env_prefix={},
        lower_priority=False,
        containment=None,
        trim_caches_fn=trim_caches_stub)
    result_json = os.path.join(self.tempdir, 'result.json')
    ret = run_isolated.run_tha_test(data, result_json)
    self.assertEqual(0, ret)

    # It uploaded back. Assert the store has a new item containing foo.

    with open(result_json) as f:
      result = json.load(f)

    output_digest = result['cas_output_root']['digest']
    dest = os.path.join(self.tempdir, 'outputs')
    self._server.download(
        output_digest['hash'] + '/' + str(output_digest['size_bytes']), dest)

    self.assertExpectedTree(
        {
            os.path.join('foodir', 'foo2_sl'): 'foo2',
            'foo1': 'foo1',
            os.path.join('bardir', 'bar1'): 'bar1',
        }, dest)

    self.assertCountEqual(os.listdir(dest), ['foodir', 'foo1', 'bardir'])
    self.assertEqual(os.listdir(os.path.join(dest, 'foodir')), ['foo2_sl'])
    self.assertEqual(os.listdir(os.path.join(dest, 'bardir')), ['bar1'])


class RunIsolatedJsonTest(RunIsolatedTestBase):
  # Similar to RunIsolatedTest but adds the hacks to process ISOLATED_OUTDIR to
  # generate a json result file.
  def setUp(self):
    super(RunIsolatedJsonTest, self).setUp()

    # Starts a full CAS server mock and have run_tha_test() uploads results
    # back after the task completed.
    self._server = cas_util.LocalCAS(self.tempdir)
    self._server.start()
    os.environ['RUN_ISOLATED_CAS_ADDRESS'] = self._server.address

  def tearDown(self):
    self._server.stop()
    super(RunIsolatedJsonTest, self).tearDown()

  def test_main_json(self):
    sub_cmd = [
        'python3',
        '-c',
        'import sys; open(sys.argv[1], "wb").write(b"generated data\\n")',
        '${ISOLATED_OUTDIR}/out.txt',
    ]

    out = os.path.join(self.tempdir, 'res.json')
    cmd = self.DISABLE_CIPD_FOR_TESTS + [
        '--no-log',
        '--named-cache-root',
        os.path.join(self.tempdir, 'named_cache'),
        '--json',
        out,
        '--root-dir',
        self.tempdir,
        '--',
    ] + sub_cmd
    ret = run_isolated.main(cmd)
    self.assertEqual(0, ret)

    cas_hash = (
        '5868195adddf130eeb09a939bc40a17033fb058288b7b0bec0144dcb07e9cf78')

    expected = {
        'exit_code': 0,
        'had_hard_timeout': False,
        'internal_failure': None,
        'cas_output_root': {
            # Local cas server uses its own default instance.
            'cas_instance': None,
            'digest': {
                'hash': cas_hash,
                'size_bytes': 81
            },
        },
        'outputs_ref': None,
        'stats': {
            'trim_caches': {},
            'isolated': {
                'download': {},
                'upload': {
                    'items_cold': [15, 81],
                    'items_hot': None,
                    'result': 'success'
                },
            },
            'named_caches': {
                'install': {},
                'uninstall': {},
            },
            'cleanup': {},
        },
        'version': 5,
    }
    actual = tools.read_json(out)
    # duration can be exactly 0 due to low timer resolution, especially but not
    # exclusively on Windows.
    self.assertLessEqual(0, actual.pop('duration'))
    self.assertLessEqual(0, actual['stats']['trim_caches'].pop('duration'))
    actual_upload_stats = actual['stats']['isolated']['upload']
    self.assertLessEqual(0, actual_upload_stats.pop('duration'))
    named_caches_stats = actual['stats']['named_caches']
    self.assertLessEqual(0, named_caches_stats['install'].pop('duration'))
    self.assertLessEqual(0, named_caches_stats['uninstall'].pop('duration'))
    self.assertLessEqual(0, actual['stats']['cleanup'].pop('duration'))
    for i in ('items_cold', 'items_hot'):
      if actual_upload_stats[i]:
        actual_upload_stats[i] = large.unpack(
            base64.b64decode(actual_upload_stats[i]))
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  test_env.main()
