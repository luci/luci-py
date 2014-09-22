#!/usr/bin/env python
# coding=utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import StringIO
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

# Import everything that does not require sys.path hack first.
import local_test_runner
import logging_utils

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

import url_helper
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


class TestLocalTestRunner(net_utils.TestCase):
  def setUp(self):
    super(TestLocalTestRunner, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='local_test_runner')
    self.work_dir = os.path.join(self.root_dir, 'work')
    os.chdir(self.root_dir)
    os.mkdir(self.work_dir)

  def tearDown(self):
    os.chdir(BASE_DIR)
    shutil.rmtree(self.root_dir)
    super(TestLocalTestRunner, self).tearDown()

  def _incrementing_time(self):
    """Makes 'duration' deterministic."""
    # Use a list so 'times' can be mutated.
    times = [0.]
    def get_time():
      times[0] += 1.
      return times[0]
    self.mock(time, 'time', get_time)

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
    local_test_runner.download_data(self.root_dir, items)
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
    server = url_helper.XsrfRemote('https://localhost:1/')

    runs = []
    def run_command(swarming_server, index, task_details, work_dir):
      self.assertEqual(server, swarming_server)
      self.assertEqual(self.work_dir, work_dir)
      self.assertTrue(isinstance(task_details, local_test_runner.TaskDetails))
      runs.append(index)
      return 23
    self.mock(local_test_runner, 'run_command', run_command)

    manifest = os.path.join(self.root_dir, 'manifest')
    with open(manifest, 'wb') as f:
      data = {
        'bot_id': 'localhost',
        'commands': [['a'], ['b', 'c']],
        'env': {'d': 'e'},
        'data': [('https://localhost:1/f', 'foo.zip')],
        'hard_timeout': 10,
        'io_timeout': 11,
        'task_id': 23,
      }
      json.dump(data, f)

    # run_command() returned 23 so load_and_run() returns False.
    self.assertEqual(False, local_test_runner.load_and_run(manifest, server))

  def test_run_command(self):
    def check_final(kwargs):
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': 0,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': False,
              'output': 'hi\n',
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
        {
          'data': {'command_index': 0, 'id': 'localhost', 'task_id': 23},
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
    server = url_helper.XsrfRemote('https://localhost:1/')

    task_details = local_test_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [[sys.executable, '-c', 'print(\'hi\')']],
          'data': [],
          'env': {},
          'hard_timeout': 6,
          'io_timeout': 6,
          'task_id': 23,
        })
    # This runs the command for real.
    self.assertEqual(
        0, local_test_runner.run_command(server, 0, task_details, '.'))

  def test_run_command_fail(self):
    def check_final(kwargs):
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': 1,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': False,
              'output': 'hi\n',
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
        {
          'data': {'command_index': 0, 'id': 'localhost', 'task_id': 23},
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
    server = url_helper.XsrfRemote('https://localhost:1/')

    task_details = local_test_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [
            [sys.executable, '-c', 'import sys; print(\'hi\'); sys.exit(1)'],
          ],
          'data': [],
          'env': {},
          'hard_timeout': 6,
          'io_timeout': 6,
          'task_id': 23,
        })
    # This runs the command for real.
    self.assertEqual(
        1, local_test_runner.run_command(server, 0, task_details, '.'))

  def test_run_command_hard_timeout(self):
    # This runs the command for real.
    def check_final(kwargs):
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': -9,
              'hard_timeout': True,
              'id': 'localhost',
              'io_timeout': False,
              'output': 'hi\n',
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
        {
          'data': {'command_index': 0, 'id': 'localhost', 'task_id': 23},
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
    server = url_helper.XsrfRemote('https://localhost:1/')

    task_details = local_test_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [
            [
              sys.executable, '-u', '-c',
              'import time; print(\'hi\'); time.sleep(10)',
            ],
          ],
          'data': [],
          'env': {},
          'hard_timeout': 1,
          'io_timeout': 10,
          'task_id': 23,
        })
    self.assertEqual(
        -9, local_test_runner.run_command(server, 0, task_details, '.'))

  def test_run_command_io_timeout(self):
    # This runs the command for real.
    def check_final(kwargs):
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': -9,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': True,
              'output': 'hi\n',
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
        {
          'data': {'command_index': 0, 'id': 'localhost', 'task_id': 23},
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
    server = url_helper.XsrfRemote('https://localhost:1/')

    task_details = local_test_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [
            [
              sys.executable, '-u', '-c',
              'import time; print(\'hi\'); time.sleep(10)',
            ],
          ],
          'data': [],
          'env': {},
          'hard_timeout': 10,
          'io_timeout': 1,
          'task_id': 23,
        })
    self.assertEqual(
        -9, local_test_runner.run_command(server, 0, task_details, '.'))

  def test_run_command_large(self):
    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      """Mocks the process so we can control how data is returned."""
      def __init__(self2, cmd, cwd, env, stdout, stderr, stdin):
        self.assertEqual(task_details.commands[0], cmd)
        self.assertEqual('./', cwd)
        self.assertEqual(os.environ, env)
        self.assertEqual(subprocess.PIPE, stdout)
        self.assertEqual(subprocess.STDOUT, stderr)
        self.assertEqual(subprocess.PIPE, stdin)
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
      self.assertLess(0, kwargs['data'].pop('duration'))
      self.assertEqual(
          {
            'data': {
              'command_index': 0,
              'exit_code': 0,
              'hard_timeout': False,
              'id': 'localhost',
              'io_timeout': False,
              'output': 'hi!\n',
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
          'data': {'command_index': 0, 'id': 'localhost', 'task_id': 23},
          'headers': {'X-XSRF-Token': 'token'},
        },
        {},
      ),
      (
        'https://localhost:1/swarming/api/v1/bot/task_update/23',
        {
          'data': {
            'command_index': 0,
            'id': 'localhost',
            'output': 'hi!\n' * 100002,
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
    server = url_helper.XsrfRemote('https://localhost:1/')
    task_details = local_test_runner.TaskDetails(
        {
          'bot_id': 'localhost',
          'commands': [['large', 'executable']],
          'data': [],
          'env': {},
          'hard_timeout': 60,
          'io_timeout': 60,
          'task_id': 23,
        })
    self.assertEqual(
        0, local_test_runner.run_command(server, 0, task_details, './'))

  def test_main(self):
    def load_and_run(manifest, swarming_server):
      self.assertEqual('foo', manifest)
      self.assertEqual('http://localhost', swarming_server.url)
      return 0

    self.mock(local_test_runner, 'load_and_run', load_and_run)
    self.assertEqual(
        local_test_runner.RESTART_CODE,
        local_test_runner.main(['-S', 'http://localhost', '-f', 'foo']))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging_utils.prepare_logging(None)
  logging_utils.set_console_level(
      logging.DEBUG if '-v' in sys.argv else logging.CRITICAL+1)
  unittest.main()
