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
import logging_utils
import slave_machine
import url_helper
import zipped_archive

THIS_FILE = os.path.abspath(__file__)
BASE_DIR = os.path.dirname(THIS_FILE)
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

CLIENT_TESTS = os.path.join(ROOT_DIR, '..', '..', 'client', 'tests')
sys.path.insert(0, CLIENT_TESTS)

# Creates a server mock for functions in net.py.
import net_utils


class TestSlaveMachine(net_utils.TestCase):
  def setUp(self):
    super(TestSlaveMachine, self).setUp()
    self.root_dir = tempfile.mkdtemp(prefix='slave_machine')
    self.old_cwd = os.getcwd()
    os.chdir(self.root_dir)

  def tearDown(self):
    os.chdir(self.old_cwd)
    shutil.rmtree(self.root_dir)
    super(TestSlaveMachine, self).tearDown()

  def test_get_attributes(self):
    self.assertEqual(
        ['dimensions', 'id', 'ip'],
        sorted(slave_machine.get_attributes()))

  def test_get_attributes_failsafe(self):
    self.assertEqual(
        ['dimensions', 'id', 'ip'],
        sorted(slave_machine.get_attributes_failsafe()))

  def test_post_error_task(self):
    self.mock(logging, 'error', lambda *_: None)
    self.expected_requests(
        [
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {'data': {}, 'headers': {'X-XSRF-Token-Request': '1'}},
            {'xsrf_token': 'token'},
          ),
          (
            'https://localhost:1/swarming/api/v1/bot/task_error/23',
            {
              'data': {
                'id': 'localhost',
                'message': 'error',
                'task_id': 23,
              },
              'headers': {'X-XSRF-Token': 'token'},
            },
            {},
          ),
        ])
    server = url_helper.XsrfRemote('https://localhost:1/')
    attributes = {'id': 'localhost', 'foo': 'bar'}
    slave_machine.post_error_task(server, attributes, 'error', 23)

  def test_run_bot(self):
    # Test the run_bot() loop.
    self.mock(zipped_archive, 'generate_version', lambda: '123')

    class Foo(Exception):
      pass

    called = []
    expected_attribs = slave_machine.get_attributes()
    expected_attribs['version'] = '123'

    def poll_server(remote, attributes, sleep_streak):
      self.assertEqual(remote, server)
      self.assertEqual(expected_attribs, attributes)
      called.append(sleep_streak)
      if sleep_streak == 5:
        raise Exception('Jumping out of the loop')
      return sleep_streak + 1
    self.mock(slave_machine, 'poll_server', poll_server)

    def post_error(remote, attributes, e):
      self.assertEqual(remote, server)
      self.assertEqual(expected_attribs, attributes)
      self.assertEqual('Jumping out of the loop', e)
      # Necessary to get out of the loop.
      raise Foo()

    self.mock(slave_machine, 'post_error', post_error)

    self.expected_requests(
        [
          ('https://localhost:1/server_ping', {}, None, None),
          (
            'https://localhost:1/auth/api/v1/accounts/self/xsrf_token',
            {
              'data': {'attributes': expected_attribs},
              'headers': {'X-XSRF-Token-Request': '1'},
            },
            {'xsrf_token': 'token'},
          ),
        ])
    server = url_helper.XsrfRemote('https://localhost:1/')
    with self.assertRaises(Foo):
      slave_machine.run_bot(server, None)

  def test_poll_server_sleep(self):
    slept = []
    self.mock(time, 'sleep', slept.append)
    self.mock(slave_machine, 'run_manifest', self.fail)
    self.mock(slave_machine, 'update_bot', self.fail)
    self.mock(slave_machine, 'post_error', self.fail)

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
              'data': {'attributes': {}, 'sleep_streak': 1},
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'sleep',
              'duration': 1.24,
            },
          ),
        ])
    server = url_helper.XsrfRemote('https://localhost:1/')
    self.assertEqual(2, slave_machine.poll_server(server, {}, 1))
    self.assertEqual([1.24], slept)

  def test_poll_server_run(self):
    manifest = []
    self.mock(time, 'sleep', self.fail)
    self.mock(
        slave_machine, 'run_manifest', lambda *args: manifest.append(args))
    self.mock(slave_machine, 'update_bot', self.fail)
    self.mock(slave_machine, 'post_error', self.fail)

    attribs = {'b': 'c'}
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
              'data': {'attributes': attribs, 'sleep_streak': 1},
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'run',
              'manifest': {'foo': 'bar'},
            },
          ),
        ])
    server = url_helper.XsrfRemote('https://localhost:1/')
    self.assertEqual(0, slave_machine.poll_server(server, attribs, 1))
    expected = [
      (server, {'b': 'c'}, {'foo': 'bar'}),
    ]
    self.assertEqual(expected, manifest)

  def test_poll_server_update(self):
    update = []
    self.mock(time, 'sleep', self.fail)
    self.mock(slave_machine, 'run_manifest', self.fail)
    self.mock(slave_machine, 'update_bot', lambda *args: update.append(args))
    self.mock(slave_machine, 'post_error', self.fail)

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
              'data': {'attributes': {}, 'sleep_streak': 1},
              'headers': {'X-XSRF-Token': 'token'},
            },
            {
              'cmd': 'update',
              'version': '123',
            },
          ),
        ])
    server = url_helper.XsrfRemote('https://localhost:1/')
    self.assertEqual(0, slave_machine.poll_server(server, {}, 1))
    self.assertEqual([(server, '123')], update)

  def test_run_manifest(self):
    self.mock(slave_machine, 'post_error', self.fail)

    # Method should have "self" as first argument - pylint: disable=E0213
    class Popen(object):
      def __init__(self2, cmd, cwd, stdout, stderr):
        self2.returncode = None
        expected = [
          sys.executable, THIS_FILE, 'local_test_runner',
          '-S', 'https://localhost:1', '-f', 'work/test_run.json',
        ]
        self.assertEqual(expected, cmd)
        self.assertEqual(slave_machine.ROOT_DIR, cwd)
        self.assertEqual(subprocess.PIPE, stdout)
        self.assertEqual(subprocess.STDOUT, stderr)

      def communicate(self2):
        self2.returncode = 0
        return 'foo', None
    self.mock(subprocess, 'Popen', Popen)

    server = url_helper.XsrfRemote('https://localhost:1/')
    manifest = {'task_id': 24}
    slave_machine.run_manifest(server, {}, manifest)

  def test_update_bot(self):
    self.mock(slave_machine, 'post_error', self.fail)
    self.mock(slave_machine, 'THIS_FILE', 'swarming_bot.1.zip')
    self.mock(url_helper, 'DownloadFile', lambda *_: True)
    calls = []
    cmd = [sys.executable, 'swarming_bot.2.zip', 'start_slave', '--survive']
    if sys.platform in ('cygwin', 'win32'):
      self.mock(subprocess, 'call', lambda *args: calls.append(args))
      self.mock(os, 'execv', self.fail)
      expected = [cmd]
    else:
      self.mock(subprocess, 'call', self.fail)
      self.mock(os, 'execv', lambda *args: calls.append(args))
      expected = [(sys.executable, cmd)]

    server = url_helper.XsrfRemote('https://localhost:1/')
    slave_machine.update_bot(server, '123')
    self.assertEqual(expected, calls)

  def test_get_config(self):
    expected = {u'server': u'http://localhost:8080'}
    self.assertEqual(expected, slave_machine.get_config())

  def test_main(self):
    self.mock(slave_machine, 'post_error', self.fail)

    def check(x):
      self.assertEqual(logging.WARNING, x)
    self.mock(logging_utils, 'set_console_level', check)

    def run_bot(remote, error):
      self.assertEqual('http://localhost:8080', remote.url)
      self.assertEqual(None, error)
      return 0
    self.mock(slave_machine, 'run_bot', run_bot)

    self.assertEqual(0, slave_machine.main([]))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
