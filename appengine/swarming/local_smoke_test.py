#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server, Swarming bot and Swarming client.

It starts both a Swarming server and a Swarming bot and triggers tasks with the
Swarming client to ensure the system works end to end.
"""

import json
import glob
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib

APP_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(APP_DIR, 'swarming_bot')
CLIENT_DIR = os.path.join(APP_DIR, '..', '..', 'client')

from tools import start_bot
from tools import start_servers


class SwarmingClient(object):
  def __init__(self, swarming_server):
    self._swarming_server = swarming_server
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_client')
    self._index = 0

  def task_trigger(self, args):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      cmd = [
        '--user', 'joe@localhost', '-d', 'cpu', 'x86', '--dump-json', tmp,
        '--raw-cmd',
      ]
      cmd.extend(args)
      assert not self._run('trigger', cmd), args
      with open(tmp, 'r') as f:
        data = json.load(f)
        return data['tasks'].popitem()[1]['task_id']
    finally:
      os.remove(tmp)

  def task_collect(self, task_id):
    """Collects the results for a task."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      # swarming.py collect will return the exit code of the task.
      self._run('collect', ['--task-summary-json', tmp, task_id])
      with open(tmp, 'r') as f:
        return json.load(f)
    finally:
      os.remove(tmp)

  def cleanup(self):
    if self._tmpdir:
      shutil.rmtree(self._tmpdir)
      self._tmpdir = None

  def dump_log(self):
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, 'Client calls'
    print >> sys.stderr, '-' * 60
    for i in xrange(self._index):
      with open(os.path.join(self._tmpdir, 'client_%d.log' % i), 'rb') as f:
        log = f.read().strip('\n')
      for l in log.splitlines():
        sys.stderr.write('  %s\n' % l)

  def _run(self, command, args):
    name = os.path.join(self._tmpdir, 'client_%d.log' % self._index)
    self._index += 1
    cmd = [
      sys.executable, 'swarming.py', command, '-S', self._swarming_server,
      '--verbose',
    ] + args
    with open(name, 'wb') as f:
      f.write('\nRunning: %s\n' % ' '.join(cmd))
      f.flush()
      p = subprocess.Popen(
          cmd, stdout=f, stderr=subprocess.STDOUT, cwd=CLIENT_DIR)
      p.communicate()
      return p.returncode


def gen_expected(**kwargs):
  expected = {
    u'abandoned_ts': None,
    u'bot_id': unicode(socket.getfqdn().split('.', 1)[0]),
    u'children_task_ids': [],
    u'cost_saved_usd': None,
    u'deduped_from': None,
    u'exit_codes': [0],
    u'failure': False,
    u'internal_failure': False,
    u'isolated_out': None,
    u'name': u'',
    u'outputs': [u'hi\n'],
    u'outputs_ref': None,
    u'properties_hash': None,
    u'server_versions': [u'1'],
    u'state': 0x70,  # task_result.State.COMPLETED.
    u'tags': [u'cpu:x86', u'priority:100', u'user:joe@localhost'],
    u'try_number': 1,
    u'user': u'joe@localhost',
  }
  assert set(expected).issuperset(kwargs)
  expected.update(kwargs)
  return expected


class SwarmingTestCase(unittest.TestCase):
  """Test case class for Swarming integration tests."""
  def setUp(self):
    super(SwarmingTestCase, self).setUp()
    self._bot = None
    self._client = None
    self._servers = None

  def tearDown(self):
    # Kill bot, kill server, print logs if failed, delete tmpdir, call super.
    try:
      try:
        try:
          try:
            if self._bot:
              self._bot.stop()
          finally:
            if self._servers:
              self._servers.stop()
        finally:
          if self.has_failed() or is_verbose():
            # Print out the logs before deleting them.
            if self._bot:
              self._bot.dump_log()
            if self._servers:
              self._servers.dump_log()
            if self._client:
              self._client.dump_log()
      finally:
        if self._client:
          self._client.cleanup()
    finally:
      super(SwarmingTestCase, self).tearDown()

  def finish_setup(self):
    """Uploads bot_config.py and starts a bot.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    self._servers = start_servers.LocalServers(False)
    self._servers.start()
    self._bot = start_bot.LocalBot(self._servers.swarming_server.url)
    self._bot.start()

    # Replace bot_config.py.
    with open(os.path.join(BOT_DIR, 'bot_config.py'), 'rb') as f:
      bot_config_content = f.read() + '\n'
    # This will restart the bot.
    res = self._servers.http_client.request(
        '/restricted/upload/bot_config',
        body=urllib.urlencode({'script': bot_config_content}))
    if not res:
      raise Exception('Failed to upload bot_config')

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def test_integration(self):
    """Runs a few task requests and wait for results."""
    self.finish_setup()
    def cmd(exit_code=0):
      return [
        'python', '-c', 'import sys; print(\'hi\'); sys.exit(%d)' % exit_code,
      ]

    # tuple(task_request, expectation)
    tasks = [
      (
        ['-T', 'simple_success', '--'] + cmd(),
        gen_expected(name=u'simple_success'),
      ),
      (
        ['-T', 'simple_failure', '--'] + cmd(1),
        gen_expected(
          name=u'simple_failure', exit_codes=[1], failure=True),
      ),
      (
        ['-T', 'invalid', '--', 'unknown_invalid_command'],
        gen_expected(
            name=u'invalid',
            exit_codes=[1],
            failure=True,
            outputs=[
              u'Command "unknown_invalid_command" failed to start.\n'
              u'Error: [Errno 2] No such file or directory',
            ]),
      ),
      (
        [
          # Need to flush to ensure it will be sent to the server.
          '-T', 'hard_timeout', '--hard-timeout', '1', '--',
          'python', '-c',
          'import time,sys; sys.stdout.write(\'hi\\n\'); '
            'sys.stdout.flush(); time.sleep(120)',
        ],
        gen_expected(
            name=u'hard_timeout',
            exit_codes=[-signal.SIGTERM],
            failure=True,
            state=0x40),  # task_result.State.TIMED_OUT
      ),
      (
        [
          # Need to flush to ensure it will be sent to the server.
          '-T', 'io_timeout', '--io-timeout', '1', '--',
          'python', '-c',
          'import time,sys; sys.stdout.write(\'hi\\n\'); '
            'sys.stdout.flush(); time.sleep(120)',
        ],
        gen_expected(
            name=u'io_timeout',
            exit_codes=[-signal.SIGTERM],
            failure=True,
            state=0x40),  # task_result.State.TIMED_OUT
      ),
      (
        ['-T', 'ending_simple_success', '--'] + cmd(),
        gen_expected(name=u'ending_simple_success'),
      ),
    ]

    # tuple(task_id, expectation)
    self._client = SwarmingClient(self._servers.swarming_server.url)
    running_tasks = [
      (self._client.task_trigger(args), expected) for args, expected in tasks
    ]

    for task_id, expectation in running_tasks:
      self.assertResults(expectation, self._client.task_collect(task_id))

  def assertResults(self, expected, result):
    self.assertEqual(['shards'], result.keys())
    self.assertEqual(1, len(result['shards']))
    result = result['shards'][0].copy()
    # These are not deterministic (or I'm too lazy to calculate the value).
    self.assertTrue(result.pop('bot_version'))
    self.assertTrue(result.pop('costs_usd'))
    self.assertTrue(result.pop('created_ts'))
    self.assertTrue(result.pop('completed_ts'))
    self.assertTrue(result.pop('durations'))
    self.assertTrue(result.pop('id'))
    self.assertTrue(result.pop('modified_ts'))
    self.assertTrue(result.pop('started_ts'))
    self.assertEqual(expected, result)


def is_verbose():
  return '-v' in sys.argv


if __name__ == '__main__':
  if is_verbose():
    logging.basicConfig(level=logging.INFO)
    unittest.TestCase.maxDiff = None
  else:
    logging.basicConfig(level=logging.ERROR)
  unittest.main()
