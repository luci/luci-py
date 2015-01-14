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
sys.path.insert(0, APP_DIR)

import test_env
test_env.setup_test_env()

from server import bot_archive
from support import local_app


class LocalBot(object):
  """A local running Swarming bot."""
  def __init__(self, swarming_server, log_dir):
    self.tmpdir = tempfile.mkdtemp(prefix='swarming_bot')
    self._swarming_server = swarming_server
    self._log_dir = log_dir
    self._bot_zip = os.path.join(self.tmpdir, 'swarming_bot.zip')
    self._proc = None
    urllib.urlretrieve(swarming_server + '/get_slave_code', self._bot_zip)

  def start(self):
    """Starts the local Swarming bot."""
    assert not self._proc
    cmd = [sys.executable, self._bot_zip, 'start_slave']
    env = os.environ.copy()
    with open(os.path.join(self._log_dir, 'bot_config_stdout.log'), 'wb') as f:
      f.write('Running: %s\n' % cmd)
      f.flush()
      self._proc = subprocess.Popen(
          cmd, cwd=self.tmpdir, preexec_fn=os.setsid, stdout=f, env=env,
          stderr=subprocess.STDOUT)

  def stop(self):
    """Stops the local Swarming bot."""
    if not self._proc:
      return
    if self._proc.poll() is None:
      try:
        # TODO(maruel): os.killpg() doesn't exist on Windows.
        os.killpg(self._proc.pid, signal.SIGKILL)
        self._proc.wait()
      except OSError:
        pass
    else:
      # The bot should have quit normally when it self-updates.
      assert not self._proc.returncode

  def cleanup(self):
    shutil.rmtree(self.tmpdir)


class SwarmingClient(object):
  def __init__(self, swarming_server, log_dir):
    self._swarming_server = swarming_server
    self._log_dir = log_dir
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

  def _run(self, command, args):
    name = os.path.join(self._log_dir, '%s_%d.log' % (command, self._index))
    self._index += 1
    cmd = [
      sys.executable, 'swarming.py', command, '-S', self._swarming_server,
      '--verbose',
    ] + args
    with open(name, 'wb') as f:
      f.write('%s\n' % ' '.join(cmd))
      f.flush()
      p = subprocess.Popen(
          cmd, stdout=f, stderr=subprocess.STDOUT, cwd=CLIENT_DIR)
      p.communicate()
      return p.returncode


def gen_expected(**kwargs):
  expected = {
    u'abandoned_ts': None,
    u'bot_id': unicode(socket.getfqdn().split('.', 1)[0]),
    # TODO(maruel): This won't be zero when this test is run on a GCE VM.
    u'costs_usd': [0.],
    u'deduped_from': None,
    u'exit_codes': [0],
    u'failure': False,
    u'internal_failure': False,
    u'isolated_out': None,
    u'name': u'',
    u'properties_hash': None,
    u'outputs': [u'hi\n'],
    u'server_versions': [u'1'],
    u'state': 0x70,  # task_result.State.COMPLETED.
    u'try_number': 1,
    u'user': u'joe@localhost',
  }
  assert set(expected).issuperset(kwargs)
  expected.update(kwargs)
  return expected


def dump_logs(log_dir, swarming_bot_dir):
  """Prints tou stderr all logs found in the temporary directory."""
  files = [os.path.join(log_dir, i) for i in os.listdir(log_dir)]
  files.extend(glob.glob(os.path.join(swarming_bot_dir, '*.log')))
  files.sort()
  for i in files:
    sys.stderr.write('\n%s:\n' % i)
    with open(i, 'rb') as f:
      for l in f:
        sys.stderr.write('  ' + l)


class SwarmingTestCase(unittest.TestCase):
  """Test case class for Swarming integration tests."""
  def setUp(self):
    super(SwarmingTestCase, self).setUp()
    self._bot = None
    self._raw_client = None
    self._remote = None
    self._server = None
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_smoke')
    self._log_dir = os.path.join(self._tmpdir, 'logs')
    os.mkdir(self._log_dir)

  def tearDown(self):
    # Kill bot, kill server, print logs if failed, delete tmpdir, call super.
    try:
      try:
        try:
          try:
            try:
              self._bot.stop()
            finally:
              self._server.stop()
          finally:
            if self.has_failed():
              # Print out the logs before deleting them.
              dump_logs(self._log_dir, self._bot.tmpdir)
              self._server.dump_log()
        finally:
          self._bot.cleanup()
      finally:
        # In the end, delete the temporary directory.
        shutil.rmtree(self._tmpdir)
    finally:
      super(SwarmingTestCase, self).tearDown()

  def finish_setup(self):
    """Uploads bot_config.py and starts a bot.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    self._server = local_app.LocalApplication(APP_DIR, 9050)
    self._server.start()
    self._server.ensure_serving()
    self._raw_client = self._server.client
    self._bot = LocalBot(self._server.url, self._log_dir)
    self._bot.start()

    # Replace bot_config.py.
    self._raw_client.login_as_admin('smoke-test@example.com')
    self._raw_client.url_opener.addheaders.append(
        ('X-XSRF-Token', self._server.client.xsrf_token))
    with open(os.path.join(BOT_DIR, 'bot_config.py'), 'rb') as f:
      bot_config_content = f.read() + '\n'
    # This will restart the bot.
    res = self._raw_client.request(
        '/restricted/upload/bot_config',
        body=urllib.urlencode({'script': bot_config_content}))
    self.assertTrue(res)

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
    client = SwarmingClient(self._server.url, self._log_dir)
    running_tasks = [
      (client.task_trigger(args), expected) for args, expected in tasks
    ]

    for task_id, expectation in running_tasks:
      self.assertResults(expectation, client.task_collect(task_id))

  def assertResults(self, expected, result):
    self.assertEqual(['shards'], result.keys())
    self.assertEqual(1, len(result['shards']))
    result = result['shards'][0].copy()
    # These are not deterministic (or I'm too lazy to calculate the value).
    self.assertTrue(result.pop('bot_version'))
    self.assertTrue(result.pop('created_ts'))
    self.assertTrue(result.pop('completed_ts'))
    self.assertTrue(result.pop('durations'))
    self.assertTrue(result.pop('id'))
    self.assertTrue(result.pop('modified_ts'))
    self.assertTrue(result.pop('started_ts'))
    self.assertEqual(expected, result)


if __name__ == '__main__':
  if '-v' in sys.argv:
    logging.basicConfig(level=logging.INFO)
    unittest.TestCase.maxDiff = None
  else:
    logging.basicConfig(level=logging.ERROR)
  unittest.main()
