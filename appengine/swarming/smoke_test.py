#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server and Swarming bot.

It starts both a Swarming server and a swarming bot and triggers mock tests to
ensure the system works end to end.
"""

import glob
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib

APP_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(APP_DIR, 'swarming_bot')
sys.path.insert(0, APP_DIR)

import test_env
test_env.setup_test_env()

from server import bot_archive
from support import local_app


# Modified in "if __name__ == '__main__'" below.
VERBOSE = False


def setup_bot(swarming_bot_dir, host):
  """Setups the slave code in a temporary directory so it can be modified."""
  with open(os.path.join(BOT_DIR, 'start_slave.py'), 'rb') as f:
    start_slave_content = f.read()

  # Creates a functional but invalid swarming_bot.zip.
  zip_content = bot_archive.get_swarming_bot_zip(
      BOT_DIR, host, {'start_slave.py': start_slave_content, 'invalid': 'foo'})

  swarming_bot_zip = os.path.join(swarming_bot_dir, 'swarming_bot.zip')
  with open(swarming_bot_zip, 'wb') as f:
    f.write(zip_content)
  logging.info(
      'Generated %s (%d bytes)',
      swarming_bot_zip, os.stat(swarming_bot_zip).st_size)


def start_bot(cwd, log_dir):
  cmd = [
    sys.executable,
    os.path.join(cwd, 'swarming_bot.zip'),
    'start_slave',
  ]
  with open(os.path.join(log_dir, 'start_slave_stdout.log'), 'wb') as f:
    f.write('Running: %s\n' % cmd)
    f.flush()
    return subprocess.Popen(
        cmd, cwd=cwd, preexec_fn=os.setsid,
        stdout=f, stderr=subprocess.STDOUT)


def kill_bot(bot_proc):
  if not bot_proc:
    return
  if bot_proc.poll() is None:
    try:
      # TODO(maruel): os.killpg() doesn't exist on Windows.
      os.killpg(bot_proc.pid, signal.SIGKILL)
      bot_proc.wait()
    except OSError:
      pass
  else:
    # The bot should have quit normally when it self-updates.
    assert not bot_proc.returncode


def trigger(client, name):
  request = {
    'name': name,
    'priority': 10,
    'properties': {
      'commands': [['python', '-c', 'print(\'hi\')']],
      'data': [],
      'dimensions': {'cpu': 'x86'},
      'env': {},
      'execution_timeout_secs': 3600,
      'io_timeout_secs': 1200,
    },
    'scheduling_expiration_secs': 24*60*60,
    'tags': [],
    'user': 'joe@localhost',
  }
  response = client.json_request(
      '/swarming/api/v1/client/request', request).body
  task_id = response['task_id']
  logging.info('Triggered %s', task_id)
  return task_id


def get_result(client, task_id):
  start = time.time()
  while time.time() < (start + 60):
    response = client.json_request(
        '/swarming/api/v1/client/task/%s' % task_id).body
    if response['state'] not in (0x10, 0x20):
      logging.info('Task %s completed:\n%s', task_id, response)
      return response['state'] == 0x70
    time.sleep(2 if VERBOSE else 0.01)
  return False


def dump_logs(log_dir, swarming_bot_dir):
  for i in sorted(os.listdir(log_dir)):
    sys.stderr.write('\n%s:\n' % i)
    with open(os.path.join(log_dir, i), 'rb') as f:
      for l in f:
        sys.stderr.write('  ' + l)
  for i in sorted(
      glob.glob(os.path.join(swarming_bot_dir, '*.log'))):
    sys.stderr.write('\n%s:\n' % i)
    with open(os.path.join(log_dir, i), 'rb') as f:
      for l in f:
        sys.stderr.write('  ' + l)


class SwarmingTestCase(unittest.TestCase):
  """Test case class for Swarming integration tests."""
  def setUp(self):
    super(SwarmingTestCase, self).setUp()
    self._bot_proc = None
    self.tmpdir = None
    self.swarming_bot_dir = None
    self.log_dir = None
    self.remote = None

    self._server = local_app.LocalApplication(APP_DIR, 9050)

    self.tmpdir = tempfile.mkdtemp(prefix='swarming')
    self.swarming_bot_dir = os.path.join(self.tmpdir, 'swarming_bot')
    self.log_dir = os.path.join(self.tmpdir, 'logs')
    os.mkdir(self.swarming_bot_dir)
    os.mkdir(self.log_dir)

    # Start the server first since it is a tad slow to start.
    self._server.start()
    setup_bot(self.swarming_bot_dir, self._server.url)
    self._server.ensure_serving()
    self.client = self._server.client

  def tearDown(self):
    # Kill bot, kill server, print logs if failed, delete tmpdir, call super.
    try:
      try:
        try:
          try:
            kill_bot(self._bot_proc)
          finally:
            self._server.stop()
        finally:
          if self.has_failed() or VERBOSE:
            # Print out the logs before deleting them.
            dump_logs(self.log_dir, self.swarming_bot_dir)
            self._server.dump_log()
      finally:
        # In the end, delete the temporary directory.
        shutil.rmtree(self.tmpdir)
    finally:
      super(SwarmingTestCase, self).tearDown()

  def finish_setup(self):
    """Uploads start_slave.py and starts a bot.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    self.client.login_as_admin('smoke-test@example.com')
    self.client.url_opener.addheaders.append(
        ('X-XSRF-Token', self._server.client.xsrf_token))
    with open(os.path.join(BOT_DIR, 'start_slave.py'), 'rb') as f:
      start_slave_content = f.read() + '\n'
    self._bot_proc = start_bot(self.swarming_bot_dir, self.log_dir)

    # This will likely restart the bot.
    res = self.client.request(
        '/restricted/upload_start_slave',
        body=urllib.urlencode({'script': start_slave_content}))
    self.assertTrue(res)

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def test_integration(self):
    self.finish_setup()

    running_tasks = [
      trigger(self.client, 'foo'),
      trigger(self.client, 'bar'),
    ]

    for task_id in running_tasks:
      self.assertTrue(get_result(self.client, task_id))


if __name__ == '__main__':
  VERBOSE = '-v' in sys.argv
  logging.basicConfig(level=logging.INFO if VERBOSE else logging.ERROR)
  if VERBOSE:
    unittest.TestCase.maxDiff = None
  unittest.main()
