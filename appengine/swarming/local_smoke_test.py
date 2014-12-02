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
import socket
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
  """Setups the bot code in a temporary directory so it can be modified."""
  with open(os.path.join(BOT_DIR, 'bot_config.py'), 'rb') as f:
    bot_config_content = f.read()

  # Creates a functional but invalid swarming_bot.zip.
  zip_content = bot_archive.get_swarming_bot_zip(
      BOT_DIR, host, {'bot_config.py': bot_config_content, 'invalid': 'foo'})

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
  with open(os.path.join(log_dir, 'bot_config_stdout.log'), 'wb') as f:
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


def gen_request(exit_code=0, properties=None, **kwargs):
  out = {
    'name': None,
    'priority': 10,
    'properties': {
      'commands': [
        ['python', '-c', 'import sys; print(\'hi\'); sys.exit(%d)' % exit_code],
      ],
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
  assert set(out).issuperset(kwargs)
  out.update(kwargs)
  if properties:
    assert set(out['properties']).issuperset(properties)
    out['properties'].update(properties)
  return out


def gen_expected(**kwargs):
  expected = {
    u'abandoned_ts': None,
    u'bot_id': unicode(socket.getfqdn().split('.', 1)[0]),
    u'deduped_from': None,
    u'exit_codes': [0],
    u'failure': False,
    u'internal_failure': False,
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


def trigger(client, request):
  """Triggers a task, returns kwargs and task_id."""
  response = client.json_request(
      '/swarming/api/v1/client/request', request).body
  if not 'task_id' in response:
    raise ValueError(response)
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
      # Manually append outputs to the result.
      outputs = client.json_request(
          '/swarming/api/v1/client/task/%s/output/all' % task_id).body
      assert set(outputs) == {u'outputs'}
      response[u'outputs'] = outputs[u'outputs']
      return response
    time.sleep(2 if VERBOSE else 0.01)
  return {}


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
    """Uploads bot_config.py and starts a bot.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    self.client.login_as_admin('smoke-test@example.com')
    self.client.url_opener.addheaders.append(
        ('X-XSRF-Token', self._server.client.xsrf_token))
    with open(os.path.join(BOT_DIR, 'bot_config.py'), 'rb') as f:
      bot_config_content = f.read() + '\n'
    self._bot_proc = start_bot(self.swarming_bot_dir, self.log_dir)

    # This will likely restart the bot.
    res = self.client.request(
        '/restricted/upload/bot_config',
        body=urllib.urlencode({'script': bot_config_content}))
    self.assertTrue(res)

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def test_integration(self):
    """Runs a few task requests and wait for results."""
    self.finish_setup()

    # tuple(task_request, expectation)
    tasks = [
      (gen_request(name='foo'), gen_expected(name=u'foo')),
      (
        gen_request(name='failed', exit_code=1),
        gen_expected(
          name=u'failed', exit_codes=[1], failure=True),
      ),
      (
        gen_request(
            name='invalid',
            properties={'commands': [['unknown_invalid_command']]}),
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
        gen_request(
            name='hard_timeout',
            properties={
              'commands': [
                [
                  'python', '-c',
                  # Need to flush to ensure it will be sent to the server.
                  'import time,sys; sys.stdout.write(\'hi\\n\'); '
                    'sys.stdout.flush(); time.sleep(120)',
                ],
              ],
              'execution_timeout_secs': 1,
            }),
        gen_expected(
            name=u'hard_timeout',
            exit_codes=[-9],
            failure=True,
            state=0x40),  # task_result.State.TIMED_OUT
      ),
      (
        gen_request(
            name='io_timeout',
            properties={
              'commands': [
                [
                  'python', '-c',
                  # Need to flush to ensure it will be sent to the server.
                  'import time,sys; sys.stdout.write(\'hi\\n\'); '
                    'sys.stdout.flush(); time.sleep(120)',
                ],
              ],
              'io_timeout_secs': 1,
            }),
        gen_expected(
            name=u'io_timeout',
            exit_codes=[-9],
            failure=True,
            state=0x40),  # task_result.State.TIMED_OUT
      ),
      (gen_request(name='bar'), gen_expected(name=u'bar')),
    ]

    # tuple(task_id, expectation)
    running_tasks = [
      (trigger(self.client, request), expected) for request, expected in tasks
    ]

    for task_id, expectation in running_tasks:
      self.assertResults(expectation, get_result(self.client, task_id))

  def assertResults(self, expected, result):
    result = result.copy()
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
  VERBOSE = '-v' in sys.argv
  logging.basicConfig(level=logging.INFO if VERBOSE else logging.ERROR)
  if VERBOSE:
    unittest.TestCase.maxDiff = None
  unittest.main()
