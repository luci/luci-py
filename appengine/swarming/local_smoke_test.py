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

sys.path.insert(0, BOT_DIR)

# This is only needed on Windows.
import test_env_bot
test_env_bot.init_symlinks(BOT_DIR)

from api import os_utilities


# Signal as seen by the process.
SIGNAL_TERM = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM


# For the isolated tests that outputs a file named result.txt containing 'hey'.
ISOLATE_HELLO_WORLD = {
  'variables': {
    'command': ['python', '-u', 'hello_world.py'],
    'files': ['hello_world.py'],
  },
}

RESULT_HEY_ISOLATED_OUT = {
  u'isolated': u'f10f4c42b38ca01726610f9575ba695468c32108',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=f10f4c42b38ca01726610f9575ba695468c32108',
}

RESULT_HEY_OUTPUTS_REF = {
  u'isolated': u'f10f4c42b38ca01726610f9575ba695468c32108',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=f10f4c42b38ca01726610f9575ba695468c32108',
}


class SwarmingClient(object):
  def __init__(self, swarming_server, isolate_server):
    self._swarming_server = swarming_server
    self._isolate_server = isolate_server
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_client')
    self._index = 0

  def isolate(self, isolate_path, isolated_path):
    cmd = [
      sys.executable, 'isolate.py', 'archive',
      '-I', self._isolate_server,
      '--namespace', 'default-gzip',
      '-i', isolate_path,
      '-s', isolated_path,
    ]
    isolated_hash = subprocess.check_output(cmd, cwd=CLIENT_DIR).split()[0]
    logging.debug('%s = %s', isolated_path, isolated_hash)
    return isolated_hash

  def task_trigger_raw(self, args):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      cmd = [
        '--user', 'joe@localhost',
        '-d', 'cpu', 'x86',
        '--dump-json', tmp,
        '--raw-cmd',
      ]
      cmd.extend(args)
      assert not self._run('trigger', cmd), args
      with open(tmp, 'rb') as f:
        data = json.load(f)
        task_id = data['tasks'].popitem()[1]['task_id']
        logging.debug('task_id = %s', task_id)
        return task_id
    finally:
      os.remove(tmp)

  def task_trigger_isolated(self, name, isolated_hash, extra=None):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      cmd = [
        '--user', 'joe@localhost',
        '-d', 'cpu', 'x86',
        '--dump-json', tmp,
        '--task-name', name,
        '-I',  self._isolate_server,
        '--namespace', 'default-gzip',
        isolated_hash,
      ]
      if extra:
        cmd.extend(extra)
      assert not self._run('trigger', cmd)
      with open(tmp, 'rb') as f:
        data = json.load(f)
        task_id = data['tasks'].popitem()[1]['task_id']
        logging.debug('task_id = %s', task_id)
        return task_id
    finally:
      os.remove(tmp)

  def task_collect(self, task_id):
    """Collects the results for a task."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      tmpdir = tempfile.mkdtemp(prefix='swarming_smoke_test')
      try:
        # swarming.py collect will return the exit code of the task.
        args = [
          '--task-summary-json', tmp, task_id, '--task-output-dir', tmpdir,
          '--timeout', '20',
        ]
        self._run('collect', args)
        with open(tmp, 'rb') as f:
          content = f.read()
        try:
          summary = json.loads(content)
        except ValueError:
          print >> sys.stderr, 'Bad json:\n%s' % content
          raise
        outputs = {}
        for root, _, files in os.walk(tmpdir):
          for i in files:
            p = os.path.join(root, i)
            name = p[len(tmpdir)+1:]
            with open(p, 'rb') as f:
              outputs[name] = f.read()
        return summary, outputs
      finally:
        shutil.rmtree(tmpdir)
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
    """Runs swarming.py and capture the stdout to a log file.

    The log file will be printed by the test framework in case of failure or
    verbose mode.
    """
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
    u'bot_dimensions': None,
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


class Test(unittest.TestCase):
  maxDiff = 2000
  client = None
  dimensions = None
  servers = None

  @classmethod
  def setUpClass(cls):
    cls.dimensions = os_utilities.get_dimensions()

  def gen_expected(self, **kwargs):
    return gen_expected(bot_dimensions=self.dimensions, **kwargs)

  def test_raw_bytes(self):
    # A string of a letter 'A', UTF-8 BOM then UTF-16 BOM then UTF-EDBCDIC then
    # invalid UTF-8 and the letter 'B'. It is double escaped so it can be passed
    # down the shell.
    invalid_bytes = 'A\\xEF\\xBB\\xBF\\xFE\\xFF\\xDD\\x73\\x66\\x73\\xc3\\x28B'
    args = [
      '-T', 'non_utf8', '--',
      'python', '-u', '-c', 'print(\'' + invalid_bytes + '\')',
    ]
    summary = self.gen_expected(
        name=u'non_utf8',
        # The string is mostly converted to 'Replacement Character'.
        outputs=[u'A\ufeff\ufffd\ufffd\ufffdsfs\ufffd(B\n'])
    self.assertOneTask(args, summary, {})

  def test_invalid_command(self):
    args = ['-T', 'invalid', '--', 'unknown_invalid_command']
    err = (
      '[Error 2] The system cannot find the file specified'
      if sys.platform == 'win32' else '[Errno 2] No such file or directory')
    summary = self.gen_expected(
        name=u'invalid',
        exit_codes=[1],
        failure=True,
        outputs=[
          u'Command "unknown_invalid_command" failed to start.\n'
          u'Error: %s' % err,
        ])
    self.assertOneTask(args, summary, {})

  def test_hard_timeout(self):
    args = [
      # Need to flush to ensure it will be sent to the server.
      '-T', 'hard_timeout', '--hard-timeout', '1', '--',
      'python', '-u', '-c',
      'import time,sys; sys.stdout.write(\'hi\\n\'); '
        'sys.stdout.flush(); time.sleep(120)',
    ]
    summary = self.gen_expected(
        name=u'hard_timeout',
        exit_codes=[SIGNAL_TERM],
        failure=True,
        state=0x40)  # task_result.State.TIMED_OUT
    self.assertOneTask(args, summary, {})

  def test_io_timeout(self):
    args = [
      # Need to flush to ensure it will be sent to the server.
      '-T', 'io_timeout', '--io-timeout', '1', '--',
      'python', '-u', '-c',
      'import time,sys; sys.stdout.write(\'hi\\n\'); '
        'sys.stdout.flush(); time.sleep(120)',
    ]
    summary = self.gen_expected(
        name=u'io_timeout',
        exit_codes=[SIGNAL_TERM],
        failure=True,
        state=0x40)  # task_result.State.TIMED_OUT
    self.assertOneTask(args, summary, {})

  def test_success_fails(self):
    def get_hello_world(exit_code=0):
      return [
        'python', '-u', '-c',
        'import sys; print(\'hi\'); sys.exit(%d)' % exit_code,
      ]
    # tuple(task_request, expectation)
    tasks = [
      (
        ['-T', 'simple_success', '--'] + get_hello_world(),
        (self.gen_expected(name=u'simple_success'), {}),
      ),
      (
        ['-T', 'simple_failure', '--'] + get_hello_world(1),
        (
          self.gen_expected(
              name=u'simple_failure', exit_codes=[1], failure=True),
          {},
        ),
      ),
      (
        ['-T', 'ending_simple_success', '--'] + get_hello_world(),
        (self.gen_expected(name=u'ending_simple_success'), {}),
      ),
    ]

    # tuple(task_id, expectation)
    running_tasks = [
      (self.client.task_trigger_raw(args), expected) for args, expected in tasks
    ]

    for task_id, (summary, files) in running_tasks:
      actual_summary, actual_files = self.client.task_collect(task_id)
      self.assertResults(summary, actual_summary)
      actual_files.pop('summary.json')
      self.assertEqual(files, actual_files)

  def test_update_continue(self):
    # Run a task, force the bot to update, run another task, ensure both tasks
    # used different bot version.
    args = ['-T', 'simple_success', '--', 'python', '-u', '-c', 'print(\'hi\')']
    summary = self.gen_expected(name=u'simple_success')
    bot_version1 = self.assertOneTask(args, summary, {})

    # Replace bot_config.py.
    with open(os.path.join(BOT_DIR, 'config', 'bot_config.py'), 'rb') as f:
      bot_config_content = f.read() + '\n'

    # This will restart the bot. This ensures the update mechanism works.
    # TODO(maruel): Convert to a real API. Can only be accessed by admin-level
    # account.
    res = self.servers.http_client.request(
        '/restricted/upload/bot_config',
        body=urllib.urlencode({'script': bot_config_content}))
    self.assertEqual(200, res.http_code, res.body)
    bot_version2 = self.assertOneTask(args, summary, {})
    self.assertNotEqual(bot_version1, bot_version2)

  def test_isolated(self):
    # Make an isolated file, archive it.
    hello_world = '\n'.join((
        'import os',
        'import sys',
        'print(\'hi\')',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey\')'))
    expected_summary = self.gen_expected(
        name=u'isolated_task',
        isolated_out=RESULT_HEY_ISOLATED_OUT,
        outputs=[u'hi\n'],
        outputs_ref=RESULT_HEY_OUTPUTS_REF)
    expected_files = {os.path.join('0', 'result.txt'): 'hey'}
    self._run_isolated(
        hello_world, 'isolated_task', ['--', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files)

  def test_isolated_hard_timeout(self):
    # Make an isolated file, archive it, have it time out. Similar to
    # test_hard_timeout. The script doesn't handle signal so it failed the grace
    # period.
    hello_world = '\n'.join((
        'import os',
        'import sys',
        'import time',
        'sys.stdout.write(\'hi\\n\')',
        'sys.stdout.flush()',
        'time.sleep(120)',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey\')'))
    expected_summary = self.gen_expected(
        name=u'isolated_hard_timeout',
        exit_codes=[SIGNAL_TERM],
        failure=True,
        state=0x40)  # task_result.State.TIMED_OUT
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    self._run_isolated(
        hello_world, 'isolated_hard_timeout',
        ['--hard-timeout', '1', '--', '${ISOLATED_OUTDIR}'],
        expected_summary, {})

  def test_isolated_hard_timeout_grace(self):
    # Make an isolated file, archive it, have it time out. Similar to
    # test_hard_timeout. The script handles signal so it send results back.
    hello_world = '\n'.join((
        'import os',
        'import signal',
        'import sys',
        'import time',
        'l = []',
        'def handler(signum, _):',
        '  l.append(signum)',
        '  sys.stdout.write(\'got signal %d\\n\' % signum)',
        '  sys.stdout.flush()',
        'signal.signal(signal.%s, handler)' %
            ('SIGBREAK' if sys.platform == 'win32' else 'SIGTERM'),
        'sys.stdout.write(\'hi\\n\')',
        'sys.stdout.flush()',
        'while not l:',
        '  try:',
        '    time.sleep(0.01)',
        '  except IOError:',
        '    print(\'ioerror\')',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey\')'))
    expected_summary = self.gen_expected(
        name=u'isolated_hard_timeout_grace',
        isolated_out=RESULT_HEY_ISOLATED_OUT,
        outputs=[u'hi\ngot signal 15\n'],
        outputs_ref=RESULT_HEY_OUTPUTS_REF,
        failure=True,
        state=0x40)  # task_result.State.TIMED_OUT
    expected_files = {os.path.join('0', 'result.txt'): 'hey'}
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    self._run_isolated(
        hello_world, 'isolated_hard_timeout_grace',
        ['--hard-timeout', '1', '--', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files)

  def _run_isolated(self, hello_world, name, args, expected_summary,
      expected_files):
    # Shared code for all test_isolated_* test cases.
    tmpdir = tempfile.mkdtemp(prefix='swarming_smoke')
    try:
      isolate_path = os.path.join(tmpdir, 'i.isolate')
      isolated_path = os.path.join(tmpdir, 'i.isolated')
      with open(isolate_path, 'wb') as f:
        json.dump(ISOLATE_HELLO_WORLD, f)
      with open(os.path.join(tmpdir, 'hello_world.py'), 'wb') as f:
        f.write(hello_world)
      isolated_hash = self.client.isolate(isolate_path, isolated_path)
      task_id = self.client.task_trigger_isolated(
          name, isolated_hash, extra=args)
      actual_summary, actual_files = self.client.task_collect(task_id)
      self.assertResults(expected_summary, actual_summary)
      actual_files.pop('summary.json')
      self.assertEqual(expected_files, actual_files)
    finally:
      shutil.rmtree(tmpdir)

  def assertResults(self, expected, result):
    self.assertEqual(['shards'], result.keys())
    self.assertEqual(1, len(result['shards']))
    self.assertTrue(result['shards'][0])
    result = result['shards'][0].copy()
    # These are not deterministic (or I'm too lazy to calculate the value).
    bot_version = result.pop('bot_version')
    self.assertTrue(bot_version)
    self.assertTrue(result.pop('costs_usd'))
    self.assertTrue(result.pop('created_ts'))
    self.assertTrue(result.pop('completed_ts'))
    self.assertTrue(result.pop('durations'))
    self.assertTrue(result.pop('id'))
    self.assertTrue(result.pop('modified_ts'))
    self.assertTrue(result.pop('started_ts'))
    self.assertEqual(expected, result)
    return bot_version

  def assertOneTask(self, args, expected_summary, expected_files):
    """Runs a single task at a time."""
    task_id = self.client.task_trigger_raw(args)
    actual_summary, actual_files = self.client.task_collect(task_id)
    bot_version = self.assertResults(expected_summary, actual_summary)
    actual_files.pop('summary.json')
    self.assertEqual(expected_files, actual_files)
    return bot_version


def cleanup(bot, client, servers, print_all, leak):
  """Kills bot, kills server, print logs if failed, delete tmpdir."""
  try:
    try:
      try:
        if bot:
          bot.stop(leak)
      finally:
        if servers:
          servers.stop(leak)
    finally:
      if print_all:
        if bot:
          bot.dump_log()
        if servers:
          servers.dump_log()
        if client:
          client.dump_log()
  finally:
    if client and not leak:
      client.cleanup()


def main():
  verbose = '-v' in sys.argv
  leak = bool('--leak' in sys.argv)
  if leak:
    sys.argv.remove('--leak')
  if verbose:
    logging.basicConfig(level=logging.INFO)
    unittest.TestCase.maxDiff = None
  else:
    logging.basicConfig(level=logging.ERROR)

  # Force language to be English, otherwise the error messages differ from
  # expectations.
  os.environ['LANG'] = 'en_US.UTF-8'
  os.environ['LANGUAGE'] = 'en_US.UTF-8'

  bot = None
  client = None
  servers = None
  failed = True
  try:
    servers = start_servers.LocalServers(False)
    servers.start()
    bot = start_bot.LocalBot(servers.swarming_server.url)
    bot.start()
    client = SwarmingClient(
        servers.swarming_server.url, servers.isolate_server.url)
    # Test cases only interract with the client; except for test_update_continue
    # which mutates the bot.
    Test.client = client
    Test.servers = servers
    failed = not unittest.main(exit=False).result.wasSuccessful()
  except KeyboardInterrupt:
    print >> sys.stderr, '<Ctrl-C>'
    if bot:
      bot.kill()
  finally:
    cleanup(bot, client, servers, failed or verbose, leak)
  return int(failed)


if __name__ == '__main__':
  sys.exit(main())
