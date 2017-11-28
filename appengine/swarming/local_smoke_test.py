#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Integration test for the Swarming server, Swarming bot and Swarming client.

It starts both a Swarming server and a Swarming bot and triggers tasks with the
Swarming client to ensure the system works end to end.
"""

import base64
import json
import logging
import os
import re
import signal
import socket
import sys
import tempfile
import textwrap
import time
import unittest
import urllib

APP_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(APP_DIR, 'swarming_bot')
CLIENT_DIR = os.path.join(APP_DIR, '..', '..', 'client')

from tools import start_bot
from tools import start_servers

sys.path.insert(0, CLIENT_DIR)
from third_party.depot_tools import fix_encoding
from utils import file_path
from utils import large
from utils import subprocess42
sys.path.pop(0)

sys.path.insert(0, BOT_DIR)
import test_env_bot
test_env_bot.setup_test_env()

from api import os_utilities


# Signal as seen by the process.
SIGNAL_TERM = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM


# Timeout to wait for the operations, so that the smoke test doesn't hang
# indefinitely.
TIMEOUT_SECS = 20


# For the isolated tests that outputs a file named result.txt containing 'hey'.
ISOLATE_HELLO_WORLD = {
  'variables': {
    'command': ['python', '-u', 'hello_world.py'],
    'files': ['hello_world.py'],
  },
}

# Update these hashes everytime isolated_format.py is updated:
RESULT_HEY_ISOLATED_OUT = {
  u'isolated': u'5c64883277eb00bceafe3659f182e194cffc5d96',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=5c64883277eb00bceafe3659f182e194cffc5d96',
}

RESULT_HEY_OUTPUTS_REF = {
  u'isolated': u'5c64883277eb00bceafe3659f182e194cffc5d96',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=5c64883277eb00bceafe3659f182e194cffc5d96',
}

RESULT_HEY2_ISOLATED_OUT = {
  u'isolated': u'7f89142465cba8d465b1f0a3f6e6f95ad28cc56b',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=7f89142465cba8d465b1f0a3f6e6f95ad28cc56b',
}

RESULT_SECRET_OUTPUT = {
  u'isolated': u'd2eca4d860e4f1728272f6a736fd1c9ac6e98c4f',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=d2eca4d860e4f1728272f6a736fd1c9ac6e98c4f',
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
    isolated_hash = subprocess42.check_output(cmd, cwd=CLIENT_DIR).split()[0]
    logging.debug('%s = %s', isolated_path, isolated_hash)
    return isolated_hash

  def task_trigger_raw(self, args):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test', suffix='.json')
    os.close(h)
    try:
      cmd = [
        '--user', 'joe@localhost',
        '-d', 'pool', 'default',
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
        '-d', 'pool', 'default',
        '--dump-json', tmp,
        '--task-name', name,
        '-I',  self._isolate_server,
        '--namespace', 'default-gzip',
        '-s', isolated_hash,
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
          '--timeout', str(TIMEOUT_SECS), '--perf',
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
        file_path.rmtree(tmpdir)
    finally:
      os.remove(tmp)

  def task_query(self, task_id):
    """Query a task result without waiting for it to complete."""
    return json.loads(self._capture('query', ['task/%s/result' % task_id]))

  def terminate(self, bot_id):
    task_id = self._capture('terminate', [bot_id]).strip()
    logging.info('swarming.py terminate returned %r', task_id)
    if not task_id:
      return 1
    return self._run('collect', ['--timeout', str(TIMEOUT_SECS), task_id])

  def cleanup(self):
    if self._tmpdir:
      file_path.rmtree(self._tmpdir)
      self._tmpdir = None

  def query_bot(self):
    """Returns the bot's properties."""
    data = json.loads(self._capture('query', ['bots/list', '--limit', '10']))
    if not data.get('items'):
      return None
    assert len(data['items']) == 1
    return data['items'][0]

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

    Returns:
      The process exit code.
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
      p = subprocess42.Popen(
          cmd, stdout=f, stderr=subprocess42.STDOUT, cwd=CLIENT_DIR)
      p.communicate()
      return p.returncode

  def _capture(self, command, args):
    cmd = [
      sys.executable, 'swarming.py', command, '-S', self._swarming_server,
    ] + args
    p = subprocess42.Popen(cmd, stdout=subprocess42.PIPE, cwd=CLIENT_DIR)
    return p.communicate()[0]


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
    u'tags': [
      u'pool:default',
      u'priority:100',
      u'service_account:none',
      u'user:joe@localhost',
    ],
    u'try_number': 1,
    u'user': u'joe@localhost',
  }
  # This is not part of the 'old' protocol that is emulated in
  # convert_to_old_format.py in //client/swarming.py.
  keys = set(expected) | {u'performance_stats'}
  assert keys.issuperset(kwargs)
  expected.update({unicode(k): v for k, v in kwargs.iteritems()})
  return expected


class Test(unittest.TestCase):
  maxDiff = None
  client = None
  servers = None
  bot = None

  def setUp(self):
    super(Test, self).setUp()
    self.dimensions = os_utilities.get_dimensions()
    # Reset the bot's isolated cache at the start of each task, so that the
    # cache reuse data becomes deterministic. Only restart the bot when it had a
    # named cache because it takes multiple seconds to to restart the bot. :(
    #
    # TODO(maruel): 'isolated_upload' is not deterministic because the isolate
    # server not cleared.
    start = time.time()
    while True:
      old = self.client.query_bot()
      if old:
        break
      if time.time() - start > TIMEOUT_SECS:
        self.fail('Bot took too long to start')

    started_ts = json.loads(old['state'])['started_ts']
    logging.info('setUp: started_ts was %s', started_ts)
    had_cache = any(u'caches' == i['key'] for i in old['dimensions'])
    self.bot.wipe_cache(had_cache)

    # The bot restarts due to wipe_cache(True) so wait for the bot to come back
    # online. It may takes a few loop.
    start = time.time()
    while True:
      if time.time() - start > TIMEOUT_SECS:
        self.fail('Bot took too long to start after wipe_cache()')
      state = self.client.query_bot()
      if not state:
        time.sleep(0.1)
        continue
      if not had_cache:
        break
      new_started_ts = json.loads(state['state'])['started_ts']
      logging.info('setUp: new_started_ts is %s', new_started_ts)
      # This assumes that starting the bot and running the previous test case
      # took more than 1s.
      if not started_ts or new_started_ts != started_ts:
        dimensions = {i['key']: i['value'] for i in state['dimensions']}
        self.assertNotIn(u'caches', dimensions)
        break

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
    summary = self.gen_expected(
        name=u'invalid',
        exit_codes=[1],
        failure=True,
        outputs=re.compile(
            u'^<The executable does not exist or a dependent library is '
            u'missing>'))
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

  def test_isolated(self):
    # Make an isolated file, archive it.
    # Assert that the environment variable SWARMING_TASK_ID is set.
    hello_world = '\n'.join((
        'import os',
        'import sys',
        'print(\'hi\')',
        'assert os.environ.get("SWARMING_TASK_ID")',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey\')'))
    expected_summary = self.gen_expected(
        name=u'isolated_task',
        isolated_out=RESULT_HEY_ISOLATED_OUT,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([len(hello_world), 200]),
            u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(len(hello_world) + 200),
          },
          u'isolated_upload': {
            u'items_cold': [3, 118],
            u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': u'121',
          },
        },
        outputs=[u'hi\n'],
        outputs_ref=RESULT_HEY_OUTPUTS_REF)
    expected_files = {os.path.join('0', 'result.txt'): 'hey'}
    self._run_isolated(
        hello_world, 'isolated_task', ['--', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files)

  def test_isolated_command(self):
    # Command is specified in Swarming task, still with isolated file.
    # Confirms that --env and --env-prefix work.
    hello_world = '\n'.join((
        'import os',
        'import sys',
        'print(\'hi\')',
        'assert "SWARMING_TASK_ID" not in os.environ',
        'cwd = os.path.realpath(os.getcwd()).rstrip(os.sep)',
        'path = os.environ["PATH"].split(os.pathsep)',
        'print(os.path.realpath(path[0]).replace(cwd, "$CWD"))',
        'with open(os.path.join(sys.argv[1], \'FOO.txt\'), \'wb\') as f:',
        '  f.write(os.environ["FOO"])',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey2\')',
    ))
    computed_PATH = os.sep.join(['$CWD', 'local', 'path'])

    # Hard code the size of the isolated file.
    isolated_size = 138
    expected_summary = self.gen_expected(
        name=u'separate_cmd',
        isolated_out=RESULT_HEY2_ISOLATED_OUT,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([len(hello_world), isolated_size]),
            u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(
                len(hello_world) + isolated_size),
          },
          u'isolated_upload': {
            u'items_cold': [3, 4, 191],
            u'items_hot': [],
            u'num_items_cold': u'3',
            u'total_bytes_items_cold': u'198',
          },
        },
        outputs=[u'hi\n%s\n' % computed_PATH],
        outputs_ref=RESULT_HEY2_ISOLATED_OUT)
    expected_files = {
      os.path.join('0', 'result.txt'): 'hey2',
      os.path.join('0', 'FOO.txt'): 'bar',
    }
    self._run_isolated(
        hello_world, 'separate_cmd',
        ['--raw-cmd',
         '--env', 'FOO', 'bar',
         '--env', 'SWARMING_TASK_ID', '',
         '--env-prefix', 'PATH', 'local/path',
         '--', 'python', 'hello_world.py', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files,
        isolated_content={'variables': {'files': ['hello_world.py']}})

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
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([len(hello_world), 200]),
            u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(len(hello_world) + 200),
          },
          u'isolated_upload': {
            u'items_cold': [],
            u'items_hot': [],
          },
        },
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
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([200, len(hello_world)]),
            u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(len(hello_world) + 200),
          },
          u'isolated_upload': {
            u'items_cold': [],
            u'items_hot': [3, 118],
            u'num_items_hot': u'2',
            u'total_bytes_items_hot': u'121',
          },
        },
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

  def test_idempotent_reuse(self):
    hello_world = 'print "hi"\n'
    expected_summary = self.gen_expected(
      name=u'idempotent_reuse',
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sorted([len(hello_world), 199]),
          u'items_hot': [],
            u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(len(hello_world) + 199),
        },
        u'isolated_upload': {
          u'items_cold': [],
          u'items_hot': [],
        },
      },
      properties_hash =
          u'3d6dcab82b1c098b27c7a7cbb6ddcb3d41689c42ca7fe5fafd53f9105af6efea',
    )
    task_id = self._run_isolated(
        hello_world, 'idempotent_reuse', ['--idempotent'], expected_summary, {})
    expected_summary[u'costs_usd'] = None
    expected_summary.pop('performance_stats')
    expected_summary[u'cost_saved_usd'] = 0.02
    expected_summary[u'deduped_from'] = task_id[:-1] + '1'
    expected_summary[u'try_number'] = 0
    expected_summary[u'properties_hash'] = None
    self._run_isolated(
        hello_world, 'idempotent_reuse', ['--idempotent'], expected_summary, {},
        deduped=True)

  def test_secret_bytes(self):
    hello_world = textwrap.dedent("""
      import sys
      import os
      import json

      print "hi"

      with open(os.environ['LUCI_CONTEXT'], 'r') as f:
        data = json.load(f)

      with open(os.path.join(sys.argv[1], 'sekret'), 'w') as f:
        print >> f, data['swarming']['secret_bytes'].decode('base64')
    """)
    expected_summary = self.gen_expected(
      name=u'secret_bytes',
      isolated_out=RESULT_SECRET_OUTPUT,
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sorted([200, len(hello_world)]),
          u'items_hot': [],
          u'num_items_cold': u'2',
            u'total_bytes_items_cold': unicode(len(hello_world) + 200),
        },
        u'isolated_upload': {
          u'items_cold': [7, 114],
          u'items_hot': [],
          u'num_items_cold': u'2',
          u'total_bytes_items_cold': u'121',
        },
      },
      outputs_ref=RESULT_SECRET_OUTPUT,
    )
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test_secret')
    os.write(h, 'foobar')
    os.close(h)
    try:
      self._run_isolated(
          hello_world, 'secret_bytes',
          ['--secret-bytes-path', tmp, '--', '${ISOLATED_OUTDIR}'],
          expected_summary, {os.path.join('0', 'sekret'): 'foobar\n'})
    finally:
      os.remove(tmp)

  def test_local_cache(self):
    # First task creates the cache, second copy the content to the output
    # directory. Each time it's the exact same script.
    dimensions = {
        i['key']: i['value'] for i in self.client.query_bot()['dimensions']}
    self.assertEqual(set(self.dimensions), set(dimensions))
    self.assertNotIn(u'cache', set(dimensions))
    script = '\n'.join((
      'import os, shutil, sys',
      'p = "p/b/a.txt"',
      'if not os.path.isfile(p):',
      '  with open(p, "wb") as f:',
      '    f.write("Yo!")',
      'else:',
      '  shutil.copy(p, sys.argv[1])',
      'print "hi"'))
    sizes = sorted([len(script), 200])
    expected_summary = self.gen_expected(
      name=u'cache_first',
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sizes,
          u'items_hot': [],
          u'num_items_cold': u'2',
          u'total_bytes_items_cold': unicode(sum(sizes)),
        },
        u'isolated_upload': {
          u'items_cold': [],
          u'items_hot': [],
        },
      },
    )
    self._run_isolated(
        script, 'cache_first',
        ['--named-cache', 'fuu', 'p/b', '--', '${ISOLATED_OUTDIR}/yo'],
        expected_summary, {})

    # Second run with a cache available.
    out = {
      u'isolated': u'63fc667fd217ebabdf60ca143fe25998b5ea5c77',
      u'isolatedserver': u'http://localhost:10050',
      u'namespace': u'default-gzip',
      u'view_url':
        u'http://localhost:10050/browse?namespace=default-gzip'
          u'&hash=63fc667fd217ebabdf60ca143fe25998b5ea5c77',
    }
    expected_summary = self.gen_expected(
      name=u'cache_second',
      isolated_out=out,
      outputs_ref=out,
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': unicode(len(sizes)),
          u'initial_size': unicode(sum(sizes)),
          u'items_cold': [],
          u'items_hot': sizes,
          u'num_items_hot': u'2',
          u'total_bytes_items_hot': unicode(sum(sizes)),
        },
        u'isolated_upload': {
          u'items_cold': [3, 110],
          u'items_hot': [],
          u'num_items_cold': u'2',
          u'total_bytes_items_cold': u'113',
        },
      },
    )
    # The previous task caused the bot to have a named cache.
    expected_summary['bot_dimensions'] = (
        expected_summary['bot_dimensions'].copy())
    expected_summary['bot_dimensions'][u'caches'] = [u'fuu']
    self._run_isolated(
        script, 'cache_second',
        ['--named-cache', 'fuu', 'p/b', '--', '${ISOLATED_OUTDIR}/yo'],
        expected_summary,
        {'0/yo': 'Yo!'})

    # Check that the bot now has a cache dimension by independently querying.
    expected = set(self.dimensions)
    expected.add(u'caches')
    dimensions = {
        i['key']: i['value'] for i in self.client.query_bot()['dimensions']}
    self.assertEqual(expected, set(dimensions))

  def test_priority(self):
    # Make a test that keeps the bot busy, while all the other tasks are being
    # created with priorities that are out of order. Then it unblocks the bot
    # and asserts the tasks are run in the expected order, based on priority and
    # created_ts.
    h, tmp = tempfile.mkstemp(prefix='swarming_smoke_test')
    os.close(h)

    # List of tuple(task_name, priority, task_id).
    tasks = []
    tags = [u'pool:default', u'service_account:none', u'user:joe@localhost']
    try:
      args = [
        '-T', 'wait', '--priority', '20', '--',
        'python', '-u', '-c',
        # Cheezy wait.
        ('import os,time;'
         '[time.sleep(0.1) for _ in xrange(100000) if os.path.exists(\'%s\')];'
        'print(\'hi\')') % tmp,
      ]
      wait_task_id = self.client.task_trigger_raw(args)
      # Assert that the 'wait' task has started but not completed, otherwise
      # this defeats the purpose.
      state = stats = None
      start = time.time()
      # 15 seconds is a long time.
      while time.time() - start < 15.:
        stats = self.client.task_query(wait_task_id)
        state = stats[u'state']
        if state == u'RUNNING':
          break
        self.assertEqual(u'PENDING', state, stats)
        time.sleep(0.01)
      self.assertEqual(u'RUNNING', state, stats)

      # This is the order of the priorities used for each task triggered. In
      # particular, below it asserts that the priority 8 tasks are run in order
      # of created_ts.
      for i, priority in enumerate((9, 8, 6, 7, 8)):
        task_name = '%d-p%d' % (i, priority)
        args = [
          '-T', task_name, '--priority', str(priority), '--',
          'python', '-u', '-c', 'print(\'%d\')' % priority,
        ]
        tasks.append((task_name, priority, self.client.task_trigger_raw(args)))

      # And the wait task is still running, so that all tasks above are pending,
      # thus are given a chance to run in priority order.
      stats = self.client.task_query(wait_task_id)
      self.assertEqual(u'RUNNING', stats[u'state'], stats)

      # Ensure the tasks under test are pending.
      for task_name, priority, task_id in tasks:
        stats = self.client.task_query(task_id)
      self.assertEqual(u'PENDING', stats[u'state'], stats)
    finally:
      # Unblock the wait_task_id on the bot.
      os.remove(tmp)

    # Ensure the initial wait task is completed. This will cause all the pending
    # tasks to be run. Now, will they be run in the expected order? That is the
    # question!
    actual_summary, actual_files = self.client.task_collect(wait_task_id)
    t = tags[:] + [u'priority:20']
    self.assertResults(
        self.gen_expected(name=u'wait', tags=sorted(t)), actual_summary)
    self.assertEqual(['summary.json'], actual_files.keys())

    # List of tuple(task_name, priority, task_id, results).
    results = []
    # Collect every tasks.
    for task_name, priority, task_id in tasks:
      actual_summary, actual_files = self.client.task_collect(task_id)
      t = tags[:] + [u'priority:%d' % priority]
      expected_summary = self.gen_expected(
          name=task_name, tags=sorted(t), outputs=[u'%d\n' % priority])
      self.assertResults(expected_summary, actual_summary)
      self.assertEqual(['summary.json'], actual_files.keys())
      results.append(
          (task_name, priority, task_id, actual_summary[u'shards'][0]))

    # Now assert that they ran in the expected order. started_ts encoding means
    # string sort is equivalent to timestamp sort.
    results.sort(key=lambda x: x[3][u'started_ts'])
    expected = ['2-p6', '3-p7', '1-p8', '4-p8', '0-p9']
    self.assertEqual(expected, [r[0] for r in results])

  def _run_isolated(self, hello_world, name, args, expected_summary,
      expected_files, deduped=False, isolated_content=None):
    """Runs hello_world.py as an isolated file."""
    # Shared code for all test_isolated_* test cases.
    tmpdir = tempfile.mkdtemp(prefix='swarming_smoke')
    try:
      isolate_path = os.path.join(tmpdir, 'i.isolate')
      isolated_path = os.path.join(tmpdir, 'i.isolated')
      with open(isolate_path, 'wb') as f:
        json.dump(isolated_content or ISOLATE_HELLO_WORLD, f)
      with open(os.path.join(tmpdir, 'hello_world.py'), 'wb') as f:
        f.write(hello_world)
      isolated_hash = self.client.isolate(isolate_path, isolated_path)
      task_id = self.client.task_trigger_isolated(
          name, isolated_hash, extra=args)
      actual_summary, actual_files = self.client.task_collect(task_id)
      self.assertResults(expected_summary, actual_summary, deduped=deduped)
      actual_files.pop('summary.json')
      self.assertEqual(expected_files, actual_files)
      return task_id
    finally:
      file_path.rmtree(tmpdir)

  def assertResults(self, expected, result, deduped=False):
    self.assertEqual([u'shards'], result.keys())
    self.assertEqual(1, len(result[u'shards']))
    self.assertTrue(result[u'shards'][0], result)
    result = result[u'shards'][0].copy()
    self.assertFalse(result.get(u'abandoned_ts'))
    # These are not deterministic (or I'm too lazy to calculate the value).
    if expected.get(u'performance_stats'):
      self.assertLess(
          0, result[u'performance_stats'].pop(u'bot_overhead'))
      self.assertLess(
          0,
          result[u'performance_stats'][u'isolated_download'].pop(u'duration'))
      self.assertLess(
          0, result[u'performance_stats'][u'isolated_upload'].pop(u'duration'))
      for k in (u'isolated_download', u'isolated_upload'):
        for j in (u'items_cold', u'items_hot'):
          result[u'performance_stats'][k][j] = large.unpack(
              base64.b64decode(result[u'performance_stats'][k].get(j, '')))
    else:
      perf_stats = result.pop(u'performance_stats', None)
      if perf_stats:
        # Ignore bot_overhead, everything else should be empty.
        perf_stats.pop(u'bot_overhead', None)
        self.assertFalse(perf_stats.pop(u'isolated_download', None))
        self.assertFalse(perf_stats.pop(u'isolated_upload', None))
        self.assertFalse(perf_stats)

    bot_version = result.pop(u'bot_version')
    self.assertTrue(bot_version)
    if result[u'costs_usd'] is not None:
      expected.pop(u'costs_usd', None)
      self.assertLess(0, result.pop(u'costs_usd'))
    if result[u'cost_saved_usd'] is not None:
      expected.pop(u'cost_saved_usd', None)
      self.assertLess(0, result.pop(u'cost_saved_usd'))
    self.assertTrue(result.pop(u'created_ts'))
    self.assertTrue(result.pop(u'completed_ts'))
    self.assertLess(0, result.pop(u'durations'))
    task_id = result.pop(u'id')
    run_id = result.pop(u'run_id')
    self.assertTrue(task_id)
    self.assertTrue(task_id.endswith('0'), task_id)
    if not deduped:
      self.assertEqual(task_id[:-1] + '1', run_id)
    self.assertTrue(result.pop(u'modified_ts'))
    self.assertTrue(result.pop(u'started_ts'))

    if getattr(expected.get(u'outputs'), 'match', None):
      expected_outputs = expected.pop(u'outputs')
      outputs = '\n'.join(result.pop(u'outputs'))
      self.assertTrue(
          expected_outputs.match(outputs),
          '%s does not match %s' % (outputs, expected_outputs.pattern))

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
  fix_encoding.fix_encoding()
  verbose = '-v' in sys.argv
  leak = bool('--leak' in sys.argv)
  if leak:
    # Note that --leak will not guarantee that 'c' and 'isolated_cache' are
    # kept. Only the last test case will leak these two directories.
    sys.argv.remove('--leak')
  if verbose:
    logging.basicConfig(level=logging.INFO)
    Test.maxDiff = None
  else:
    logging.basicConfig(level=logging.ERROR)

  # Force language to be English, otherwise the error messages differ from
  # expectations.
  os.environ['LANG'] = 'en_US.UTF-8'
  os.environ['LANGUAGE'] = 'en_US.UTF-8'

  # So that we don't get goofed up when running this test on swarming :)
  os.environ.pop('SWARMING_TASK_ID', None)
  os.environ.pop('SWARMING_SERVER', None)
  os.environ.pop('ISOLATE_SERVER', None)

  # Ensure ambient identity doesn't leak through during local smoke test.
  os.environ.pop('LUCI_CONTEXT', None)

  bot = None
  client = None
  servers = None
  failed = True
  try:
    servers = start_servers.LocalServers(False)
    servers.start()
    bot = start_bot.LocalBot(servers.swarming_server.url)
    Test.bot = bot
    bot.start()
    client = SwarmingClient(
        servers.swarming_server.url, servers.isolate_server.url)
    # Test cases only interract with the client; except for test_update_continue
    # which mutates the bot.
    Test.client = client
    Test.servers = servers
    failed = not unittest.main(exit=False).result.wasSuccessful()

    # Then try to terminate the bot sanely. After the terminate request
    # completed, the bot process should have terminated. Give it a few
    # seconds due to delay between sending the event that the process is
    # shutting down vs the process is shut down.
    if client.terminate(bot.bot_id) != 0:
      print >> sys.stderr, 'swarming.py terminate failed'
      failed = True
    try:
      bot.wait(10)
    except subprocess42.TimeoutExpired:
      print >> sys.stderr, 'Bot is still alive after swarming.py terminate'
      failed = True
  except KeyboardInterrupt:
    print >> sys.stderr, '<Ctrl-C>'
    failed = True
    if bot is not None and bot.poll() is None:
      bot.kill()
      bot.wait()
  finally:
    cleanup(bot, client, servers, failed or verbose, leak)
  return int(failed)


if __name__ == '__main__':
  sys.exit(main())
