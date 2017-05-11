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
  u'isolated': u'0616f86b24065a0595e58088925567ae54a8157c',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=0616f86b24065a0595e58088925567ae54a8157c',
}

RESULT_HEY2_OUTPUTS_REF = {
  u'isolated': u'0616f86b24065a0595e58088925567ae54a8157c',
  u'isolatedserver': u'http://localhost:10050',
  u'namespace': u'default-gzip',
  u'view_url':
    u'http://localhost:10050/browse?namespace=default-gzip'
    '&hash=0616f86b24065a0595e58088925567ae54a8157c',
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
          '--timeout', '20', '--perf',
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

  def terminate(self, bot_id):
    return self._run('terminate', ['--wait', bot_id])

  def cleanup(self):
    if self._tmpdir:
      file_path.rmtree(self._tmpdir)
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
  dimensions = None
  servers = None
  bot = None

  @classmethod
  def setUpClass(cls):
    cls.dimensions = os_utilities.get_dimensions()

  def setUp(self):
    super(Test, self).setUp()
    # Reset the bot's cache at the start of each task, so that the cache reuse
    # data becomes deterministic.
    # Main caveat is 'isolated_upload' as the isolate server is not cleared.
    self.bot.wipe_cache()

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
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([len(hello_world), 200]),
            u'items_hot': [],
          },
          u'isolated_upload': {
            u'items_cold': [3, 118],
            u'items_hot': [],
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
    hello_world = '\n'.join((
        'import os',
        'import sys',
        'print(\'hi\')',
        'with open(os.path.join(sys.argv[1], \'result.txt\'), \'wb\') as f:',
        '  f.write(\'hey2\')'))
    expected_summary = self.gen_expected(
        name=u'separate_cmd',
        isolated_out=RESULT_HEY2_ISOLATED_OUT,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted([len(hello_world), 157]),
            u'items_hot': [],
          },
          u'isolated_upload': {
            u'items_cold': [4, 118],
            u'items_hot': [],
          },
        },
        outputs=[u'hi\n'],
        outputs_ref=RESULT_HEY2_OUTPUTS_REF)
    expected_files = {os.path.join('0', 'result.txt'): 'hey2'}
    self._run_isolated(
        hello_world, 'separate_cmd',
        ['--raw-cmd', '--', 'python', 'hello_world.py', '${ISOLATED_OUTDIR}'],
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
          },
          u'isolated_upload': {
            u'items_cold': [],
            u'items_hot': [3, 118],
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
        },
        u'isolated_upload': {
          u'items_cold': [],
          u'items_hot': [],
        },
      },
      properties_hash =
          u'082928de84d0a65839d227dcea2f5a947898929c77c1602b68c46d7d4588c1f5',
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
        },
        u'isolated_upload': {
          u'items_cold': [7, 114],
          u'items_hot': [],
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
        },
        u'isolated_upload': {
          u'items_cold': [3, 110],
          u'items_hot': [],
        },
      },
    )
    self._run_isolated(
        script, 'cache_second',
        ['--named-cache', 'fuu', 'p/b', '--', '${ISOLATED_OUTDIR}/yo'],
        expected_summary,
        {'0/yo': 'Yo!'})

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
    self.assertTrue(result[u'shards'][0])
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
    if client.terminate(bot.bot_id) is not 0:
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
