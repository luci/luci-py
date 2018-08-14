#!/usr/bin/env python
# coding=utf-8
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Integration test for the Swarming server, Swarming bot and Swarming client.

It starts a Swarming server, Isolate server and a Swarming bot, then triggers
tasks with the Swarming client to ensure the system works end to end.
"""

import base64
import contextlib
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
from utils import fs
from utils import file_path
from utils import large
from utils import subprocess42
sys.path.pop(0)

sys.path.insert(0, BOT_DIR)
import test_env_bot
test_env_bot.setup_test_env()

from api import os_utilities


# Use a variable because it is corrupting my text editor from the 80s.
# One important thing to note is that this character U+1F310 is not in the BMP
# so it tests more accurately emojis like 💣 (U+1F4A3) and 💩(U+1F4A9).
HELLO_WORLD = u'hello_🌐'


# Signal as seen by the process.
SIGNAL_TERM = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM


# Timeout to wait for the operations, so that the smoke test doesn't hang
# indefinitely.
TIMEOUT_SECS = 20


# The default isolated command is to map and run HELLO_WORLD.
DEFAULT_ISOLATE_HELLO = """{
  "variables": {
    "command": ["python", "-u", "%(name)s.py"],
    "files": ["%(name)s.py"],
  },
}""" % {'name': HELLO_WORLD.encode('utf-8')}


def _script(content):
  """Deindents and encode to utf-8."""
  return textwrap.dedent(content.encode('utf-8'))


class SwarmingClient(object):
  def __init__(self, swarming_server, isolate_server, tmpdir):
    self._swarming_server = swarming_server
    self._isolate_server = isolate_server
    self._tmpdir = tmpdir
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
    h, tmp = tempfile.mkstemp(
        dir=self._tmpdir, prefix='trigger_raw', suffix='.json')
    os.close(h)
    cmd = [
      '--user', 'joe@localhost',
      '-d', 'pool', 'default',
      '--dump-json', tmp,
      '--raw-cmd',
    ]
    cmd.extend(args)
    assert not self._run('trigger', cmd), args
    with fs.open(tmp, 'rb') as f:
      data = json.load(f)
      task_id = data['tasks'].popitem()[1]['task_id']
      logging.debug('task_id = %s', task_id)
      return task_id

  def task_trigger_isolated(self, name, isolated_hash, extra=None):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(
        dir=self._tmpdir, prefix='trigger_isolated', suffix='.json')
    os.close(h)
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
    with fs.open(tmp, 'rb') as f:
      data = json.load(f)
      task_id = data['tasks'].popitem()[1]['task_id']
      logging.debug('task_id = %s', task_id)
      return task_id

  def task_trigger_post(self, request):
    """Triggers a task with the very raw RPC and returns the task id.

    It does an HTTP POST at tasks/new with the provided data in 'request'.
    """
    out = self._capture('post', ['tasks/new'], request)
    try:
      data = json.loads(out)
    except ValueError:
      logging.info('Data could not be decoded: %r', out)
      return None
    task_id = data['task_id']
    logging.debug('task_id = %s', task_id)
    return task_id

  def task_collect(self, task_id, timeout=TIMEOUT_SECS):
    """Collects the results for a task.

    Returns:
      - Result summary as saved by the tool
      - output files as a dict
    """
    tmp = os.path.join(self._tmpdir, task_id + '.json')
    tmpdir = unicode(os.path.join(self._tmpdir, task_id))
    if os.path.isdir(tmpdir):
      for i in xrange(100000):
        t = '%s_%d' % (tmpdir, i)
        if not os.path.isdir(t):
          tmpdir = t
          break
    os.mkdir(tmpdir)
    # swarming.py collect will return the exit code of the task.
    args = [
      '--task-summary-json', tmp, task_id, '--task-output-dir', tmpdir,
      '--timeout', str(timeout), '--perf',
    ]
    self._run('collect', args)
    with fs.open(tmp, 'rb') as f:
      data = f.read()
    try:
      summary = json.loads(data)
    except ValueError:
      print >> sys.stderr, 'Bad json:\n%s' % data
      raise
    outputs = {}
    for root, _, files in fs.walk(tmpdir):
      for i in files:
        p = os.path.join(root, i)
        name = p[len(tmpdir)+1:]
        with fs.open(p, 'rb') as f:
          outputs[name] = f.read()
    return summary, outputs

  def task_cancel(self, task_id, args):
    """Cancels a task."""
    return self._capture('cancel', list(args) + [str(task_id)], '') == ''

  def task_result(self, task_id):
    """Queries a task result without waiting for it to complete."""
    # collect --timeout 0 now works the same.
    return json.loads(self._capture('query', ['task/%s/result' % task_id], ''))

  def task_stdout(self, task_id):
    """Returns current task stdout without waiting for it to complete."""
    raw = self._capture('query', ['task/%s/stdout' % task_id], '')
    return json.loads(raw).get('output')

  def terminate(self, bot_id):
    task_id = self._capture('terminate', [bot_id], '').strip()
    logging.info('swarming.py terminate returned %r', task_id)
    if not task_id:
      return 1
    return self._run('collect', ['--timeout', str(TIMEOUT_SECS), task_id])

  def query_bot(self):
    """Returns the bot's properties."""
    raw = self._capture('query', ['bots/list', '--limit', '10'], '')
    data = json.loads(raw)
    if not data.get('items'):
      return None
    assert len(data['items']) == 1
    return data['items'][0]

  def dump_log(self):
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, 'Client calls'
    print >> sys.stderr, '-' * 60
    for i in xrange(self._index):
      with fs.open(os.path.join(self._tmpdir, 'client_%d.log' % i), 'rb') as f:
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
    name = os.path.join(self._tmpdir, u'client_%d.log' % self._index)
    self._index += 1
    cmd = [
      sys.executable, 'swarming.py', command, '-S', self._swarming_server,
      '--verbose',
    ] + args
    with fs.open(name, 'wb') as f:
      f.write('\nRunning: %s\n' % ' '.join(cmd))
      f.flush()
      p = subprocess42.Popen(
          cmd, stdout=f, stderr=subprocess42.STDOUT, cwd=CLIENT_DIR)
      p.communicate()
      return p.returncode

  def _capture(self, command, args, stdin):
    name = os.path.join(self._tmpdir, u'client_%d.log' % self._index)
    self._index += 1
    cmd = [
      sys.executable, 'swarming.py', command, '-S', self._swarming_server,
      '--log-file', name
    ] + args
    with fs.open(name, 'wb') as f:
      f.write('\nRunning: %s\n' % ' '.join(cmd))
    p = subprocess42.Popen(
        cmd, stdin=subprocess42.PIPE, stdout=subprocess42.PIPE, cwd=CLIENT_DIR)
    return p.communicate(stdin)[0]


def gen_expected(**kwargs):
  expected = {
    u'abandoned_ts': None,
    u'bot_dimensions': None,
    u'bot_id': unicode(socket.getfqdn().split('.', 1)[0]),
    u'children_task_ids': [],
    u'cost_saved_usd': None,
    u'current_task_slice': u'0',
    u'deduped_from': None,
    u'exit_codes': [0],
    u'failure': False,
    u'internal_failure': False,
    u'isolated_out': None,
    u'name': u'',
    u'outputs': [u'hi\n'],
    u'outputs_ref': None,
    u'server_versions': [u'N/A'],
    u'state': 0x70,  # task_result.State.COMPLETED.
    u'tags': [
      u'pool:default',
      u'priority:200',
      u'service_account:none',
      u'swarming.pool.template:none',
      u'swarming.pool.version:pools_cfg_rev',
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
  leak = False

  def setUp(self):
    super(Test, self).setUp()
    self.dimensions = os_utilities.get_dimensions()
    # The bot forcibly adds server_version.
    self.dimensions[u'server_version'] = [u'N/A']
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

  def test_raw_bytes_and_dupe_dimensions(self):
    # A string of a letter 'A', UTF-8 BOM then UTF-16 BOM then UTF-EDBCDIC then
    # invalid UTF-8 and the letter 'B'. It is double escaped so it can be passed
    # down the shell.
    #
    # Leverage the occasion to also specify the same dimension key twice with
    # different values, and ensure it works.
    invalid_bytes = 'A\\xEF\\xBB\\xBF\\xFE\\xFF\\xDD\\x73\\x66\\x73\\xc3\\x28B'
    args = [
      '-T', 'non_utf8',
      # It's pretty much guaranteed 'os' has at least two values.
      '-d', 'os', self.dimensions['os'][0],
      '-d', 'os', self.dimensions['os'][1],
      '--',
      'python', '-u', '-c', 'print(\'' + invalid_bytes + '\')',
    ]
    summary = self.gen_expected(
        name=u'non_utf8',
        # The string is mostly converted to 'Replacement Character'.
        outputs=[u'A\ufeff\ufffd\ufffd\ufffdsfs\ufffd(B\n'],
        tags=sorted([
          u'os:' + self.dimensions['os'][0],
          u'os:' + self.dimensions['os'][1],
          u'pool:default',
          u'priority:200',
          u'service_account:none',
          u'swarming.pool.template:none',
          u'swarming.pool.version:pools_cfg_rev',
          u'user:joe@localhost',
        ]))
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
    def get_cmd(name, exit_code):
      return [
        '-T', name, '--',
        'python', '-u', '-c',
        'import sys; print(\'hi\'); sys.exit(%d)' % exit_code,
      ]
    # tuple(task_request, expectation)
    tasks = [
      (
        get_cmd('simple_success', 0),
        (self.gen_expected(name=u'simple_success'), {}),
      ),
      (
        get_cmd('simple_failure', 1),
        (
          self.gen_expected(
              name=u'simple_failure', exit_codes=[1], failure=True),
          {},
        ),
      ),
      (
        get_cmd('ending_simple_success', 0),
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
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        # coding=utf-8
        import os
        import sys
        print('hi')
        assert os.environ.get("SWARMING_TASK_ID")
        with open(os.path.join(sys.argv[1], u'💣.txt'), 'wb') as f:
          f.write('test_isolated')
        """),
    }
    isolated_out = self._out(u'f067c9cf13dcc90f2fe269499d44082150876126')
    items_in = [sum(len(c) for c in content.itervalues()), 214]
    items_out = [len('test_isolated'), 125]
    expected_summary = self.gen_expected(
        name=u'isolated_task',
        isolated_out=isolated_out,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'total_bytes_items_cold': unicode(sum(items_in)),
          },
          u'isolated_upload': {
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'total_bytes_items_cold': unicode(sum(items_out)),
          },
        },
        outputs=[u'hi\n'],
        outputs_ref=isolated_out)
    expected_files = {
      os.path.join(u'0', u'💣.txt'.encode('utf-8')): 'test_isolated',
    }
    self._run_isolated(
        content, 'isolated_task', ['--', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files, deduped=False,
        isolate_content=DEFAULT_ISOLATE_HELLO)

  def test_isolated_command(self):
    # Command is specified in Swarming task, still with isolated file.
    # Confirms that --relative-cwd, --env and --env-prefix work.
    content = {
      os.path.join(u'base', HELLO_WORLD + u'.py'): _script(u"""
        # coding=utf-8
        import os
        import sys
        print(sys.argv[1])
        assert "SWARMING_TASK_ID" not in os.environ
        # cwd is in base/
        cwd = os.path.realpath(os.getcwd())
        base = os.path.basename(cwd)
        assert base == 'base', base
        cwd = os.path.dirname(cwd)
        path = os.environ["PATH"].split(os.pathsep)
        print(os.path.realpath(path[0]).replace(cwd, "$CWD"))
        with open(os.path.join(sys.argv[2], 'FOO.txt'), 'wb') as f:
          f.write(os.environ["FOO"])
        with open(os.path.join(sys.argv[2], 'result.txt'), 'wb') as f:
          f.write('hey2')
        """),
    }
    isolated_out = self._out(u'fc04fe5eee668c35c81db590a76c0da9cbcdae90')
    items_in = [sum(len(c) for c in content.itervalues()), 150]
    items_out = [len('hey2'), len(u'bar💩'.encode('utf-8')), 191]
    expected_summary = self.gen_expected(
        name=u'separate_cmd',
        isolated_out=isolated_out,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'total_bytes_items_cold': unicode(sum(items_in)),
          },
          u'isolated_upload': {
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'total_bytes_items_cold': unicode(sum(items_out)),
          },
        },
        outputs=[u'hi💩\n%s\n' % os.sep.join(['$CWD', 'local', 'path'])],
        outputs_ref=isolated_out)
    expected_files = {
      os.path.join('0', 'result.txt'): 'hey2',
      os.path.join('0', 'FOO.txt'): u'bar💩'.encode('utf-8'),
    }
    # Use a --raw-cmd instead of a command in the isolated file. This is the
    # future!
    isolate_content = '{"variables": {"files": ["%s"]}}' % os.path.join(
        u'base', HELLO_WORLD + u'.py').encode( 'utf-8')
    self._run_isolated(
        content, 'separate_cmd',
        ['--raw-cmd',
         '--relative-cwd', 'base',
         '--env', 'FOO', u'bar💩',
         '--env', 'SWARMING_TASK_ID', '',
         '--env-prefix', 'PATH', 'local/path',
         '--', 'python', HELLO_WORLD + u'.py', u'hi💩',
         '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files, deduped=False,
        isolate_content=isolate_content)

  def test_isolated_hard_timeout(self):
    # Make an isolated file, archive it, have it time out. Similar to
    # test_hard_timeout. The script doesn't handle signal so it failed the grace
    # period.
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        import os
        import sys
        import time
        sys.stdout.write('hi\\n')
        sys.stdout.flush()
        time.sleep(120)
        with open(os.path.join(sys.argv[1], 'result.txt'), 'wb') as f:
          f.write('test_isolated_hard_timeout')
        """),
    }
    items_in = [sum(len(c) for c in content.itervalues()), 214]
    expected_summary = self.gen_expected(
        name=u'isolated_hard_timeout',
        exit_codes=[SIGNAL_TERM],
        failure=True,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'total_bytes_items_cold': unicode(sum(items_in)),
          },
          u'isolated_upload': {
            u'items_cold': [],
            u'items_hot': [],
          },
        },
        state=0x40)  # task_result.State.TIMED_OUT
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    self._run_isolated(
        content, 'isolated_hard_timeout',
        ['--hard-timeout', '1', '--', '${ISOLATED_OUTDIR}'],
        expected_summary, {}, deduped=False,
        isolate_content=DEFAULT_ISOLATE_HELLO)

  def test_isolated_hard_timeout_grace(self):
    # Make an isolated file, archive it, have it time out. Similar to
    # test_hard_timeout. The script handles signal so it send results back.
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        import os
        import signal
        import sys
        import threading
        bit = threading.Event()
        def handler(signum, _):
          bit.set()
          sys.stdout.write('got signal %%d\\n' %% signum)
          sys.stdout.flush()
        signal.signal(signal.%s, handler)
        sys.stdout.write('hi\\n')
        sys.stdout.flush()
        while not bit.wait(0.01):
          pass
        with open(os.path.join(sys.argv[1], 'result.txt'), 'wb') as f:
          f.write('test_isolated_hard_timeout_grace')
        """ % ('SIGBREAK' if sys.platform == 'win32' else 'SIGTERM')
        ),
    }
    isolated_out = self._out(u'53b4ab0562f05135e20ec91b473e5ca2282edc2b')
    items_in = [sum(len(c) for c in content.itervalues()), 214]
    items_out = [len('test_isolated_hard_timeout_grace'), 119]
    expected_summary = self.gen_expected(
        name=u'isolated_hard_timeout_grace',
        isolated_out=isolated_out,
        performance_stats={
          u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'total_bytes_items_cold': unicode(sum(items_in)),
          },
          u'isolated_upload': {
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'total_bytes_items_cold': unicode(sum(items_out)),
          },
        },
        outputs=[u'hi\ngot signal 15\n'],
        outputs_ref=isolated_out,
        failure=True,
        state=0x40)  # task_result.State.TIMED_OUT
    expected_files = {
      os.path.join('0', 'result.txt'): 'test_isolated_hard_timeout_grace',
    }
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    self._run_isolated(
        content, 'isolated_hard_timeout_grace',
        ['--hard-timeout', '1', '--', '${ISOLATED_OUTDIR}'],
        expected_summary, expected_files, deduped=False,
        isolate_content=DEFAULT_ISOLATE_HELLO)

  def test_idempotent_reuse(self):
    content = {HELLO_WORLD + u'.py': 'print "hi"\n'}
    items_in = [sum(len(c) for c in content.itervalues()), 213]
    expected_summary = self.gen_expected(
      name=u'idempotent_reuse',
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sorted(items_in),
          u'items_hot': [],
          u'num_items_cold': unicode(len(items_in)),
          u'total_bytes_items_cold': unicode(sum(items_in)),
        },
        u'isolated_upload': {
          u'items_cold': [],
          u'items_hot': [],
        },
      },
    )
    task_id = self._run_isolated(
        content, 'idempotent_reuse', ['--idempotent'], expected_summary, {},
        deduped=False, isolate_content=DEFAULT_ISOLATE_HELLO)

    # The task name changes, there's a bit less data but the rest is the same.
    expected_summary[u'name'] = u'idempotent_reuse2'
    expected_summary[u'costs_usd'] = None
    expected_summary.pop('performance_stats')
    expected_summary[u'cost_saved_usd'] = 0.02
    expected_summary[u'deduped_from'] = task_id[:-1] + '1'
    expected_summary[u'try_number'] = 0
    self._run_isolated(
        content, 'idempotent_reuse2', ['--idempotent'], expected_summary, {},
        deduped=True, isolate_content=DEFAULT_ISOLATE_HELLO)

  def test_secret_bytes(self):
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        import sys
        import os
        import json

        print "hi"

        with open(os.environ['LUCI_CONTEXT'], 'r') as f:
          data = json.load(f)

        with open(os.path.join(sys.argv[1], 'sekret'), 'w') as f:
          print >> f, data['swarming']['secret_bytes'].decode('base64')
      """),
    }
    isolated_out = self._out(u'd2eca4d860e4f1728272f6a736fd1c9ac6e98c4f')
    items_in = [sum(len(c) for c in content.itervalues()), 214]
    items_out = [len('foobar\n'), 114]
    expected_summary = self.gen_expected(
      name=u'secret_bytes',
      isolated_out=isolated_out,
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sorted(items_in),
          u'items_hot': [],
          u'num_items_cold': unicode(len(items_in)),
          u'total_bytes_items_cold': unicode(sum(items_in)),
        },
        u'isolated_upload': {
          u'items_cold': sorted(items_out),
          u'items_hot': [],
          u'num_items_cold': unicode(len(items_out)),
          u'total_bytes_items_cold': unicode(sum(items_out)),
        },
      },
      outputs_ref=isolated_out,
    )
    tmp = os.path.join(self.tmpdir, 'test_secret_bytes')
    with fs.open(tmp, 'wb') as f:
      f.write('foobar')
    self._run_isolated(
        content, 'secret_bytes',
        ['--secret-bytes-path', tmp, '--', '${ISOLATED_OUTDIR}'],
        expected_summary, {os.path.join('0', 'sekret'): 'foobar\n'},
        deduped=False, isolate_content=DEFAULT_ISOLATE_HELLO)

  def test_local_cache(self):
    # First task creates the cache, second copy the content to the output
    # directory. Each time it's the exact same script.
    dimensions = {
        i['key']: i['value'] for i in self.client.query_bot()['dimensions']}
    self.assertEqual(set(self.dimensions), set(dimensions))
    self.assertNotIn(u'cache', set(dimensions))
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        import os, shutil, sys
        p = "p/b/a.txt"
        if not os.path.isfile(p):
          with open(p, "wb") as f:
            f.write("Yo!")
        else:
          shutil.copy(p, sys.argv[1])
        print "hi"
        """),
    }
    items_in = [sum(len(c) for c in content.itervalues()), 214]
    expected_summary = self.gen_expected(
      name=u'cache_first',
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': u'0',
          u'initial_size': u'0',
          u'items_cold': sorted(items_in),
          u'items_hot': [],
          u'num_items_cold': unicode(len(items_in)),
          u'total_bytes_items_cold': unicode(sum(items_in)),
        },
        u'isolated_upload': {
          u'items_cold': [],
          u'items_hot': [],
        },
      },
    )
    self._run_isolated(
        content, 'cache_first',
        ['--named-cache', 'fuu', 'p/b', '--', '${ISOLATED_OUTDIR}/yo'],
        expected_summary, {}, deduped=False,
        isolate_content=DEFAULT_ISOLATE_HELLO)

    # Second run with a cache available.
    isolated_out = self._out(u'63fc667fd217ebabdf60ca143fe25998b5ea5c77')
    items_out = [3, 110]
    expected_summary = self.gen_expected(
      name=u'cache_second',
      isolated_out=isolated_out,
      outputs_ref=isolated_out,
      performance_stats={
        u'isolated_download': {
          u'initial_number_items': unicode(len(items_in)),
          u'initial_size': unicode(sum(items_in)),
          u'items_cold': [],
          # Items are hot.
          u'items_hot': items_in,
          u'num_items_hot': unicode(len(items_in)),
          u'total_bytes_items_hot': unicode(sum(items_in)),
        },
        u'isolated_upload': {
          u'items_cold': sorted(items_out),
          u'items_hot': [],
          u'num_items_cold': unicode(len(items_out)),
          u'total_bytes_items_cold': unicode(sum(items_out)),
        },
      },
    )
    # The previous task caused the bot to have a named cache.
    expected_summary['bot_dimensions'] = (
        expected_summary['bot_dimensions'].copy())
    expected_summary['bot_dimensions'][u'caches'] = [u'fuu']
    self._run_isolated(
        content, 'cache_second',
        ['--named-cache', 'fuu', 'p/b', '--', '${ISOLATED_OUTDIR}/yo'],
        expected_summary,
        {'0/yo': 'Yo!'}, deduped=False, isolate_content=DEFAULT_ISOLATE_HELLO)

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
    # List of tuple(task_name, priority, task_id).
    tasks = []
    tags = [
      u'pool:default',
      u'service_account:none',
      u'swarming.pool.template:none',
      u'swarming.pool.version:pools_cfg_rev',
      u'user:joe@localhost',
    ]
    with self._make_wait_task('test_priority'):
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
      # Ensure the tasks under test are pending.
      for task_name, priority, task_id in tasks:
        result = self.client.task_result(task_id)
      self.assertEqual(u'PENDING', result[u'state'], result)

    # The wait task is done. This will cause all the pending tasks to be run.
    # Now, will they be run in the expected order? That is the question!
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

  def test_cancel_pending(self):
    # Cancel a pending task. Triggering a task for an unknown dimension will
    # result in state NO_RESOURCE, so instead a dummy task is created to make
    # sure the bot cannot reap the second one.
    args = [
      '-T', 'cancel_pending',
      '--', 'python', '-u', '-c', 'print(\'hi\')'
    ]
    with self._make_wait_task('test_cancel_pending'):
      task_id = self.client.task_trigger_raw(args)
      actual, _ = self.client.task_collect(task_id, timeout=-1)
      self.assertEqual(
          0x20,  # task_result.PENDING
          actual[u'shards'][0][u'state'])
      self.assertTrue(self.client.task_cancel(task_id, []))
    actual, _ = self.client.task_collect(task_id, timeout=-1)
    self.assertEqual(
        0x60,  # task_result.State.CANCELED
        actual[u'shards'][0][u'state'], actual[u'shards'][0])

  def test_cancel_running(self):
    # Cancel a running task. Make sure the target process handles the signal
    # well with a graceful termination via SIGTERM.
    content = {
      HELLO_WORLD + u'.py': _script(u"""
        import signal
        import sys
        import threading
        bit = threading.Event()
        def handler(signum, _):
          bit.set()
          sys.stdout.write('got signal %%d\\n' %% signum)
          sys.stdout.flush()
        signal.signal(signal.%s, handler)
        sys.stdout.write('hi\\n')
        sys.stdout.flush()
        while not bit.wait(0.01):
          pass
        sys.exit(23)
        """) % ('SIGBREAK' if sys.platform == 'win32' else 'SIGTERM'),
    }
    task_id = self._start_isolated(
        content, 'cancel_running', ['--', '${ISOLATED_OUTDIR}'],
        isolate_content=DEFAULT_ISOLATE_HELLO)

    # Wait for the task to start on the bot.
    self._wait_for_state(task_id, u'PENDING', u'RUNNING')
    # Make sure the signal handler is set by looking that 'hi' was printed,
    # otherwise it could continue looping.
    start = time.time()
    out = None
    while time.time() - start < 45.:
      out = self.client.task_stdout(task_id)
      if out == 'hi\n':
        break
    self.assertEqual(out, 'hi\n')

    # Cancel it.
    self.assertTrue(self.client.task_cancel(task_id, ['--kill-running']))

    result = self._wait_for_state(task_id, u'RUNNING', u'KILLED')
    # Make sure the exit code is what the script returned, which means the
    # script in fact did handle the SIGTERM and returned properly.
    self.assertEqual(u'23', result[u'exit_code'])

  def test_task_slice(self):
    request = {
      'name': 'task_slice',
      'priority': 40,
      'task_slices': [
        {
          'expiration_secs': 120,
          'properties': {
            'command': ['python', '-c', 'print("first")'],
            'dimensions': [
              {
                'key': 'pool',
                'value': 'default',
              },
            ],
            'grace_period_secs': 30,
            'execution_timeout_secs': 30,
            'io_timeout_secs': 30,
          },
        },
        {
          'expiration_secs': 120,
          'properties': {
            'command': ['python', '-c', 'print("second")'],
            'dimensions': [
              {
                'key': 'pool',
                'value': 'default',
              },
            ],
            'grace_period_secs': 30,
            'execution_timeout_secs': 30,
            'io_timeout_secs': 30,
          },
        },
      ],
    }
    task_id = self.client.task_trigger_post(json.dumps(request))
    expected_summary = self.gen_expected(
        name=u'task_slice',
        outputs=[u'first\n'],
        tags=[
          u'pool:default',
          u'priority:40',
          u'service_account:none',
          u'swarming.pool.template:none',
          u'swarming.pool.version:pools_cfg_rev',
          u'user:None',
        ],
        user=u'')
    actual_summary, _ = self.client.task_collect(task_id)
    self.assertResults(expected_summary, actual_summary, deduped=False)

  def test_task_slice_fallback(self):
    # The first one shall be skipped.
    request = {
      'name': 'task_slice_fallback',
      'priority': 40,
      'task_slices': [
        {
          # Really long expiration that would cause the smoke test to abort
          # under normal condition.
          'expiration_secs': 1200,
          'properties': {
            'command': ['python', '-c', 'print("first")'],
            'dimensions': [
              {
                'key': 'pool',
                'value': 'default',
              },
              {
                'key': 'invalidkey',
                'value': 'invalidvalue',
              },
            ],
            'grace_period_secs': 30,
            'execution_timeout_secs': 30,
            'io_timeout_secs': 30,
          },
        },
        {
          'expiration_secs': 120,
          'properties': {
            'command': ['python', '-c', 'print("second")'],
            'dimensions': [
              {
                'key': 'pool',
                'value': 'default',
              },
            ],
            'grace_period_secs': 30,
            'execution_timeout_secs': 30,
            'io_timeout_secs': 30,
          },
        },
      ],
    }
    task_id = self.client.task_trigger_post(json.dumps(request))
    expected_summary = self.gen_expected(
        name=u'task_slice_fallback',
        current_task_slice=u'1',
        outputs=[u'second\n'],
        tags=[
          # Bug!
          u'invalidkey:invalidvalue',
          u'pool:default',
          u'priority:40',
          u'service_account:none',
          u'swarming.pool.template:none',
          u'swarming.pool.version:pools_cfg_rev',
          u'user:None',
        ],
        user=u'')
    actual_summary, _ = self.client.task_collect(task_id)
    self.assertResults(expected_summary, actual_summary, deduped=False)

  def test_no_resource(self):
    request = {
      'name': 'no_resource',
      'priority': 40,
      'task_slices': [
        {
          # Really long expiration that would cause the smoke test to abort
          # under normal conditions.
          'expiration_secs': 1200,
          'properties': {
            'command': ['python', '-c', 'print("hello")'],
            'dimensions': [
              {
                'key': 'pool',
                'value': 'default',
              },
              {
                'key': 'bad_key',
                'value': 'bad_value',
              },
            ],
            'grace_period_secs': 30,
            'execution_timeout_secs': 30,
            'io_timeout_secs': 30,
          },
        },
      ],
    }
    task_id = self.client.task_trigger_post(json.dumps(request))
    actual_summary, _ = self.client.task_collect(task_id)
    summary = actual_summary[u'shards'][0]
    # Immediately cancelled.
    self.assertEqual(
        0x100,  # task_result.State.NO_RESOURCE
        summary[u'state'])
    self.assertTrue(summary[u'abandoned_ts'])

  @contextlib.contextmanager
  def _make_wait_task(self, name):
    """Creates a dummy task that keeps the bot busy, while other things are
    being done.
    """
    signal_file = os.path.join(self.tmpdir, name)
    fs.open(signal_file, 'wb').close()
    args = [
      '-T', 'wait', '--priority', '20', '--',
      'python', '-u', '-c',
      # Cheezy wait.
      ('import os,time;'
        'print(\'hi\');'
        '[time.sleep(0.1) for _ in xrange(100000) if os.path.exists(\'%s\')];'
        'print(\'hi again\')') % signal_file,
    ]
    wait_task_id = self.client.task_trigger_raw(args)
    # Assert that the 'wait' task has started but not completed, otherwise
    # this defeats the purpose.
    self._wait_for_state(wait_task_id, u'PENDING', u'RUNNING')
    yield
    # Double check.
    result = self.client.task_result(wait_task_id)
    self.assertEqual(u'RUNNING', result[u'state'], result)
    # Unblock the wait_task_id on the bot.
    os.remove(signal_file)
    # Ensure the initial wait task is completed.
    actual_summary, actual_files = self.client.task_collect(wait_task_id)
    tags = [
      u'pool:default',
      u'priority:20',
      u'service_account:none',
      u'swarming.pool.template:none',
      u'swarming.pool.version:pools_cfg_rev',
      u'user:joe@localhost',
    ]
    self.assertResults(
        self.gen_expected(
            name=u'wait', tags=tags, outputs=['hi\nhi again\n']),
        actual_summary)
    self.assertEqual(['summary.json'], actual_files.keys())

  def _run_isolated(
      self, contents, name, args, expected_summary, expected_files,
      deduped, isolate_content):
    """Runs a python script as an isolated file."""
    task_id = self._start_isolated(contents, name, args, isolate_content)
    actual_summary, actual_files = self.client.task_collect(task_id)
    self.assertResults(expected_summary, actual_summary, deduped=deduped)
    actual_files.pop('summary.json')
    self.assertEqual(expected_files, actual_files)
    return task_id

  def _start_isolated(self, contents, name, args, isolate_content):
    # Shared code for all test_isolated_* test cases.
    root = os.path.join(self.tmpdir, name)
    # Refuse reusing the same task name twice, it makes the whole test suite
    # more manageable.
    self.assertFalse(os.path.isdir(root), root)
    os.mkdir(root)
    isolate_path = os.path.join(root, 'i.isolate')
    isolated_path = os.path.join(root, 'i.isolated')
    with fs.open(isolate_path, 'wb') as f:
      f.write(isolate_content)
    for relpath, content in contents.iteritems():
      p = os.path.join(root, relpath)
      d = os.path.dirname(p)
      if not os.path.isdir(d):
        os.makedirs(d)
      with fs.open(p, 'wb') as f:
        f.write(content)
    isolated_hash = self.client.isolate(isolate_path, isolated_path)
    return self.client.task_trigger_isolated(
        name, isolated_hash, extra=args)

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
        self.assertEqual(perf_stats.get(u'isolated_download', {}).pop(
            u'initial_number_items'), u'0')
        self.assertEqual(perf_stats.get(u'isolated_download', {}).pop(
            u'initial_size'), u'0')
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

  def _wait_for_state(self, task_id, current, new):
    """Waits for the task to start on the bot."""
    state = result = None
    start = time.time()
    # 45 seconds is a long time.
    # TODO(maruel): Make task_runner use exponential backoff instead of
    # hardcoded 10s/30s, which makes these tests tremendously slower than
    # necessary.
    # https://crbug.com/825500
    while time.time() - start < 45.:
      result = self.client.task_result(task_id)
      state = result[u'state']
      if state == new:
        break
      self.assertEqual(current, state, result)
      time.sleep(0.01)
    self.assertEqual(new, state, result)
    return result

  def _out(self, isolated_hash):
    return {
      u'isolated': isolated_hash,
      u'isolatedserver': unicode(self.servers.isolate_server.url),
      u'namespace': u'default-gzip',
      u'view_url': u'%s/browse?namespace=default-gzip&hash=%s' %
          (self.servers.isolate_server.url, isolated_hash),
    }


def cleanup(bot, client, servers, print_all):
  """Kills bot, kills server, print logs if failed, delete tmpdir."""
  try:
    if bot:
      bot.stop()
  finally:
    if servers:
      servers.stop()
  if print_all:
    if bot:
      bot.dump_log()
    if servers:
      servers.dump_log()
    if client:
      client.dump_log()
  if not Test.leak:
    file_path.rmtree(Test.tmpdir)


def main():
  fix_encoding.fix_encoding()
  verbose = '-v' in sys.argv
  Test.leak = bool('--leak' in sys.argv)
  Test.tmpdir = unicode(tempfile.mkdtemp(prefix='local_smoke_test'))
  if Test.leak:
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
    servers = start_servers.LocalServers(False, Test.tmpdir)
    servers.start()
    botdir = os.path.join(Test.tmpdir, 'bot')
    os.mkdir(botdir)
    bot = start_bot.LocalBot(servers.swarming_server.url, True, botdir)
    Test.bot = bot
    bot.start()
    client = SwarmingClient(
        servers.swarming_server.url, servers.isolate_server.url, Test.tmpdir)
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
    cleanup(bot, client, servers, failed or verbose)
  return int(failed)


if __name__ == '__main__':
  sys.exit(main())
