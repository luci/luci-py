#!/usr/bin/env vpython
# coding=utf-8
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Integration test for the Swarming server, bot and client and Isolate server
and client.

It starts a Swarming server, Isolate server and a Swarming bot, then triggers
tasks with the Swarming client to ensure the system works end to end.
"""

from __future__ import print_function

import argparse
import base64
import contextlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

APP_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(APP_DIR, 'swarming_bot')
LUCI_DIR = os.path.dirname(os.path.dirname(APP_DIR))
CLIENT_DIR = os.path.join(LUCI_DIR, 'client')
sys.path.insert(0, CLIENT_DIR)
sys.path.insert(0, os.path.join(CLIENT_DIR, 'tests'))
sys.path.insert(0, os.path.join(CLIENT_DIR, 'third_party'))

# client/
from utils import large

# client/tests
import cas_util

# client/third_party/
from depot_tools import fix_encoding
sys.path.pop(0)
sys.path.pop(0)

sys.path.insert(0, BOT_DIR)
import test_env_bot
test_env_bot.setup_test_env()

# swarming/
sys.path.insert(0, APP_DIR)
from tools import start_bot
from tools import start_servers


CAS_CLI = os.path.join(LUCI_DIR, 'luci-go', 'cas')
ISOLATE_CLI = os.path.join(LUCI_DIR, 'luci-go', 'isolate')
SWARMING_CLI = os.path.join(LUCI_DIR, 'luci-go', 'swarming')

# Use a variable because it is corrupting my text editor from the 80s.
# One important thing to note is that this character U+1F310 is not in the BMP
# so it tests more accurately emojis like 💣 (U+1F4A3) and 💩(U+1F4A9).
HELLO_WORLD = u'hello_🌐'

# Signal as seen by the process.
SIGNAL_TERM = -1073741510 if sys.platform == 'win32' else -signal.SIGTERM

# Timeout to wait for the operations, so that the smoke test doesn't hang
# indefinitely.
TIMEOUT_SECS = 20

DEFAULT_COMMAND = ["python", "-u", "%s.py" % HELLO_WORLD]

# The default isolated command is to map and run HELLO_WORLD.
DEFAULT_ISOLATE_HELLO = """{
  'variables': {
    'files': ['%(name)s.py'],
  },
}""" % {
    'name': HELLO_WORLD.encode('utf-8')
}


def _script(content):
  """Deindents and encode to utf-8."""
  return textwrap.dedent(content.encode('utf-8'))


class SwarmingClient(object):

  def __init__(self, swarming_server, cas_addr, tmpdir):
    self._swarming_server = swarming_server
    self._cas_addr = cas_addr
    self._tmpdir = tmpdir
    self._index = 0

  def ensure_logged_out(self):
    assert not self._run_swarming('logout', []), 'swarming logout failed'

  def isolate(self, isolate_path):
    """Archives a .isolate file into the isolate server and returns the isolated
    hash.
    """
    dump_json = os.path.join(self._tmpdir, 'digest.json')
    cmd = [
        ISOLATE_CLI,
        'archive',
        '-cas-addr',
        self._cas_addr,
        '-i',
        isolate_path,
        '-dump-json',
        dump_json,
        '-log-level',
        'debug',
    ]
    logging.debug('SwarmingClient.isolate: executing command. %s', cmd)
    with open(self._rotate_logfile(), 'wb') as f:
      f.write('\nRunning: %s\n' % ' '.join(cmd))
      p = subprocess.Popen(cmd, stdout=f, stderr=f)
      p.communicate()
    assert p.returncode == 0, ('Failed to isolate files. exit_code=%d, cmd=%s' %
                               (p.returncode, cmd))
    with open(dump_json) as f:
      data = json.load(f)
    digest = data.values()[0]
    logging.debug('CAS digest = %s', digest)
    return digest

  def task_trigger(self, args):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(
        dir=self._tmpdir, prefix='trigger_raw', suffix='.json')
    os.close(h)
    cmd = [
        '-user',
        'joe@localhost',
        '-d',
        'pool=default',
        '-dump-json',
        tmp,
    ]
    cmd.extend(args)
    assert not self._run_swarming('trigger',
                                  cmd), 'Failed to trigger a task. cmd=%s' % cmd
    with open(tmp, 'rb') as f:
      data = json.load(f)
      task_id = data['tasks'][0]['task_id']
      logging.debug('task_id = %s', task_id)
      return task_id

  def task_trigger_with_cas(self, digest, name, extra):
    """Triggers a task and return the task id."""
    h, tmp = tempfile.mkstemp(dir=self._tmpdir,
                              prefix='trigger_with_cas',
                              suffix='.json')
    os.close(h)
    cmd = [
        '-user',
        'joe@localhost',
        '-d',
        'pool=default',
        '-dump-json',
        tmp,
        '-task-name',
        name,
        '-cas-instance',
        'projects/test/instances/default_instance',
        '-digest',
        digest,
    ]
    if extra:
      cmd.extend(extra)
    assert not self._run_swarming('trigger', cmd)
    with open(tmp, 'rb') as f:
      data = json.load(f)
    task_id = data['tasks'][0]['task_id']
    logging.debug('task_id = %s', task_id)
    return task_id

  def task_trigger_raw(self, request):
    """Triggers a task with the very raw RPC and returns the task id.
    """
    input_json = os.path.join(self._tmpdir, 'task_trigger_raw_input.json')
    with open(input_json, 'wb') as f:
      json.dump({'requests': [request]}, f)
    output_json = os.path.join(self._tmpdir, 'task_trigger_raw_output.json')
    args = [
        '-json-input',
        input_json,
        '-json-output',
        output_json,
    ]
    ret = self._run_swarming('spawn-tasks', args)
    assert ret == 0, 'Failed to spawn a task. exit_code=%d, args=%s' % (ret,
                                                                        args)
    with open(output_json, 'rb') as f:
      data = json.load(f)
      task_id = data['tasks'][0]['task_id']
      logging.debug('task_id = %s', task_id)
      return task_id

  def task_collect(self,
                   task_id,
                   timeout=TIMEOUT_SECS,
                   wait=True,
                   include_output=True):
    """Collects the results for a task.

    Returns:
      - Result summary as saved by the tool
      - output files as a dict
    """
    tmp = os.path.join(self._tmpdir, task_id + '.json')
    # swarming collect will return the exit code of the task.
    args = [
        '-cas-addr',
        self._cas_addr,
        '-task-summary-json',
        tmp,
        '-task-output-stdout',
        'json' if include_output else 'none',
        '-output-dir',
        self._tmpdir,
        '-perf',
        '-task-summary-python',
    ]
    if wait:
      args += [
          '-timeout',
          '%ds' % timeout,
      ]
    else:
      args.append('-wait=false')
    args.append(task_id)
    self._run_swarming('collect', args)
    with open(tmp, 'rb') as f:
      data = f.read()
    try:
      summary = json.loads(data)
    except ValueError:
      print('Bad json:\n%s' % data, file=sys.stderr)
      raise
    file_outputs = {}
    outdir = os.path.join(self._tmpdir, task_id)
    for root, _, files in os.walk(outdir):
      for i in files:
        p = os.path.join(root, i)
        name = p[len(outdir) + 1:]
        with open(p, 'rb') as f:
          file_outputs[name] = f.read()
    return summary, file_outputs

  def task_cancel(self, task_id, kill_running=False):
    """Cancels a task."""
    args = []
    if kill_running:
      args.append('-kill-running')
    args.append(task_id)
    ret = self._run_swarming('cancel', args)
    assert ret == 0, 'Failed to cancel task. exit_code=%d, args=%s' % (ret,
                                                                       args)

  def task_result(self, task_id):
    """Gets a task result without waiting for it to complete."""
    result, _ = self.task_collect(task_id, wait=False, include_output=False)
    return result['shards'][0]

  def task_stdout(self, task_id):
    """Returns current task stdout without waiting for it to complete."""
    result, _ = self.task_collect(task_id, wait=False)
    return result['shards'][0]['output']

  def terminate(self, bot_id):
    args = [
        '-wait',
        bot_id,
    ]
    return self._run_swarming('termiante', args)

  def query_bot(self):
    """Returns the bot's properties."""
    tmp = os.path.join(self._tmpdir, 'bots.json')
    args = [
        '-json',
        tmp,
    ]
    ret = self._run_swarming('bots', args)
    assert ret == 0, 'Failed to fetch bots. exit_code=%d' % ret
    with open(tmp, 'rb') as f:
      bots = json.load(f)
    if not bots:
      return
    return bots[0]

  def dump_log(self):
    print('-' * 60, file=sys.stderr)
    print('Client calls', file=sys.stderr)
    print('-' * 60, file=sys.stderr)
    for i in range(self._index):
      with open(os.path.join(self._tmpdir, 'client_%d.log' % i), 'rb') as f:
        log = f.read().strip('\n')
      for l in log.splitlines():
        sys.stderr.write('  %s\n' % l)

  def _rotate_logfile(self):
    filename = os.path.join(self._tmpdir, u'client_%d.log' % self._index)
    self._index += 1
    return filename

  def _run_swarming(self, command, args):
    """Runs swarming command and capture the stdout to a log file.

    The log file will be printed by the test framework in case of failure or
    verbose mode.

    Returns:
      The process exit code.
    """
    cmd = [
        SWARMING_CLI,
        command,
        '-verbose',
    ]
    if command != 'logout':
      cmd += [
          '-S',
          self._swarming_server,
      ]
    cmd += args
    logging.debug('SwarmingClient._run_swarming: executing command. %s', cmd)
    with open(self._rotate_logfile(), 'wb') as f:
      f.write('\nRunning: %s\n' % ' '.join(cmd))
      f.flush()
      p = subprocess.Popen(cmd, stdout=f, stderr=f)
      p.communicate()
      return p.returncode


def gen_expected(**kwargs):
  expected = {
      u'bot_dimensions':
      None,
      u'bot_id':
      unicode(socket.getfqdn().split('.', 1)[0]),
      u'current_task_slice':
      u'0',
      u'exit_code':
      u'0',
      u'name':
      u'',
      u'output':
      re.compile(u'(\\s|\\S)*hi\n'),
      u'server_versions': [u'N/A'],
      u'state':
      u'COMPLETED',
      u'tags': [
          u'authenticated:bot:whitelisted-ip',
          u'pool:default',
          u'priority:200',
          u'realm:none',
          u'service_account:none',
          u'swarming.pool.template:none',
          u'swarming.pool.version:pools_cfg_rev',
          u'user:joe@localhost',
      ],
      u'user':
      u'joe@localhost',
  }
  expected.update({unicode(k): v for k, v in kwargs.items()})
  return expected


def get_bot_dimensions():
  return json.loads(
      subprocess.check_output(
          ['vpython3',
           os.path.join(APP_DIR, 'tools', 'os_utilities.py')]))['dimensions']


class Test(unittest.TestCase):
  maxDiff = None
  client = None
  servers = None
  bot = None
  leak = False
  # This test can't pass when running via test runner
  # run by sequential_test_runner.py
  no_run = 1

  def setUp(self):
    super(Test, self).setUp()

    self.maxDiff = None
    self.dimensions = get_bot_dimensions()
    # The bot forcibly adds server_version, and bot_config.
    self.dimensions[u'server_version'] = [u'N/A']
    self.dimensions[u'bot_config'] = [u'bot_config.py']
    # Reset the bot's isolated cache at the start of each task, so that the
    # cache reuse data becomes deterministic. Only restart the bot when it had a
    # named cache because it takes multiple seconds to to restart the bot. :(
    #
    # TODO(maruel): 'isolated_upload' is not deterministic because the isolate
    # server not cleared.
    start = time.time()
    while True:
      old = self.client.query_bot()
      if old and 'dimensions' in old:
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
      if not state or not any(
          (d['key'] == 'server_version' for d in state['dimensions'])):
        time.sleep(0.1)
        continue
      if not had_cache:
        break
      new_started_ts = json.loads(state['state'])['started_ts']
      logging.info('setUp: new_started_ts is %s', new_started_ts)
      # This assumes that starting the bot and running the previous test case
      # took more than 1s.
      if not started_ts or new_started_ts != started_ts:
        self.assertNotIn(u'caches', state['dimensions'])
        break

  def gen_expected(self, **kwargs):
    dims = [{
        u'key': k,
        u'value': v
    } for k, v in sorted(self.dimensions.items()) if not k == 'python']
    return gen_expected(bot_dimensions=dims, **kwargs)

  def test_raw_bytes(self):
    # A string of a letter 'A', UTF-8 BOM then UTF-16 BOM then UTF-EDBCDIC then
    # invalid UTF-8 and the letter 'B'. It is double escaped so it can be passed
    # down the shell.
    invalid_bytes = 'A\\xEF\\xBB\\xBF\\xFE\\xFF\\xDD\\x73\\x66\\x73\\xc3\\x28B'
    args = [
        '-task-name',
        'non_utf8',
        '-d',
        'os=%s' % self.dimensions['os'][0],
        '--',
        'python',
        '-u',
        '-c',
        'print(\'' + invalid_bytes + '\')',
    ]
    summary = self.gen_expected(
        name=u'non_utf8',
        # The string is mostly converted to 'Replacement Character'.
        output=re.compile(u'(\\s|\\S)*A﻿���sfs�\\(B'),
        tags=sorted([
            u'authenticated:bot:whitelisted-ip',
            u'os:' + self.dimensions['os'][0],
            u'pool:default',
            u'priority:200',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ]))
    self.assertOneTask(args, summary, {})

  def test_invalid_command(self):
    args = ['-task-name', 'invalid', '--', 'unknown_invalid_command']
    summary = self.gen_expected(
        name=u'invalid',
        exit_code=u'1',
        failure=True,
        output=re.compile(
            r'(\s|\S)*'
            r'<The executable does not exist, a dependent library '
            r'is missing or the command line is too long>\n'
            r'<Check for missing .so/.dll in the .isolate or GN file or '
            r'length of command line args>'))
    self.assertOneTask(args, summary, {})

  def test_hard_timeout(self):
    args = [
        # Need to flush to ensure it will be sent to the server.
        '-task-name',
        'hard_timeout',
        '-hard-timeout',
        '1',
        '--',
        'python',
        '-u',
        '-c',
        'import time,sys; sys.stdout.write(\'hi\\n\'); '
        'sys.stdout.flush(); time.sleep(120)',
    ]
    summary = self.gen_expected(
        name=u'hard_timeout',
        exit_code=unicode(SIGNAL_TERM),
        failure=True,
        state=u'TIMED_OUT')
    self.assertOneTask(args, summary, {})

  def test_io_timeout(self):
    args = [
        # Need to flush to ensure it will be sent to the server.
        '-task-name',
        'io_timeout',
        '-io-timeout',
        '2',
        '--',
        'python',
        '-u',
        '-c',
        'import time,sys; sys.stdout.write(\'hi\\n\'); '
        'sys.stdout.flush(); time.sleep(120)',
    ]
    summary = self.gen_expected(
        name=u'io_timeout',
        exit_code=unicode(SIGNAL_TERM),
        failure=True,
        state=u'TIMED_OUT')
    self.assertOneTask(args, summary, {})

  def test_success_fails(self):

    def get_cmd(name, exit_code):
      return [
          '-task-name',
          name,
          '--',
          'python',
          '-u',
          '-c',
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
                    name=u'simple_failure', exit_code=u'1', failure=True),
                {},
            ),
        ),
        (
            get_cmd('ending_simple_success', 0),
            (self.gen_expected(name=u'ending_simple_success'), {}),
        ),
    ]

    # tuple(task_id, expectation)
    running_tasks = [(self.client.task_trigger(args), expected)
                     for args, expected in tasks]

    for task_id, (summary, files) in running_tasks:
      actual_summary, actual_files = self.client.task_collect(task_id)
      performance_stats = actual_summary['shards'][0].pop('performance_stats')
      self.assertPerformanceStatsEmpty(performance_stats)
      self.assertResults(summary, actual_summary)
      self.assertEqual(files, actual_files)

  def test_with_cas(self):
    # Archive input file.
    # Assert that the environment variable SWARMING_TASK_ID is set.
    content = {
        HELLO_WORLD + u'.py':
            _script(u"""
        # coding=utf-8
        import os
        import sys
        print('hi')
        assert os.environ.get("SWARMING_TASK_ID")
        with open(os.path.join(sys.argv[1], u'💣.txt'), 'wb') as f:
          f.write('test_isolated')
        """),
    }
    # The problem here is that we don't know the isolzed size yet, so we need to
    # do this first.
    name = 'isolated_task'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'isolated_task',
        output=re.compile(u'(\\s|\\S)*hi\n'),
        tags=[
            u'authenticated:bot:whitelisted-ip', u'pool:default',
            u'priority:200', u'realm:none', u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev', u'user:joe@localhost'
        ])
    expected_files = {
        u'💣.txt': 'test_isolated',
    }
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name, ['--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'],
        expected_summary,
        expected_files,
        deduped=False)
    output_root_size = int(output_root['digest']['size_bytes'])
    items_out = [output_root_size, len('test_isolated')]
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_out)),
            u'total_bytes_items_hot': u'0',
        },
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

  def test_command_env_with_cas(self):
    # Confirms that --relative-cwd, --env and --env-prefix work.
    content = {
        os.path.join(u'base', HELLO_WORLD + u'.py'):
            _script(u"""
        # coding=utf-8
        import os
        import sys
        print(sys.argv[1])
        assert "SWARMING_TASK_ID" in os.environ
        # cwd is in base/
        cwd = os.path.realpath(os.getcwd())
        base = os.path.basename(cwd)
        assert base == 'base', base
        cwd = os.path.dirname(cwd)
        paths = os.environ["PATH"].split(os.pathsep)
        path = [p for p in paths if p.startswith(cwd)][0]
        print(os.path.realpath(path).replace(cwd, "$CWD"))
        with open(os.path.join(sys.argv[2], 'FOO.txt'), 'wb') as f:
          f.write(os.environ["FOO"])
        with open(os.path.join(sys.argv[2], 'result.txt'), 'wb') as f:
          f.write('hey2')
        """),
    }
    isolate_content = "{'variables': {'files': ['%s']}}" % os.path.join(
        u'base', HELLO_WORLD + u'.py').encode('utf-8')
    name = 'command_env'
    digest = self._archive(name, content, isolate_content)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_output = u'hi💩\n%s\n' % os.sep.join(['$CWD', 'local', 'path'])
    expected_summary = self.gen_expected(
        name=u'command_env',
        output=re.compile(u'(\\s|\\S)*%s' % re.escape(expected_output)),
        tags=[
            u'authenticated:bot:whitelisted-ip', u'pool:default',
            u'priority:200', u'realm:none', u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev', u'user:joe@localhost'
        ])
    expected_files = {
        'result.txt': 'hey2',
        'FOO.txt': u'bar💩'.encode('utf-8'),
    }
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name, [
            '-relative-cwd', 'base', '-env', 'FOO=bar💩', '-env',
            'SWARMING_TASK_ID=""', '-env-prefix', 'PATH=local/path', '--',
            'python', HELLO_WORLD + u'.py', u'hi💩', '${ISOLATED_OUTDIR}'
        ],
        expected_summary,
        expected_files,
        deduped=False)
    output_root_size = int(output_root['digest']['size_bytes'])
    items_out = [
        len('hey2'),
        len(u'bar💩'.encode('utf-8')),
        output_root_size,
    ]
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_out)),
            u'total_bytes_items_hot': u'0',
        },
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

  def test_hard_timeout_with_cas(self):
    # Similar to test_hard_timeout. The script doesn't handle signal so it
    # failed the grace period.
    content = {
        HELLO_WORLD + u'.py':
        _script(u"""
        import os
        import sys
        import time
        sys.stdout.write('hi\\n')
        sys.stdout.flush()
        time.sleep(120)
        with open(os.path.join(sys.argv[1], 'result.txt'), 'wb') as f:
          f.write('test_hard_timeout_with_cas')
        """),
    }
    name = 'hard_timeout'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'hard_timeout',
        exit_code=unicode(SIGNAL_TERM),
        failure=True,
        tags=[
            u'authenticated:bot:whitelisted-ip',
            u'pool:default',
            u'priority:200',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ],
        state=u'TIMED_OUT')
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name,
        ['-hard-timeout', '1', '--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'],
        expected_summary, {},
        deduped=False)
    self.assertIsNone(output_root)
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {},
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

  def test_hard_timeout_grace_with_cas(self):
    # Similar to test_hard_timeout. The script handles signal so it send
    # results back.
    content = {
        HELLO_WORLD + u'.py':
        _script(u"""
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
          f.write('test_hard_timeout_grace_with_cas')
        """ % ('SIGBREAK' if sys.platform == 'win32' else 'SIGTERM')),
    }
    name = 'hard_timeout_grace'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'hard_timeout_grace',
        output=re.compile(u'(\\s|\\S)*hi\ngot signal 15\n'),
        failure=True,
        tags=[
            u'authenticated:bot:whitelisted-ip',
            u'pool:default',
            u'priority:200',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ],
        state=u'TIMED_OUT')
    expected_summary.pop('exit_code')
    expected_files = {
        'result.txt': 'test_hard_timeout_grace_with_cas',
    }
    # Hard timeout is enforced by run_isolated, I/O timeout by task_runner.
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name,
        ['-hard-timeout', '1', '--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'],
        expected_summary,
        expected_files,
        deduped=False)
    output_root_size = int(output_root['digest']['size_bytes'])
    items_out = [len('test_hard_timeout_grace_with_cas'), output_root_size]
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_out)),
            u'total_bytes_items_hot': u'0',
        },
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

  def test_idempotent_reuse(self):
    content = {HELLO_WORLD + u'.py': 'print("hi")\n'}
    name = 'idempotent_reuse'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'idempotent_reuse',
        tags=[
            u'authenticated:bot:whitelisted-ip',
            u'pool:default',
            u'priority:200',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ])
    task_id, output_root, performance_stats = self._run_with_cas(
        digest,
        name, ['--idempotent', '--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'],
        expected_summary, {},
        deduped=False)
    self.assertIsNone(output_root)
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {},
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

    # The task name changes, there's a bit less data but the rest is the same.
    expected_summary = self.gen_expected(
        name=u'idempotent_reuse2',
        cost_saved_usd=0.02,
        deduped_from=task_id[:-1] + u'1',
        tags=[
            u'authenticated:bot:whitelisted-ip',
            u'pool:default',
            u'priority:200',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ])
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        'idempotent_reuse2',
        ['--idempotent', '--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'],
        expected_summary, {},
        deduped=True)
    self.assertIsNone(output_root)
    self.assertIsNone(performance_stats)

  def test_secret_bytes(self):
    content = {
        HELLO_WORLD + u'.py':
            _script(u"""
        from __future__ import print_function

        import sys
        import os
        import json

        print("hi")

        with open(os.environ['LUCI_CONTEXT'], 'r') as f:
          data = json.load(f)

        with open(os.path.join(sys.argv[1], 'sekret'), 'w') as f:
          print(data['swarming']['secret_bytes'].decode('base64'), file=f)
      """),
    }
    name = 'secret_bytes'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'secret_bytes',
        tags=[
            u'authenticated:bot:whitelisted-ip', u'pool:default',
            u'priority:200', u'realm:none', u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev', u'user:joe@localhost'
        ])
    tmp = os.path.join(self.tmpdir, 'test_secret_bytes')
    with open(tmp, 'wb') as f:
      f.write('foobar')
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name, ['--secret-bytes-path', tmp, '--'] + DEFAULT_COMMAND +
        ['${ISOLATED_OUTDIR}'],
        expected_summary, {'sekret': 'foobar\n'},
        deduped=False)
    output_root_size = int(output_root['digest']['size_bytes'])
    items_out = [len('foobar\n'), output_root_size]
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_out)),
            u'total_bytes_items_hot': u'0',
        },
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

  def test_local_cache(self):
    # First task creates the cache, second copy the content to the output
    # directory. Each time it's the exact same script.
    dimensions = {
        i['key']: i['value'] for i in self.client.query_bot()['dimensions']
    }
    self.assertEqual(set(self.dimensions), set(dimensions))
    self.assertNotIn(u'cache', set(dimensions))
    content = {
        HELLO_WORLD + u'.py':
            _script(u"""
        import os, shutil, sys
        p = "p/b/a.txt"
        if not os.path.isfile(p):
          with open(p, "wb") as f:
            f.write("Yo!")
        else:
          shutil.copy(p, sys.argv[1])
        print("hi")
        """),
    }
    name = 'cache_first'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    content_size = sum(len(c) for c in content.values())
    items_in = [content_size]
    expected_summary = self.gen_expected(
        name=u'cache_first',
        tags=[
            u'authenticated:bot:whitelisted-ip', u'pool:default',
            u'priority:200', u'realm:none', u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev', u'user:joe@localhost'
        ])
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        name, ['-named-cache', 'fuu=p/b', '--'] + DEFAULT_COMMAND +
        ['${ISOLATED_OUTDIR}/yo'],
        expected_summary, {},
        deduped=False)
    self.assertIsNone(output_root)
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_in),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_in)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_in)),
            u'total_bytes_items_hot': u'0',
        },
        u'isolated_upload': {},
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

    # Second run with a cache available.
    expected_summary = self.gen_expected(
        name=u'cache_second',
        tags=[
            u'authenticated:bot:whitelisted-ip', u'pool:default',
            u'priority:200', u'realm:none', u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev', u'user:joe@localhost'
        ])
    # The previous task caused the bot to have a named cache.
    # pylint: disable=not-an-iterable,unsubscriptable-object
    expected_summary['bot_dimensions'] = (expected_summary['bot_dimensions'][:])
    self.assertNotIn('caches',
                     [i['key'] for i in expected_summary['bot_dimensions']])
    expected_summary['bot_dimensions'] = sorted([{
        u'key': u'caches',
        u'value': [u'fuu']
    }] + expected_summary['bot_dimensions'])
    _, output_root, performance_stats = self._run_with_cas(
        digest,
        'cache_second', ['-named-cache', 'fuu=p/b', '--'] + DEFAULT_COMMAND +
        ['${ISOLATED_OUTDIR}/yo'],
        expected_summary, {'yo': 'Yo!'},
        deduped=False)
    output_root_size = int(output_root['digest']['size_bytes'])
    items_out = [len('Yo!'), output_root_size]
    expected_performance_stats = {
        u'cache_trim': {},
        u'package_installation': {},
        u'named_caches_install': {},
        u'named_caches_uninstall': {},
        u'isolated_download': {
            # TODO(crbug.com/1268298): cas client doesn't set
            # initial_{number_items, size}.
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': [],
            # Items are hot.
            u'items_hot': sorted(items_in),
            u'num_items_cold': u'0',
            u'num_items_hot': unicode(len(items_in)),
            u'total_bytes_items_cold': u'0',
            u'total_bytes_items_hot': unicode(sum(items_in)),
        },
        u'isolated_upload': {
            u'initial_number_items': u'0',
            u'initial_size': u'0',
            u'items_cold': sorted(items_out),
            u'items_hot': [],
            u'num_items_cold': unicode(len(items_out)),
            u'num_items_hot': u'0',
            u'total_bytes_items_cold': unicode(sum(items_out)),
            u'total_bytes_items_hot': u'0',
        },
        u'cleanup': {},
    }
    self.assertPerformanceStats(expected_performance_stats, performance_stats)

    # Check that the bot now has a cache dimension by independently querying.
    expected = set(self.dimensions)
    expected.add(u'caches')
    dimensions = {
        i['key']: i['value'] for i in self.client.query_bot()['dimensions']
    }
    self.assertEqual(expected, set(dimensions))

  def test_priority(self):
    if self.bot.python != sys.executable:
      self.skipTest('crbug.com/1010816')
    # Make a test that keeps the bot busy, while all the other tasks are being
    # created with priorities that are out of order. Then it unblocks the bot
    # and asserts the tasks are run in the expected order, based on priority and
    # created_ts.
    # List of tuple(task_name, priority, task_id).
    tasks = []
    tags = [
        u'authenticated:bot:whitelisted-ip',
        u'pool:default',
        u'realm:none',
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
        task_name = u'%d-p%d' % (i, priority)
        args = [
            '-task-name',
            task_name,
            '-priority',
            str(priority),
            '--',
            'python',
            '-u',
            '-c',
            'print(\'%d\')' % priority,
        ]
        tasks.append((task_name, priority, self.client.task_trigger(args)))
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
      actual_summary, _ = self.client.task_collect(task_id)
      performance_stats = actual_summary['shards'][0].pop('performance_stats')
      self.assertPerformanceStatsEmpty(performance_stats)
      t = tags[:] + [u'priority:%d' % priority]
      expected_summary = self.gen_expected(
          name=task_name, tags=sorted(t), output=u'%d\n' % priority)
      self.assertResults(expected_summary, actual_summary)
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
        '-task-name', 'cancel_pending', '--', 'python', '-u', '-c',
        'print(\'hi\')'
    ]
    with self._make_wait_task('test_cancel_pending'):
      task_id = self.client.task_trigger(args)
      result = self.client.task_result(task_id)
      self.assertEqual(u'PENDING', result[u'state'])
      self.client.task_cancel(task_id)
    result = self.client.task_result(task_id)
    self.assertEqual(u'CANCELED', result[u'state'], result)

  def test_kill_running(self):
    # Kill a running task. Make sure the target process handles the signal
    # well with a graceful termination via SIGTERM.
    content = {
        HELLO_WORLD + u'.py':
            _script(u"""
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
    name = 'kill_running'
    digest = self._archive(name, content, DEFAULT_ISOLATE_HELLO)
    # Do not use self._run_with_cas() here since we want to kill it, not wait
    # for it to complete.
    task_id = self.client.task_trigger_with_cas(
        digest, name, ['--'] + DEFAULT_COMMAND + ['${ISOLATED_OUTDIR}'])

    # Wait for the task to start on the bot.
    self._wait_for_state(task_id, u'PENDING', u'RUNNING')
    # Make sure the signal handler is set by looking that 'hi' was printed,
    # otherwise it could continue looping.
    start = time.time()
    out = None
    while time.time() - start < 45.:
      out = cas_util.filter_out_go_logs(self.client.task_stdout(task_id))
      if out == 'hi\n':
        break
    self.assertTrue(re.compile('(\\s|\\S)*hi\n'))

    # Cancel it.
    self.client.task_cancel(task_id, kill_running=True)

    result = self._wait_for_state(task_id, u'RUNNING', u'KILLED')
    # Make sure the exit code is what the script returned, which means the
    # script in fact did handle the SIGTERM and returned properly.
    self.assertEqual(u'23', result[u'exit_code'])

  def test_task_slice(self):
    request = {
        'name':
        'task_slice',
        'priority':
        '40',
        'user':
        'joe@localhost',
        'task_slices': [
            {
                'expiration_secs': '120',
                'properties': {
                    'command': ['python', '-c', 'print("first")'],
                    'dimensions': [
                        {
                            'key': 'pool',
                            'value': 'default',
                        },
                    ],
                    'grace_period_secs': '30',
                    'execution_timeout_secs': '30',
                    'io_timeout_secs': '30',
                },
            },
            {
                'expiration_secs': '120',
                'properties': {
                    'command': ['python', '-c', 'print("second")'],
                    'dimensions': [
                        {
                            'key': 'pool',
                            'value': 'default',
                        },
                    ],
                    'grace_period_secs': '30',
                    'execution_timeout_secs': '30',
                    'io_timeout_secs': '30',
                },
            },
        ],
    }
    task_id = self.client.task_trigger_raw(request)
    expected_summary = self.gen_expected(
        name=u'task_slice',
        output=re.compile('(\\s|\\S)*first$'),
        tags=[
            u'authenticated:bot:whitelisted-ip',
            u'pool:default',
            u'priority:40',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ])
    actual_summary, _ = self.client.task_collect(task_id)
    performance_stats = actual_summary['shards'][0].pop('performance_stats')
    self.assertPerformanceStatsEmpty(performance_stats)
    self.assertResults(expected_summary, actual_summary, deduped=False)

  def test_task_slice_fallback(self):
    # The first one shall be skipped.
    request = {
        'name':
        'task_slice_fallback',
        'priority':
        '40',
        'user':
        'joe@localhost',
        'task_slices': [
            {
                # Really long expiration that would cause the smoke test to
                # abort under normal condition.
                'expiration_secs': '1200',
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
                    'grace_period_secs':
                    '30',
                    'execution_timeout_secs':
                    '30',
                    'io_timeout_secs':
                    '30',
                },
            },
            {
                'expiration_secs': '120',
                'properties': {
                    'command': ['python', '-c', 'print("second")'],
                    'dimensions': [
                        {
                            'key': 'pool',
                            'value': 'default',
                        },
                    ],
                    'grace_period_secs': '30',
                    'execution_timeout_secs': '30',
                    'io_timeout_secs': '30',
                },
            },
        ],
    }
    task_id = self.client.task_trigger_raw(request)
    expected_summary = self.gen_expected(
        name=u'task_slice_fallback',
        current_task_slice=u'1',
        output=re.compile(u'(\\s|\\S)*second\n'),
        tags=[
            # Bug!
            u'authenticated:bot:whitelisted-ip',
            u'invalidkey:invalidvalue',
            u'pool:default',
            u'priority:40',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ])
    actual_summary, _ = self.client.task_collect(task_id)
    performance_stats = actual_summary['shards'][0].pop('performance_stats')
    self.assertPerformanceStatsEmpty(performance_stats)
    self.assertResults(expected_summary, actual_summary, deduped=False)

  def test_no_resource(self):
    request = {
        'name':
        'no_resource',
        'priority':
        '40',
        'task_slices': [
            {
                # Really long expiration that would cause the smoke test to
                # abort under normal conditions.
                'expiration_secs': '1200',
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
                    'grace_period_secs':
                    '30',
                    'execution_timeout_secs':
                    '30',
                    'io_timeout_secs':
                    '30',
                },
            },
        ],
    }
    task_id = self.client.task_trigger_raw(request)
    actual_summary, _ = self.client.task_collect(task_id)
    summary = actual_summary[u'shards'][0]
    # Immediately cancelled.
    self.assertEqual(u'NO_RESOURCE', summary[u'state'])
    self.assertTrue(summary[u'abandoned_ts'])

  @contextlib.contextmanager
  def _make_wait_task(self, name):
    """Creates a dummy task that keeps the bot busy, while other things are
    being done.
    """
    signal_file = os.path.join(self.tmpdir, name)
    open(signal_file, 'wb').close()
    args = [
        '-task-name',
        'wait',
        '-priority',
        '20',
        '--',
        'python',
        '-u',
        '-c',
        # Cheezy wait.
        ('import os,time;'
         'print(\'hi\');'
         '[time.sleep(0.1) for _ in range(100000) if os.path.exists(\'%s\')];'
         'print(\'hi again\')') % signal_file,
    ]
    wait_task_id = self.client.task_trigger(args)
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
    actual_summary, _ = self.client.task_collect(wait_task_id)
    tags = [
        u'authenticated:bot:whitelisted-ip',
        u'pool:default',
        u'priority:20',
        u'realm:none',
        u'service_account:none',
        u'swarming.pool.template:none',
        u'swarming.pool.version:pools_cfg_rev',
        u'user:joe@localhost',
    ]
    performance_stats = actual_summary['shards'][0].pop('performance_stats')
    self.assertPerformanceStatsEmpty(performance_stats)
    self.assertResults(
        self.gen_expected(name=u'wait',
                          tags=tags,
                          output=re.compile(u'(\\S|\\s)*hi\nhi again\n')),
        actual_summary)

  def _run_with_cas(self, digest, name, args, expected_summary, expected_files,
                    deduped):
    """Triggers a Swarming task and asserts results.

    It runs a python script archived as an isolated file.

    Returns:
      tuple of:
        task_id
        cas_output_root if any, or None
        performance_stats if any, or None
    """
    task_id = self.client.task_trigger_with_cas(digest, name, args)
    actual_summary, actual_files = self.client.task_collect(task_id)
    self.assertIsNotNone(actual_summary['shards'][0], actual_summary)
    output_root = actual_summary['shards'][0].pop('cas_output_root', None)
    performance_stats = actual_summary[u'shards'][0].pop(
        'performance_stats', None)
    # filter luci-go client logs out.
    output = actual_summary['shards'][0]['output']
    actual_summary['shards'][0]['output'] = cas_util.filter_out_go_logs(output)
    self.assertResults(expected_summary, actual_summary, deduped=deduped)
    self.assertEqual(expected_files, actual_files)
    return task_id, output_root, performance_stats

  def _archive(self, name, contents, isolate_content):
    """Archives data to the isolate server or RBE-CAS.

    Returns the CAS digest.
    """
    # Shared code for all test_isolated_* test cases.
    root = os.path.join(self.tmpdir, name)
    # Refuse reusing the same task name twice, it makes the whole test suite
    # more manageable.
    self.assertFalse(os.path.isdir(root), root)
    os.mkdir(root)
    isolate_path = os.path.join(root, 'i.isolate')
    with open(isolate_path, 'wb') as f:
      f.write(isolate_content)
    for relpath, content in contents.items():
      p = os.path.join(root, relpath)
      d = os.path.dirname(p)
      if not os.path.isdir(d):
        os.makedirs(d)
      with open(p, 'wb') as f:
        f.write(content)
    return self.client.isolate(isolate_path)

  def assertResults(self, expected, result, deduped=False):
    """Compares the outputs of a swarming task."""
    self.assertEqual([u'shards'], result.keys())
    self.assertEqual(1, len(result[u'shards']))
    self.assertTrue(result[u'shards'][0], result)
    result = result[u'shards'][0].copy()
    self.assertFalse(result.get(u'abandoned_ts'))
    bot_version = result.pop(u'bot_version')
    self.assertTrue(bot_version)
    if result.get(u'costs_usd') is not None:
      expected.pop(u'costs_usd', None)
      self.assertLess(0, result.pop(u'costs_usd'))
    if result.get(u'cost_saved_usd') is not None:
      expected.pop(u'cost_saved_usd', None)
      self.assertLess(0, result.pop(u'cost_saved_usd'))
    self.assertTrue(result.pop(u'created_ts'))
    self.assertTrue(result.pop(u'completed_ts'))
    self.assertLess(0, result.pop(u'duration'))
    task_id = result.pop(u'task_id')
    run_id = result.pop(u'run_id')
    self.assertTrue(task_id)
    self.assertTrue(task_id.endswith('0'), task_id)
    if not deduped:
      self.assertEqual(task_id[:-1] + '1', run_id)
    self.assertTrue(result.pop(u'bot_idle_since_ts'))
    self.assertTrue(result.pop(u'modified_ts'))
    self.assertTrue(result.pop(u'started_ts'))

    if getattr(expected.get(u'output'), 'match', None):
      expected_output = expected.pop(u'output')
      output = result.pop('output')
      self.assertTrue(
          expected_output.match(output),
          '%s does not match %s' % (output, expected_output.pattern))

    # Bot python version may be different.
    result[u'bot_dimensions'] = sorted(
        [d for d in result[u'bot_dimensions'] if not d['key'] == 'python'])

    self.assertEqual(expected, result)
    return bot_version

  def assertOneTask(self, args, expected_summary, expected_files):
    """Runs a single task at a time."""
    task_id = self.client.task_trigger(args)
    actual_summary, actual_files = self.client.task_collect(task_id)
    self.assertIsNotNone(actual_summary['shards'][0], actual_summary)
    performance_stats = actual_summary['shards'][0].pop('performance_stats')
    self.assertPerformanceStatsEmpty(performance_stats)
    bot_version = self.assertResults(expected_summary, actual_summary)
    self.assertEqual(expected_files, actual_files)
    return bot_version

  def assertPerformanceStatsEmpty(self, actual):
    self.assertLess(0, actual.pop(u'bot_overhead'))
    self.assertLess(0, actual[u'cache_trim'].pop(u'duration'))
    self.assertLessEqual(0, actual[u'named_caches_install'].pop(u'duration', 0))
    self.assertLessEqual(0,
                         actual[u'named_caches_uninstall'].pop(u'duration', 0))
    self.assertLess(0, actual[u'cleanup'].pop(u'duration'))
    self.assertEqual(
        {
            u'cache_trim': {},
            u'package_installation': {},
            u'named_caches_install': {},
            u'named_caches_uninstall': {},
            u'isolated_download': {},
            u'isolated_upload': {},
            u'cleanup': {},
        }, actual)

  def assertPerformanceStats(self, expected, actual):
    # These are not deterministic (or I'm too lazy to calculate the value).
    self.assertLess(0, actual.pop(u'bot_overhead'))
    self.assertLess(0, actual[u'cache_trim'].pop(u'duration'))
    self.assertLessEqual(0, actual[u'named_caches_install'].pop(u'duration', 0))
    self.assertLessEqual(0,
                         actual[u'named_caches_uninstall'].pop(u'duration', 0))
    self.assertLess(0, actual[u'isolated_download'].pop(u'duration'))
    self.assertLessEqual(0, actual[u'isolated_upload'].pop(u'duration', 0))
    self.assertLess(0, actual[u'cleanup'].pop(u'duration'))
    for k in (u'isolated_download', u'isolated_upload'):
      for j in (u'items_cold', u'items_hot'):
        if j in actual[k]:
          actual[k][j] = large.unpack(base64.b64decode(actual[k][j]))
    self.assertEqual(expected, actual)

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
    shutil.rmtree(Test.tmpdir)


def process_arguments():
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument('-v', dest='verbose', action='store_true')
  parser.add_argument('--leak', action='store_true')
  args, _ = parser.parse_known_args()

  if args.verbose:
    logging.basicConfig(
        format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
        level=logging.DEBUG)

    Test.maxDiff = None
  else:
    logging.basicConfig(level=logging.ERROR)

  if args.leak:
    # Note that --leak will not guarantee that 'c' and 'isolated_cache' are
    # kept. Only the last test case will leak these two directories.
    sys.argv.remove('--leak')
    Test.leak = args.leak

  return args


def main():
  fix_encoding.fix_encoding()
  args = process_arguments()
  Test.tmpdir = unicode(tempfile.mkdtemp(prefix='local_smoke_test'))

  # Force language to be English, otherwise the error messages differ from
  # expectations.
  os.environ['LANG'] = 'en_US.UTF-8'
  os.environ['LANGUAGE'] = 'en_US.UTF-8'

  # So that we don't get goofed up when running this test on swarming :)
  os.environ.pop('SWARMING_TASK_ID', None)
  os.environ.pop('SWARMING_SERVER', None)

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
    bot = start_bot.LocalBot(servers.swarming_server.url,
                             servers.cas_server.address, True, botdir,
                             'vpython3')
    Test.bot = bot
    bot.start()
    client = SwarmingClient(servers.swarming_server.url,
                            servers.cas_server.address, Test.tmpdir)
    # All requests in local_smoke_test.py are expected to be authenticated with
    # IP address.
    # If it's logged in, luci-go clients send auth header unexpectedly.
    client.ensure_logged_out()
    # Test cases only interact with the client; except for test_update_continue
    # which mutates the bot.
    Test.client = client
    Test.servers = servers
    failed = not unittest.main(exit=False).result.wasSuccessful()

    # Then try to terminate the bot sanely. After the terminate request
    # completed, the bot process should have terminated. Give it a few
    # seconds due to delay between sending the event that the process is
    # shutting down vs the process is shut down.
    if client.terminate(bot.bot_id) != 0:
      print('swarming terminate failed', file=sys.stderr)
      failed = True
    bot.wait()
  except KeyboardInterrupt:
    print('<Ctrl-C>', file=sys.stderr)
    failed = True
    if bot is not None and bot.poll() is None:
      bot.kill()
      bot.wait()
  finally:
    cleanup(bot, client, servers, failed or args.verbose)
  return int(failed)


if __name__ == '__main__':
  sys.exit(main())
