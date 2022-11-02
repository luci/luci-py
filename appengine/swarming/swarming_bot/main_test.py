#!/usr/bin/env vpython3
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import json
import logging
import os
import re
import sys
import tempfile
import unittest

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env_bot
test_env_bot.setup_test_env()

# client/third_party/
from depot_tools import auto_stub
from depot_tools import fix_encoding

# client/
from utils import file_path
from utils import subprocess42
from utils import tools

import swarmingserver_bot_fake
from bot_code import bot_main


class TestCase(auto_stub.TestCase):

  def setUp(self):
    super(TestCase, self).setUp()
    tools.clear_cache_all()
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_main')
    self._zip_file = os.path.join(self._tmpdir, 'swarming_bot.zip')
    code, _ = swarmingserver_bot_fake.gen_zip(self.url)
    with open(self._zip_file, 'wb') as f:
      f.write(code)

  def tearDown(self):
    try:
      file_path.rmtree(self._tmpdir)
    finally:
      super(TestCase, self).tearDown()


class SimpleMainTest(TestCase):
  @property
  def url(self):
    return 'http://localhost:1'

  def test_attributes(self):
    actual = json.loads(
        subprocess42.check_output(
            [sys.executable, self._zip_file, 'attributes'],
            stderr=subprocess42.PIPE))

    botobj = bot_main.get_bot(bot_main.get_config())
    expected = bot_main.get_attributes(botobj)

    # get_config() doesn't work when called outside of a zip, so patch the
    # server_version manually with the default value in config/config.json.
    self.assertEqual(['N/A'], expected['dimensions']['server_version'])
    expected['dimensions']['server_version'] = ['1']

    NON_DETERMINISTIC = ('cwd', 'disks', 'nb_files_in_temp', 'pid',
                         'running_time', 'started_ts', 'uptime', 'temp',
                         'original_bot_id')
    for key in NON_DETERMINISTIC:
      actual['state'].pop(key, None)
      expected['state'].pop(key, None)
    del actual['version']
    del expected['version']
    self.assertAlmostEqual(
        actual['state'].pop('cost_usd_hour'),
        expected['state'].pop('cost_usd_hour'),
        places=5)
    self.assertEqual(expected, actual)

  def test_version(self):
    version = subprocess42.check_output(
        [sys.executable, self._zip_file, 'version'], stderr=subprocess42.PIPE)
    lines = version.strip().split()
    self.assertEqual(1, len(lines), lines)
    self.assertTrue(re.match(br'^[0-9a-f]{64}$', lines[0]), lines[0])


class MainTest(TestCase):
  def setUp(self):
    self._server = swarmingserver_bot_fake.Server()
    super(MainTest, self).setUp()

  def tearDown(self):
    try:
      self._server.close()
    finally:
      super(MainTest, self).tearDown()

  @property
  def url(self):
    return self._server.url

  @unittest.skipIf(
      sys.platform == 'win32',
      'TODO(crbug.com/1017545): It gets stuck at proc.communicate()')
  def test_run_bot_signal(self):
    # Test SIGTERM signal handling. Run it as an external process to not mess
    # things up.
    proc = subprocess42.Popen([sys.executable, self._zip_file, 'start_slave'],
                              stdout=subprocess42.PIPE,
                              stderr=subprocess42.STDOUT,
                              detached=True)

    # Wait for the grand-child process to poll the server.
    self._server.has_polled.wait(60)
    self.assertEqual(True, self._server.has_polled.is_set())
    proc.terminate()
    out, _ = proc.communicate()
    if proc.returncode:
      print('ERROR LOG:')
      print(out)
    self.assertEqual(0, proc.returncode)
    events = self._server.get_bot_events()
    for event in events:
      event.pop('dimensions')
      event.pop('state')
      event.pop('version')
    expected = [
        {
            'event': 'bot_shutdown',
            'message': 'Signal was received',
        },
    ]
    if sys.platform == 'win32':
      # Sadly, the signal handler generate an error.
      # TODO(maruel): Fix one day.
      self.assertEqual('bot_error', events.pop(0)['event'])
    self.assertEqual(expected, events)


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
