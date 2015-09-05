#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import BaseHTTPServer
import json
import logging
import os
import re
import shutil
import SocketServer
import subprocess
import sys
import tempfile
import threading
import time
import unittest

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

import test_env_bot
test_env_bot.setup_test_env()

from utils import subprocess42

# swarming/ for bot_archive and to import GAE SDK. This is important to note
# that this is the only test that needs the GAE SDK, due to the use of
# bot_archive to ensure the .zip generated is valid.
sys.path.insert(0, os.path.dirname(BOT_DIR))

# This imports ../test_env.py.
import test_env
test_env.setup_test_env()

from depot_tools import auto_stub

from server import bot_archive

from bot_code import bot_main


class FakeSwarmingHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """Minimal Swarming server fake implementation."""
  def do_GET(self):
    if self.path == '/swarming/api/v1/bot/server_ping':
      self.send_response(200)
      return
    self.server.testcase.fail(self.path)
    self.send_response(500)

  def do_POST(self):
    length = int(self.headers['Content-Length'])
    data = json.loads(self.rfile.read(length))
    if self.path == '/swarming/api/v1/bot/handshake':
      return self._send_json({'xsrf_token': 'fine'})
    if self.path == '/swarming/api/v1/bot/event':
      self.server.server.add_event(data)
      return self._send_json({})
    if self.path == '/swarming/api/v1/bot/poll':
      self.server.server.has_polled.set()
      return self._send_json({'cmd': 'sleep', 'duration': 60})
    self.server.testcase.fail(self.path)
    self.send_response(500)

  def do_PUT(self):
    self.server.testcase.fail(self.path)
    self.send_response(500)

  def log_message(self, fmt, *args):
    logging.info(
        '%s - - [%s] %s\n', self.client_address[0], self.log_date_time_string(),
        fmt % args)

  def _send_json(self, data):
    self.send_response(200)
    self.send_header('Content-type', 'application/json')
    self.end_headers()
    json.dump(data, self.wfile)


class FakeSwarmingServer(object):
  """Fake a Swarming server for local testing."""
  def __init__(self, testcase):
    self._lock = threading.Lock()
    self._httpd = SocketServer.TCPServer(('localhost', 0), FakeSwarmingHandler)
    self._httpd.server = self
    self._httpd.testcase = testcase
    self._thread = threading.Thread(target=self._run)
    self._thread.daemon = True
    self._thread.start()
    self._events = []
    self.has_polled = threading.Event()

  @property
  def url(self):
    return 'http://%s:%d' % (
        self._httpd.server_address[0], self._httpd.server_address[1])

  def add_event(self, data):
    with self._lock:
      self._events.append(data)

  def get_events(self):
    with self._lock:
      return self._events[:]

  def shutdown(self):
    self._httpd.shutdown()

  def _run(self):
    self._httpd.serve_forever()


def _gen_zip(url):
  with open(os.path.join(BOT_DIR, 'config', 'bot_config.py'), 'rb') as f:
    bot_config_content = f.read()
  return bot_archive.get_swarming_bot_zip(
      BOT_DIR, url, '1', {'config/bot_config.py': bot_config_content})


class TestCase(auto_stub.TestCase):
  def setUp(self):
    super(TestCase, self).setUp()
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_main')
    self._zip_file = os.path.join(self._tmpdir, 'swarming_bot.zip')
    with open(self._zip_file, 'wb') as f:
      f.write(_gen_zip(self.url))

  def tearDown(self):
    try:
      shutil.rmtree(self._tmpdir)
    finally:
      super(TestCase, self).tearDown()


class SimpleMainTest(TestCase):
  @property
  def url(self):
    return 'http://localhost:1'

  def test_attributes(self):
    actual = json.loads(subprocess.check_output(
        [sys.executable, self._zip_file, 'attributes'], stderr=subprocess.PIPE))
    expected = bot_main.get_attributes()
    for key in (
        u'cwd', u'disks', u'nb_files_in_temp', u'running_time', u'started_ts'):
      del actual[u'state'][key]
      del expected[u'state'][key]
    del actual[u'version']
    del expected[u'version']
    self.assertAlmostEqual(
        actual[u'state'].pop(u'cost_usd_hour'),
        expected[u'state'].pop(u'cost_usd_hour'),
        places=6)
    self.assertEqual(expected, actual)

  def test_version(self):
    version = subprocess.check_output(
        [sys.executable, self._zip_file, 'version'], stderr=subprocess.PIPE)
    lines = version.strip().split()
    self.assertEqual(1, len(lines), lines)
    self.assertTrue(re.match(r'^[0-9a-f]{40}$', lines[0]), lines[0])


class MainTest(TestCase):
  def setUp(self):
    self._server = FakeSwarmingServer(self)
    super(MainTest, self).setUp()

  def tearDown(self):
    try:
      self._server.shutdown()
    finally:
      super(MainTest, self).tearDown()

  @property
  def url(self):
    return self._server.url

  def test_run_bot_signal(self):
    # Test SIGTERM signal handling. Run it as an external process to not mess
    # things up.
    proc = subprocess42.Popen(
        [sys.executable, self._zip_file, 'start_slave'],
        stdout=subprocess42.PIPE,
        stderr=subprocess42.PIPE,
        detached=True)

    # Wait for the grand-child process to poll the server.
    self._server.has_polled.wait(60)
    self.assertEqual(True, self._server.has_polled.is_set())
    proc.terminate()
    out, _ = proc.communicate()
    self.assertEqual(0, proc.returncode)
    self.assertEqual('', out)
    events = self._server.get_events()
    for event in events:
      event.pop('dimensions')
      event.pop('state')
      event.pop('version')
    expected = [
      {
        u'event': u'bot_shutdown',
        u'message': u'Signal was received',
      },
    ]
    if sys.platform == 'win32':
      # Sadly, the signal handler generate an error.
      # TODO(maruel): Fix one day.
      self.assertEqual(u'bot_error', events.pop(0)['event'])
    self.assertEqual(expected, events)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
