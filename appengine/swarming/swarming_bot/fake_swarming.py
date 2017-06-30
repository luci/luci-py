# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import BaseHTTPServer
import json
import logging
import os
import SocketServer
import sys
import threading

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, os.path.join(os.path.dirname(BOT_DIR), 'server'))

import bot_archive

sys.path.pop(0)


class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
  """Minimal Swarming server fake implementation."""
  def do_GET(self):
    if self.path == '/swarming/api/v1/bot/server_ping':
      self.send_response(200)
      return
    if self.path == '/auth/api/v1/server/oauth_config':
      return self._send_json({
          'client_id': 'id',
          'client_not_so_secret': 'hunter2',
          'primary_url': self.server.server.url,
        })
    self.server.testcase.fail(self.path)
    self.send_response(500)

  def do_POST(self):
    length = int(self.headers['Content-Length'])
    data = json.loads(self.rfile.read(length))

    if self.path == '/auth/api/v1/accounts/self/xsrf_token':
      return self._send_json({'xsrf_token': 'a'})

    if self.path == '/swarming/api/v1/bot/event':
      self.server.server.add_event(data)
      return self._send_json({})

    if self.path == '/swarming/api/v1/bot/handshake':
      return self._send_json({'xsrf_token': 'fine'})

    if self.path == '/swarming/api/v1/bot/poll':
      self.server.server.has_polled.set()
      return self._send_json({'cmd': 'sleep', 'duration': 60})

    if self.path.startswith('/swarming/api/v1/bot/task_update/'):
      task_id = self.path[len('/swarming/api/v1/bot/task_update/'):]
      self.server.server.task_update(task_id, data)
      return self._send_json({'ok': True})

    self.server.testcase.fail(self.path)
    self.send_response(500)

  def do_PUT(self):
    self.server.testcase.fail(self.path)
    self.send_response(500)

  def log_message(self, fmt, *args):
    logging.info(
        '%s - - [%s] %s',
        self.client_address[0], self.log_date_time_string(),
        fmt % args)

  def _send_json(self, data):
    self.send_response(200)
    self.send_header('Content-type', 'application/json')
    self.end_headers()
    json.dump(data, self.wfile)


class Server(object):
  """Fake a Swarming server for local testing."""
  def __init__(self, testcase):
    self._lock = threading.Lock()
    self._httpd = SocketServer.TCPServer(('localhost', 0), Handler)
    self._httpd.server = self
    self._httpd.testcase = testcase
    self._thread = threading.Thread(target=self._run)
    self._thread.daemon = True
    self._thread.start()
    self._events = []
    self._tasks = {}
    self.has_polled = threading.Event()

  @property
  def url(self):
    return 'http://%s:%d' % (
        self._httpd.server_address[0], self._httpd.server_address[1])

  def add_event(self, data):
    with self._lock:
      self._events.append(data)

  def task_update(self, task_id, data):
    with self._lock:
      self._tasks.setdefault(task_id, []).append(data)

  def get_events(self):
    with self._lock:
      return self._events[:]

  def get_tasks(self):
    with self._lock:
      return self._tasks.copy()

  def shutdown(self):
    self._httpd.shutdown()

  def _run(self):
    self._httpd.serve_forever()


def gen_zip(url):
  """Returns swarming_bot.zip content."""
  with open(os.path.join(BOT_DIR, 'config', 'bot_config.py'), 'rb') as f:
    bot_config_content = f.read()
  return bot_archive.get_swarming_bot_zip(
      BOT_DIR, url, '1', {'config/bot_config.py': bot_config_content}, None)
