# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import json
import os
import sys
import threading

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(
    0,
    os.path.join(os.path.dirname(BOT_DIR), '..', '..', '..', 'client', 'tests'))
import httpserver
sys.path.pop(0)

sys.path.insert(0, os.path.join(os.path.dirname(BOT_DIR), 'server'))
import bot_archive
sys.path.pop(0)


def gen_zip(url):
  """Returns swarming_bot.zip content."""
  with open(os.path.join(BOT_DIR, 'config', 'bot_config.py'), 'rb') as f:
    bot_config_content = f.read()
  return bot_archive.get_swarming_bot_zip(
      BOT_DIR, url, '1', {'config/bot_config.py': bot_config_content}, None)


class Handler(httpserver.Handler):
  """Minimal Swarming bot server fake implementation."""
  def do_GET(self):
    if self.path == '/swarming/api/v1/bot/server_ping':
      self.send_response(200)
      return None
    if self.path == '/auth/api/v1/server/oauth_config':
      return self.send_json({
          'client_id': 'id',
          'client_not_so_secret': 'hunter2',
          'primary_url': self.server.url,
        })
    raise NotImplementedError(self.path)

  def do_POST(self):
    data = json.loads(self.read_body())

    if self.path == '/auth/api/v1/accounts/self/xsrf_token':
      return self.send_json({'xsrf_token': 'a'})

    if self.path == '/swarming/api/v1/bot/event':
      self.server.parent.add_event(data)
      return self.send_json({})

    if self.path == '/swarming/api/v1/bot/handshake':
      return self.send_json({'xsrf_token': 'fine'})

    if self.path == '/swarming/api/v1/bot/poll':
      self.server.parent.has_polled.set()
      return self.send_json({'cmd': 'sleep', 'duration': 60})

    if self.path.startswith('/swarming/api/v1/bot/task_update/'):
      task_id = self.path[len('/swarming/api/v1/bot/task_update/'):]
      self.server.parent.task_update(task_id, data)
      return self.send_json({'ok': True})

    if self.path.startswith('/swarming/api/v1/bot/task_error'):
      task_id = self.path[len('/swarming/api/v1/bot/task_error/'):]
      self.server.parent.task_error(task_id, data)
      return self.send_json({'resp': 1})

    raise NotImplementedError(self.path)

  def do_PUT(self):
    raise NotImplementedError(self.path)


class Server(httpserver.Server):
  """Fake a Swarming bot API server for local testing."""
  _HANDLER_CLS = Handler

  def __init__(self):
    super(Server, self).__init__()
    self._lock = threading.Lock()
    self._events = []
    self._tasks = {}
    self._errors = {}
    self.has_polled = threading.Event()

  def add_event(self, data):
    with self._lock:
      self._events.append(data)

  def task_update(self, task_id, data):
    with self._lock:
      self._tasks.setdefault(task_id, []).append(data)

  def task_error(self, task_id, data):
    with self._lock:
      self._errors.setdefault(task_id, []).append(data)

  def get_events(self):
    with self._lock:
      return self._events[:]

  def get_tasks(self):
    with self._lock:
      return self._tasks.copy()

  def get_errors(self):
    with self._lock:
      return self._errors.copy()
