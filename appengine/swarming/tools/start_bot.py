#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Starts a local bot to connect to a local server."""

from __future__ import print_function

import argparse
import glob
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
LUCI_DIR = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
CLIENT_DIR = os.path.join(LUCI_DIR, 'client')
GO_CLIENT_DIR = os.path.join(LUCI_DIR, 'luci-go')
sys.path.insert(0, CLIENT_DIR)
sys.path.insert(0, os.path.join(CLIENT_DIR, 'third_party'))
# third_party/
from depot_tools import fix_encoding
from six.moves import urllib

sys.path.pop(0)
sys.path.pop(0)


def _safe_rm(path):
  if os.path.exists(path):
    try:
      shutil.rmtree(path)
    except OSError as e:
      logging.error('Failed to delete %s: %s', path, e)


class LocalBot(object):
  """A local running Swarming bot.

  It creates its own temporary directory to download the zip and run tasks
  locally.
  """

  def __init__(self, swarming_server_url, cas_addr, redirect, botdir,
               python=None):
    self._botdir = botdir
    self._swarming_server_url = swarming_server_url
    self._cas_addr = cas_addr
    self._proc = None
    self._logs = {}
    self._redirect = redirect
    self.python = python or sys.executable

  def wipe_cache(self, restart):
    """Blows away this bot's cache and restart it.

    There's just too much risk of the bot failing over so it's not worth not
    restarting it.
    """
    if restart:
      logging.info('wipe_cache(): Restarting the bot')
      self.stop()
      # Deletion needs to happen while the bot is not running to ensure no side
      # effect.
      # These values are from ./swarming_bot/bot_code/bot_main.py.
      _safe_rm(os.path.join(self._botdir, 'c'))
      _safe_rm(os.path.join(self._botdir, 'isolated_cache'))
      self.start()
    else:
      logging.info('wipe_cache(): wiping cache without telling the bot')
      _safe_rm(os.path.join(self._botdir, 'c'))
      _safe_rm(os.path.join(self._botdir, 'isolated_cache'))

  @property
  def bot_id(self):
    # TODO(maruel): Big assumption.
    return socket.getfqdn().split('.')[0]

  @property
  def log(self):
    """Returns the log output. Only set after calling stop()."""
    return '\n'.join(self._logs.values()) if self._logs else None

  def start(self):
    """Starts the local Swarming bot."""
    assert not self._proc
    bot_zip = os.path.join(self._botdir, 'swarming_bot.zip')
    urllib.request.urlretrieve(self._swarming_server_url + '/bot_code', bot_zip)
    tmpdir = os.path.join(self._botdir, 'tmp')
    if not os.path.exists(tmpdir):
      os.makedirs(tmpdir)
    env = os.environ.copy()
    env['TEMP'] = tmpdir
    env['RUN_ISOLATED_CAS_ADDRESS'] = self._cas_addr
    env['LUCI_GO_CLIENT_DIR'] = GO_CLIENT_DIR
    cmd = [self.python, bot_zip, 'start_slave', '--test-mode']
    if self._redirect:
      logs = os.path.join(self._botdir, 'logs')
      if not os.path.isdir(logs):
        os.mkdir(logs)
      with open(os.path.join(logs, 'bot_stdout.log'), 'wb') as f:
        self._proc = subprocess.Popen(cmd,
                                      cwd=self._botdir,
                                      stdout=f,
                                      stderr=f,
                                      env=env)
    else:
      self._proc = subprocess.Popen(cmd, cwd=self._botdir, env=env)

  def stop(self):
    """Stops the local Swarming bot. Returns the process exit code."""
    if not self._proc:
      return None
    if self._proc.poll() is None:
      try:
        self._proc.send_signal(signal.SIGTERM)
        # TODO(maruel): SIGKILL after N seconds.
        self._proc.wait()
      except OSError:
        pass
    exit_code = self._proc.returncode
    for i in sorted(glob.glob(os.path.join(self._botdir, 'logs', '*.log'))):
      self._read_log(i)
    self._proc = None
    return exit_code

  def poll(self):
    """Polls the process to know if it exited."""
    if self._proc:
      self._proc.poll()

  def wait(self):
    """Waits for the process to normally exit."""
    if self._proc:
      return self._proc.wait()

  def kill(self):
    """Kills the child forcibly."""
    if self._proc:
      self._proc.kill()

  def dump_log(self):
    """Prints dev_appserver log to stderr, works only if app is stopped."""
    print('-' * 60, file=sys.stderr)
    print('swarming_bot log', file=sys.stderr)
    print('-' * 60, file=sys.stderr)
    if not self._logs:
      print('<N/A>', file=sys.stderr)
    else:
      for name, content in sorted(self._logs.items()):
        sys.stderr.write(name + ':\n')
        for l in content.strip('\n').splitlines():
          sys.stderr.write('  %s\n' % l)
    print('-' * 60, file=sys.stderr)

  def _read_log(self, path):
    try:
      with open(path, 'rb') as f:
        self._logs[os.path.basename(path)] = f.read()
    except (IOError, OSError):
      pass


def main():
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument('server', help='Swarming server to connect bot to.')
  parser.add_argument('cas-addr', help='CAS server to connect bot to.')
  args = parser.parse_args()
  fix_encoding.fix_encoding()
  botdir = tempfile.mkdtemp(prefix='start_bot')
  try:
    bot = LocalBot(args.server, args.cas_addr, False, botdir)
    try:
      bot.start()
      bot.wait()
      bot.dump_log()
    except KeyboardInterrupt:
      print('<Ctrl-C> received; stopping bot', file=sys.stderr)
    finally:
      exit_code = bot.stop()
  finally:
    shutil.rmtree(botdir)
  return exit_code


if __name__ == '__main__':
  sys.exit(main())
