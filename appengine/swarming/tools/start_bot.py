#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Starts a local bot to connect to a local server."""

import os
import signal
import shutil
import subprocess
import sys
import tempfile
import urllib


class LocalBot(object):
  """A local running Swarming bot.

  It creates its own temporary directory to download the zip and run tasks
  locally.
  """
  def __init__(self, swarming_server_url):
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_bot')
    self._swarming_server_url = swarming_server_url
    self._proc = None
    self._log = None

  @property
  def log(self):
    """Returns the log output. Only set after calling stop()."""
    return self._log

  def start(self):
    """Starts the local Swarming bot."""
    assert not self._proc
    bot_zip = os.path.join(self._tmpdir, 'swarming_bot.zip')
    urllib.urlretrieve(self._swarming_server_url + '/bot_code', bot_zip)
    cmd = [sys.executable, bot_zip, 'start_slave']
    env = os.environ.copy()
    with open(os.path.join(self._tmpdir, 'bot_config_stdout.log'), 'wb') as f:
      self._proc = subprocess.Popen(
          cmd, cwd=self._tmpdir, preexec_fn=os.setsid, stdout=f, env=env,
          stderr=f)

  def stop(self):
    """Stops the local Swarming bot. Returns the process exit code."""
    if not self._proc:
      return None
    if self._proc.poll() is None:
      try:
        os.killpg(self._proc.pid, signal.SIGTERM)
        self._proc.wait()
      except OSError:
        pass
    exit_code = self._proc.returncode
    if self._tmpdir:
      with open(os.path.join(self._tmpdir, 'bot_config_stdout.log'), 'rb') as f:
        self._log = f.read()
      shutil.rmtree(self._tmpdir)
      self._tmpdir = None
    self._proc = None
    return exit_code

  def wait(self):
    """Wait for the process to normally exit."""
    self._proc.wait()

  def dump_log(self):
    """Prints dev_appserver log to stderr, works only if app is stopped."""
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, 'swarming_bot log'
    print >> sys.stderr, '-' * 60
    for l in self._log.strip('\n').splitlines():
      sys.stderr.write('  %s\n' % l)
    print >> sys.stderr, '-' * 60


def main():
  if len(sys.argv) != 2:
    print >> sys.stderr, 'Specify url to Swarming server'
    return 1
  bot = LocalBot(sys.argv[1])
  try:
    bot.start()
    bot.wait()
    bot.dump_log()
  except KeyboardInterrupt:
    print >> sys.stderr, '<Ctrl-C> received; stopping bot'
  finally:
    exit_code = bot.stop()
  return exit_code


if __name__ == '__main__':
  sys.exit(main())
