#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Starts a local bot to connect to a local server."""

import glob
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
  def __init__(self, swarming_server_url, redirect=True):
    self._tmpdir = tempfile.mkdtemp(prefix='swarming_bot')
    self._swarming_server_url = swarming_server_url
    self._proc = None
    self._logs = {}
    self._redirect = redirect

  @property
  def log(self):
    """Returns the log output. Only set after calling stop()."""
    return '\n'.join(self._logs.itervalues()) if self._logs else None

  def start(self):
    """Starts the local Swarming bot."""
    assert not self._proc
    bot_zip = os.path.join(self._tmpdir, 'swarming_bot.zip')
    urllib.urlretrieve(self._swarming_server_url + '/bot_code', bot_zip)
    cmd = [sys.executable, bot_zip, 'start_slave']
    env = os.environ.copy()
    kwargs = {}
    if sys.platform != 'win32':
      kwargs['preexec_fn'] = os.setsid
    if self._redirect:
      logs = os.path.join(self._tmpdir, 'logs')
      if not os.path.isdir(logs):
        os.mkdir(logs)
      with open(os.path.join(logs, 'bot_stdout.log'), 'wb') as f:
        self._proc = subprocess.Popen(
            cmd, cwd=self._tmpdir, stdout=f, env=env, stderr=f, **kwargs)
    else:
      self._proc = subprocess.Popen(cmd, cwd=self._tmpdir, env=env, **kwargs)

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
    if self._tmpdir:
      for i in sorted(glob.glob(os.path.join(self._tmpdir, 'logs', '*.log'))):
        self._read_log(i)
      try:
        shutil.rmtree(self._tmpdir)
      except OSError:
        print >> sys.stderr, 'Leaking %s' % self._tmpdir
      self._tmpdir = None
    self._proc = None
    return exit_code

  def wait(self):
    """Waits for the process to normally exit."""
    self._proc.wait()

  def kill(self):
    """Kills the child forcibly."""
    if self._proc:
      self._proc.kill()

  def dump_log(self):
    """Prints dev_appserver log to stderr, works only if app is stopped."""
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, 'swarming_bot log'
    print >> sys.stderr, '-' * 60
    if not self._logs:
      print >> sys.stderr, '<N/A>'
    else:
      for name, content in sorted(self._logs.iteritems()):
        sys.stderr.write(name + ':\n')
        for l in content.strip('\n').splitlines():
          sys.stderr.write('  %s\n' % l)
    print >> sys.stderr, '-' * 60

  def _read_log(self, path):
    try:
      with open(path, 'rb') as f:
        self._logs[os.path.basename(path)] = f.read()
    except (IOError, OSError):
      pass


def main():
  if len(sys.argv) != 2:
    print >> sys.stderr, 'Specify url to Swarming server'
    return 1
  bot = LocalBot(sys.argv[1], False)
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
